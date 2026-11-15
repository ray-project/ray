import json
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

import ray.data
import ray.train
import ray.train.collective
from ray.data.datasource import FileShuffleConfig
from ray.train import DataConfig, DatasetCheckpointConfig
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.v2._internal.callbacks.datasets import DatasetsCallback
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import create_dummy_run_context


def filter_state_dict(state_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Filter out metadata from the state dict and constant fields."""
    return {k: v for k, v in state_dict.items() if k in ["epoch_idx", "checkpoint_idx"]}


def read_checkpoint_files_for_state_dict(
    state_dict: Dict[str, Any], root_path: Path, id_column: str = "id"
) -> List[Any]:
    """Read the row IDs from all checkpoint files covered by the state dict."""
    checkpoint_idx = state_dict["checkpoint_idx"]
    epoch = state_dict["epoch_idx"]
    checkpointed_row_ids = []
    for rank_dir in root_path.glob("rank=*"):
        for i in range(checkpoint_idx + 1):
            checkpoint_path = rank_dir / f"epoch={epoch}" / f"checkpoint={i}"
            for checkpoint_file in checkpoint_path.glob("*.parquet"):
                checkpointed_row_ids.extend(
                    pq.read_table(checkpoint_file).column(id_column).to_pylist()
                )
    return checkpointed_row_ids


@pytest.mark.parametrize("restore_from_end_of_epoch", [True, False])
@pytest.mark.parametrize("generate_id_column", [True, False])
def test_e2e_with_ray_train(
    ray_start_4_cpus, tmp_path, restore_from_end_of_epoch, generate_id_column
):
    """Test that the checkpointing works end-to-end with Ray Train.

    Run for 2 epochs.
    Create checkpoints at 1/3, 2/3 and all the way through each epoch.
    Inject errors at 2/3 of epoch 0.
    Restore from 2/3 of epoch 0, and continue through epoch 1 to completion.
    If restore_from_end_of_epoch is True, also inject an error at the end of epoch 0
    to test that epoch 1 can start from scratch upon restoring.

    Check that the checkpointed row ids contain all seen batches from workers
    at that point.
    Check that the total number of seen rows at the end of 2 epochs is correct.
    """
    data_checkpoint_path = tmp_path / "data_checkpoints"
    train_checkpoint_path = tmp_path / "train_checkpoints"

    num_epochs = 2
    world_size = 2
    num_batches_per_worker = 30
    batch_size = 10
    num_mid_epoch_checkpoints = 2
    checkpoint_at_batches = [
        i * (num_batches_per_worker // (num_mid_epoch_checkpoints + 1))
        for i in range(1, num_mid_epoch_checkpoints + 1)
    ]
    error_at = [(0, checkpoint_at_batches[-1])]
    end_of_epoch_error_at = [0] if restore_from_end_of_epoch else []
    total_rows = num_batches_per_worker * world_size * batch_size
    ds = ray.data.range(total_rows)

    ctx = ray.data.DataContext.get_current()
    ctx.default_hash_shuffle_parallelism = 1

    id_column = "generated_id" if generate_id_column else "id"
    if generate_id_column:
        # Generated IDs require a Parquet read.
        ds.write_parquet(str(tmp_path / "parquet"))
        ds = ray.data.read_parquet(str(tmp_path / "parquet"))

    def train_fn(config):
        rank = ray.train.get_context().get_world_rank()

        seen_rows = 0
        consumed_batches = 0
        start_epoch = 0
        state_dict = None

        checkpoint = ray.train.get_checkpoint()
        if checkpoint:
            checkpoint_data = load_dict_checkpoint(checkpoint)
            consumed_batches = checkpoint_data["consumed_batches"]
            start_epoch = checkpoint_data["epoch"]
            epoch_finished = checkpoint_data["epoch_finished"]
            seen_rows = checkpoint_data["seen_rows"]

            if epoch_finished:
                start_epoch = checkpoint_data["epoch"] + 1
                consumed_batches = 0

            state_dict = checkpoint_data["data_state"]

            print(
                f"[RESTORING] from checkpoint at epoch {start_epoch}, "
                f"batch {consumed_batches}, state_dict: {state_dict}"
            )

        ds_iter = ray.train.get_dataset_shard(
            "train", state_dict=state_dict if rank == 0 else None
        )
        should_restore = state_dict is not None and not epoch_finished
        assert ds_iter.get_context().checkpoint_config._should_restore == should_restore

        for epoch in range(start_epoch, num_epochs):
            if epoch > start_epoch:
                # Should have disabled checkpoint restoration for subsequent epochs.
                assert ds_iter.get_context().checkpoint_config._should_restore is False

            consumed_batches_this_epoch = consumed_batches % num_batches_per_worker

            seen_ids = []
            for batch in ds_iter.iter_batches(batch_size=batch_size):
                if generate_id_column:
                    # The generated id column should be auto-filtered out of the batch.
                    assert "generated_id" not in batch.keys()

                seen_ids.extend(batch["id"].tolist())

                consumed_batches_this_epoch += 1
                consumed_batches += 1
                seen_rows += len(batch["id"])

                if consumed_batches_this_epoch in checkpoint_at_batches:
                    state_dict = ds_iter.state_dict()
                    rank_0_state_dict = ray.train.collective.broadcast_from_rank_zero(
                        state_dict
                    )
                    assert rank_0_state_dict == state_dict

                    checkpointed_row_ids = read_checkpoint_files_for_state_dict(
                        state_dict, data_checkpoint_path, id_column
                    )
                    assert (
                        len(checkpointed_row_ids)
                        == consumed_batches_this_epoch * batch_size * world_size
                    )

                    if not generate_id_column:
                        # Check that the checkpointed row ids contains all seen batches from workers.
                        assert set(seen_ids) <= set(checkpointed_row_ids)

                    with create_dict_checkpoint(
                        {
                            "epoch": epoch,
                            "epoch_finished": False,
                            "consumed_batches": consumed_batches,
                            "seen_rows": seen_rows,
                            "data_state": state_dict,
                        }
                    ) as checkpoint:
                        ray.train.report(
                            {},
                            checkpoint=checkpoint if rank == 0 else None,
                        )
                    ray.train.collective.barrier()

                if (epoch, consumed_batches_this_epoch) in error_at:
                    raise RuntimeError(
                        f"[MID-EPOCH ERROR] at batch {consumed_batches}, state_dict: {state_dict}"
                    )

            state_dict = ds_iter.state_dict()
            rank_0_state_dict = ray.train.collective.broadcast_from_rank_zero(
                state_dict
            )
            assert rank_0_state_dict == state_dict
            assert filter_state_dict(state_dict) == {
                "epoch_idx": epoch + 1,
                "checkpoint_idx": -1,
            }
            checkpointed_row_ids = read_checkpoint_files_for_state_dict(
                state_dict, data_checkpoint_path, id_column
            )
            assert checkpointed_row_ids == []

            with create_dict_checkpoint(
                {
                    "epoch": epoch,
                    "consumed_batches": consumed_batches,
                    "seen_rows": seen_rows,
                    "epoch_finished": True,
                    "data_state": state_dict,
                }
            ) as checkpoint:
                ray.train.report(
                    {},
                    checkpoint=checkpoint if rank == 0 else None,
                )
            ray.train.collective.barrier()

            if epoch in end_of_epoch_error_at:
                raise RuntimeError(
                    f"[END-OF-EPOCH ERROR] at epoch {epoch}, state_dict: {state_dict}"
                )

        assert seen_rows == (total_rows * num_epochs) // world_size

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(num_workers=world_size),
        datasets={"train": ds},
        run_config=ray.train.RunConfig(
            storage_path=str(train_checkpoint_path),
            failure_config=ray.train.FailureConfig(max_failures=2),
        ),
        dataset_config=ray.train.DataConfig(
            dataset_checkpoint_configs={
                "train": DatasetCheckpointConfig(
                    checkpoint_path=str(data_checkpoint_path),
                    id_column=id_column,
                    generate_id_column=generate_id_column,
                )
            }
        ),
    )
    trainer.fit()


def test_default_checkpoint_path(tmp_path):
    """Test that the default checkpoint path is updated to
    `storage_path/name/ray_data_checkpoints/{dataset_name}`.
    The RunConfig(filesystem) should also be taken as the default."""
    original_ds_ckpt_config = DatasetCheckpointConfig(
        id_column="id", checkpoint_path=None, override_filesystem=None
    )
    fs = LocalFileSystem()

    callback = DatasetsCallback(
        train_run_context=create_dummy_run_context(
            run_config=ray.train.RunConfig(
                storage_path=str(tmp_path), name="test", storage_filesystem=fs
            ),
            dataset_config=DataConfig(
                dataset_checkpoint_configs={"train": original_ds_ckpt_config}
            ),
        ),
        datasets={"train": MagicMock()},
    )
    updated_ckpt_config = callback._data_config.dataset_checkpoint_configs["train"]

    assert updated_ckpt_config.checkpoint_path == str(
        tmp_path / "test" / "ray_data_checkpoints" / "train"
    )
    assert updated_ckpt_config.override_filesystem is fs

    # Original user provided config should not be modified.
    assert original_ds_ckpt_config.checkpoint_path is None
    assert original_ds_ckpt_config.override_filesystem is None


def test_override_filesystem(tmp_path):
    """Test that the override_filesystem is taken over the RunConfig(filesystem)."""
    mock_fs = MagicMock()
    mock_fs._marker = "dummy"

    original_ds_ckpt_config = DatasetCheckpointConfig(
        id_column="id", checkpoint_path=None, override_filesystem=mock_fs
    )

    callback = DatasetsCallback(
        train_run_context=create_dummy_run_context(
            run_config=ray.train.RunConfig(storage_path=str(tmp_path), name="test"),
            dataset_config=DataConfig(
                dataset_checkpoint_configs={"train": original_ds_ckpt_config}
            ),
        ),
        datasets={"train": MagicMock()},
    )
    updated_ckpt_config = callback._data_config.dataset_checkpoint_configs["train"]

    # Default checkpoint path should still be set.
    assert updated_ckpt_config.checkpoint_path == str(
        tmp_path / "test" / "ray_data_checkpoints" / "train"
    )
    assert updated_ckpt_config.override_filesystem._marker == "dummy"


def test_custom_checkpoint_path_infers_filesystem(tmp_path):
    """Test that the RunConfig filesystem isn't used for a user-provided
    checkpoint path, so that the filesystem is inferred from the path instead."""
    checkpoint_path = f"file://{tmp_path}/custom"
    callback = DatasetsCallback(
        train_run_context=create_dummy_run_context(
            run_config=ray.train.RunConfig(
                storage_path=str(tmp_path),
                name="test",
                storage_filesystem=LocalFileSystem(),
            ),
            dataset_config=DataConfig(
                dataset_checkpoint_configs={
                    "train": DatasetCheckpointConfig(
                        id_column="id", checkpoint_path=checkpoint_path
                    )
                }
            ),
        ),
        datasets={"train": MagicMock()},
    )
    updated_ckpt_config = callback._data_config.dataset_checkpoint_configs["train"]
    assert updated_ckpt_config.checkpoint_path == checkpoint_path
    assert updated_ckpt_config.override_filesystem is None


def test_multiple_datasets_checkpoint_paths(tmp_path):
    """Test that each dataset gets its own default checkpoint path,
    and that datasets can't share a checkpoint path."""

    def create_callback(checkpoint_configs):
        return DatasetsCallback(
            train_run_context=create_dummy_run_context(
                run_config=ray.train.RunConfig(storage_path=str(tmp_path), name="test"),
                dataset_config=DataConfig(
                    dataset_checkpoint_configs=checkpoint_configs
                ),
            ),
            datasets={name: MagicMock() for name in checkpoint_configs},
        )

    callback = create_callback(
        {
            "train": DatasetCheckpointConfig(id_column="id"),
            "val": DatasetCheckpointConfig(id_column="id"),
        }
    )
    checkpoint_configs = callback._data_config.dataset_checkpoint_configs
    assert checkpoint_configs["train"].checkpoint_path == str(
        tmp_path / "test" / "ray_data_checkpoints" / "train"
    )
    assert checkpoint_configs["val"].checkpoint_path == str(
        tmp_path / "test" / "ray_data_checkpoints" / "val"
    )

    shared_path = str(tmp_path / "shared")
    with pytest.raises(ValueError, match="different `checkpoint_path`"):
        create_callback(
            {
                "train": DatasetCheckpointConfig(
                    id_column="id", checkpoint_path=shared_path
                ),
                "val": DatasetCheckpointConfig(
                    id_column="id", checkpoint_path=shared_path
                ),
            }
        )


def test_data_config_validation():
    """Test that the data config checkpoint configuration validation works."""
    DataConfig(
        datasets_to_split="all",
        dataset_checkpoint_configs={"train": MagicMock(), "val": MagicMock()},
    )

    with pytest.raises(NotImplementedError):
        DataConfig(
            datasets_to_split=["train"],
            dataset_checkpoint_configs={"train": MagicMock(), "val": MagicMock()},
        )


def test_execution_idx_restored_from_checkpoint(ray_start_4_cpus, tmp_path):
    """Test that data_context._execution_idx is correctly restored from checkpoint.

    This test verifies that when a training job fails and restores from a checkpoint,
    the execution_idx is correctly restored so that data ordering is preserved.

    We run two trainings for 3 epochs each:
    - Training 1: No failures (baseline)
    - Training 2: Fails mid-epoch 1, then fails at end of epoch 2

    Both trainings should produce the same data order per epoch.
    """

    num_epochs = 3
    world_size = 1
    num_batches = 10
    batch_size = 10
    total_rows = num_batches * batch_size

    parquet_path = tmp_path / "parquet_data"
    parquet_path.mkdir()
    for i in range(total_rows):
        file_path = parquet_path / f"file_{i:03d}.parquet"
        pq.write_table(pa.table({"id": [i]}), file_path)

    # Set preserve_order to ensure deterministic data ordering
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    ds = ray.data.read_parquet(str(parquet_path), shuffle=FileShuffleConfig(seed=42))

    def run_training(
        name: str,
        fail_mid_epoch: int = None,
        fail_end_epoch: int = None,
    ) -> dict:
        """Run a training and return the data order for each epoch."""
        data_checkpoint_path = tmp_path / f"{name}_data_checkpoints"
        train_checkpoint_path = tmp_path / f"{name}_train_checkpoints"
        data_order_path = tmp_path / f"{name}_data_order.json"

        def train_fn(config):
            rank = ray.train.get_context().get_world_rank()

            data_state_dict = None
            is_mid_epoch_restore = False
            start_epoch = 0

            checkpoint = ray.train.get_checkpoint()
            if checkpoint:
                checkpoint_data = load_dict_checkpoint(checkpoint)
                data_state_dict = checkpoint_data.get("data_state")

                # The data iterator's state_dict already contains epoch_idx and checkpoint_idx
                # checkpoint_idx >= 0 means mid-epoch, checkpoint_idx == -1 means epoch boundary
                if data_state_dict:
                    start_epoch = data_state_dict["epoch_idx"]
                    is_mid_epoch_restore = data_state_dict["checkpoint_idx"] >= 0

            ds_iter = ray.train.get_dataset_shard(
                "train", state_dict=data_state_dict if rank == 0 else None
            )

            data_order_file = config["data_order_path"]
            if data_order_file.exists():
                all_epoch_data = json.loads(data_order_file.read_text())
            else:
                all_epoch_data = {}

            for epoch in range(start_epoch, config["num_epochs"]):
                epoch_key = str(epoch)

                # If restoring mid-epoch, start with existing partial data
                if is_mid_epoch_restore and epoch_key in all_epoch_data:
                    epoch_data = list(all_epoch_data[epoch_key])
                else:
                    epoch_data = []

                for batch in ds_iter.iter_batches(batch_size=config["batch_size"]):
                    epoch_data.extend(batch["id"].tolist())

                    # Create checkpoint mid-epoch (after half the rows)
                    if len(epoch_data) == config["total_rows"] // 2:
                        iter_state_dict = ds_iter.state_dict()

                        # Just save data_state - it already contains epoch_idx and checkpoint_idx
                        with create_dict_checkpoint(
                            {"data_state": iter_state_dict}
                        ) as ckpt:
                            ray.train.report({}, checkpoint=ckpt if rank == 0 else None)

                        # Fail mid-epoch if configured
                        if (
                            config["fail_mid_epoch"] == epoch
                            and not is_mid_epoch_restore
                        ):
                            all_epoch_data[epoch_key] = epoch_data
                            data_order_file.write_text(json.dumps(all_epoch_data))
                            raise RuntimeError(f"Injected mid-epoch {epoch} failure")

                # End of epoch - save data order
                all_epoch_data[epoch_key] = epoch_data
                data_order_file.write_text(json.dumps(all_epoch_data))

                iter_state_dict = ds_iter.state_dict()
                with create_dict_checkpoint({"data_state": iter_state_dict}) as ckpt:
                    ray.train.report({}, checkpoint=ckpt if rank == 0 else None)

                # Fail at end of epoch if configured
                if config["fail_end_epoch"] == epoch and not is_mid_epoch_restore:
                    raise RuntimeError(f"Injected end-of-epoch {epoch} failure")

                # Reset flag after first epoch completes successfully
                is_mid_epoch_restore = False

        trainer = DataParallelTrainer(
            train_fn,
            train_loop_config={
                "num_epochs": num_epochs,
                "total_rows": total_rows,
                "batch_size": batch_size,
                "fail_mid_epoch": fail_mid_epoch,
                "fail_end_epoch": fail_end_epoch,
                "data_order_path": data_order_path,
            },
            scaling_config=ray.train.ScalingConfig(num_workers=world_size),
            datasets={"train": ds},
            run_config=ray.train.RunConfig(
                storage_path=str(train_checkpoint_path),
                failure_config=ray.train.FailureConfig(max_failures=2),
            ),
            dataset_config=ray.train.DataConfig(
                dataset_checkpoint_configs={
                    "train": DatasetCheckpointConfig(
                        checkpoint_path=str(data_checkpoint_path),
                        id_column="id",
                    )
                }
            ),
        )
        trainer.fit()

        # Return the data order
        return json.loads(data_order_path.read_text())

    # Run training 1: no failures (baseline)
    baseline_data_order = run_training("baseline")

    # Run training 2: fail mid-epoch 1, then fail at end of epoch 2
    failure_data_order = run_training(
        "with_failures",
        fail_mid_epoch=1,
        fail_end_epoch=2,
    )

    # Verify both trainings produced the same data for each epoch
    for epoch in range(num_epochs):
        epoch_key = str(epoch)
        assert epoch_key in baseline_data_order, f"Epoch {epoch} missing from baseline"
        assert (
            epoch_key in failure_data_order
        ), f"Epoch {epoch} missing from failure run"

        baseline_data = baseline_data_order[epoch_key]
        failure_data = failure_data_order[epoch_key]

        # Both runs should have the same number of rows per epoch
        assert len(baseline_data) == len(failure_data) == total_rows, (
            f"Row count mismatch at epoch {epoch}:\n"
            f"  Baseline: {len(baseline_data)}\n"
            f"  With failures: {len(failure_data)}\n"
            f"  Expected: {total_rows}"
        )

        # Both runs should see the same SET of data in each epoch
        baseline_set = set(baseline_data)
        failure_set = set(failure_data)
        assert baseline_set == failure_set, (
            f"Data set mismatch at epoch {epoch}:\n"
            f"  Missing in failure: {baseline_set - failure_set}\n"
            f"  Extra in failure: {failure_set - baseline_set}"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
