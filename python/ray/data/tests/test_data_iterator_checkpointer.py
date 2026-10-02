from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data import FileShuffleConfig
from ray.data._internal.block_batching.interfaces import Batch, BatchMetadata
from ray.data.checkpoint.data_iterator_checkpointer import (
    RowIDBasedDataIteratorCheckpointer,
    RowIDBasedStateDict,
)
from ray.data.checkpoint.interfaces import TrainingIngestCheckpointConfig
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _create_batch(row_ids: List[int]) -> Batch:
    table = pa.table({"id": pa.array(row_ids, type=pa.int64())})
    return Batch(
        metadata=BatchMetadata(batch_idx=0, row_ids=table),
        data=table,
    )


def filter_state_dict(state_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Filter out metadata from the state dict and constant fields."""
    return {k: v for k, v in state_dict.items() if k in ["epoch_idx", "checkpoint_idx"]}


def read_checkpoint_files_for_state_dict(
    state_dict: dict, root_path: Path, id_column: str = "id"
) -> list:
    checkpoint_idx = state_dict["checkpoint_idx"]
    epoch = state_dict["epoch_idx"]
    # Get all checkpoints up to and including checkpoint_idx
    checkpointed_row_ids = []
    for rank_dir in root_path.glob("rank=*"):
        for i in range(checkpoint_idx + 1):
            checkpoint_path = rank_dir / f"epoch={epoch}" / f"checkpoint={i}"
            for checkpoint_file in checkpoint_path.glob("*.parquet"):
                checkpointed_row_ids.extend(
                    pq.read_table(checkpoint_file).column(id_column).to_pylist()
                )
    return checkpointed_row_ids


def _read_checkpoint_files(root_path: Path) -> List[int]:
    """Read all row IDs from all checkpoint files in the given directory."""
    row_ids = []
    for checkpoint_path in root_path.glob("*/*.parquet"):
        row_ids.extend(pq.read_table(checkpoint_path).column("id").to_pylist())
    return sorted(row_ids)


def test_generate_id_column_not_implemented():
    with pytest.raises(NotImplementedError, match="generate_id_column"):
        TrainingIngestCheckpointConfig(id_column="id", generate_id_column=True)


def test_basic(tmp_path):
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpointer.start_epoch()

    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))
    checkpointer.record_yielded_batch(_create_batch([4, 5, 6]))

    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 0,
        "checkpoint_idx": 0,
    }
    checkpoint_path = tmp_path.joinpath(
        "rank=0", "epoch=0", "checkpoint=0", "chunk_0.parquet"
    )
    assert checkpoint_path.is_file()
    assert pq.read_table(checkpoint_path).column("id").to_pylist() == [1, 2, 3, 4, 5, 6]


def test_periodic_flush_on_file_size_threshold(tmp_path):
    """Test that the checkpointer flushes row IDs to a checkpoint files when the
    in-memory row ids size exceeds a threshold.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpointer.TARGET_CHECKPOINT_SIZE_BYTES = 6 * 8  # 6 int64s
    checkpointer.start_epoch()

    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))

    checkpoint_path = tmp_path.joinpath(
        "rank=0", "epoch=0", "checkpoint=0", "chunk_0.parquet"
    )
    assert not checkpoint_path.is_file()

    checkpointer.record_yielded_batch(_create_batch([4, 5, 6]))

    # We've reached the threshold, which triggers a flush.
    wait_for_condition(lambda: checkpoint_path.is_file(), timeout=1)

    assert checkpoint_path.is_file()
    assert pq.read_table(checkpoint_path).column("id").to_pylist() == [1, 2, 3, 4, 5, 6]


def test_force_flush(tmp_path):
    """Tests forcing a flush of staged row IDs to a checkpoint file."""
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpointer.start_epoch()

    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))
    checkpointer.record_yielded_batch(_create_batch([4, 5, 6]))

    checkpoint_path = tmp_path.joinpath(
        "rank=0", "epoch=0", "checkpoint=0", "chunk_0.parquet"
    )
    assert not checkpoint_path.is_file()

    checkpointer._flush_all_staged_row_ids()

    assert checkpoint_path.is_file()
    assert pq.read_table(checkpoint_path).column("id").to_pylist() == [1, 2, 3, 4, 5, 6]


def test_multi_worker_checkpoint_commit(tmp_path):
    """Test that the checkpointer correctly handles multiple workers committing
    checkpoints to the same checkpoint directory.
    """
    world_size = 4
    checkpointers = [
        RowIDBasedDataIteratorCheckpointer(
            checkpoint_config=TrainingIngestCheckpointConfig(
                checkpoint_path=str(tmp_path), id_column="id"
            ),
            world_rank=i,
            world_size=world_size,
        )
        for i in range(world_size)
    ]

    for checkpointer in checkpointers:
        checkpointer.start_epoch()

    for i, checkpointer in enumerate(checkpointers):
        checkpointer.record_yielded_batch(_create_batch([i * 3, i * 3 + 1, i * 3 + 2]))

    for i, checkpointer in enumerate(checkpointers):
        assert filter_state_dict(checkpointer.state_dict()) == {
            "epoch_idx": 0,
            "checkpoint_idx": 0,
        }

    for rank in range(world_size):
        checkpoint_path = tmp_path.joinpath(
            f"rank={rank}", "epoch=0", "checkpoint=0", "chunk_0.parquet"
        )
        assert checkpoint_path.is_file()
        assert pq.read_table(checkpoint_path).column("id").to_pylist() == list(
            range(rank * 3, (rank + 1) * 3)
        )


def test_state_dict_across_epoch_lifecycle(tmp_path):
    """Test that `state_dict` works across the full epoch lifecycle.

    Also, check that `state_dict` is idempotent and doesn't advance the checkpoint index
    more than necessary if called multiple times in a row.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpointer.TARGET_CHECKPOINT_SIZE_BYTES = 6 * 8  # 6 int64s

    # Before starting an epoch, `state_dict` returns a dummy state dict.
    # NOTE: These for loops check `state_dict` idempotency.
    for _ in range(2):
        assert filter_state_dict(checkpointer.state_dict()) == {
            "epoch_idx": 0,
            "checkpoint_idx": -1,
        }

    checkpointer.start_epoch()

    # Start of epoch.
    for _ in range(2):
        assert filter_state_dict(checkpointer.state_dict()) == {
            "epoch_idx": 0,
            "checkpoint_idx": -1,
        }

    # First checkpoint.
    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))
    for _ in range(2):
        assert filter_state_dict(checkpointer.state_dict()) == {
            "epoch_idx": 0,
            "checkpoint_idx": 0,
        }
    assert tmp_path.joinpath("rank=0", "epoch=0", "checkpoint=0").is_dir()
    assert _read_checkpoint_files(tmp_path.joinpath("rank=0", "epoch=0")) == list(
        range(1, 4)
    )

    # Second checkpoint.
    checkpointer.record_yielded_batch(_create_batch([4, 5, 6]))
    checkpointer.record_yielded_batch(_create_batch([7, 8, 9]))
    checkpointer.record_yielded_batch(_create_batch([10, 11, 12]))
    for _ in range(2):
        assert filter_state_dict(checkpointer.state_dict()) == {
            "epoch_idx": 0,
            "checkpoint_idx": 1,
        }
    assert tmp_path.joinpath("rank=0", "epoch=0", "checkpoint=1").is_dir()
    assert _read_checkpoint_files(tmp_path.joinpath("rank=0", "epoch=0")) == list(
        range(1, 13)
    )

    # End of epoch.
    checkpointer.end_epoch()
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": -1,
    }

    # Start of new epoch.
    checkpointer.start_epoch()
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": -1,
    }
    # No directory should be created at this epoch boundary.
    assert not tmp_path.joinpath("rank=0", "epoch=1", "checkpoint=-1").is_dir()


@pytest.mark.parametrize("delete_checkpoints_after_epoch", [True, False])
def test_end_epoch(tmp_path, delete_checkpoints_after_epoch):
    """Test that the checkpointer correctly handles ending an epoch.

    Ending an epoch should flush any staged row IDs to a checkpoint file.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path),
            id_column="id",
            delete_checkpoints_after_epoch=delete_checkpoints_after_epoch,
        )
    )
    checkpointer.start_epoch()
    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))
    checkpointer.end_epoch()

    checkpoint_path = tmp_path.joinpath(
        "rank=0", "epoch=0", "checkpoint=0", "chunk_0.parquet"
    )
    if delete_checkpoints_after_epoch:
        assert checkpointer._delete_task is not None
        checkpointer._delete_task.result()
        assert not checkpoint_path.is_file()
    else:
        assert checkpoint_path.is_file()
        assert pq.read_table(checkpoint_path).column("id").to_pylist() == [1, 2, 3]

    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": -1,
    }


def test_unfinished_epoch(tmp_path):
    """Test that the checkpointer correctly handles starting a new epoch
    after an unfinished one.

    Example usage in user code:

    for epoch in range(num_epochs):
        step_idx = 0
        max_steps_per_epoch = 10
        for batch in ds.iter_batches():
            if step_idx >= max_steps_per_epoch:
                break
            step_idx += 1
            ...
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpointer.start_epoch()
    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))

    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 0,
        "checkpoint_idx": 0,
    }

    # Start of new epoch, before finishing the previous one.
    checkpointer.start_epoch()
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": -1,
    }


def test_checkpoint_path(tmp_path):
    """Test that the checkpoint path is correctly constructed."""
    world_rank = 1
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        ),
        world_rank=world_rank,
        world_size=2,
    )
    checkpointer._epoch_idx = 3
    checkpointer._latest_committed_checkpoint_idx = 13
    checkpointer._chunk_idx = 15

    assert checkpointer._get_current_checkpoint_path() == str(
        tmp_path.joinpath("rank=1", "epoch=3", "checkpoint=14", "chunk_15.parquet")
    )


def test_setup_new_checkpoint_directory(tmp_path):
    """Test that the checkpointer clears an existing checkpoint directory
    if it exists before writing to it.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    checkpoint_path = tmp_path.joinpath("rank=0", "epoch=0", "checkpoint=0")
    checkpoint_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.joinpath("dummy").touch()

    checkpointer._setup_new_checkpoint_directory(str(checkpoint_path))
    assert checkpoint_path.is_dir()
    assert not checkpoint_path.joinpath("dummy").is_file()


def test_load_state_dict_from_mid_epoch(tmp_path):
    """Test that the checkpointer state can continue from a mid-epoch state dict."""
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        ),
        state_dict=RowIDBasedStateDict(
            epoch_idx=1,
            checkpoint_idx=8,
            root_checkpoint_path=str(tmp_path),
            id_column="id",
        ),
    )
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": 8,
    }

    checkpointer.start_epoch()

    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": 8,
    }

    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": 9,
    }

    checkpointer.end_epoch()
    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 2,
        "checkpoint_idx": -1,
    }


def test_load_state_dict_from_start_or_end_of_epoch(tmp_path):
    """Test that the checkpointer state can continue from a state dict
    loaded from the start/end of an epoch.

    [ Epoch 0 ] (epoch=1, checkpoint_idx=-1) [Epoch 1]

    Example in user code:

    for epoch in range(num_epochs):
        for batch in ds.iter_batches():
            ...
        # Resuming from this state dict should just start a new epoch.
        state_dict = checkpointer.state_dict()

    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        ),
        state_dict=RowIDBasedStateDict(
            epoch_idx=1,
            checkpoint_idx=-1,
            root_checkpoint_path=str(tmp_path),
            id_column="id",
        ),
    )
    checkpointer.start_epoch()

    assert filter_state_dict(checkpointer.state_dict()) == {
        "epoch_idx": 1,
        "checkpoint_idx": -1,
    }


@pytest.mark.parametrize(
    "state_dict",
    [
        {
            "epoch_idx": 1,
            "checkpoint_idx": 8,
            "root_checkpoint_path": "dummy",
            "id_column": "id",
        },
        {
            "epoch_idx": 2,
            "checkpoint_idx": -1,
            "root_checkpoint_path": "dummy",
            "id_column": "id",
        },
    ],
)
def test_load_state_dict_equivalence(tmp_path, state_dict):
    """Test that calling `state_dict` after loading a state dict
    returns the same state dict.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        ),
        state_dict=RowIDBasedStateDict.from_dict(state_dict),
    )
    assert filter_state_dict(checkpointer.state_dict()) == filter_state_dict(state_dict)


@patch("pyarrow.parquet.write_table", side_effect=RuntimeError("mock error"))
@pytest.mark.parametrize("when_to_raise", ["periodic", "forced"])
def test_flush_exception(mock_write_table, tmp_path, when_to_raise):
    """Test that the checkpointer raises an exception if a flush operation fails."""
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )
    if when_to_raise == "periodic":
        # Immediately trigger a periodic flush.
        checkpointer.TARGET_CHECKPOINT_SIZE_BYTES = 0

    checkpointer.start_epoch()

    checkpointer.record_yielded_batch(_create_batch([1, 2, 3]))

    with pytest.raises(RuntimeError, match="Failed to flush"):
        filter_state_dict(checkpointer.state_dict())

    with pytest.raises(RuntimeError, match="Failed to flush"):
        checkpointer.record_yielded_batch(_create_batch([4, 5, 6]))


@pytest.mark.parametrize("reinit_iter", [True, False])
def test_iter_batches_with_checkpointing(ray_start_10_cpus, tmp_path, reinit_iter):
    """Test that iter_batches with checkpointing works correctly.

    Create multiple checkpoints per epoch, for several epochs.
    Create checkpoints at 1/3 and 2/3 way through each epoch.
    A checkpoint is also created at the end of each epoch.
    """
    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path), id_column="id"
        )
    )

    num_epochs = 2
    num_batches = 120
    batch_size = 10
    num_mid_epoch_checkpoints = 2
    checkpoint_at_batches = [
        i * (num_batches // (num_mid_epoch_checkpoints + 1))
        for i in range(1, num_mid_epoch_checkpoints + 1)
    ]

    ds = ray.data.range(num_batches * batch_size)
    ds_iter = ds.iterator()
    ds_iter._enable_checkpointing(checkpointer)

    state_dict = ds_iter.state_dict()
    assert filter_state_dict(state_dict) == {
        "epoch_idx": 0,
        "checkpoint_idx": -1,
    }
    # There should be no row_ids associated with a state dict initially.
    assert read_checkpoint_files_for_state_dict(state_dict, tmp_path) == []

    batch_iter = None
    for epoch in range(num_epochs):
        checkpoint_idx = 0
        consumed_batches = 0
        if batch_iter is None or reinit_iter:
            batch_iter = ds_iter.iter_batches(batch_size=batch_size)

        seen_ids = []
        for batch in batch_iter:
            consumed_batches += 1
            seen_ids.extend(batch["id"])
            if consumed_batches in checkpoint_at_batches:
                state_dict = ds_iter.state_dict()
                assert filter_state_dict(state_dict) == {
                    "epoch_idx": epoch,
                    "checkpoint_idx": checkpoint_idx,
                }
                assert set(seen_ids) == set(
                    read_checkpoint_files_for_state_dict(state_dict, tmp_path)
                )
                checkpoint_idx += 1

        assert filter_state_dict(ds_iter.state_dict()) == {
            "epoch_idx": epoch + 1,
            "checkpoint_idx": -1,
        }
        # There should be no row_ids associated with a state dict at an epoch boundary.
        assert (
            read_checkpoint_files_for_state_dict(ds_iter.state_dict(), tmp_path) == []
        )


def test_data_context_execution_idx_from_checkpoint(
    ray_start_10_cpus, restore_data_context, tmp_path
):
    """Test that data_context._execution_idx is set from checkpoint state dict.

    When restoring from a checkpoint, the epoch_idx from the state dict should
    be used for file shuffling. This ensures deterministic shuffling across
    restorations at the same epoch.
    """

    parquet_path = tmp_path / "parquet"
    parquet_path.mkdir()
    num_files = 10
    for i in range(num_files):
        file_path = parquet_path / f"file_{i}.parquet"
        pq.write_table(pa.table({"id": [i]}), file_path)

    shuffle_config = FileShuffleConfig(seed=42)

    ctx = ray.data.DataContext.get_current()
    # Preserve order so that the output order only depends on the file shuffle.
    ctx.execution_options.preserve_order = True
    ctx._execution_idx = 0
    ds_epoch_0 = ray.data.read_parquet(str(parquet_path), shuffle=shuffle_config)
    epoch_0_order = [row["id"] for row in ds_epoch_0.take_all()]

    ctx._execution_idx = 5
    ds_epoch_5 = ray.data.read_parquet(str(parquet_path), shuffle=shuffle_config)
    epoch_5_order = [row["id"] for row in ds_epoch_5.take_all()]

    assert (
        epoch_0_order != epoch_5_order
    ), "Shuffle order should differ between epochs when reseed_after_epoch=True"

    checkpointer = RowIDBasedDataIteratorCheckpointer(
        checkpoint_config=TrainingIngestCheckpointConfig(
            checkpoint_path=str(tmp_path / "checkpoints"), id_column="id"
        ),
        state_dict=RowIDBasedStateDict(
            epoch_idx=5,
            checkpoint_idx=3,
            root_checkpoint_path=str(tmp_path / "checkpoints"),
            id_column="id",
        ),
    )
    state_dict = checkpointer.state_dict()
    assert (
        state_dict["epoch_idx"] == 5
    ), "Checkpoint state dict should contain the restored epoch_idx"

    ctx._execution_idx = state_dict["epoch_idx"]
    ds_epoch_5_again = ray.data.read_parquet(str(parquet_path), shuffle=shuffle_config)
    epoch_5_order_again = [row["id"] for row in ds_epoch_5_again.take_all()]

    assert (
        epoch_5_order == epoch_5_order_again
    ), "Shuffle order should be deterministic for the same epoch_idx"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
