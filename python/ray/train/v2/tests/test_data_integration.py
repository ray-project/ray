import asyncio
import os
import tempfile
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray.data
import ray.train
from ray.data import (
    DataContext,
    ExecutionOptions,
    ExecutionResources,
    FileShuffleConfig,
)
from ray.data._internal.iterator.stream_split_iterator import StreamSplitDataIterator
from ray.data.tests.conftest import restore_data_context  # noqa: F401
from ray.train.v2._internal.callbacks.datasets import DatasetsCallback
from ray.train.v2._internal.data_integration.interfaces import DatasetShardMetadata
from ray.train.v2._internal.execution.worker_group.worker_group import (
    WorkerGroupContext,
)
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.tests.util import (
    DummyObjectRefWrapper,
    DummyWorkerGroup,
    create_dummy_run_context,
)


@pytest.mark.parametrize("num_workers", [1, 2])
def test_dataset_sharding_across_workers(ray_start_4_cpus, num_workers):
    """Tests that the dataset shards properly across a variety of num_workers."""
    NUM_ROWS = 1000

    train_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        with pytest.raises(KeyError):
            ray.train.get_dataset_shard("val")

        train_ds = ray.train.get_dataset_shard("train")
        num_rows = 0
        for batch in train_ds.iter_batches():
            num_rows += len(batch["id"])
        assert num_rows == NUM_ROWS // num_workers

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ray.train.ScalingConfig(num_workers=num_workers),
    )
    trainer.fit()


@pytest.mark.parametrize("datasets_to_split", ["all", ["train"], []])
def test_multiple_datasets(ray_start_4_cpus, datasets_to_split):
    """Tests that the dataset is sharded across a variety of num_workers."""
    NUM_ROWS = 1000
    NUM_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    val_ds = ray.data.range(NUM_ROWS)

    def train_fn():
        for dataset_name in ["train", "val"]:
            ds = ray.train.get_dataset_shard(dataset_name)
            num_rows = 0
            for batch in ds.iter_batches():
                num_rows += len(batch["id"])

            if datasets_to_split == "all" or dataset_name in datasets_to_split:
                assert num_rows == NUM_ROWS // NUM_WORKERS
            else:
                assert num_rows == NUM_ROWS

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds, "val": val_ds},
        dataset_config=ray.train.DataConfig(datasets_to_split=datasets_to_split),
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_data_config_validation():
    with pytest.raises(TypeError, match="`datasets_to_split` should be.*"):
        ray.train.DataConfig(datasets_to_split="hello")
    with pytest.raises(TypeError, match="`datasets_to_split` should be.*"):
        ray.train.DataConfig(datasets_to_split={})


def test_datasets_callback(ray_start_4_cpus):
    """Check that the `DatasetsCallback` correctly configures the
    dataset shards and execution options."""
    NUM_WORKERS = 2

    train_ds = ray.data.range(1000)
    valid_ds = ray.data.range(1000)

    data_config = ray.train.DataConfig(datasets_to_split=["train"])
    scaling_config = ray.train.ScalingConfig(
        num_workers=NUM_WORKERS, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    worker_group_context = WorkerGroupContext(
        run_attempt_id="attempt_1",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=scaling_config.num_workers,
        resources_per_worker=scaling_config.resources_per_worker,
    )
    train_run_context = create_dummy_run_context(
        dataset_config=data_config,
        scaling_config=scaling_config,
    )
    worker_group = DummyWorkerGroup(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
    )
    worker_group._start()

    callback = DatasetsCallback(
        train_run_context=train_run_context,
        datasets={"train": train_ds, "valid": valid_ds},
    )
    dataset_manager_for_each_worker = callback.before_init_train_context(
        worker_group.get_workers()
    )["dataset_shard_provider"]
    assert len(dataset_manager_for_each_worker) == NUM_WORKERS

    dataset_manager = dataset_manager_for_each_worker[0]
    processed_train_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="train")
    )
    processed_valid_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="valid")
    )

    assert isinstance(processed_train_ds, StreamSplitDataIterator)
    assert not isinstance(processed_valid_ds, StreamSplitDataIterator)

    # Under the V2 cluster autoscaler (default), the scaling policy registers training resources
    # with the AutoscalingCoordinator, so exclude_resources should not be set.
    assert (
        processed_train_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources.zero()
    )
    assert (
        processed_valid_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources.zero()
    )


def test_data_context_propagation(ray_start_4_cpus, restore_data_context):  # noqa: F811
    """Tests that the DataContext from the driver is propagated to the Train workers."""
    data_context = DataContext.get_current()
    data_context.set_config("foo", "bar")
    train_ds = ray.data.range(2)

    def train_fn():
        assert DataContext.get_current().get_config("foo") == "bar"

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds},
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_configure_execution_options_carryover_context():
    """Tests that execution options in DataContext
    carry over to DataConfig automatically."""

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.execution_options.verbose_progress = True

    data_config = ray.train.DataConfig()

    ingest_options = data_config.default_ingest_options()
    assert ingest_options.preserve_order is True
    assert ingest_options.verbose_progress is True


@pytest.mark.parametrize("enable_shard_locality", [True, False])
def test_configure_locality(enable_shard_locality):
    data_config = ray.train.DataConfig(enable_shard_locality=enable_shard_locality)

    mock_ds = MagicMock()
    mock_ds.streaming_split = MagicMock()
    mock_ds.copy = MagicMock(return_value=mock_ds)
    world_size = 2
    worker_handles = [MagicMock() for _ in range(world_size)]
    worker_node_ids = ["node" + str(i) for i in range(world_size)]
    data_config.configure(
        datasets={"train": mock_ds},
        world_size=world_size,
        worker_handles=worker_handles,
        worker_node_ids=worker_node_ids,
    )
    mock_ds.streaming_split.assert_called_once()
    mock_ds.streaming_split.assert_called_with(
        world_size,
        equal=True,
        locality_hints=worker_node_ids if enable_shard_locality else None,
    )


@pytest.mark.parametrize("cache_random_preprocessing", [True, False])
def test_per_epoch_preprocessing(ray_start_4_cpus, cache_random_preprocessing):
    """Random preprocessing should change per-epoch."""
    NUM_ROWS = 32
    NUM_WORKERS = 2

    ds = ray.data.range(NUM_ROWS, override_num_blocks=NUM_ROWS).random_shuffle()
    if cache_random_preprocessing:
        # Materialize the dataset to cache the random preprocessing.
        # In this case, every epoch should use the same random preprocessing.
        ds = ds.materialize()

    def train_fn():
        ds = ray.train.get_dataset_shard("train")
        epoch_0 = [row["id"] for row in ds.iter_rows()]
        epoch_1 = [row["id"] for row in ds.iter_rows()]

        assert len(epoch_0) == len(epoch_1) == NUM_ROWS // NUM_WORKERS
        if cache_random_preprocessing:
            assert epoch_0 == epoch_1
        else:
            assert epoch_0 != epoch_1, (epoch_0, epoch_1)

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": ds},
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


@pytest.mark.parametrize("different_seeds_across_executions", [True, False])
def test_parquet_file_shuffle_with_executions(
    ray_start_4_cpus,
    restore_data_context,  # noqa: F811
    different_seeds_across_executions,  # noqa: F811,
):
    """Test that Parquet file shuffling produces:
    1. Different results across executions when different_seeds_across_executions=True
       (FileShuffleConfig with reseed_after_execution=True: seed = seed + execution_idx)
    2. Same results across executions when different_seeds_across_executions=False
       (FileShuffleConfig with seed: seed remains constant)
    3. Same results for different datasets with same shuffle config per execution
    """
    NUM_WORKERS = 2
    NUM_EXECUTIONS = 5
    NUM_FILES = 15

    # Create temporary directory for test files
    with tempfile.TemporaryDirectory() as tmp_path:

        def write_parquet_file(path, file_index):
            """Write a Parquet file with unique data for each file."""
            data = {
                "file_id": [file_index] * 10,
                "row_id": range(10 * file_index, 10 * (file_index + 1)),
                "value": [f"file_{file_index}_row_{i}" for i in range(10)],
            }
            table = pa.Table.from_pydict(data)
            pq.write_table(table, path)

        # Create multiple Parquet files
        paths = [
            os.path.join(tmp_path, f"test_file_{i}.parquet") for i in range(NUM_FILES)
        ]
        for i, path in enumerate(paths):
            write_parquet_file(path, i)

        # Configure execution with preserve_order to ensure deterministic results
        execution_options = ExecutionOptions()
        execution_options.preserve_order = True

        # Create shuffle config based on parameter
        if different_seeds_across_executions:
            shuffle_config = FileShuffleConfig(seed=42)
        else:
            shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)

        # Create two datasets with the same shuffle config
        ds1 = ray.data.read_parquet(paths, shuffle=shuffle_config)
        ds2 = ray.data.read_parquet(paths, shuffle=shuffle_config)

        data_config = ray.train.DataConfig(execution_options=execution_options)

        def train_fn():
            # Get dataset shards for both datasets
            train_ds1 = ray.train.get_dataset_shard("train1")
            train_ds2 = ray.train.get_dataset_shard("train2")

            # Collect results across multiple executions
            ds1_execution_results = []
            ds2_execution_results = []

            for execution_idx in range(NUM_EXECUTIONS):
                ds1_execution_data = list(train_ds1.iter_rows())
                ds1_execution_results.append(ds1_execution_data)

            for execution_idx in range(NUM_EXECUTIONS):
                ds2_execution_data = list(train_ds2.iter_rows())
                ds2_execution_results.append(ds2_execution_data)

            # Assertion 1: For the same execution, ds1 and ds2 should yield identical results
            # (deterministic shuffling with same base_seed)
            for i in range(NUM_EXECUTIONS):
                assert ds1_execution_results[i] == ds2_execution_results[i], (
                    f"Execution {i}: ds1 and ds2 should produce identical results "
                    f"for the same execution with the same shuffle seed"
                )

            # Convert results to hashable format for uniqueness check
            def make_hashable(rows):
                """Convert a list of dicts to a hashable tuple representation."""
                return tuple(tuple(sorted(row.items())) for row in rows)

            ds1_hashable_results = {
                make_hashable(result) for result in ds1_execution_results
            }
            ds2_hashable_results = {
                make_hashable(result) for result in ds2_execution_results
            }

            # Assertion 2: Different executions produce different results vs same results
            # based on whether seed varies by execution_idx
            if different_seeds_across_executions:
                # seed varies by execution, so expect variation
                assert len(ds1_hashable_results) == NUM_EXECUTIONS, (
                    f"ds1 should produce different results across executions, "
                    f"but got {len(ds1_hashable_results)} unique results out of {NUM_EXECUTIONS}"
                )
                assert len(ds2_hashable_results) == NUM_EXECUTIONS, (
                    f"ds2 should produce different results across executions, "
                    f"but got {len(ds2_hashable_results)} unique results out of {NUM_EXECUTIONS}"
                )
            else:
                # seed is constant, so expect no variation
                assert len(ds1_hashable_results) == 1, (
                    f"ds1 should produce the same results across all executions, "
                    f"but got {len(ds1_hashable_results)} unique results out of {NUM_EXECUTIONS}"
                )
                assert len(ds2_hashable_results) == 1, (
                    f"ds2 should produce the same results across all executions, "
                    f"but got {len(ds2_hashable_results)} unique results out of {NUM_EXECUTIONS}"
                )

            # Additional verification: Check that the total number of rows is consistent
            for execution_idx in range(NUM_EXECUTIONS):
                assert (
                    len(ds1_execution_results[execution_idx])
                    == (NUM_FILES * 10) // NUM_WORKERS
                )
                assert (
                    len(ds2_execution_results[execution_idx])
                    == (NUM_FILES * 10) // NUM_WORKERS
                )

        trainer = DataParallelTrainer(
            train_fn,
            datasets={"train1": ds1, "train2": ds2},
            dataset_config=data_config,
            scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
        )
        trainer.fit()


@pytest.mark.parametrize("exclude_resources", [None, ExecutionResources(cpu=2, gpu=1)])
def test_data_config_exclude_resources(ray_start_4_cpus, exclude_resources):
    execution_options = ExecutionOptions(exclude_resources=exclude_resources)
    data_config = ray.train.DataConfig(execution_options=execution_options)

    NUM_WORKERS = 2

    def check_exclude_resources(config):
        ds = ray.train.get_dataset_shard("train")
        exclude_resources = config.get("exclude_resources") or ExecutionResources.zero()

        # Under the V2 cluster autoscaler (default), training resources are
        # registered with the AutoscalingCoordinator, so exclude_resources
        # only contains what the user explicitly set.
        expected_exclude_resources = exclude_resources
        assert (
            ds.get_context().execution_options.exclude_resources
            == expected_exclude_resources
        )

    ds = ray.data.range(1)
    trainer = DataParallelTrainer(
        check_exclude_resources,
        train_loop_config={"exclude_resources": exclude_resources},
        datasets={"train": ds},
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


@pytest.mark.parametrize(
    "resource_limits", [None, ExecutionResources.for_limits(cpu=2, gpu=1)]
)
def test_data_config_resource_limits(ray_start_4_cpus, resource_limits):
    execution_options = ExecutionOptions(resource_limits=resource_limits)
    data_config = ray.train.DataConfig(execution_options=execution_options)

    NUM_WORKERS = 2

    def check_resource_limits(config):
        ds = ray.train.get_dataset_shard("train")
        resource_limits = (
            config.get("resource_limits") or ExecutionResources.for_limits()
        )
        assert ds.get_context().execution_options.resource_limits == resource_limits

        if not ds.get_context().execution_options.is_resource_limits_default():
            # Don't exclude train worker resources if the user already
            # set the resource_limits.
            assert (
                ds.get_context().execution_options.exclude_resources
                == ExecutionResources.zero()
            )

    ds = ray.data.range(1)
    trainer = DataParallelTrainer(
        check_resource_limits,
        train_loop_config={"resource_limits": resource_limits},
        datasets={"train": ds},
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_per_dataset_execution_options_single(ray_start_4_cpus):
    """Test that a single ExecutionOptions object applies to all datasets."""
    NUM_ROWS = 100
    NUM_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    val_ds = ray.data.range(NUM_ROWS)

    # Create execution options with specific settings
    execution_options = ExecutionOptions()
    execution_options.preserve_order = True
    execution_options.verbose_progress = True

    data_config = ray.train.DataConfig(execution_options=execution_options)

    def train_fn():
        train_shard = ray.train.get_dataset_shard("train")
        val_shard = ray.train.get_dataset_shard("val")

        # Verify both datasets have the same execution options
        assert train_shard.get_context().execution_options.preserve_order is True
        assert train_shard.get_context().execution_options.verbose_progress is True
        assert val_shard.get_context().execution_options.preserve_order is True
        assert val_shard.get_context().execution_options.verbose_progress is True

    trainer = DataParallelTrainer(
        train_fn,
        datasets={"train": train_ds, "val": val_ds},
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_per_dataset_execution_options_dict(ray_start_4_cpus):
    """Test that a dict of ExecutionOptions maps to specific datasets, and datasets not in the dict get default ingest options. Also tests resource limits."""
    NUM_ROWS = 100
    NUM_WORKERS = 2

    train_ds = ray.data.range(NUM_ROWS)
    val_ds = ray.data.range(NUM_ROWS)
    test_ds = ray.data.range(NUM_ROWS)
    test_ds_2 = ray.data.range(NUM_ROWS)

    # Create different execution options for different datasets
    train_options = ExecutionOptions()
    train_options.preserve_order = True
    train_options.verbose_progress = True
    train_options.resource_limits = train_options.resource_limits.copy(cpu=4, gpu=2)

    val_options = ExecutionOptions()
    val_options.preserve_order = False
    val_options.verbose_progress = False
    val_options.resource_limits = val_options.resource_limits.copy(cpu=2, gpu=1)

    execution_options_dict = {
        "train": train_options,
        "val": val_options,
    }

    data_config = ray.train.DataConfig(execution_options=execution_options_dict)

    def train_fn():
        train_shard = ray.train.get_dataset_shard("train")
        val_shard = ray.train.get_dataset_shard("val")
        test_shard = ray.train.get_dataset_shard("test")
        test_shard_2 = ray.train.get_dataset_shard("test_2")

        # Verify each dataset in the dict gets its specific options
        assert train_shard.get_context().execution_options.preserve_order is True
        assert train_shard.get_context().execution_options.verbose_progress is True
        assert val_shard.get_context().execution_options.preserve_order is False
        assert val_shard.get_context().execution_options.verbose_progress is False

        # Verify resource limits
        assert train_shard.get_context().execution_options.resource_limits.cpu == 4
        assert train_shard.get_context().execution_options.resource_limits.gpu == 2
        assert val_shard.get_context().execution_options.resource_limits.cpu == 2
        assert val_shard.get_context().execution_options.resource_limits.gpu == 1

        # Verify dataset not in the dict gets default options
        assert (
            test_shard.get_context().execution_options.preserve_order
            == test_shard_2.get_context().execution_options.preserve_order
        )
        assert (
            test_shard.get_context().execution_options.verbose_progress
            == test_shard_2.get_context().execution_options.verbose_progress
        )
        assert (
            test_shard.get_context().execution_options.resource_limits.cpu
            == test_shard_2.get_context().execution_options.resource_limits.cpu
        )
        assert (
            test_shard.get_context().execution_options.resource_limits.gpu
            == test_shard_2.get_context().execution_options.resource_limits.gpu
        )

    trainer = DataParallelTrainer(
        train_fn,
        datasets={
            "train": train_ds,
            "val": val_ds,
            "test": test_ds,
            "test_2": test_ds_2,
        },
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_exclude_train_resources_applies_to_each_dataset(ray_start_4_cpus):
    """Test that user-defined per-dataset exclude_resources are preserved.
    Under the V2 cluster autoscaler (default), training resources are NOT added
    to exclude_resources (they are handled by the AutoscalingCoordinator), so
    only the user-defined values should appear."""
    NUM_ROWS = 100
    NUM_WORKERS = 2

    # Create different execution options for different datasets
    train_options = ExecutionOptions()
    train_options.exclude_resources = train_options.exclude_resources.copy(cpu=2, gpu=1)

    test_options = ExecutionOptions()
    test_options.exclude_resources = test_options.exclude_resources.copy(cpu=1, gpu=0)

    # val dataset not in dict, should get default options
    execution_options_dict = {
        "train": train_options,
        "test": test_options,
    }
    data_config = ray.train.DataConfig(execution_options=execution_options_dict)

    def train_fn():
        # Under the V2 cluster autoscaler, only user-defined exclude_resources
        # should be present. Training resources are NOT added to exclude_resources.

        # Check train dataset — only user-defined exclude_resources
        train_ds = ray.train.get_dataset_shard("train")
        train_exec_options = train_ds.get_context().execution_options
        assert train_exec_options.is_resource_limits_default()
        assert train_exec_options.exclude_resources.cpu == 2
        assert train_exec_options.exclude_resources.gpu == 1

        # Check test dataset — only user-defined exclude_resources
        test_ds = ray.train.get_dataset_shard("test")
        test_exec_options = test_ds.get_context().execution_options
        assert test_exec_options.is_resource_limits_default()
        assert test_exec_options.exclude_resources.cpu == 1
        assert test_exec_options.exclude_resources.gpu == 0

        # Check val dataset — no user-defined exclude_resources, so zero
        val_ds = ray.train.get_dataset_shard("val")
        val_exec_options = val_ds.get_context().execution_options
        assert val_exec_options.is_resource_limits_default()
        default_options = ray.train.DataConfig.default_ingest_options()
        assert (
            val_exec_options.exclude_resources.cpu
            == default_options.exclude_resources.cpu
        )
        assert (
            val_exec_options.exclude_resources.gpu
            == default_options.exclude_resources.gpu
        )

    trainer = DataParallelTrainer(
        train_fn,
        datasets={
            "train": ray.data.range(NUM_ROWS),
            "test": ray.data.range(NUM_ROWS),
            "val": ray.data.range(NUM_ROWS),
        },
        dataset_config=data_config,
        scaling_config=ray.train.ScalingConfig(num_workers=NUM_WORKERS),
    )
    trainer.fit()


def test_datasets_callback_v1_uses_exclude_resources(ray_start_4_cpus, monkeypatch):
    """Under the V1 cluster autoscaler, exclude_resources should still be set by DataConfig."""
    monkeypatch.setenv("RAY_DATA_CLUSTER_AUTOSCALER", "V1")

    NUM_WORKERS = 2

    train_ds = ray.data.range(1000)
    valid_ds = ray.data.range(1000)

    data_config = ray.train.DataConfig(datasets_to_split=["train"])
    scaling_config = ray.train.ScalingConfig(
        num_workers=NUM_WORKERS, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
    )

    worker_group_context = WorkerGroupContext(
        run_attempt_id="attempt_1",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=scaling_config.num_workers,
        resources_per_worker=scaling_config.resources_per_worker,
    )
    train_run_context = create_dummy_run_context(
        dataset_config=data_config,
        scaling_config=scaling_config,
    )
    worker_group = DummyWorkerGroup(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
    )
    worker_group._start()

    callback = DatasetsCallback(
        train_run_context=train_run_context,
        datasets={"train": train_ds, "valid": valid_ds},
    )
    dataset_manager_for_each_worker = callback.before_init_train_context(
        worker_group.get_workers()
    )["dataset_shard_provider"]

    dataset_manager = dataset_manager_for_each_worker[0]
    processed_train_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="train")
    )
    processed_valid_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="valid")
    )

    # Under the V1 cluster autoscaler, exclude_resources should be set with training resources.
    assert (
        processed_train_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources(cpu=NUM_WORKERS, gpu=NUM_WORKERS)
    )
    assert (
        processed_valid_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources(cpu=NUM_WORKERS, gpu=NUM_WORKERS)
    )


def test_v2_no_negative_exclude_resources(ray_start_4_cpus):
    """Regression test: under the V2 cluster autoscaler, exclude_resources is not set,
    so the scenario that previously caused negative global limits (small cluster,
    multiple datasets, large training reservation) no longer fails.

    Before the fix, with 4 CPUs, 2 datasets (2 executors), and 3 CPUs for training:
    each executor gets 4 // 2 = 2 CPUs, minus 3 exclude_resources = -1 CPU -> assertion error.
    """
    NUM_WORKERS = 3

    train_ds = ray.data.range(100)
    valid_ds = ray.data.range(100)

    data_config = ray.train.DataConfig(datasets_to_split=["train"])
    # 3 workers * 1 CPU each = 3 CPUs for training, leaving 1 CPU for data.
    # With 2 datasets, each data executor gets 4 // 2 = 2 CPUs from coordinator.
    # If exclude_resources were set to 3, that would give 2 - 3 = -1 -> crash.
    scaling_config = ray.train.ScalingConfig(num_workers=NUM_WORKERS)

    worker_group_context = WorkerGroupContext(
        run_attempt_id="attempt_1",
        train_fn_ref=DummyObjectRefWrapper(lambda: None),
        num_workers=scaling_config.num_workers,
        resources_per_worker=scaling_config.resources_per_worker,
    )
    train_run_context = create_dummy_run_context(
        dataset_config=data_config,
        scaling_config=scaling_config,
    )
    worker_group = DummyWorkerGroup(
        train_run_context=train_run_context,
        worker_group_context=worker_group_context,
    )
    worker_group._start()

    callback = DatasetsCallback(
        train_run_context=train_run_context,
        datasets={"train": train_ds, "valid": valid_ds},
    )
    dataset_manager_for_each_worker = callback.before_init_train_context(
        worker_group.get_workers()
    )["dataset_shard_provider"]

    dataset_manager = dataset_manager_for_each_worker[0]
    processed_train_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="train")
    )
    processed_valid_ds = dataset_manager.get_dataset_shard(
        DatasetShardMetadata(dataset_name="valid")
    )

    # Under the V2 cluster autoscaler (default), exclude_resources should be
    # zero regardless of how many training resources are reserved.
    assert (
        processed_train_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources.zero()
    )
    assert (
        processed_valid_ds._base_dataset.context.execution_options.exclude_resources
        == ExecutionResources.zero()
    )


def test_fixed_scaling_policy_coordinator_lifecycle():
    """Test that FixedScalingPolicy registers training resources with the
    AutoscalingCoordinator on start, periodically re-requests to keep
    the reservation alive, and cancels on shutdown/abort."""
    from unittest.mock import patch

    from freezegun import freeze_time

    from ray.data._internal.cluster_autoscaler.default_autoscaling_coordinator import (
        ResourceRequestPriority,
    )
    from ray.train.v2._internal.execution.scaling_policy import (
        AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
        AUTOSCALING_REQUESTS_INTERVAL_S,
    )
    from ray.train.v2._internal.execution.scaling_policy.fixed import (
        FixedScalingPolicy,
    )

    resources_per_worker = {"CPU": 4, "GPU": 1}
    num_workers = 2
    scaling_config = ray.train.ScalingConfig(
        num_workers=num_workers,
        use_gpu=True,
        resources_per_worker=resources_per_worker,
    )

    mock_coordinator = MagicMock()
    expected_request_kwargs = dict(
        requester_id="train-test-run-123",
        resources=[resources_per_worker] * num_workers,
        expire_after_s=AUTOSCALING_REQUESTS_EXPIRE_TIME_S,
        priority=ResourceRequestPriority.HIGH,
    )

    with patch(
        "ray.get",
        side_effect=lambda x, **_: x,
    ):
        policy = FixedScalingPolicy(scaling_config)
        # Inject mock coordinator
        policy.__dict__["_autoscaling_coordinator"] = mock_coordinator

        with freeze_time() as frozen_time:
            # Simulate controller start
            mock_run_context = MagicMock()
            mock_run_context.run_id = "test-run-123"
            policy.after_controller_start(mock_run_context)

            assert policy._requester_id == "train-test-run-123"

            # Verify request_resources was called with the correct arguments
            mock_coordinator.request_resources.remote.assert_called_once_with(
                **expected_request_kwargs
            )

            # Calling make_decision immediately should NOT re-request (interval not passed)
            worker_group_state = MagicMock()
            worker_group_status = MagicMock()
            policy.make_decision_for_running_worker_group(
                worker_group_state=worker_group_state,
                worker_group_status=worker_group_status,
            )
            assert mock_coordinator.request_resources.remote.call_count == 1

            # Advance past the interval — should re-request
            frozen_time.tick(AUTOSCALING_REQUESTS_INTERVAL_S)
            policy.make_decision_for_running_worker_group(
                worker_group_state=worker_group_state,
                worker_group_status=worker_group_status,
            )
            assert mock_coordinator.request_resources.remote.call_count == 2
            mock_coordinator.request_resources.remote.assert_called_with(
                **expected_request_kwargs
            )

        # Simulate controller shutdown
        asyncio.run(policy.before_controller_shutdown())
        mock_coordinator.cancel_request.remote.assert_called_once_with(
            requester_id="train-test-run-123",
        )

        # Reset and test abort path
        mock_coordinator.cancel_request.remote.reset_mock()
        policy.before_controller_abort()
        mock_coordinator.cancel_request.remote.assert_called_once_with(
            requester_id="train-test-run-123",
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
