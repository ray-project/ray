import csv
import os
import random
from typing import List, Literal, Union

import numpy as np
import pandas as pd
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileSelector, FileType, LocalFileSystem
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators import Read, Write
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.planner.checkpoint.plan_read_op import (
    _get_checkpoint_map_transformer,
    plan_read_op_with_checkpoint_filter,
)
from ray.data._internal.planner.checkpoint.plan_write_op import (
    WRITE_UUID_KWARG_NAME,
    _generate_base_filename,
    _generate_prepare_checkpoint_transform,
)
from ray.data.block import BlockAccessor
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_filter import (
    IdColumnCheckpointManager,
    NumpyArrayBasedCheckpointFilter,
)
from ray.data.checkpoint.checkpoint_writer import (
    PENDING_CHECKPOINT_SUFFIX,
    BatchBasedCheckpointWriter,
)
from ray.data.checkpoint.interfaces import (
    CheckpointBackend,
    InvalidCheckpointingConfig,
)
from ray.data.checkpoint.util import PrefixTrie
from ray.data.context import DataContext
from ray.data.datasource import BlockBasedFileDatasink, RowBasedFileDatasink
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# User-provided ID column name
ID_COL = "id"
# Number of rows in the sample data
SAMPLE_DATA_NUM_ROWS = 10

# Auto-use `restore_data_context` for each test and apply 300-second timeout to all tests.
pytestmark = [
    pytest.mark.usefixtures("restore_data_context"),
    pytest.mark.timeout(300),
]


@pytest.fixture
def generate_sample_data_csv(tmp_path):
    def _generate(id_type: Literal["int", "str"] = "int"):
        # Generate a dummy dataset with SAMPLE_DATA_NUM_ROWS rows and columns [ID_COL, "col1"]
        ids = (
            [f"id_{i}" for i in range(SAMPLE_DATA_NUM_ROWS)]
            if id_type == "str"
            else list(range(SAMPLE_DATA_NUM_ROWS))
        )

        data = [{ID_COL: id_val, "col1": random.random()} for id_val in ids]

        f_path = os.path.join(tmp_path, "sample_data.csv")
        with open(f_path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        return f_path

    return _generate


@pytest.fixture
def checkpoint_path(tmp_path):
    """Fixture to provide a temporary checkpoint path."""
    return str(tmp_path / "checkpoint")


@pytest.fixture
def data_output_path(data_path):
    """Fixture to provide a standardized data output path."""
    return os.path.join(data_path, "output")


@pytest.fixture
def generate_sample_data_parquet(tmp_path):
    def _generate():
        f_dir = os.path.join(tmp_path, "sample_data_parquet")
        os.makedirs(f_dir, exist_ok=True)
        # Generate a dummy dataset with SAMPLE_DATA_NUM_ROWS rows and columns [ID_COL, "col1"]
        df = pd.DataFrame(
            [{ID_COL: i, "col1": random.random()} for i in range(SAMPLE_DATA_NUM_ROWS)]
        )

        f_path = os.path.join(f_dir, "sample_data.parquet")
        # Write 3 row groups per file with uneven distribution of rows per row group
        table = pa.table(df)
        row_group_size = max(1, SAMPLE_DATA_NUM_ROWS // 3)
        pq.write_table(table, f_path, row_group_size=row_group_size)
        return f_dir

    return _generate


@pytest.fixture
def generate_sample_physical_plan(generate_sample_data_csv, tmp_path):
    ctx = ray.data.DataContext.get_current()

    datasource = CSVDatasource(generate_sample_data_csv())

    read_op = Read(datasource, datasource, -1, None)
    write_path = os.path.join(tmp_path, "output")
    write_op = Write(ParquetDatasink(write_path), input_dependencies=[read_op])
    logical_plan = LogicalPlan(write_op, ctx)
    physical_plan, _ = get_execution_plan(logical_plan)
    yield physical_plan


def _get_batch_based_files(ckpt_path: str, fs) -> List[str]:
    """Get checkpoint file paths for batch-based backends."""
    if fs is None:
        if not os.path.exists(ckpt_path):
            return []
        return [os.path.join(ckpt_path, f) for f in os.listdir(ckpt_path)]
    else:
        files = fs.get_file_info(
            FileSelector(_unwrap_protocol(ckpt_path), allow_not_found=True)
        )
        return [file_info.path for file_info in files if file_info.is_file]


def _read_batch_file_ids(file_paths: List[str], id_column: str, fs) -> List[int]:
    """Read IDs from batch-based checkpoint files."""
    ids = []
    for file_path in file_paths:
        if fs is None:
            table = pa.parquet.read_table(file_path)
        else:
            with fs.open_input_file(file_path) as f:
                table = pa.parquet.read_table(f)
        df = table.to_pandas()
        ids.extend(df[id_column].tolist())
    return ids


def read_ids_from_checkpoint_files(config: CheckpointConfig) -> List[Union[int, str]]:
    """Reads the checkpoint files and returns a sorted list of IDs which have been checkpointed."""
    # Batch-based backends
    if config.backend in (
        CheckpointBackend.FILE_STORAGE,
        CheckpointBackend.CLOUD_OBJECT_STORAGE,
    ):
        file_paths = _get_batch_based_files(config.checkpoint_path, config.filesystem)
        return sorted(
            _read_batch_file_ids(file_paths, config.id_column, config.filesystem)
        )

    else:
        raise ValueError(f"Invalid backend: {config.backend}")


class TestCheckpointConfig:
    @pytest.mark.parametrize("id_column", ["", 1])
    def test_invalid_id_column(self, id_column, local_path):
        with pytest.raises(
            InvalidCheckpointingConfig,
            match="Checkpoint ID column",
        ):
            CheckpointConfig(id_column, local_path)

    def test_override_backend_emits_deprecation_warning(self):
        with pytest.warns(FutureWarning, match="deprecated"):
            CheckpointConfig(
                ID_COL,
                "s3://bucket/path",
                override_backend=CheckpointBackend.FILE_STORAGE,
            )

    def test_default_checkpoint_path(self, s3_path, monkeypatch):
        with pytest.raises(
            InvalidCheckpointingConfig,
            match="CheckpointConfig.checkpoint_path",
        ):
            CheckpointConfig(ID_COL, None)

        default_bucket = s3_path
        monkeypatch.setenv(
            CheckpointConfig.DEFAULT_CHECKPOINT_PATH_BUCKET_ENV_VAR, default_bucket
        )

        config = CheckpointConfig(ID_COL, None)
        assert (
            config.checkpoint_path
            == f"{default_bucket}/{CheckpointConfig.DEFAULT_CHECKPOINT_PATH_DIR}"
        )

    @pytest.mark.parametrize("checkpoint_path", ["tmp/", "s3:/tmp", "s4://tmp"])
    def test_invalid_checkpoint_path(self, checkpoint_path):
        with pytest.raises(
            InvalidCheckpointingConfig,
            match="Invalid checkpoint path",
        ):
            CheckpointConfig(ID_COL, checkpoint_path)

    @pytest.mark.parametrize(
        "checkpoint_path",
        [
            lazy_fixture("local_path"),
            lazy_fixture("s3_path"),
        ],
    )
    def test_infer_filesystem_and_backend(self, checkpoint_path):
        config = CheckpointConfig(ID_COL, checkpoint_path)
        if checkpoint_path.startswith("/"):
            assert isinstance(config.filesystem, pyarrow.fs.LocalFileSystem)
            assert config.backend == CheckpointBackend.FILE_STORAGE
        else:
            assert isinstance(config.filesystem, pyarrow.fs.S3FileSystem)
            assert config.backend == CheckpointBackend.CLOUD_OBJECT_STORAGE

    @pytest.mark.parametrize(
        "checkpoint_path,fs,backend",
        [
            (
                lazy_fixture("local_path"),
                lazy_fixture("local_fs"),
                CheckpointBackend.FILE_STORAGE,
            ),
            (
                lazy_fixture("s3_path"),
                lazy_fixture("s3_fs"),
                CheckpointBackend.FILE_STORAGE,
            ),
            (
                lazy_fixture("local_path"),
                lazy_fixture("local_fs"),
                CheckpointBackend.CLOUD_OBJECT_STORAGE,
            ),
            (
                lazy_fixture("s3_path"),
                lazy_fixture("s3_fs"),
                CheckpointBackend.CLOUD_OBJECT_STORAGE,
            ),
        ],
    )
    def test_override_filesystem_and_backend(self, checkpoint_path, fs, backend):
        config = CheckpointConfig(
            ID_COL, checkpoint_path, override_filesystem=fs, override_backend=backend
        )
        assert config.filesystem is fs
        assert config.backend is backend

    def test_skip_inference_with_overrides(self):
        """Test that filesystem inference is skipped when override is provided."""
        # Inferring filesystem will fail if the path doesn't exist.
        path = "s3://non-existing-bucket/"
        fs = pyarrow.fs.S3FileSystem()
        config = CheckpointConfig(
            ID_COL,
            path,
            override_filesystem=fs,
        )
        assert config.filesystem is fs
        assert config.backend is CheckpointBackend.CLOUD_OBJECT_STORAGE


@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
        (
            CheckpointBackend.FILE_STORAGE,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
        ),
        (
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
        ),
    ],
)
def test_checkpoint_end_to_end(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """The end-to-end test for checkpoint."""

    class TestActor:
        def __init__(self):
            pass

        def __call__(self, batch):
            return batch

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        override_filesystem=fs,
        override_backend=backend,
    )

    csv_file = generate_sample_data_csv()

    # Generate checkpoint file
    checkpointed_ids = list(range(SAMPLE_DATA_NUM_ROWS // 2))
    expected_remaining_ids = sorted(
        set(range(SAMPLE_DATA_NUM_ROWS)) - set(checkpointed_ids)
    )

    ckpt_unwrapped = _unwrap_protocol(ckpt_path)
    if fs is None:
        os.makedirs(ckpt_unwrapped, exist_ok=True)
        ckpt_table = pa.table({ID_COL: checkpointed_ids})
        pq.write_table(
            ckpt_table, os.path.join(ckpt_unwrapped, "pre_checkpoint.parquet")
        )
    else:
        fs.create_dir(ckpt_unwrapped)
        ckpt_table = pa.table({ID_COL: checkpointed_ids})
        ckpt_file_path = os.path.join(ckpt_unwrapped, "pre_checkpoint.parquet")
        with fs.open_output_stream(ckpt_file_path) as f:
            pq.write_table(ckpt_table, f)

    ds = ray.data.read_csv(csv_file)

    # Execute the dataset with checkpointing enabled.
    ds = ds.map_batches(TestActor, concurrency=1)
    ds.write_parquet(data_output_path, filesystem=fs)

    # When execution succeeds, checkpoint data should be automatically deleted.
    # Check that the checkpoint directory is empty or doesn't exist
    if ctx.checkpoint_config.delete_checkpoint_on_success:
        try:
            unwrapped_path = _unwrap_protocol(ckpt_path)
            # Try to get file info for the checkpoint directory
            files = ctx.checkpoint_config.filesystem.get_file_info(
                pyarrow.fs.FileSelector(unwrapped_path, recursive=True)
            )
            # If we can get file info, the directory exists and should be empty
            assert (
                len(files) == 0
            ), f"Checkpoint directory should be empty but contains {len(files)} files"
        except (FileNotFoundError, OSError):
            # If directory doesn't exist, that's also fine (cleanup worked)
            pass

    # Ensure that the written data only contains the non-checkpointed rows.
    # Disable checkpointing before reading back to avoid filtering.
    ctx.checkpoint_config = None
    ds_readback = ray.data.read_parquet(data_output_path, filesystem=fs)
    actual_output = sorted([row[ID_COL] for row in ds_readback.iter_rows()])
    assert actual_output == expected_remaining_ids, (
        f"Expected only non-checkpointed IDs {expected_remaining_ids}, "
        f"but got {actual_output}"
    )


@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
        (
            CheckpointBackend.FILE_STORAGE,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
        ),
        (
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
        ),
    ],
)
def test_full_dataset_executed_for_non_write(
    ray_start_10_cpus_shared,
    generate_sample_data_parquet,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """Tests that for an already fully checkpointed Dataset,
    calling `schema()` and `count()` should not skip checkpointing
    and should execute the full Dataset to get the correct information.
    """

    ctx = ray.data.DataContext.get_current()
    ctx.default_hash_shuffle_parallelism = 1
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        override_filesystem=fs,
        override_backend=backend,
    )

    parquet_dir = generate_sample_data_parquet()

    ds = ray.data.read_parquet(parquet_dir)

    ds = ds.map(lambda row: row)

    # Get the schema and count prior to writing the dataset.
    schema_before_write = ds.schema()
    count_before_write = ds.count()

    ds.write_parquet(data_output_path, filesystem=fs)

    # Recreate the same dataset, so that it will skip checkpointed rows.
    ds2 = ray.data.read_parquet(parquet_dir)
    ds2 = ds2.map(lambda row: row)

    # Check that when re-running a dataset which has already been completely
    # checkpointed, it does not skip any rows during `schema()` and `count()` calls.
    assert ds2.schema() == schema_before_write
    assert ds2.count() == count_before_write


@pytest.mark.parametrize(
    "ds_factory",
    [
        (lazy_fixture("generate_sample_data_parquet")),
        (lazy_fixture("generate_sample_data_parquet")),
    ],
)
@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
        (
            CheckpointBackend.FILE_STORAGE,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
        ),
        (
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
        ),
    ],
)
def test_recovery_no_missing_rows(
    ray_start_10_cpus_shared,
    ds_factory,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """Tests that recovery is at_least_once: no missing rows after retry."""

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True
    ctx.default_hash_shuffle_parallelism = 1
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")

    # Ensure checkpoint directory exists
    os.makedirs(ckpt_path, exist_ok=True)

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        override_filesystem=fs,
        override_backend=backend,
    )

    # Catch the custom TestException raised by FailActor.
    ctx.raise_original_map_exception = True

    @ray.remote(num_cpus=0)
    class Coordinator:
        def __init__(self):
            self._should_fail = True

        def disable_failure(self):
            self._should_fail = False

        def should_fail(self):
            return self._should_fail

    coordinator_actor = Coordinator.remote()

    class TestException(Exception):
        pass

    class FailActor:
        """Simple passthrough actor, which fails after a certain number of rows."""

        def __init__(self, coordinator_actor, max_num_items, checkpoint_config):
            self._should_fail = ray.get(coordinator_actor.should_fail.remote())
            self._max_num_items = max_num_items
            self._checkpoint_config = checkpoint_config

        def __call__(self, batch):
            # Get the ID column name from the checkpoint config
            id_col = self._checkpoint_config.id_column

            # Process each row in the batch
            ids = batch[id_col]

            for _, id in enumerate(ids):
                if self._should_fail and id == 2:
                    raise TestException(f"FailActor: Failing on row {id}")

            return batch

    # Use the ds_factory to create the dataset
    local_data_path = ds_factory()
    ds = ray.data.read_parquet(local_data_path)

    # Get the actual number of items from the dataset
    max_num_items = ds.count()

    ds = ds.map_batches(
        FailActor,
        fn_constructor_args=[coordinator_actor, max_num_items, ctx.checkpoint_config],
        concurrency=1,
        batch_size=None,
        num_cpus=1.1,  # Use a different num_cpus to avoid operator fusion.
    )

    # Should fail in the middle.
    with pytest.raises(TestException):
        ds.write_parquet(data_output_path, filesystem=fs, concurrency=1)

    ray.get(coordinator_actor.disable_failure.remote())
    # When executing the same dataset again, this should skip the already
    # checkpointed rows.
    ds.write_parquet(data_output_path, filesystem=fs, concurrency=1)

    # When execution succeeds, checkpoint data should be automatically deleted.
    assert read_ids_from_checkpoint_files(ctx.checkpoint_config) == []

    # Get the ID column name from the checkpoint config
    id_col = ctx.checkpoint_config.id_column

    # Disable checkpointing prior to reading back the data, so we don't skip any rows.
    ctx.checkpoint_config = None

    # Ensure that the written data is correct.
    ds_readback = ray.data.read_parquet(data_output_path, filesystem=fs)

    # Checkpointing is at_least_once, so duplicates are allowed; ensure no missing IDs.
    actual_output = [row[id_col] for row in ds_readback.iter_rows()]
    expected_output = list(range(max_num_items))
    assert set(actual_output) == set(expected_output)


@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
        (
            CheckpointBackend.FILE_STORAGE,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
        ),
        (
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
        ),
    ],
)
def test_manual_checkpoint_filters_ids(
    ray_start_10_cpus_shared,
    generate_sample_data_parquet,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """Manually write checkpoint IDs and verify they are filtered from output."""
    ctx = ray.data.DataContext.get_current()
    ctx.default_hash_shuffle_parallelism = 1
    ckpt_path = os.path.join(data_path, "test_manual_checkpoint_files")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        override_filesystem=fs,
        override_backend=backend,
    )

    checkpoint_ids = [1, 3, 7]
    df = pd.DataFrame({ID_COL: checkpoint_ids})
    checkpoint_writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    checkpoint_writer.write_block_checkpoint(BlockAccessor.for_block(df))
    assert set(read_ids_from_checkpoint_files(ctx.checkpoint_config)) == set(
        checkpoint_ids
    )

    parquet_dir = generate_sample_data_parquet()
    ds = ray.data.read_parquet(parquet_dir)
    ds.write_parquet(data_output_path, filesystem=fs)

    # Disable checkpointing prior to reading back the data, so we don't skip any rows.
    ctx.checkpoint_config = None
    ds_readback = ray.data.read_parquet(data_output_path, filesystem=fs)
    actual_output = sorted([row[ID_COL] for row in ds_readback.iter_rows()])
    expected_output = sorted(
        [i for i in range(SAMPLE_DATA_NUM_ROWS) if i not in set(checkpoint_ids)]
    )
    assert actual_output == expected_output


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_pending_checkpoint_write_and_commit(fs, base_path):
    """Test the two-phase commit (2PC) checkpoint write and commit workflow.

    This verifies that:
    1. write_pending_checkpoint() creates a checkpoint file with PENDING suffix
    2. The pending checkpoint exists but the committed version does not
    3. commit_checkpoint() renames the pending file to committed (removes suffix)
    4. After commit, the pending file no longer exists and committed file exists
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    fs.create_dir(_unwrap_protocol(checkpoint_path))
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    df = pd.DataFrame({ID_COL: [1, 2], "col1": [0.1, 0.2]})

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id="test_000000_000000",
    )
    assert pending is not None
    assert fs.get_file_info(pending.pending_path).type != FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type == FileType.NotFound

    writer.commit_checkpoint(pending)
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type != FileType.NotFound

    # Read back and verify contents
    with fs.open_input_file(pending.committed_path) as f:
        table = pq.read_table(f)
    assert table.column(ID_COL).to_pylist() == [1, 2]


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_commit_checkpoint_idempotent_already_committed(fs, base_path):
    """Test that commit_checkpoint is idempotent when already committed.

    If the committed file already exists and pending doesn't, calling
    commit_checkpoint should succeed without error (no-op).
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    fs.create_dir(_unwrap_protocol(checkpoint_path))
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    df = pd.DataFrame({ID_COL: [1, 2]})

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id="test_000000_000000",
    )
    assert pending is not None

    # First commit succeeds
    writer.commit_checkpoint(pending)
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type != FileType.NotFound

    # Second commit should be idempotent (no error)
    writer.commit_checkpoint(pending)
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type != FileType.NotFound


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_commit_checkpoint_idempotent_both_exist(fs, base_path):
    """Test that commit_checkpoint cleans up when both files exist.

    If both committed and pending files exist (edge case), the pending
    file should be deleted and committed file preserved.
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    fs.create_dir(_unwrap_protocol(checkpoint_path))
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    df = pd.DataFrame({ID_COL: [1, 2]})

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id="test_000000_000000",
    )
    assert pending is not None

    # Commit normally
    writer.commit_checkpoint(pending)

    # Manually recreate the pending file to simulate edge case
    with fs.open_output_stream(pending.pending_path) as f:
        f.write(b"dummy")

    assert fs.get_file_info(pending.pending_path).type != FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type != FileType.NotFound

    # Commit should clean up the pending file
    writer.commit_checkpoint(pending)
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type != FileType.NotFound


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_commit_checkpoint_neither_exists(fs, base_path):
    """Test that commit_checkpoint raises error when neither file exists."""
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    fs.create_dir(_unwrap_protocol(checkpoint_path))
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    df = pd.DataFrame({ID_COL: [1, 2]})

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id="test_000000_000000",
    )
    assert pending is not None

    # Delete the pending file to simulate missing state
    fs.delete_file(pending.pending_path)
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound
    assert fs.get_file_info(pending.committed_path).type == FileType.NotFound

    # Commit should raise FileNotFoundError
    with pytest.raises(FileNotFoundError):
        writer.commit_checkpoint(pending)


@pytest.mark.parametrize("data_file_exists", [True, False])
@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_clean_pending_checkpoint(
    ray_start_10_cpus_shared, fs, base_path, data_file_exists
):
    """Test pending checkpoint cleanup removes incomplete writes.

    When a write fails after creating a pending checkpoint but before commit,
    the cleanup process must:
    1. Find pending checkpoint files, build prefix trie from their basenames
    2. Delete associated data files matching the prefix (if they exist)
    3. Delete the pending checkpoint files

    This test verifies cleanup works correctly whether the data file was
    actually written (data_file_exists=True) or not (data_file_exists=False).
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    data_dir = os.path.join(base_path, "data")
    for p in [checkpoint_path, data_dir]:
        fs.create_dir(_unwrap_protocol(p))

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)

    # Write a pending checkpoint (simulating pre-write phase)
    df = pd.DataFrame({ID_COL: [0]})
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id="test_000000_000000",
    )
    assert pending is not None
    assert fs.get_file_info(pending.pending_path).type != FileType.NotFound

    # Optionally create a data file matching the pending checkpoint prefix
    data_file = os.path.join(_unwrap_protocol(data_dir), "test_000000_000000.csv")
    if data_file_exists:
        with fs.open_output_stream(data_file) as f:
            f.write(b"id\n0\n")

    checkpoint_manager = IdColumnCheckpointManager(ctx.checkpoint_config)
    checkpoint_manager._clean_pending_checkpoints(data_dir, fs)

    # Data file should be deleted (if it existed)
    if data_file_exists:
        assert fs.get_file_info(data_file).type == FileType.NotFound
    # Pending checkpoint should be deleted
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_clean_pending_checkpoint_with_partitioned_data(
    ray_start_10_cpus_shared, fs, base_path
):
    """Test pending checkpoint cleanup removes files in partition subdirectories.

    When using ParquetDatasink with partition_cols, data files are written to
    subdirectories like output/col=val/file.parquet. The cleanup process must
    recursively search subdirectories to find and delete data files matching
    pending checkpoint prefixes.
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    data_dir = os.path.join(base_path, "data")
    for p in [checkpoint_path, data_dir]:
        fs.create_dir(_unwrap_protocol(p))

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)

    # Write a pending checkpoint for "pending_task" (simulating failed write)
    base_filename = "pending_task"
    df = pd.DataFrame({ID_COL: [0, 1, 2]})
    id_column_data = BlockAccessor.for_block(df).to_arrow()[ID_COL]
    pending = writer.write_pending_checkpoint(
        id_column_data,
        checkpoint_id=base_filename,
    )
    assert pending is not None

    # Create data files matching the pending checkpoint in partition subdirectories
    partition_dirs = ["partition_col=a", "partition_col=b"]
    pending_data_files = []
    for partition_dir in partition_dirs:
        partition_path = os.path.join(_unwrap_protocol(data_dir), partition_dir)
        fs.create_dir(partition_path)
        data_file = os.path.join(partition_path, f"{base_filename}-0.parquet")
        with fs.open_output_stream(data_file) as f:
            f.write(b"dummy data")
        pending_data_files.append(data_file)

    # Also create a matching file directly in base dir
    base_data_file = os.path.join(
        _unwrap_protocol(data_dir), f"{base_filename}-0.parquet"
    )
    with fs.open_output_stream(base_data_file) as f:
        f.write(b"dummy data")
    pending_data_files.append(base_data_file)

    # Create unrelated data files that should NOT be deleted
    unrelated_files = []
    for partition_dir in partition_dirs:
        partition_path = os.path.join(_unwrap_protocol(data_dir), partition_dir)
        data_file = os.path.join(partition_path, "other_task-0.parquet")
        with fs.open_output_stream(data_file) as f:
            f.write(b"dummy data")
        unrelated_files.append(data_file)

    # Verify all files were created
    for f in pending_data_files + unrelated_files:
        assert (
            fs.get_file_info(f).type != FileType.NotFound
        ), f"Expected file to exist: {f}"

    # Run cleanup
    checkpoint_manager = IdColumnCheckpointManager(ctx.checkpoint_config)
    checkpoint_manager._clean_pending_checkpoints(data_dir, fs)

    # Verify data files matching pending checkpoint were deleted
    for f in pending_data_files:
        assert (
            fs.get_file_info(f).type == FileType.NotFound
        ), f"Expected pending data file to be deleted: {f}"

    # Verify unrelated data files still exist
    for f in unrelated_files:
        assert (
            fs.get_file_info(f).type != FileType.NotFound
        ), f"Expected unrelated file to still exist: {f}"

    # Verify pending checkpoint was also deleted
    assert fs.get_file_info(pending.pending_path).type == FileType.NotFound


def test_clean_pending_checkpoints_task_failure(ray_start_10_cpus_shared, tmp_path):
    """Test that _clean_pending_checkpoints raises when the cleanup task fails.

    This verifies that:
    1. When the underlying Ray task fails, the exception is propagated
    2. The error is logged properly before re-raising
    """
    ctx = ray.data.DataContext.get_current()
    # Use a non-existent path to trigger a failure when trying to list files
    checkpoint_path = os.path.join(tmp_path, "nonexistent", "deeply", "nested", "path")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
    )

    checkpoint_manager = IdColumnCheckpointManager(ctx.checkpoint_config)

    # The cleanup task should fail because the checkpoint directory doesn't exist
    # and get_file_info on a non-existent path will raise an error
    with pytest.raises(ray.exceptions.RayTaskError):
        checkpoint_manager._clean_pending_checkpoints("/dummy/data/path")


def test_prepare_checkpoint_transform_writes_pending(tmp_path):
    """Test that the pre-write checkpoint transform writes a pending checkpoint.

    This verifies that:
    1. _generate_prepare_checkpoint_transform() creates a transform that writes
       a pending checkpoint file
    2. The pending checkpoint filename matches the base filename with .pending suffix
    """

    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            file.write(b"")

    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    os.makedirs(checkpoint_path, exist_ok=True)
    data_output_path = os.path.join(tmp_path, "output")
    os.makedirs(data_output_path, exist_ok=True)
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
    )

    datasink = MockFileDatasink(data_output_path, file_format="csv")
    checkpoint_writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    transform = _generate_prepare_checkpoint_transform(ctx, datasink, checkpoint_writer)

    ctx_task = TaskContext(task_idx=0, op_name="test")
    ctx_task.kwargs[WRITE_UUID_KWARG_NAME] = "test-write-uuid"

    df = pd.DataFrame({ID_COL: [0], "col1": [0.1]})
    list(transform._apply_transform(ctx_task, [df]))

    pending_files = [
        f
        for f in os.listdir(checkpoint_path)
        if f.endswith(f"{PENDING_CHECKPOINT_SUFFIX}.parquet")
    ]
    assert len(pending_files) == 1

    # Verify pending checkpoint filename matches the base filename
    base_filename = _generate_base_filename(datasink, ctx_task)
    assert pending_files[0] == f"{base_filename}{PENDING_CHECKPOINT_SUFFIX}.parquet"


def test_2pc_fail_retry_cleans_pending_checkpoints(
    ray_start_10_cpus_shared,
    tmp_path,
):
    """Test end-to-end 2PC cleanup: fail during write, retry succeeds after cleanup.

    This is an integration test for the full two-phase commit cleanup flow:
    1. First write attempt: pre-write creates pending checkpoint, datasink
       writes partial data then fails, leaving pending checkpoint files
    2. Retry: the checkpoint filter's _clean_pending_checkpoints() detects
       pending files, deletes them along with partial data files
    3. Second write attempt: succeeds, writing complete data with no duplicates

    Verifies that after cleanup, all pending checkpoints are removed and
    the final output contains the correct data.
    """
    ctx = ray.data.DataContext.get_current()
    ctx.raise_original_map_exception = True
    ctx.default_hash_shuffle_parallelism = 1

    checkpoint_path = os.path.join(tmp_path, "checkpoint")
    data_output_path = os.path.join(tmp_path, "output")
    os.makedirs(checkpoint_path, exist_ok=True)

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
    )

    @ray.remote(num_cpus=0)
    class FailController:
        def __init__(self):
            self._should_fail = True

        def should_fail(self):
            return self._should_fail

        def disable_failure(self):
            self._should_fail = False

    controller = FailController.remote()

    class FailOnceCSVDatasink(BlockBasedFileDatasink):
        def __init__(self, path: str, controller):
            super().__init__(path, file_format="csv")
            self._controller = controller

        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            if ray.get(self._controller.should_fail.remote()):
                # Write a partial file and then fail to simulate incomplete write.
                file.write(b"id\n0\n")
                raise RuntimeError("Simulated write failure")
            block.to_pandas().to_csv(file, index=False)

    datasink = FailOnceCSVDatasink(data_output_path, controller)
    ds = ray.data.range(SAMPLE_DATA_NUM_ROWS, override_num_blocks=1)

    with pytest.raises(RuntimeError, match="Simulated write failure"):
        ds.write_datasink(datasink, ray_remote_args={"max_retries": 0})

    # After failure, there should be pending checkpoint files (written in
    # pre-write phase, before the data write failed)
    pending_files = [
        f
        for f in os.listdir(checkpoint_path)
        if f.endswith(f"{PENDING_CHECKPOINT_SUFFIX}.parquet")
    ]
    assert pending_files, "Expected pending checkpoint files after failed write."

    ray.get(controller.disable_failure.remote())
    ds.write_datasink(datasink, ray_remote_args={"max_retries": 0})

    # After successful retry, pending checkpoints should be cleaned up
    pending_files_after = [
        f
        for f in os.listdir(checkpoint_path)
        if f.endswith(f"{PENDING_CHECKPOINT_SUFFIX}.parquet")
    ]
    assert pending_files_after == []

    ctx.checkpoint_config = None
    ds_readback = ray.data.read_csv(data_output_path)
    actual_output = sorted([row[ID_COL] for row in ds_readback.iter_rows()])
    expected_output = sorted(range(SAMPLE_DATA_NUM_ROWS))
    assert actual_output == expected_output


@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
        (
            CheckpointBackend.FILE_STORAGE,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
        ),
        (
            CheckpointBackend.CLOUD_OBJECT_STORAGE,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
        ),
    ],
)
def test_skip_checkpoint_flag(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    backend,
    fs,
    data_path,
):
    """Test that for a valid Dataset with checkpointing enabled, calling methods like
    `schema()` and `count()` should skip checkpointing and not create any checkpoint
    files. Subsequently calling `write_xxx()` on the same dataset should have
    checkpointing enabled."""

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")
    ctx.checkpoint_config = CheckpointConfig(
        ID_COL,
        ckpt_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
        override_backend=backend,
    )

    def generate_ds():
        ds = ray.data.read_csv(generate_sample_data_csv())

        ds = ds.map(lambda row: row)
        return ds

    ds = generate_ds()

    # Calling `ds.schema()` should skip checkpointing.
    assert ds.schema() is not None
    assert len(read_ids_from_checkpoint_files(ctx.checkpoint_config)) == 0

    # Calling `ds.count()` should skip checkpointing.
    ds = generate_ds()
    assert ds.count() is not None
    assert len(read_ids_from_checkpoint_files(ctx.checkpoint_config)) == 0

    # Calling `ds.write_xxx()` afterwards should enable checkpointing.
    ds.write_parquet(os.path.join(data_path, "output"), filesystem=fs)

    # Check what checkpoint files exist
    checkpoint_files = read_ids_from_checkpoint_files(ctx.checkpoint_config)

    assert len(checkpoint_files) == SAMPLE_DATA_NUM_ROWS


def test_checkpoint_with_missing_id_column(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    tmp_path,
):
    """Test that checkpointing fails gracefully when the configured id_column doesn't exist in the data."""

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
    # Configure checkpointing with an id_column that doesn't exist in the CSV data
    ctx.checkpoint_config = CheckpointConfig(
        id_column="nonexistent_column",
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )

    def generate_ds():
        ds = ray.data.read_csv(generate_sample_data_csv())
        ds = ds.map(lambda row: row)
        return ds

    ds = generate_ds()

    # The write operation should fail because the id_column doesn't exist
    with pytest.raises(
        ValueError,
        match="ID column nonexistent_column is absent in the block to be written",
    ):
        ds.write_parquet(os.path.join(tmp_path, "output"))


def test_dict_checkpoint_config(checkpoint_path):
    """Test that a dict checkpoint config can be used to create a CheckpointConfig."""
    context = ray.data.DataContext.get_current()
    fs = LocalFileSystem()
    context.checkpoint_config = {
        "id_column": ID_COL,
        "checkpoint_path": checkpoint_path,
        "override_filesystem": fs,
        "override_backend": "CLOUD_OBJECT_STORAGE",
    }
    assert context.checkpoint_config.id_column == ID_COL
    assert context.checkpoint_config.checkpoint_path == checkpoint_path
    assert context.checkpoint_config.filesystem is fs
    assert context.checkpoint_config.backend == CheckpointBackend.CLOUD_OBJECT_STORAGE


def test_write_block_checkpoint_with_pandas_df(restore_data_context, tmp_path):
    ctx = ray.data.DataContext.get_current()

    ctx.checkpoint_config = CheckpointConfig(
        ID_COL,
        str(tmp_path),
    )
    df = pd.DataFrame({ID_COL: [0, 1]})
    expected_ids = [0, 1]

    checkpoint_writer = BatchBasedCheckpointWriter(ctx.checkpoint_config)
    checkpoint_writer.write_block_checkpoint(BlockAccessor.for_block(df))

    assert len(os.listdir(tmp_path)) == 1
    checkpoint_filename = os.listdir(tmp_path)[0]
    checkpoint_path = tmp_path / checkpoint_filename

    table = pa.parquet.read_table(checkpoint_path)
    df = table.to_pandas()
    written_ids = df[ID_COL].tolist()
    assert written_ids == expected_ids


def test_filter_rows_for_block():
    """Test NumpyArrayBasedCheckpointFilter.filter_rows_for_block."""

    # Common test setup
    checkpoint_path = "/mock/path"

    # Test with simple ID column
    config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
    )

    # Create a mock block.
    block = pyarrow.table(
        {
            ID_COL: list(range(10)),
            "data": [str(i) for i in range(10)],
        }
    )
    # Create a mock checkpointed_ids with multiple chunks.
    chunk1 = pyarrow.table({ID_COL: [1, 2, 4]})
    chunk2 = pyarrow.table({ID_COL: [6, 8, 9, 11]})
    chunk3 = pyarrow.table({ID_COL: [12, 13]})
    checkpointed_ids = pyarrow.concat_tables([chunk1, chunk2, chunk3])
    assert len(checkpointed_ids[ID_COL].chunks) == 3

    checkpoint_ids_array = []
    for ckpt_chunk in checkpointed_ids[ID_COL].chunks:
        checkpoint_ids_array.append(
            transform_pyarrow.to_numpy(ckpt_chunk, zero_copy_only=False)
        )
    checkpointed_ids_ndarray = np.concatenate(checkpoint_ids_array)
    checkpointed_ids_ref = ray.put(checkpointed_ids_ndarray)

    expected_block = pyarrow.table(
        {
            ID_COL: [0, 3, 5, 7],
            "data": ["0", "3", "5", "7"],
        }
    )

    filter_instance = NumpyArrayBasedCheckpointFilter(config, checkpointed_ids_ref)
    filtered_block = filter_instance.filter_rows_for_block(
        block=block,
    )

    assert filtered_block.equals(expected_block)


def test_checkpoint_restore_after_full_execution(
    ray_start_10_cpus_shared,
    tmp_path,
    generate_sample_data_parquet,
    checkpoint_path,
):
    """Test checkpoint restore after full execution of data pipeline. This is
    done by retaining the checkpoint metadata files with
    delete_checkpoint_on_success=False.
    """

    def run_simple_pipeline(
        checkpoint_config: CheckpointConfig, input_path: str, output_path: str
    ) -> int:
        """Run a simple pipeline with checkpointing."""
        from ray.data.datasource import WriteResult

        ctx = DataContext.get_current()
        ctx.checkpoint_config = checkpoint_config
        ctx.default_hash_shuffle_parallelism = 1
        ds = ray.data.read_parquet(input_path)

        # Patch `on_write_complete` to get the WriteResult.
        num_rows_written = None
        original_on_write_complete = ParquetDatasink.on_write_complete

        def patched_on_write_complete(self, write_result: WriteResult[None]):
            nonlocal num_rows_written
            num_rows_written = write_result.num_rows
            return original_on_write_complete(self, write_result)

        ParquetDatasink.on_write_complete = patched_on_write_complete

        ds.write_parquet(output_path)
        return int(num_rows_written)

    # Create test paths
    input_data_path = generate_sample_data_parquet()
    data_output_path = str(tmp_path / "output")

    # Create checkpoint config
    checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        override_backend=CheckpointBackend.FILE_STORAGE,
        delete_checkpoint_on_success=False,
    )

    # First run: create checkpoint
    num_rows_first = run_simple_pipeline(
        checkpoint_config, input_data_path, data_output_path
    )
    assert (
        num_rows_first == SAMPLE_DATA_NUM_ROWS
    ), f"Expected {SAMPLE_DATA_NUM_ROWS} rows, got {num_rows_first}"

    # Check if checkpoint files were created
    assert os.path.exists(checkpoint_path), "No checkpoint directory created!"

    # Second run: should use checkpoint
    num_rows_second = run_simple_pipeline(
        checkpoint_config, input_data_path, data_output_path
    )
    assert (
        num_rows_second == 0  # No rows should be written
    ), f"Expected 0 rows, got {num_rows_second}"


@pytest.mark.parametrize(
    "data_path",
    [
        (lazy_fixture("local_path")),
    ],
)
def test_checkpoint_map_transformer(
    ray_start_10_cpus_shared,
    data_path,
):
    """Test checkpoint map transformer."""
    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL, checkpoint_path=ckpt_path
    )

    checkpointed_ids_ndarray = np.array([1, 3, 5, 7, 9], dtype=np.int64)
    checkpointed_ids_ref = ray.put(checkpointed_ids_ndarray)

    map_transformer = _get_checkpoint_map_transformer(ctx, checkpointed_ids_ref)
    map_transformer.init()

    block = pyarrow.table(
        {
            ID_COL: list(range(10)),
            "data": [str(i) for i in range(10)],
        }
    )
    filtered_blocks = map_transformer.apply_transform(
        input_blocks=[block],
        ctx=TaskContext(task_idx=0, op_name="test_checkpoint"),
    )

    filtered_block = next(iter(filtered_blocks))
    expected_block = pyarrow.table(
        {
            ID_COL: [0, 2, 4, 6, 8],
            "data": ["0", "2", "4", "6", "8"],
        }
    )
    assert filtered_block.equals(expected_block)


@pytest.mark.parametrize(
    "data_path",
    [
        (lazy_fixture("local_path")),
    ],
)
def test_plan_read_op_with_checkpoint_filter_no_checkpoint_dir(
    ray_start_10_cpus_shared, generate_sample_data_csv, data_path
):
    """Test that when checkpoint directory does not exist,
    plan_read_op_with_checkpoint_filter returns the original read physical operator."""
    ctx = ray.data.DataContext.get_current()

    csv_file = generate_sample_data_csv()
    datasource = CSVDatasource(csv_file)

    # checkpoint_path points to a non-existent directory
    ckpt_path = os.path.join(str(data_path), "non_existent_ckpt_dir")
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )

    read_op = Read(datasource, datasource, -1, None)
    physical_op = plan_read_op_with_checkpoint_filter(
        None,
        None,
        op=read_op,
        physical_children=[],
        data_context=ctx,
    )

    # Should return the original ReadCSV op
    assert physical_op.name == "ReadCSV"


@pytest.mark.parametrize(
    "data_path",
    [
        (lazy_fixture("local_path")),
    ],
)
def test_plan_read_op_with_checkpoint_filter_empty_checkpoint_dir(
    ray_start_10_cpus_shared, generate_sample_data_csv, data_path
):
    """Test that when checkpoint directory exists but is empty,
    plan_read_op_with_checkpoint_filter returns the original read physical operator."""
    ctx = ray.data.DataContext.get_current()

    csv_file = generate_sample_data_csv()
    datasource = CSVDatasource(csv_file)

    # Create an empty checkpoint directory
    ckpt_path = os.path.join(str(data_path), "empty_ckpt_dir")
    os.makedirs(ckpt_path, exist_ok=True)

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )

    read_op = Read(datasource, datasource, -1, None)
    physical_op = plan_read_op_with_checkpoint_filter(
        None,
        None,
        op=read_op,
        physical_children=[],
        data_context=ctx,
    )

    # Should return the original ReadCSV op
    assert physical_op.name == "ReadCSV"


@pytest.mark.parametrize(
    "data_path",
    [
        (lazy_fixture("local_path")),
    ],
)
def test_plan_read_op_with_checkpoint_filter_with_valid_checkpoint(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    data_path,
):
    """Test that when a valid checkpoint exists,
    plan_read_op_with_checkpoint_filter returns a CheckpointFilter MapOperator."""
    ctx = ray.data.DataContext.get_current()

    csv_file = generate_sample_data_csv()
    datasource = CSVDatasource(csv_file)

    # Create a checkpoint directory with valid checkpoint data
    ckpt_path = os.path.join(str(data_path), "valid_ckpt_dir")
    os.makedirs(ckpt_path, exist_ok=True)

    # Write some checkpoint IDs (e.g., IDs 0-4 are already processed)
    checkpointed_ids = pa.table({ID_COL: list(range(5))})
    pq.write_table(checkpointed_ids, os.path.join(ckpt_path, "ckpt_0.parquet"))

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        delete_checkpoint_on_success=False,
    )

    read_op = Read(datasource, datasource, -1, None)
    physical_op = plan_read_op_with_checkpoint_filter(
        None,
        None,
        op=read_op,
        physical_children=[],
        data_context=ctx,
    )

    # Should return a CheckpointFilter MapOperator
    assert isinstance(physical_op, MapOperator)
    assert physical_op.name == "CheckpointFilter"


@pytest.mark.parametrize(
    "backend,fs,data_path",
    [
        (CheckpointBackend.FILE_STORAGE, None, lazy_fixture("local_path")),
    ],
)
def test_checkpoint_with_string_typed_id(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """Test the checkpoint when the ID column is of type string."""

    class TestActor:
        def __init__(self):
            pass

        def __call__(self, batch):
            return batch

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=ckpt_path,
        override_filesystem=fs,
        override_backend=backend,
    )

    csv_file = generate_sample_data_csv(id_type="str")

    # Generate checkpoint file
    checkpointed_ids = [f"id_{id}" for id in range(SAMPLE_DATA_NUM_ROWS // 2)]
    expected_remaining_ids = sorted(
        {f"id_{id}" for id in range(SAMPLE_DATA_NUM_ROWS)} - set(checkpointed_ids)
    )

    ckpt_unwrapped = _unwrap_protocol(ckpt_path)

    os.makedirs(ckpt_unwrapped, exist_ok=True)
    ckpt_table = pa.table({ID_COL: checkpointed_ids})
    pq.write_table(ckpt_table, os.path.join(ckpt_unwrapped, "pre_checkpoint.parquet"))

    ds = ray.data.read_csv(csv_file)

    # Execute the dataset with checkpointing enabled.
    ds = ds.map_batches(TestActor, concurrency=1)
    ds.write_parquet(data_output_path, filesystem=fs)

    # Ensure that the written data only contains the non-checkpointed rows.
    # Disable checkpointing before reading back to avoid filtering.
    ctx.checkpoint_config = None
    ds_readback = ray.data.read_parquet(data_output_path, filesystem=fs)
    actual_output = sorted([row[ID_COL] for row in ds_readback.iter_rows()])
    assert actual_output == expected_remaining_ids, (
        f"Expected only non-checkpointed IDs {expected_remaining_ids}, "
        f"but got {actual_output}"
    )


class FailAfterWriteParquetDatasink(ParquetDatasink):
    """Test helper: ParquetDatasink that fails AFTER writing data (simulates post-write crash).

    This simulates the failure scenario where:
    - Data is successfully written to the output file
    - But the process crashes/fails before the checkpoint can be committed
    This is the critical case that 2PC is designed to handle - the data file
    exists but is "uncommitted" and should be cleaned up on recovery.
    """

    def __init__(self, path: str, fail_threshold: int = 100, **kwargs):
        super().__init__(path, **kwargs)
        self._fail_threshold = fail_threshold

    def write(self, blocks, ctx):
        blocks_list = list(blocks)

        # Check if any block has id > threshold
        should_fail = False
        for block in blocks_list:
            accessor = BlockAccessor.for_block(block)
            df = accessor.to_pandas()
            if ID_COL in df.columns and df[ID_COL].max() > self._fail_threshold:
                should_fail = True
                break

        # First, write the blocks normally
        result = super().write(iter(blocks_list), ctx)

        # Then fail if threshold exceeded (simulates post-write failure)
        if should_fail:
            raise RuntimeError(
                f"Simulated failure: block contains {ID_COL} > {self._fail_threshold}"
            )

        return result


def test_partial_failure_no_duplicates(
    ray_start_10_cpus_shared,
    tmp_path,
):
    """Test checkpoint deduplication: partial failure + retry produces no duplicate rows.

    This is the key correctness test for the checkpoint deduplication feature.
    It verifies that when a write pipeline fails partway through:
    1. Already-committed rows (from successful blocks before failure) are tracked
    2. Uncommitted rows (from blocks that failed after writing data) are cleaned up
    3. On retry, only uncommitted rows are re-written
    4. Final output has exactly the expected rows with NO duplicates

    The test uses run_tag to verify which rows came from which run, confirming
    that committed rows from run 1 are preserved and not re-written in run 2.

    Note: This test requires ray.shutdown() + ray.init() mid-test to flush
    in-flight checkpoint writes, which is incompatible with mock S3 (pyarrow's
    S3 subsystem gets finalized during shutdown). The checkpoint storage layer
    is already tested with S3 in other parameterized tests.
    """
    num_rows = 1000
    fail_threshold = 100

    # Create paths
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    checkpoint_path_dir = tmp_path / "checkpoint"
    for path in [input_path, output_path, checkpoint_path_dir]:
        path.mkdir(exist_ok=True)

    # Create sample data (1000 rows with unique IDs)
    df = pd.DataFrame(
        {ID_COL: range(num_rows), "value": [f"row_{i}" for i in range(num_rows)]}
    )
    df.to_parquet(input_path / "data.parquet", index=False)

    # Configure checkpointing
    ctx = DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=str(checkpoint_path_dir),
        delete_checkpoint_on_success=False,
    )

    def add_run_tag(batch, run_tag):
        """Add a run_tag column to identify which run wrote the data."""
        batch["run_tag"] = [run_tag] * len(batch[ID_COL])
        return batch

    # Run 1: Use the failing datasink - should write some blocks then fail
    with pytest.raises(RuntimeError, match="Simulated failure"):
        ds = ray.data.read_parquet(str(input_path))
        ds = ds.repartition(200)  # 200 blocks
        ds = ds.map_batches(lambda b: add_run_tag(b, "first"), batch_size=None)
        failing_datasink = FailAfterWriteParquetDatasink(
            str(output_path), fail_threshold=fail_threshold
        )
        ds.write_datasink(failing_datasink, ray_remote_args={"max_retries": 0})

    # Shutdown Ray to ensure all in-flight tasks complete before checking state.
    # This addresses the race condition where background tasks may still be writing
    # checkpoint files after the exception is raised.
    ray.shutdown()
    ray.init()

    # Run 2: Use regular write_parquet - should resume from checkpoint
    ds2 = ray.data.read_parquet(str(input_path))
    ds2 = ds2.repartition(200)
    ds2 = ds2.map_batches(lambda b: add_run_tag(b, "second"), batch_size=None)
    ds2.write_parquet(str(output_path))

    result = ray.data.read_parquet(str(output_path)).to_pandas()

    assert len(result) == num_rows, f"Expected {num_rows} rows, got {len(result)}"

    # Check for duplicates
    assert result[ID_COL].is_unique, (
        f"Duplicate IDs found: "
        f"{sorted(result[result.duplicated(ID_COL, keep=False)][ID_COL].unique().tolist())}"
    )

    # Verify that some rows came from first run (before failure) and rest from second
    run_tag_counts = result["run_tag"].value_counts()
    assert "first" in run_tag_counts.index, "Expected some rows from first run"
    assert "second" in run_tag_counts.index, "Expected some rows from second run"


def test_partial_failure_no_duplicates_partitioned(
    ray_start_10_cpus_shared,
    tmp_path,
):
    """Test checkpoint deduplication with multi-level partitioned parquet output.

    Same as test_partial_failure_no_duplicates, but writes partitioned output
    using 3 partition columns, creating deeply nested subdirectories
    (e.g., output/region=us/category=x/tier=1/file.parquet). This exercises
    the recovery path where data files must be found via recursive search
    using the data_file_dir passed through the call chain.

    Note: This test requires ray.shutdown() + ray.init() mid-test to flush
    in-flight checkpoint writes, which is incompatible with mock S3 (pyarrow's
    S3 subsystem gets finalized during shutdown). The checkpoint storage layer
    is already tested with S3 in other parameterized tests.
    """
    num_rows = 1000
    fail_threshold = 100

    # Create paths
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    checkpoint_path_dir = tmp_path / "checkpoint"
    for path in [input_path, output_path, checkpoint_path_dir]:
        path.mkdir(exist_ok=True)

    # Create sample data with 3 partition columns for deeply nested output
    # (e.g., output/region=us/category=x/tier=1/file.parquet)
    regions = ["us", "eu"]
    categories = ["x", "y", "z"]
    tiers = [1, 2]
    df = pd.DataFrame(
        {
            ID_COL: range(num_rows),
            "value": [f"row_{i}" for i in range(num_rows)],
            "region": [regions[i % len(regions)] for i in range(num_rows)],
            "category": [categories[i % len(categories)] for i in range(num_rows)],
            "tier": [tiers[i % len(tiers)] for i in range(num_rows)],
        }
    )
    df.to_parquet(input_path / "data.parquet", index=False)

    # Configure checkpointing
    ctx = DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=str(checkpoint_path_dir),
        delete_checkpoint_on_success=False,
    )

    def add_run_tag(batch, run_tag):
        """Add a run_tag column to identify which run wrote the data."""
        batch["run_tag"] = [run_tag] * len(batch[ID_COL])
        return batch

    partition_cols = ["region", "category", "tier"]

    # Run 1: Use the failing datasink with partition_cols
    with pytest.raises(RuntimeError, match="Simulated failure"):
        ds = ray.data.read_parquet(str(input_path))
        ds = ds.repartition(200)
        ds = ds.map_batches(lambda b: add_run_tag(b, "first"), batch_size=None)
        failing_datasink = FailAfterWriteParquetDatasink(
            str(output_path),
            fail_threshold=fail_threshold,
            partition_cols=partition_cols,
        )
        ds.write_datasink(failing_datasink, ray_remote_args={"max_retries": 0})

    # Shutdown Ray to ensure all in-flight tasks complete before checking state.
    ray.shutdown()
    ray.init()

    # Run 2: Use regular write_parquet with partition_cols - should resume
    ds2 = ray.data.read_parquet(str(input_path))
    ds2 = ds2.repartition(200)
    ds2 = ds2.map_batches(lambda b: add_run_tag(b, "second"), batch_size=None)
    ds2.write_parquet(str(output_path), partition_cols=partition_cols)

    result = ray.data.read_parquet(str(output_path)).to_pandas()

    assert len(result) == num_rows, f"Expected {num_rows} rows, got {len(result)}"

    # Check for duplicates
    assert result[ID_COL].is_unique, (
        f"Duplicate IDs found: "
        f"{sorted(result[result.duplicated(ID_COL, keep=False)][ID_COL].unique().tolist())}"
    )

    # Verify that some rows came from first run (before failure) and rest from second
    run_tag_counts = result["run_tag"].value_counts()
    assert "first" in run_tag_counts.index, "Expected some rows from first run"
    assert "second" in run_tag_counts.index, "Expected some rows from second run"

    # Verify partitioned output structure: partition subdirectories should exist
    output_subdirs = [
        d for d in os.listdir(str(output_path)) if os.path.isdir(output_path / d)
    ]
    assert len(output_subdirs) > 0, "Expected partition subdirectories in output"


class TextRowDatasink(RowBasedFileDatasink):
    """Test helper: RowBasedFileDatasink that writes each row as a text file."""

    def __init__(self, path: str, **kwargs):
        super().__init__(path, file_format="txt", **kwargs)

    def write_row_to_file(self, row, file):
        file.write(f"{row[ID_COL]},{row['value']}".encode())


class FailAfterWriteTextRowDatasink(TextRowDatasink):
    """Test helper: TextRowDatasink that fails AFTER writing data."""

    def __init__(self, path: str, fail_threshold: int = 100, **kwargs):
        super().__init__(path, **kwargs)
        self._fail_threshold = fail_threshold

    def write(self, blocks, ctx):
        blocks_list = list(blocks)

        should_fail = False
        for block in blocks_list:
            accessor = BlockAccessor.for_block(block)
            df = accessor.to_pandas()
            if ID_COL in df.columns and df[ID_COL].max() > self._fail_threshold:
                should_fail = True
                break

        result = super().write(iter(blocks_list), ctx)

        if should_fail:
            raise RuntimeError(
                f"Simulated failure: block contains {ID_COL} > {self._fail_threshold}"
            )

        return result


def test_partial_failure_no_duplicates_row_based(
    ray_start_10_cpus_shared,
    tmp_path,
):
    """Test checkpoint deduplication with row-based datasink (one file per row).

    Row-based datasinks write multiple files per task, e.g.:
      write_uuid_000000_000000_000000.txt
      write_uuid_000000_000000_000001.txt
    Recovery must match all of them via the checkpoint prefix (write_uuid_000000).
    This validates the PrefixTrie-based matching works for row-based writes.
    """
    num_rows = 1000
    fail_threshold = 100

    # Create paths
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    checkpoint_path_dir = tmp_path / "checkpoint"
    for path in [input_path, output_path, checkpoint_path_dir]:
        path.mkdir(exist_ok=True)

    # Create sample data
    df = pd.DataFrame(
        {ID_COL: range(num_rows), "value": [f"row_{i}" for i in range(num_rows)]}
    )
    df.to_parquet(input_path / "data.parquet", index=False)

    # Configure checkpointing
    ctx = DataContext.get_current()
    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=str(checkpoint_path_dir),
        delete_checkpoint_on_success=False,
    )

    def add_run_tag(batch, run_tag):
        batch["run_tag"] = [run_tag] * len(batch[ID_COL])
        return batch

    # Run 1: Use the failing row-based datasink
    with pytest.raises(RuntimeError, match="Simulated failure"):
        ds = ray.data.read_parquet(str(input_path))
        ds = ds.repartition(200)
        ds = ds.map_batches(lambda b: add_run_tag(b, "first"), batch_size=None)
        failing_datasink = FailAfterWriteTextRowDatasink(
            str(output_path), fail_threshold=fail_threshold
        )
        ds.write_datasink(failing_datasink, ray_remote_args={"max_retries": 0})

    ray.shutdown()
    ray.init()

    # Run 2: Use the same row-based datasink (non-failing) to resume
    ds2 = ray.data.read_parquet(str(input_path))
    ds2 = ds2.repartition(200)
    ds2 = ds2.map_batches(lambda b: add_run_tag(b, "second"), batch_size=None)
    ds2.write_datasink(TextRowDatasink(str(output_path)))

    # Read all output text files and parse them
    output_files = [f for f in os.listdir(str(output_path)) if f.endswith(".txt")]
    rows = []
    for fname in output_files:
        with open(os.path.join(str(output_path), fname)) as f:
            content = f.read()
            id_val, value = content.split(",", 1)
            rows.append({ID_COL: int(id_val), "value": value})

    result = pd.DataFrame(rows)

    assert len(result) == num_rows, f"Expected {num_rows} rows, got {len(result)}"

    # Check for duplicates
    assert result[ID_COL].is_unique, (
        f"Duplicate IDs found: "
        f"{sorted(result[result.duplicated(ID_COL, keep=False)][ID_COL].unique().tolist())}"
    )


def test_prefix_trie():
    """Test PrefixTrie insert and has_prefix_of operations."""
    trie = PrefixTrie()
    trie.insert("abc")
    trie.insert("def")
    trie.insert("ab")

    # "ab" is a prefix of "abc", "abcd", "ab"
    assert trie.has_prefix_of("abc")
    assert trie.has_prefix_of("abcd")
    assert trie.has_prefix_of("ab")
    assert trie.has_prefix_of("abxyz")
    assert trie.has_prefix_of("def")
    assert trie.has_prefix_of("defgh")

    # No prefix matches for these
    assert not trie.has_prefix_of("a")
    assert not trie.has_prefix_of("xyz")
    assert not trie.has_prefix_of("d")
    assert not trie.has_prefix_of("de")
    assert not trie.has_prefix_of("")


def test_prefix_trie_empty():
    """Test that an empty PrefixTrie returns False for all queries."""
    trie = PrefixTrie()
    assert not trie.has_prefix_of("")
    assert not trie.has_prefix_of("abc")
    assert not trie.has_prefix_of("anything")


@pytest.mark.parametrize(
    "fs,base_path",
    [
        (lazy_fixture("local_fs"), lazy_fixture("local_path")),
        (lazy_fixture("s3_fs"), lazy_fixture("s3_path")),
    ],
    ids=["local", "s3"],
)
def test_clean_pending_checkpoints_no_pending(ray_start_10_cpus_shared, fs, base_path):
    """Test that cleanup is a no-op when there are no pending checkpoint files.

    With no pending checkpoints, no data files should be deleted. This is the
    normal case after a successful write where all checkpoints were committed.
    """
    ctx = ray.data.DataContext.get_current()
    checkpoint_path = os.path.join(base_path, "checkpoint")
    fs.create_dir(_unwrap_protocol(checkpoint_path))

    ctx.checkpoint_config = CheckpointConfig(
        id_column=ID_COL,
        checkpoint_path=checkpoint_path,
        delete_checkpoint_on_success=False,
        override_filesystem=fs,
    )

    # Create data files but no pending checkpoints
    data_dir = os.path.join(base_path, "data")
    fs.create_dir(_unwrap_protocol(data_dir))
    data_file = os.path.join(_unwrap_protocol(data_dir), "some_data.parquet")
    with fs.open_output_stream(data_file) as f:
        f.write(b"dummy")

    checkpoint_manager = IdColumnCheckpointManager(ctx.checkpoint_config)
    checkpoint_manager._clean_pending_checkpoints(data_dir, fs)

    # Data file should still exist (no pending checkpoints means nothing to clean)
    assert fs.get_file_info(data_file).type != FileType.NotFound


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
