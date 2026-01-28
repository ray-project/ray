import csv
import os
import random
from typing import List, Union

import pandas as pd
import pyarrow
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileSelector, LocalFileSystem
from pytest_lazy_fixtures import lf as lazy_fixture

import ray
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators import Read, Write
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.block import BlockAccessor
from ray.data.checkpoint.checkpoint_filter import (
    BatchBasedCheckpointFilter,
)
from ray.data.checkpoint.checkpoint_writer import (
    BatchBasedCheckpointWriter,
)
from ray.data.checkpoint.interfaces import (
    CheckpointBackend,
    CheckpointConfig,
    InvalidCheckpointingConfig,
)
from ray.data.context import DataContext
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
    def _generate():
        # Generate a dummy dataset with SAMPLE_DATA_NUM_ROWS rows and columns [ID_COL, "col1"]
        data = [
            {ID_COL: i, "col1": random.random()} for i in range(SAMPLE_DATA_NUM_ROWS)
        ]

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
    write_op = Write(read_op, ParquetDatasink(write_path))
    logical_plan = LogicalPlan(write_op, ctx)
    physical_plan = get_execution_plan(logical_plan)
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
def test_checkpoint(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    backend,
    fs,
    data_path,
    data_output_path,
):
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

    ds = ray.data.read_csv(csv_file)

    # Execute the dataset with checkpointing enabled.
    ds = ds.map_batches(TestActor, concurrency=1)
    ds.write_parquet(data_output_path, filesystem=fs)

    # Ensure that the written data is correct.
    ds_readback = ray.data.read_parquet(data_output_path, filesystem=fs)
    actual_output = sorted([row[ID_COL] for row in ds_readback.iter_rows()])
    expected_output = sorted([row[ID_COL] for row in ds.iter_rows()])
    assert actual_output == expected_output

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
def test_recovery_skips_checkpointed_rows(
    ray_start_10_cpus_shared,
    ds_factory,
    backend,
    fs,
    data_path,
    data_output_path,
):
    """Tests that for a Dataset which fails partway and is recovered,
    it skips rows which have already been checkpointed."""

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

    # For existing id column, expect integer IDs
    actual_output = sorted([row[id_col] for row in ds_readback.iter_rows()])
    expected_output = sorted(range(max_num_items))
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
    """Test BatchBasedCheckpointFilter.filter_rows_for_block."""

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

    expected_block = pyarrow.table(
        {
            ID_COL: [0, 3, 5, 7],
            "data": ["0", "3", "5", "7"],
        }
    )

    filter_instance = BatchBasedCheckpointFilter(config)
    filtered_block = filter_instance.filter_rows_for_block(
        block=block,
        checkpointed_ids=checkpointed_ids,
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
