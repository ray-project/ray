import copy
import csv
import os
import random
import time
from typing import List

import pandas as pd
import pytest
from pyarrow.fs import FileSelector
from pytest_lazyfixture import lazy_fixture

import ray
from ray.anyscale.data._internal.execution.rules.insert_checkpointing import (
    InsertCheckpointingLayerRule,
)
from ray.anyscale.data.checkpoint.interfaces import (
    CheckpointBackend,
    CheckpointConfig,
    InvalidCheckpointingConfig,
    InvalidCheckpointingOperators,
)
from ray.data._internal.datasource.csv_datasource import CSVDatasource
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import Sort
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.optimizers import LogicalOptimizer, get_execution_plan
from ray.data._internal.planner.planner import Planner
from ray.data.datasource.path_util import _unwrap_protocol
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture
def generate_sample_data_csv(tmp_path):
    # Generate a dummy dataset with 5 rows and columns ["id", "col1"]
    data = [{"id": i, "col1": random.random()} for i in range(5)]
    f_path = f"{tmp_path}/sample_data.csv"
    with open(f_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    yield f_path

    # Remove the created sample files.
    os.remove(f_path)


@pytest.fixture
def generate_sample_data_parquet(tmp_path):
    # Generate a dummy dataset with 5 rows and columns ["id", "col1"]
    f_dir = f"{tmp_path}/sample_data_parquet/"
    os.makedirs(f_dir, exist_ok=True)

    df = pd.DataFrame([{"id": i, "col1": random.random()} for i in range(5)])
    f_path = f"{f_dir}/sample_data.parquet"
    df.to_parquet(f_path)
    yield f_dir

    # Remove the created sample files.
    os.remove(f_path)


@pytest.fixture
def generate_sample_physical_plan(generate_sample_data_csv, tmp_path):
    ctx = ray.data.DataContext.get_current()

    datasource = CSVDatasource(generate_sample_data_csv)

    read_op = Read(datasource, datasource, -1, None)
    write_op = Write(read_op, ParquetDatasink(f"{tmp_path}/output"))
    logical_plan = LogicalPlan(write_op, ctx)
    physical_plan = get_execution_plan(logical_plan)
    yield physical_plan


def generate_ckpt_config(backend, output_path, id_col, fs):
    return CheckpointConfig(
        enabled=True,
        backend=backend,
        output_path=output_path,
        id_col=id_col,
        fs=fs,
    )


def get_checkpoint_files(ckpt_path) -> List[str]:
    try:
        return [f"{ckpt_path}/{fname}" for fname in os.listdir(ckpt_path)]
    except FileNotFoundError:
        return []


class TestInsertCheckpointingLayerRule:
    """Unit tests for `InsertCheckpointingLayerRule`."""

    def test_invalid_config(
        self, ray_start_10_cpus_shared, generate_sample_physical_plan, tmp_path
    ):
        ctx = ray.data.DataContext.get_current()
        physical_plan = generate_sample_physical_plan
        physical_plan._context = ctx
        ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
        base_checkpoint_config = generate_ckpt_config(
            CheckpointBackend.DISK, ckpt_path, "id", None
        )

        # Backend must be configured.
        no_backend_config = copy.copy(base_checkpoint_config)
        no_backend_config.backend = None
        ctx.checkpoint_config = no_backend_config
        with pytest.raises(InvalidCheckpointingConfig):
            InsertCheckpointingLayerRule().apply(physical_plan)

        # id column must be configured.
        no_id_config = copy.copy(base_checkpoint_config)
        no_id_config.id_col = None
        ctx.checkpoint_config = no_id_config
        with pytest.raises(
            InvalidCheckpointingConfig,
            match="Checkpoint ID column is required",
        ):
            InsertCheckpointingLayerRule().apply(physical_plan)

    def test_disallowed_op(
        self, ray_start_10_cpus_shared, generate_sample_data_csv, tmp_path
    ):
        ctx = ray.data.DataContext.get_current()

        # Generate a sample PhysicalPlan.
        datasource = CSVDatasource(generate_sample_data_csv)
        read_op = Read(datasource, datasource, -1, None)

        # Sort op is not allowed for checkpointing.
        sort_op = Sort(read_op, "id")
        write_op = Write(sort_op, ParquetDatasink(f"{tmp_path}/output"))
        logical_plan = LogicalPlan(write_op, ctx)
        optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
        physical_plan = Planner().plan(optimized_logical_plan)

        ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
        ctx.checkpoint_config = generate_ckpt_config(
            CheckpointBackend.DISK, ckpt_path, "id", None
        )

        with pytest.raises(InvalidCheckpointingOperators, match="Sort"):
            InsertCheckpointingLayerRule().apply(physical_plan)

    def test__insert_read_filter_checkpoint(
        self,
        ray_start_10_cpus_shared,
        generate_sample_physical_plan,
        tmp_path,
    ):
        rule = InsertCheckpointingLayerRule()
        new_plan = rule._insert_read_filter_checkpoint(generate_sample_physical_plan)

        # Check that a MapTransform was inserted with
        # filter_checkpointed_rows_for_blocks.
        read_op = new_plan.dag.input_dependency
        new_transform_fns = read_op._map_transformer._transform_fns
        assert any(
            "filter_checkpointed_rows_for_blocks" in str(fn) for fn in new_transform_fns
        )

    def test__insert_write_checkpoint(
        self,
        ray_start_10_cpus_shared,
        generate_sample_physical_plan,
        tmp_path,
    ):
        ctx = ray.data.DataContext.get_current()
        ctx.checkpoint_config = generate_ckpt_config(
            CheckpointBackend.DISK,
            f"{tmp_path}/ckpt",
            "id",
            None,
        )

        physical_plan = generate_sample_physical_plan
        physical_plan._context = ctx

        rule = InsertCheckpointingLayerRule()
        new_plan = rule._insert_write_checkpoint(physical_plan, ctx.checkpoint_config)

        # Check that a MapTransform was inserted with
        # write_checkpoint_for_block.
        new_transform_fns = new_plan.dag._map_transformer._transform_fns
        assert any("write_checkpoint_for_block" in str(fn) for fn in new_transform_fns)


@pytest.mark.parametrize("read_code_path", ["runtime", "oss_fallback"])
@pytest.mark.parametrize(
    "backend,fs,data_path,endpoint_url",
    [
        (CheckpointBackend.DISK, None, lazy_fixture("local_path"), None),
        (
            CheckpointBackend.DISK,
            lazy_fixture("local_fs"),
            lazy_fixture("local_path"),
            None,
        ),
        (
            CheckpointBackend.S3,
            lazy_fixture("s3_fs"),
            lazy_fixture("s3_path"),
            lazy_fixture("s3_server"),
        ),
    ],
)
def test_checkpoint(
    ray_start_10_cpus_shared,
    generate_sample_data_csv,
    read_code_path,
    backend,
    fs,
    data_path,
    endpoint_url,
):
    class TestActor:
        def __init__(self):
            pass

        def __call__(self, batch):
            return batch

    ID_COL = "id"

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(data_path, "test_checkpoint_output_files")
    ctx.checkpoint_config = generate_ckpt_config(backend, ckpt_path, ID_COL, fs)

    if read_code_path == "runtime":
        ds = ray.data.read_csv(generate_sample_data_csv)
    elif read_code_path == "oss_fallback":
        # Pass `override_num_blocks` arg to trigger fallback
        # to OSS read_api code path.
        ds = ray.data.read_csv(generate_sample_data_csv, override_num_blocks=5)
    else:
        raise Exception(f"Invalid `read_code_path`: {read_code_path}")

    ds = ds.map_batches(TestActor, concurrency=1)
    data_output_path = f"{data_path}/output/"
    ds.write_parquet(data_output_path, filesystem=fs)

    # Generate the list of expected files from the ID column.
    # Disable checkpointing prior to reading back the data, so we don't skip any rows.
    ctx.checkpoint_config.enabled = False
    ds = ray.data.read_parquet(data_output_path, filesystem=fs)
    expected_checkpoint_file_paths = []
    for row in ds.iter_rows():
        ckpt_file = _unwrap_protocol(f"{ckpt_path}/{row.get(ID_COL)}.jsonl")
        expected_checkpoint_file_paths.append(ckpt_file)

    # Get the list of actual checkpoint files in the checkpoint output path.
    if fs is None:
        actual_checkpoint_file_paths = [
            f"{ckpt_path}/{fname}" for fname in os.listdir(ckpt_path)
        ]
    else:
        files = fs.get_file_info(FileSelector(_unwrap_protocol(ckpt_path)))
        actual_checkpoint_file_paths = []
        for file_info in files:
            if file_info.is_file:
                actual_checkpoint_file_paths.append(file_info.path)

    expected_checkpoint_file_paths = set(expected_checkpoint_file_paths)
    actual_checkpoint_file_paths = set(actual_checkpoint_file_paths)

    # Check all expected checkpoint files have been created.
    assert expected_checkpoint_file_paths == actual_checkpoint_file_paths, (
        f"Missing "
        f"{len(expected_checkpoint_file_paths) - len(actual_checkpoint_file_paths)} "
        f"checkpoint files: "
        f"{expected_checkpoint_file_paths - actual_checkpoint_file_paths}"
    )


def test_full_dataset_executed_for_non_write(
    ray_start_10_cpus_shared,
    tmp_path,
    generate_sample_data_parquet,
):
    """Tests that for an already fully checkpointed Dataset,
    calling `schema()` and `count()` should not skip checkpointing
    and should execute the full Dataset to get the correct information.
    """
    ID_COL = "id"

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
    ctx.checkpoint_config = generate_ckpt_config(
        CheckpointBackend.DISK, ckpt_path, ID_COL, None
    )

    ds = ray.data.read_parquet(generate_sample_data_parquet)
    ds = ds.map(lambda row: row)

    # Get the schema and count prior to writing the dataset.
    schema_before_write = ds.schema()
    count_before_write = ds.count()

    data_output_path = f"{tmp_path}/output/"
    ds.write_parquet(data_output_path)

    # Recreate the same dataset, so that it will skip checkpointed rows.
    ds2 = ray.data.read_parquet(generate_sample_data_parquet)
    ds2 = ds2.map(lambda row: row)

    # Check that when re-running a dataset which has already been completely
    # checkpointed, it does not skip any rows during `schema()` and `count()` calls.
    assert ds2.schema() == schema_before_write
    assert ds2.count() == count_before_write


def test_recovery_skips_checkpointed_rows(
    ray_start_10_cpus_shared,
    tmp_path,
    generate_sample_data_parquet,
):
    """Tests that for a Dataset which fails partway and is recovered,
    it skips rows which have already been checkpointed."""

    ID_COL = "id"
    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
    ctx.checkpoint_config = generate_ckpt_config(
        CheckpointBackend.DISK, ckpt_path, ID_COL, None
    )
    # Catch the custom TestException raised by FailActor.
    ctx.raise_original_map_exception = True

    class TestException(Exception):
        pass

    class FailActor:
        """Simple passthrough actor, which fails after a certain number of rows."""

        def __init__(self, max_before_fail):
            self.max_before_fail = max_before_fail
            self.count = 0

        def __call__(self, row):
            if self.count > self.max_before_fail:
                raise TestException(f"FailActor: Failing on row {row['id']}")
            time.sleep(1)
            self.count += 1
            return row

    n = 10
    max_before_fail = 6
    ds = ray.data.range(n, concurrency=1)
    ds = ds.map(FailActor, fn_constructor_args=[max_before_fail], concurrency=1)

    data_output_path = f"{tmp_path}/output/"
    # Fails after writing first `max_before_fail` rows.
    with pytest.raises(TestException):
        ds.write_parquet(data_output_path)

    assert len(get_checkpoint_files(ckpt_path)) == max_before_fail

    # When executing the same dataset again, this should skip the already written
    # `max_before_fail` rows, and continue from row `max_before_fail+1` until the
    # end of the dataset. The limit of `max_before_fail` is not reached,
    # since there are only three remaining rows.
    ds.write_parquet(data_output_path)

    assert len(get_checkpoint_files(ckpt_path)) == n


def test_skip_checkpoint_flag(
    ray_start_10_cpus_shared, generate_sample_data_csv, tmp_path
):
    """Test that for a valid Dataset with checkpointing enabled, calling methods like
    `schema()` and `count()` should skip checkpointing and not create any checkpoint
    files. Subsequently calling `write_xxx()` on the same dataset should have
    checkpointing enabled."""

    ctx = ray.data.DataContext.get_current()
    ckpt_path = os.path.join(tmp_path, "test_checkpoint_output_files")
    ctx.checkpoint_config = generate_ckpt_config(
        CheckpointBackend.DISK, ckpt_path, "id", None
    )

    def generate_ds():
        ds = ray.data.read_csv(generate_sample_data_csv)
        ds = ds.map(lambda row: row)
        return ds

    # Calling `ds.schema()` should skip checkpointing.
    ds = generate_ds()
    assert ds.schema() is not None
    assert ds._plan._context._skip_checkpoint_temp
    assert len(get_checkpoint_files(ckpt_path)) == 0

    # Calling `ds.count()` should skip checkpointing.
    ds = generate_ds()
    assert ds.count() is not None
    assert ds._plan._context._skip_checkpoint_temp
    assert len(get_checkpoint_files(ckpt_path)) == 0

    # Calling `ds.write_xxx()` afterwards should enable checkpointing.
    ds.write_parquet(f"{tmp_path}/output")
    assert len(get_checkpoint_files(ckpt_path)) == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
