import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import csv

import ray
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunker,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.planner.plan_list_files_op import plan_list_files_op
from ray.data.context import DataContext


@pytest.fixture
def restore_ctx():
    context = DataContext.get_current()
    original = (
        context.use_datasource_v2,
        context.target_min_block_size,
        context.target_max_block_size,
    )
    try:
        yield context
    finally:
        (
            context.use_datasource_v2,
            context.target_min_block_size,
            context.target_max_block_size,
        ) = original


class _SmallLineDelimitedFileChunker(LineDelimitedFileChunker):
    _CHUNK_BYTE_SIZE = 8


def test_read_csv_builds_list_files_read_files_chain(tmp_path, restore_ctx):
    (tmp_path / "data.csv").write_text("id,value\n1,a\n2,b\n")
    restore_ctx.use_datasource_v2 = True

    dataset = ray.data.read_csv(str(tmp_path), partitioning=None)

    assert isinstance(dataset._logical_plan.dag, ReadFiles)
    assert isinstance(dataset._logical_plan.dag.input_dependencies[0], ListFiles)
    assert dataset.schema() == pa.schema([("id", pa.int64()), ("value", pa.string())])


def test_read_csv_v2_options_and_derived_columns(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    directory = tmp_path / "year=2026"
    directory.mkdir()
    path = directory / "data.csv"
    path.write_text("1|a\n2|b\n")
    restore_ctx.use_datasource_v2 = True

    rows = ray.data.read_csv(
        str(tmp_path),
        include_paths=True,
        read_options=csv.ReadOptions(column_names=["id", "value"]),
        parse_options=csv.ParseOptions(delimiter="|"),
    ).take_all()

    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 1, "value": "a", "year": "2026", "path": str(path)},
        {"id": 2, "value": "b", "year": "2026", "path": str(path)},
    ]


def test_read_csv_v2_invalid_format(ray_start_regular_shared, tmp_path, restore_ctx):
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"value": [1, 2]}), path)
    restore_ctx.use_datasource_v2 = True

    with pytest.raises(ValueError, match="Failed to read CSV file"):
        ray.data.read_csv(str(path), partitioning=None).materialize()


@pytest.mark.parametrize(
    "option_name", ["read_options", "parse_options", "convert_options"]
)
def test_read_csv_v1_v2_explicit_none_parity(
    option_name, ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,a\n2,b\n")
    rows_by_version = []

    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        rows_by_version.append(
            ray.data.read_csv(
                str(path), partitioning=None, **{option_name: None}
            ).take_all()
        )

    assert rows_by_version[0] == rows_by_version[1]


def test_read_csv_v2_executor_chunking_and_stats(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    from ray.data._internal.datasource_v2.csv_datasource_v2 import CSVDatasourceV2
    from ray.data.read_api import _read_datasource_v2

    path = tmp_path / "data.csv"
    path.write_text('id,value\n1,"a,b"\r\n2,c\r\n3,d\r\n4,e\r\n')
    restore_ctx.use_datasource_v2 = True
    restore_ctx.target_min_block_size = 1
    restore_ctx.target_max_block_size = 16
    datasource = CSVDatasourceV2(
        [str(path)],
        partitioning=None,
        file_chunker=_SmallLineDelimitedFileChunker(),
    )

    dataset = _read_datasource_v2(datasource, parallelism=4).materialize()

    assert sorted(dataset.take_all(), key=lambda row: row["id"]) == [
        {"id": 1, "value": "a,b"},
        {"id": 2, "value": "c"},
        {"id": 3, "value": "d"},
        {"id": 4, "value": "e"},
    ]
    stats = dataset.stats()
    assert "ListFiles" in stats
    assert "ReadFilesCSVV2" in stats
    assert re.search(r"ReadFilesCSVV2.*?4 tasks executed", stats, re.DOTALL), stats


def test_read_csv_v2_rejects_late_file_columns(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(16):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"id\n{index}\n")
        paths.append(str(path))
    late_path = tmp_path / "16.csv"
    late_path.write_text("id,extra\n16,preserved\n")
    paths.append(str(late_path))

    restore_ctx.use_datasource_v2 = False
    v1_rows = ray.data.read_csv(paths, partitioning=None).take_all()
    assert next(row for row in v1_rows if row["id"] == 16)["extra"] == "preserved"

    # DataSourceV2 intentionally fails closed instead of returning blocks whose
    # columns aren't represented by the scanner's logical schema.
    restore_ctx.use_datasource_v2 = True
    dataset = ray.data.read_csv(paths, partitioning=None)

    assert dataset.schema() == pa.schema([("id", pa.int64())])
    with pytest.raises(ValueError, match="not present in the sampled schema.*extra"):
        dataset.materialize()


def test_read_csv_v2_propagates_named_remote_args(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")
    restore_ctx.use_datasource_v2 = True

    dataset = ray.data.read_csv(
        str(path),
        partitioning=None,
        label_selector={"ray.io/test-label": "value"},
        fallback_strategy=[{"ray.io/fallback-label": "value"}],
        max_calls=7,
        resources={"custom_csv_resource": 0.25},
        accelerator_type="test-accelerator",
        runtime_env={"env_vars": {"CSV_TEST": "1"}},
    )
    read_op = dataset._logical_plan.dag
    list_files_op = read_op.input_dependencies[0]

    assert read_op.ray_remote_args["label_selector"] == {"ray.io/test-label": "value"}
    assert read_op.ray_remote_args["fallback_strategy"] == [
        {"ray.io/fallback-label": "value"}
    ]
    assert read_op.ray_remote_args["max_calls"] == 7
    assert read_op.ray_remote_args["resources"] == {"custom_csv_resource": 0.25}
    assert read_op.ray_remote_args["accelerator_type"] == "test-accelerator"
    assert read_op.ray_remote_args["runtime_env"] == {"env_vars": {"CSV_TEST": "1"}}

    expected_listing_args = {
        "label_selector": {"ray.io/test-label": "value"},
        "fallback_strategy": [{"ray.io/fallback-label": "value"}],
        "runtime_env": {"env_vars": {"CSV_TEST": "1"}},
    }
    assert list_files_op.ray_remote_args == expected_listing_args

    physical_list_files_op = plan_list_files_op(
        list_files_op, [], DataContext.get_current()
    )
    for key, value in expected_listing_args.items():
        assert physical_list_files_op._ray_remote_args[key] == value
    assert "_generator_backpressure_num_objects" not in (
        physical_list_files_op._ray_remote_args
    )


def test_read_csv_v2_file_selection_and_override_num_blocks(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    (tmp_path / "data.csv").write_text("id\n1\n")
    (tmp_path / "ignored.txt").write_text("id\n2\n")
    missing_path = tmp_path / "missing.csv"
    restore_ctx.use_datasource_v2 = True

    dataset = ray.data.read_csv(
        [str(tmp_path), str(missing_path)],
        partitioning=None,
        file_extensions=["csv"],
        ignore_missing_paths=True,
        override_num_blocks=1,
    ).materialize()

    assert dataset.take_all() == [{"id": 1}]
    assert dataset.num_blocks() == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
