import gzip
import io
import re

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import csv
from pyarrow.fs import LocalFileSystem

import ray
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunker,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.planner.plan_list_files_op import plan_list_files_op
from ray.data.context import DataContext
from ray.data.datasource.partitioning import PathPartitionFilter


@pytest.fixture
def restore_ctx():
    context = DataContext.get_current()
    original = (
        context.use_datasource_v2,
        context.target_min_block_size,
        context.target_max_block_size,
        context.execution_options.preserve_order,
    )
    try:
        yield context
    finally:
        (
            context.use_datasource_v2,
            context.target_min_block_size,
            context.target_max_block_size,
            context.execution_options.preserve_order,
        ) = original


class _SmallLineDelimitedFileChunker(LineDelimitedFileChunker):
    _CHUNK_BYTE_SIZE = 8


def test_read_csv_builds_list_files_read_files_chain(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    (tmp_path / "data.csv").write_text("id,value\n1,a\n2,b\n")
    restore_ctx.use_datasource_v2 = True

    dataset = ray.data.read_csv(str(tmp_path), partitioning=None)

    assert isinstance(dataset._logical_plan.dag, ReadFiles)
    assert isinstance(dataset._logical_plan.dag.input_dependencies[0], ListFiles)
    assert dataset.schema() == pa.schema([("id", pa.int64()), ("value", pa.string())])


def test_read_csv_v2_default_listing_does_not_require_global_input(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(2):
        path = tmp_path / f"{index}.csv"
        path.write_text(f"id\n{index}\n")
        paths.append(str(path))
    restore_ctx.use_datasource_v2 = True

    dataset = ray.data.read_csv(paths, partitioning=None)
    list_files_op = dataset._logical_plan.dag.input_dependencies[0]

    assert isinstance(list_files_op, ListFiles)
    assert not list_files_op.file_partitioner.requires_global_input


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


def test_read_csv_v1_v2_preserve_late_file_columns(
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
    restore_ctx.execution_options.preserve_order = True

    rows_by_version = []
    schemas_by_version = []
    columns_by_version = []
    selected_rows_by_version = []
    dropped_rows_by_version = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        dataset = ray.data.read_csv(paths, partitioning=None)
        schemas_by_version.append(dataset.schema())
        columns_by_version.append(dataset.columns())
        rows_by_version.append(sorted(dataset.take_all(), key=lambda row: row["id"]))
        selected_rows_by_version.append(
            sorted(
                ray.data.read_csv(paths, partitioning=None)
                .select_columns(["id"])
                .take_all(),
                key=lambda row: row["id"],
            )
        )
        dropped_rows_by_version.append(
            ray.data.read_csv(paths, partitioning=None).drop_columns(["id"]).take_all()
        )

    assert schemas_by_version[0] == schemas_by_version[1]
    assert columns_by_version[0] == columns_by_version[1]
    assert rows_by_version[0] == rows_by_version[1]
    assert rows_by_version[1][-1] == {"id": 16, "extra": "preserved"}
    assert selected_rows_by_version[0] == selected_rows_by_version[1]
    assert dropped_rows_by_version[0] == dropped_rows_by_version[1]
    assert dropped_rows_by_version[1][-1] == {"extra": "preserved"}


def test_read_csv_v1_v2_sampled_heterogeneous_schema_parity(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    integer_path = tmp_path / "0.csv"
    integer_path.write_text("id,value\n0,1\n")
    promoted_path = tmp_path / "1.csv"
    promoted_path.write_text("id,value,extra\n1,1.5,preserved\n")
    paths = [str(integer_path), str(promoted_path)]
    restore_ctx.execution_options.preserve_order = True

    results = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        dataset = ray.data.read_csv(
            paths, partitioning=None, override_num_blocks=2
        ).materialize()
        results.append(
            {
                "rows": dataset.take_all(),
                "schema": dataset.schema(),
                "columns": dataset.columns(),
                "selected": ray.data.read_csv(
                    paths, partitioning=None, override_num_blocks=2
                )
                .select_columns(["id"])
                .take_all(),
                "dropped": ray.data.read_csv(
                    paths, partitioning=None, override_num_blocks=2
                )
                .drop_columns(["id"])
                .take_all(),
            }
        )

    assert results[0] == results[1]
    rows = results[1]["rows"]
    assert list(rows[0]) == ["id", "value"]
    assert list(rows[1]) == ["id", "value", "extra"]
    assert type(rows[0]["value"]) is int
    assert type(rows[1]["value"]) is float
    assert rows[1]["extra"] == "preserved"


def test_read_csv_v1_v2_heterogeneous_hive_partition_keys(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    year_directory = tmp_path / "year=2025"
    year_directory.mkdir()
    first_path = year_directory / "0.csv"
    first_path.write_text("id\n0\n")
    month_directory = tmp_path / "year=2026" / "month=09"
    month_directory.mkdir(parents=True)
    second_path = month_directory / "1.csv"
    second_path.write_text("id\n1\n")
    paths = [str(first_path), str(second_path)]
    restore_ctx.execution_options.preserve_order = True

    rows_by_version = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        rows_by_version.append(
            ray.data.read_csv(paths, override_num_blocks=2).take_all()
        )

    assert rows_by_version[0] == rows_by_version[1]
    assert rows_by_version[1] == [
        {"id": 0, "year": "2025"},
        {"id": 1, "year": "2026", "month": "09"},
    ]


def test_read_csv_v2_rejects_duplicate_late_file_columns(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(16):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"a\n{index}\n")
        paths.append(str(path))
    duplicate_path = tmp_path / "16.csv"
    duplicate_path.write_text("a,a\n1,2\n")
    paths.append(str(duplicate_path))
    restore_ctx.use_datasource_v2 = True

    with pytest.raises(ValueError, match="duplicate column names.*a"):
        ray.data.read_csv(paths, partitioning=None).materialize()


def test_read_csv_v1_v2_late_type_promotion_parity(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(16):
        path = tmp_path / f"{index:02d}.csv"
        path.write_text(f"value\n{index}\n")
        paths.append(str(path))
    late_path = tmp_path / "16.csv"
    late_path.write_text("value\n16.5\n")
    paths.append(str(late_path))

    rows_by_version = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        rows_by_version.append(ray.data.read_csv(paths, partitioning=None).take_all())

    normalized_rows = [sorted(row["value"] for row in rows) for rows in rows_by_version]
    assert normalized_rows[0] == normalized_rows[1]
    assert 16.5 in normalized_rows[1]

    def values_with_types(rows):
        ordered = sorted(rows, key=lambda row: row["value"])
        return [(row["value"], type(row["value"])) for row in ordered]

    assert values_with_types(rows_by_version[0]) == values_with_types(
        rows_by_version[1]
    )


def test_read_csv_v2_preserves_order_through_file_partitioning(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    for index in range(4):
        (tmp_path / f"{index}.csv").write_text(f"id\n{index}\n")
    restore_ctx.use_datasource_v2 = True
    restore_ctx.execution_options.preserve_order = True
    restore_ctx.target_min_block_size = 1
    restore_ctx.target_max_block_size = 10

    dataset = ray.data.read_csv(
        str(tmp_path), partitioning=None, override_num_blocks=2
    ).materialize()

    assert dataset.take_all() == [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]
    assert dataset.num_blocks() == 2
    assert re.search(r"ReadFilesCSVV2.*?2 tasks executed", dataset.stats(), re.DOTALL)


def test_read_csv_v2_override_num_blocks_with_explicit_paths(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(4):
        path = tmp_path / f"{index}.csv"
        path.write_text(f"id\n{index}\n")
        paths.append(str(path))
    restore_ctx.use_datasource_v2 = True
    restore_ctx.execution_options.preserve_order = True

    dataset = ray.data.read_csv(
        paths, partitioning=None, override_num_blocks=2
    ).materialize()

    assert dataset.take_all() == [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]
    assert dataset.num_blocks() == 2
    assert re.search(r"ReadFilesCSVV2.*?2 tasks executed", dataset.stats(), re.DOTALL)


@pytest.mark.parametrize("infer_compression", [False, True])
def test_read_csv_v1_v2_snappy_parity(
    infer_compression, ray_start_regular_shared, tmp_path, restore_ctx
):
    import snappy

    path = tmp_path / "data.csv.snappy"
    with path.open("wb") as file:
        snappy.stream_compress(io.BytesIO(b"id\n1\n"), file)
    kwargs = (
        {}
        if infer_compression
        else {"arrow_open_stream_args": {"compression": "snappy"}}
    )

    rows_by_version = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        rows_by_version.append(
            ray.data.read_csv(str(path), partitioning=None, **kwargs).take_all()
        )

    assert rows_by_version == [[{"id": 1}], [{"id": 1}]]


def test_read_csv_v2_gzip_executor_path(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv.gz"
    with gzip.open(path, "wt") as file:
        file.write("id\n1\n2\n")
    restore_ctx.use_datasource_v2 = True

    assert ray.data.read_csv(str(path), partitioning=None).take_all() == [
        {"id": 1},
        {"id": 2},
    ]


@pytest.mark.parametrize("use_local_scheme", [False, True])
def test_read_csv_v2_local_paths_and_stream_args(
    use_local_scheme, ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")
    restore_ctx.use_datasource_v2 = True
    read_path = f"local://{path}" if use_local_scheme else str(path)
    filesystem = None if use_local_scheme else LocalFileSystem()

    assert ray.data.read_csv(
        read_path,
        filesystem=filesystem,
        partitioning=None,
        arrow_open_stream_args={"buffer_size": 1},
    ).take_all() == [{"id": 1}]


def test_read_csv_v2_subtree_filesystem(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    from pyarrow.fs import SubTreeFileSystem

    path = tmp_path / "data.csv"
    path.write_text("id\n1\n")
    restore_ctx.use_datasource_v2 = True
    filesystem = SubTreeFileSystem(str(tmp_path), LocalFileSystem())

    assert ray.data.read_csv(
        "data.csv", filesystem=filesystem, partitioning=None
    ).take_all() == [{"id": 1}]


def test_read_csv_v1_v2_seeded_file_shuffle(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    from ray.data.datasource.file_based_datasource import FileShuffleConfig

    paths = []
    for index in range(8):
        path = tmp_path / f"{index}.csv"
        path.write_text(f"id\n{index}\n")
        paths.append(str(path))
    paths = [paths[index] for index in (2, 0, 7, 1, 6, 3, 5, 4)]
    restore_ctx.execution_options.preserve_order = True
    shuffle = FileShuffleConfig(seed=1234, reseed_after_execution=False)

    def read_rows():
        return ray.data.read_csv(
            paths, partitioning=None, shuffle=shuffle, override_num_blocks=8
        ).take_all()

    restore_ctx.use_datasource_v2 = False
    v1_rows = read_rows()
    restore_ctx.use_datasource_v2 = True
    v2_rows = read_rows()

    # V2 currently permutes the path-sorted listing while V1 permutes the
    # caller's order; exact order parity is a separate listing-level change.
    # Both must be deterministic for a fixed seed and read every file once.
    assert v2_rows == read_rows()
    assert sorted(row["id"] for row in v2_rows) == list(range(8))
    assert sorted(v1_rows, key=lambda row: row["id"]) == sorted(
        v2_rows, key=lambda row: row["id"]
    )


def test_read_csv_v2_invalid_row_handler_falls_back_to_v1(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n1,valid\n2,too,many\n")
    calls_path = tmp_path / "handler-calls.txt"

    def skip_invalid_row(_row):
        with calls_path.open("a") as file:
            file.write("called\n")
        return "skip"

    restore_ctx.use_datasource_v2 = True
    dataset = ray.data.read_csv(
        str(path),
        partitioning=None,
        parse_options=csv.ParseOptions(invalid_row_handler=skip_invalid_row),
    )

    assert not isinstance(dataset._logical_plan.dag, ReadFiles)
    assert dataset.take_all() == [{"id": 1, "value": "valid"}]
    assert calls_path.read_text().splitlines() == ["called"]


def test_read_csv_v1_v2_header_only_file(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / "data.csv"
    path.write_text("id,value\n")
    rows_by_version = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        rows_by_version.append(
            ray.data.read_csv(str(path), partitioning=None).take_all()
        )

    assert rows_by_version == [[], []]


def test_read_csv_v2_partition_filter(ray_start_regular_shared, tmp_path, restore_ctx):
    data_root = tmp_path / "data"
    data_root.mkdir()
    for year in (2025, 2026):
        directory = data_root / f"year={year}"
        directory.mkdir()
        (directory / "data.csv").write_text(f"id\n{year}\n")
    restore_ctx.use_datasource_v2 = True
    calls_path = tmp_path / "partition-filter-calls.txt"

    def keep_2026(partitions):
        with calls_path.open("a") as file:
            file.write(f"{partitions.get('year')}\n")
        return partitions.get("year") == "2026"

    partition_filter = PathPartitionFilter.of(
        keep_2026,
        base_dir=str(data_root),
    )

    assert ray.data.read_csv(
        str(data_root), partition_filter=partition_filter
    ).take_all() == [{"id": 2026, "year": "2026"}]
    assert sorted(calls_path.read_text().splitlines()) == ["2025", "2026"]


def test_read_csv_v2_downstream_actor_pool(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    from ray.data import ActorPoolStrategy

    path = tmp_path / "data.csv"
    path.write_text("id\n1\n2\n")
    restore_ctx.use_datasource_v2 = True

    rows = (
        ray.data.read_csv(str(path), partitioning=None)
        .map_batches(lambda batch: batch, compute=ActorPoolStrategy(size=1))
        .take_all()
    )

    assert rows == [{"id": 1}, {"id": 2}]


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


def test_read_csv_v1_v2_override_num_blocks_split_factor_parity(
    ray_start_regular_shared, tmp_path, restore_ctx
):
    paths = []
    for index in range(3):
        path = tmp_path / f"{index}.csv"
        path.write_text(f"id\n{index}\n{index + 10}\n")
        paths.append(str(path))
    restore_ctx.execution_options.preserve_order = True

    results = []
    for use_datasource_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_datasource_v2
        dataset = ray.data.read_csv(
            paths, partitioning=None, override_num_blocks=4
        ).materialize()
        results.append((dataset.num_blocks(), dataset.take_all()))

    # Three read units can't produce exactly four blocks, so each read unit's
    # output is split by ceil(4 / 3) == 2, as V1 does: six blocks in total.
    assert results[0] == results[1]
    assert results[1][0] == 6
    assert [row["id"] for row in results[1][1]] == [0, 10, 1, 11, 2, 12]


@pytest.mark.parametrize("compressed", [False, True])
def test_single_csv_override_produces_requested_blocks(
    compressed, ray_start_regular_shared, tmp_path, restore_ctx
):
    path = tmp_path / ("data.csv.gz" if compressed else "data.csv")
    payload = "id\n" + "".join(f"{index}\n" for index in range(100))
    if compressed:
        with gzip.open(path, "wt") as file:
            file.write(payload)
    else:
        path.write_text(payload)
    for use_v2 in (False, True):
        restore_ctx.use_datasource_v2 = use_v2
        dataset = ray.data.read_csv(
            str(path), partitioning=None, override_num_blocks=4
        ).materialize()
        assert dataset.num_blocks() == 4
        assert sorted(row["id"] for row in dataset.take_all()) == list(range(100))


@pytest.mark.parametrize("actor_pool", [False, True])
def test_csv_chunks_resume_after_downstream_blocked(
    actor_pool, ray_start_regular_shared, tmp_path, restore_ctx
):
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    from ray._private.test_utils import wait_for_condition
    from ray.data import ActorPoolStrategy, TaskPoolStrategy
    from ray.data._internal.datasource_v2.csv_datasource_v2 import CSVDatasourceV2
    from ray.data.read_api import _read_datasource_v2

    @ray.remote(num_cpus=0)
    class Gate:
        def __init__(self):
            self.event = asyncio.Event()
            self.entered = False

        async def wait(self):
            self.entered = True
            await self.event.wait()

        def has_entered(self):
            return self.entered

        def release(self):
            self.event.set()

    path = tmp_path / "data.csv"
    path.write_text("id\n" + "".join(f"{index}\n" for index in range(128)))
    restore_ctx.target_max_block_size = 64
    gate = Gate.remote()

    def consume(batch):
        ray.get(gate.wait.remote())
        return batch

    datasource = CSVDatasourceV2(
        [str(path)], file_chunker=_SmallLineDelimitedFileChunker()
    )
    strategy = ActorPoolStrategy(size=1) if actor_pool else TaskPoolStrategy(size=1)
    dataset = _read_datasource_v2(datasource).map_batches(
        consume, batch_size=8, compute=strategy
    )
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(dataset.take_all)
            try:
                wait_for_condition(lambda: ray.get(gate.has_entered.remote()))
                assert not result.done()
            finally:
                ray.get(gate.release.remote())
            rows = result.result(timeout=60)
        assert sorted(row["id"] for row in rows) == list(range(128))
    finally:
        ray.kill(gate)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
