"""Public API semantics for additive mixed partition/data pruning hints."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


@pytest.mark.parametrize("operation", ["count", "projection", "limit"])
def test_mixed_or_preserves_residual_with_downstream_operations(
    tmp_path, operation, restore_data_context
):
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.use_datasource_v2 = True
        ctx.execution_options.preserve_order = True
        paths = []
        expected = []
        for year, offset in [("2020", 0), ("2021", 100), ("2023", 200)]:
            folder = tmp_path / f"year={year}"
            folder.mkdir()
            path = folder / "part.parquet"
            rows = [{"id": offset + i, "payload": f"row-{offset+i}"} for i in range(20)]
            pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=5)
            paths.append(str(path))
            expected.extend(
                dict(row, year=year) for row in rows if year == "2023" or row["id"] < 3
            )
        ds = ray.data.read_parquet(
            paths,
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
            schema=pa.schema(
                [("id", pa.int64()), ("payload", pa.string()), ("year", pa.string())]
            ),
            override_num_blocks=2,
        ).filter(expr=(col("year") == "2023") | (col("id") < 3))
        if operation == "count":
            assert ds.count() == len(expected)
        elif operation == "projection":
            assert ds.select_columns(["payload"]).take_all() == [
                {"payload": row["payload"]} for row in expected
            ]
        else:
            assert ds.limit(7).take_all() == expected[:7]
    finally:
        ray.shutdown()


@pytest.mark.parametrize(
    "dtype,value,literal,raises",
    [
        (pa.int64(), "2023", 2**80, True),
        (pa.int64(), str(2**63 - 1), 2**63, True),
        (pa.int64(), str(-(2**63)), -(2**63) - 1, True),
        (pa.string(), "2023", "\ud800", True),
        (pa.int8(), "127", 128, False),
        (pa.int64(), str(2**63 - 1), 2**63 - 1, False),
        (pa.int64(), str(-(2**63)), -(2**63), False),
    ],
)
def test_mixed_or_partition_literal_boundaries(
    tmp_path, dtype, value, literal, raises, restore_data_context
):
    import pyarrow.compute as pc

    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ray.data.DataContext.get_current().use_datasource_v2 = True
        folder = tmp_path / f"year={value}"
        folder.mkdir()
        path = folder / "part.parquet"
        pq.write_table(pa.table({"id": [10, 11, 12, 13]}), path, row_group_size=2)
        schema = pa.schema([("id", pa.int64()), ("year", dtype)])
        ds = ray.data.read_parquet(
            [str(path)],
            schema=schema,
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
            override_num_blocks=1,
        ).filter(expr=(col("year") == literal) | (col("id") < 0))
        year = pa.array([value] * 4).cast(dtype)
        if raises:
            # Match the direct Arrow operation's error type across Arrow versions.
            # Pruning every row group must not turn this into an empty success.
            with pytest.raises(
                (TypeError, ValueError, OverflowError, pa.ArrowException)
            ) as expected:
                pc.equal(year, literal)
            with pytest.raises(type(expected.value)):
                ds.take_all()
        else:
            expected_rows = (
                pa.table({"id": [10, 11, 12, 13], "year": year})
                .filter(pc.equal(year, literal))
                .to_pylist()
            )
            assert ds.take_all() == expected_rows
    finally:
        ray.shutdown()


@pytest.mark.parametrize(
    "kind",
    [
        "payload_bad_int",
        "payload_valid_int",
        "payload_bad_utf8",
        "payload_projection",
        "other_partition_bad_int",
        "other_partition_valid_int",
    ],
)
def test_mixed_or_preserves_other_field_conversions(
    tmp_path, kind, restore_data_context
):
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ray.data.DataContext.get_current().use_datasource_v2 = True
        folder = tmp_path / "year=2020"
        names = ["year"]
        fields = [("id", pa.int64()), ("year", pa.string())]
        table = pa.table({"id": [10, 11, 12, 13]})
        if kind.startswith("other_partition"):
            folder = folder / ("month=abc" if kind.endswith("bad_int") else "month=7")
            names.append("month")
            fields.append(("month", pa.int64()))
        else:
            values = (
                [b"\xff"] * 4
                if kind.endswith("bad_utf8")
                else (["123"] * 4 if kind.endswith("valid_int") else ["abc"] * 4)
            )
            table = table.append_column("payload", pa.array(values))
            fields.append(
                ("payload", pa.string() if kind.endswith("bad_utf8") else pa.int64())
            )
        folder.mkdir(parents=True)
        path = folder / "part.parquet"
        pq.write_table(table, path, row_group_size=2)
        ds = ray.data.read_parquet(
            [str(path)],
            schema=pa.schema(fields),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=names
            ),
            override_num_blocks=1,
        ).filter(expr=(col("year") == "2023") | (col("id") < 0))
        if kind == "payload_projection":
            ds = ds.select_columns(["id"])
        if "bad" in kind or kind == "payload_projection":
            with pytest.raises(pa.ArrowInvalid):
                ds.take_all()
        else:
            assert ds.take_all() == []
    finally:
        ray.shutdown()


@pytest.mark.parametrize("limit_first", [True, False])
def test_mixed_or_respects_limit_order(tmp_path, limit_first, restore_data_context):
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.use_datasource_v2 = True
        ctx.execution_options.preserve_order = True
        paths = []
        for year, start in [("2020", 10), ("2023", 20)]:
            folder = tmp_path / f"year={year}"
            folder.mkdir()
            path = folder / "part.parquet"
            pq.write_table(
                pa.table({"id": list(range(start, start + 4))}), path, row_group_size=2
            )
            paths.append(str(path))
        ds = ray.data.read_parquet(
            paths,
            schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
            override_num_blocks=1,
        )
        expr = (col("year") == "2023") | (col("id") < 0)
        if limit_first:
            assert ds.limit(3).filter(expr=expr).take_all() == []
        else:
            assert ds.filter(expr=expr).limit(3).take_all() == [
                {"id": value, "year": "2023"} for value in [20, 21, 22]
            ]
    finally:
        ray.shutdown()


@pytest.mark.parametrize("hint_first", [False, True])
@pytest.mark.parametrize("partial", [False, True])
def test_mixed_or_preserves_pushed_predicate_errors(
    tmp_path, hint_first, partial, restore_data_context
):
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ray.data.DataContext.get_current().use_datasource_v2 = True
        path = tmp_path / "year=2020" / "part.parquet"
        path.parent.mkdir()
        pq.write_table(pa.table({"id": [10, 11, 12, 13]}), path, row_group_size=2)
        ds = ray.data.read_parquet(
            str(path),
            schema=pa.schema([("id", pa.int64()), ("year", pa.string())]),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
            override_num_blocks=1,
        )
        existing = (col("id") / (col("id") - 10)) > 0
        hint = (col("year") == "2023") | (
            (col("id") >= 12) if partial else (col("id") < 0)
        )
        for expr in [hint, existing] if hint_first else [existing, hint]:
            ds = ds.filter(expr=expr)
        with pytest.raises(pa.ArrowInvalid, match="divide by zero"):
            ds.take_all()
    finally:
        ray.shutdown()


@pytest.mark.parametrize("comparison_name", ["lt", "le", "ge", "gt"])
def test_integer_partition_range_public_operations(
    tmp_path, comparison_name, restore_data_context
):
    import operator

    comparison = getattr(operator, comparison_name)
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.use_datasource_v2 = True
        ctx.execution_options.preserve_order = True
        paths, rows = [], []
        for year in [-1, 0, 1]:
            path = tmp_path / f"year={year}" / "data.parquet"
            path.parent.mkdir()
            table = pa.table({"id": [None, -2, 10, 11]})
            pq.write_table(table, path, row_group_size=2)
            paths.append(str(path))
            rows.extend(dict(r, year=year) for r in table.to_pylist())
        schema = pa.schema([("id", pa.int64()), ("year", pa.int16())])
        partitioning = Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
        )

        def read(include_paths=False):
            return ray.data.read_parquet(
                paths,
                schema=schema,
                partitioning=partitioning,
                override_num_blocks=1,
                include_paths=include_paths,
            )

        expr = comparison(col("year"), 0) | (col("id") < 0)
        oracle = pa.Table.from_pylist(rows).filter(expr.to_pyarrow()).to_pylist()
        assert read().filter(expr=expr).take_all() == oracle
        reverse = (col("id") < 0) | comparison(col("year"), 0)
        assert read().filter(expr=reverse).take_all() == oracle
        assert read().filter(expr=expr).select_columns(["id"]).take_all() == [
            {"id": r["id"]} for r in oracle
        ]
        with_paths = read(include_paths=True).filter(expr=expr).take_all()
        assert [
            {k: v for k, v in r.items() if k != "path"} for r in with_paths
        ] == oracle
        assert all(r["path"] in paths for r in with_paths)
        assert read().filter(expr=expr).limit(2).take_all() == oracle[:2]
        limited = pa.Table.from_pylist(rows[:2]).filter(expr.to_pyarrow()).to_pylist()
        assert read().limit(2).filter(expr=expr).take_all() == limited
        existing = col("id") >= 10
        expected = (
            pa.Table.from_pylist(rows)
            .filter((expr & existing).to_pyarrow())
            .to_pylist()
        )
        for first, second in [(expr, existing), (existing, expr)]:
            assert read().filter(expr=first).filter(expr=second).take_all() == expected
        # A UDF can change partition values: retain it before the complete Filter.
        def change_year(row):
            return dict(row, year=1)

        changed = [dict(row, year=1) for row in rows]
        expected_udf = (
            pa.Table.from_pylist(changed).filter(expr.to_pyarrow()).to_pylist()
        )
        assert read().map(change_year).filter(expr=expr).take_all() == expected_udf
    finally:
        ray.shutdown()


@pytest.mark.parametrize("hint_first", [False, True])
def test_integer_partition_range_preserves_reader_error(
    tmp_path, hint_first, restore_data_context
):
    ray.init(num_cpus=2, include_dashboard=False, object_store_memory=256 * 1024 * 1024)
    try:
        ray.data.DataContext.get_current().use_datasource_v2 = True
        path = tmp_path / "year=0" / "data.parquet"
        path.parent.mkdir()
        pq.write_table(pa.table({"id": [10, 11, 12, 13]}), path, row_group_size=2)
        ds = ray.data.read_parquet(
            str(path),
            schema=pa.schema([("id", pa.int64()), ("year", pa.int8())]),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["year"]
            ),
            override_num_blocks=1,
        )
        hint = (col("year") > 0) | (col("id") >= 12)
        existing = (col("id") / (col("id") - 10)) > 0
        for expr in [hint, existing] if hint_first else [existing, hint]:
            ds = ds.filter(expr=expr)
        with pytest.raises(pa.ArrowInvalid, match="divide by zero"):
            ds.take_all()
    finally:
        ray.shutdown()
