import json

import pyarrow as pa
import pytest
from pyarrow import orc, parquet

import ray
from ray.data.expressions import col


def test_union_basic(ray_start_10_cpus_shared):
    ds1 = ray.data.from_items([{"id": 0}])
    ds2 = ray.data.from_items([{"id": 1}])
    ds = ds1.union(ds2)
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1]


def test_union_schema(ray_start_10_cpus_shared):
    ds = ray.data.range(1).union(ray.data.range(1))
    assert ds.schema().names == ["id"]
    assert ds.schema().types == [pa.int64()]


def test_union_allow_missing_columns(ray_start_10_cpus_shared):
    id_field = pa.field("id", pa.int64(), nullable=False)
    text_field = pa.field("text", pa.string(), nullable=False)
    score_field = pa.field("score", pa.float64(), nullable=False)
    country_field = pa.field("country", pa.string(), nullable=False)
    old_table = pa.table(
        {"id": [1], "text": ["hi"]}, schema=pa.schema([id_field, text_field])
    )
    new_table = pa.table(
        {"score": [0.8], "id": [2]}, schema=pa.schema([score_field, id_field])
    )
    old = ray.data.from_arrow([old_table, old_table])
    new = ray.data.from_arrow(new_table)
    empty = ray.data.from_arrow(pa.schema([id_field, country_field]).empty_table())
    combined = old.union(new, empty, allow_missing_columns=True)

    expected_schema = pa.schema(
        [
            id_field,
            *(
                field.with_nullable(True)
                for field in (text_field, score_field, country_field)
            ),
        ]
    )
    assert combined.schema(fetch_if_missing=False).base_schema == expected_schema
    rows = []
    for batch in combined.iter_batches(batch_size=None, batch_format="pyarrow"):
        assert isinstance(batch, pa.Table)
        assert batch.schema == expected_schema
        rows.extend(batch.to_pylist())
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 1, "text": "hi", "score": None, "country": None},
        {"id": 1, "text": "hi", "score": None, "country": None},
        {"id": 2, "text": None, "score": 0.8, "country": None},
    ]

    missing_scores = combined.filter(expr=col("score").is_null()).select_columns("id")
    assert missing_scores.take_all() == [{"id": 1}, {"id": 1}]
    assert combined.filter(expr=col("score") > 0.5).select_columns("id").take_all() == [
        {"id": 2}
    ]
    assert list(combined.take(1)[0]) == expected_schema.names


@pytest.mark.parametrize("file_format", ["csv", "json", "jsonl", "orc"])
def test_union_allow_missing_columns_across_file_formats(
    ray_start_10_cpus_shared, tmp_path, file_format
):
    history_path = tmp_path / "history.parquet"
    parquet.write_table(pa.table({"id": [1], "text": ["old"]}), history_path)
    history = ray.data.read_parquet(str(history_path), override_num_blocks=1)

    incoming_path = tmp_path / f"incoming.{file_format}"
    incoming_rows = [{"score": 0.8, "id": 2}, {"score": 0.2, "id": 3}]
    if file_format == "csv":
        incoming_path.write_text("score,id\n0.8,2\n0.2,3\n", encoding="utf-8")
        incoming = ray.data.read_csv(str(incoming_path), override_num_blocks=1)
    elif file_format == "orc":
        orc.write_table(pa.Table.from_pylist(incoming_rows), str(incoming_path))
        incoming = ray.data.read_orc(str(incoming_path), override_num_blocks=1)
    else:
        contents = (
            json.dumps(incoming_rows)
            if file_format == "json"
            else "".join(json.dumps(row) + "\n" for row in incoming_rows)
        )
        incoming_path.write_text(contents, encoding="utf-8")
        incoming = ray.data.read_json(str(incoming_path), override_num_blocks=1)

    if incoming.schema(fetch_if_missing=False) is None:
        with pytest.raises(
            ValueError, match="Union input 2 must have a known Arrow schema"
        ):
            history.union(incoming, allow_missing_columns=True)
        # CSV and JSON readers may need to read a batch to infer an Arrow schema.
        incoming.schema()

    combined = history.union(incoming, allow_missing_columns=True)
    expected_schema = pa.schema(
        [("id", pa.int64()), ("text", pa.string()), ("score", pa.float64())]
    )
    assert combined.schema(fetch_if_missing=False).base_schema == expected_schema
    rows = []
    for batch in combined.iter_batches(batch_size=None, batch_format="pyarrow"):
        assert isinstance(batch, pa.Table)
        assert batch.schema == expected_schema
        rows.extend(batch.to_pylist())
    assert sorted(rows, key=lambda row: row["id"]) == [
        {"id": 1, "text": "old", "score": None},
        {"id": 2, "text": None, "score": 0.8},
        {"id": 3, "text": None, "score": 0.2},
    ]
    assert combined.filter(expr=col("score") > 0.5).select_columns("id").take_all() == [
        {"id": 2}
    ]


def test_union_allow_missing_columns_with_zero_column_input(ray_start_10_cpus_shared):
    no_columns = ray.data.range(2).select_columns([])
    combined_no_columns = no_columns.union(no_columns, allow_missing_columns=True)
    assert combined_no_columns.columns(fetch_if_missing=False) == []
    assert len(combined_no_columns.take_all()) == 4
    combined = no_columns.union(ray.data.range(1), allow_missing_columns=True)
    assert sorted(
        (row["id"] for row in combined.take_all()), key=lambda value: value is None
    ) == [0, None, None]


def test_union_allow_missing_columns_invalid_schemas(ray_start_10_cpus_shared):
    ds = ray.data.range(1)
    unknown = ds.map(lambda row: row)
    with pytest.raises(
        ValueError, match="Union input 2 must have a known Arrow schema"
    ):
        ds.union(unknown, allow_missing_columns=True)

    duplicate_table = pa.table([pa.array([1]), pa.array([2])], names=["id", "id"])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, duplicate_table.schema) as writer:
        writer.write_table(duplicate_table)
    duplicate = ray.data.from_arrow(sink.getvalue().to_pybytes())
    with pytest.raises(ValueError, match="Union input 2 has duplicate column names"):
        ds.union(duplicate, allow_missing_columns=True)

    conflicting = ray.data.from_items([{"id": "other"}])
    with pytest.raises(
        ValueError, match="input 2 has type string for column 'id'.*int64"
    ):
        ds.union(conflicting, allow_missing_columns=True)


def test_union_repr(ray_start_10_cpus_shared):
    ds = ray.data.range(1).union(ray.data.range(1))
    assert repr(ds) == (
        "shape: (?, 1)\n"
        "╭───────╮\n"
        "│ id    │\n"
        "│ ---   │\n"
        "│ int64 │\n"
        "╰───────╯\n"
        "(Dataset isn't materialized)"
    )


def test_union_with_preserve_order(ray_start_10_cpus_shared, restore_data_context):
    # Test for https://github.com/ray-project/ray/issues/41524
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.preserve_order = True

    # To reproduce the bug, you need three or more input datasets, and the first dataset
    # can't finish first. To emulate this behavior, the test adds a `map` to the first
    # dataset and materializes the other two.
    ds1 = ray.data.from_items([{"id": 0}]).map(lambda x: x)
    ds2 = ray.data.from_items([{"id": 1}]).materialize()
    ds3 = ray.data.from_items([{"id": 2}]).materialize()
    ds = ds1.union(ds2, ds3)

    assert [row["id"] for row in ds.take_all()] == [0, 1, 2]


def test_union_with_filter(ray_start_10_cpus_shared):
    """Test that filters are pushed through union to both branches."""
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    ds1 = ray.data.from_items([{"id": 0}, {"id": 1}, {"id": 2}])
    ds2 = ray.data.from_items([{"id": 3}, {"id": 4}, {"id": 5}])
    ds = ds1.union(ds2).filter(expr=col("id") > 2)

    # Verify the filter was pushed through the union
    optimized_plan = LogicalOptimizer().optimize(ds._logical_plan)
    actual_plan_str = optimized_plan.dag.dag_str

    # After optimization, filter should be pushed to both union branches
    # So we should see: Filter(Read), Filter(Read) -> Union
    # Not: Read, Read -> Union -> Filter
    assert "Union" in actual_plan_str
    assert "Filter" in actual_plan_str
    # Ensure Filter is before Union (pushed down), not after
    assert actual_plan_str.index("Filter") < actual_plan_str.index("Union")

    # Verify correctness
    result = sorted(row["id"] for row in ds.take_all())
    assert result == [3, 4, 5]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
