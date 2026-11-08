"""Native signed-width statistics and logical partition endpoints."""

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
    ListingPruningHint,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _read_groups(path, hint, q):
    reader = FooterReader(
        fs.LocalFileSystem(), io_concurrency=1, listing_hint=hint, filter_expr=q
    )
    try:
        return [
            (i, c.fully_matched)
            for c in reader._read_and_chunk(
                FileInfo(str(path), path.stat().st_size)
            ).row_groups
            for i in c.unit_ids
        ]
    finally:
        reader.pool.shutdown(wait=True)


@pytest.mark.parametrize("dtype", [pa.int8(), pa.int16(), pa.int32(), pa.int64()])
@pytest.mark.parametrize("partition_endpoint", ["min", "max"])
@pytest.mark.parametrize(
    "case", ["high", "low", "negative", "cross_zero", "max_eq", "min_eq", "full_range"]
)
@pytest.mark.parametrize("with_q", [False, True])
def test_native_integer_boundaries(tmp_path, dtype, partition_endpoint, case, with_q):
    low = -(2 ** (dtype.bit_width - 1))
    high = 2 ** (dtype.bit_width - 1) - 1
    values = [low, low + 1, low + 2, -3, -2, -1, -1, 0, 1, high - 2, high - 1, high]
    partition = low if partition_endpoint == "min" else high
    path = tmp_path / f"p={partition}" / "data.parquet"
    path.parent.mkdir()
    table = pa.table({"x": pa.array(values, type=dtype), "rid": range(12)})
    pq.write_table(table, path, row_group_size=3)
    metadata = pq.read_metadata(path)
    assert metadata.num_row_groups == 4
    for i in range(4):
        statistics = metadata.row_group(i).column(0).statistics
        assert statistics.min == values[3 * i]
        assert statistics.max == values[3 * i + 2]
        if i == 2:
            assert statistics.min < 0 < statistics.max
    file_predicate, native_possible = {
        "high": (col("x") >= high - 1, [3]),
        "low": (col("x") <= low + 1, [0]),
        "negative": (col("x") < 0, [0, 1, 2]),
        "cross_zero": ((col("x") >= -1) & (col("x") <= 0), [1, 2]),
        "max_eq": (col("x") == high, [3]),
        "min_eq": (col("x") == low, [0]),
        "full_range": ((col("x") >= low) & (col("x") <= high), [0, 1, 2, 3]),
    }[case]
    residual = (col("p") == low) | file_predicate
    q = col("x") >= -3 if with_q else None
    schema = table.schema.append(pa.field("p", dtype))
    scanner = ParquetScanner(
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            base_dir=str(tmp_path),
            field_names=["p"],
            field_types={"p": int},
        ),
    )
    context = scanner.listing_pruning_context()
    assert context is not None
    hint = ListingPruningHint.from_residual(context, residual)
    assert hint is not None
    baseline = _read_groups(path, None, q)
    selected = _read_groups(path, hint, q)
    baseline_ids = [1, 2, 3] if with_q else [0, 1, 2, 3]
    assert [i for i, _ in baseline] == baseline_ids
    expected = (
        baseline_ids
        if partition_endpoint == "min"
        else sorted(set(native_possible) & set(baseline_ids))
    )
    assert [i for i, _ in selected] == expected
    assert all(dict(baseline)[i] == fully for i, fully in selected)

    def result(groups):
        t = pq.ParquetFile(path).read_row_groups([i for i, _ in groups])
        t = t.append_column("p", pa.array([partition] * len(t), type=dtype))
        predicate = residual if q is None else q & residual
        return ds.dataset(t).to_table(filter=predicate.to_pyarrow())

    assert result(baseline).equals(result(selected), check_metadata=True)
    assert result(selected).schema.equals(schema, check_metadata=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
