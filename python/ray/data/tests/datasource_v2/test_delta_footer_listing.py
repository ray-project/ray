"""Which row groups the Delta listing yields.

``DeltaFileIndexer`` extends ``FooterFileIndexer``, whose footer reads run in
Ray actors, so these tests need a cluster and aren't unit tests. They build the
indexer as ``read_delta`` does and list the table, to check which row groups a
listing yields. ``tests/datasource/test_delta_v2.py`` checks the rows reads
return.
"""

import posixpath
import sys
from typing import Any, Dict, List

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.delta.delta_datasource_v2 import (
    DeltaDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    load_add_actions,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_chunking_utils import (
    _row_group_unit_id,
)
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.delta_test_utils import write_mapped_table
from ray.tests.conftest import *  # noqa

deltalake = pytest.importorskip("deltalake")


@pytest.fixture
def row_group_table(tmp_path) -> str:
    """One data file with 5 row groups of 20 rows."""
    path = str(tmp_path / "row_groups")
    deltalake.write_deltalake(
        path,
        pa.table({"x": list(range(100))}),
        writer_properties=deltalake.WriterProperties(max_row_group_size=20),
    )
    return path


def _list_files(path: str, **kwargs) -> List[Dict[str, Any]]:
    indexer = DeltaDatasourceV2(deltalake.DeltaTable(path))._get_file_indexer()
    rows = []
    for manifest in indexer.list_files(pa.array(["label"]), filesystem=None, **kwargs):
        block = manifest.as_block()
        # Listing manifests are always built as Arrow tables.
        assert isinstance(block, pa.Table)
        rows.extend(block.to_pylist())
    return rows


def _row_groups(rows: List[Dict[str, Any]]) -> List[int]:
    """Indices of the row groups that listing rows cover."""
    return [i for row in rows for i in row["__file_chunk_metadata"]["unit_ids"]]


def test_listing_skips_finished_row_groups(ray_start_regular_shared, row_group_table):
    """What a resumed job's listing does with the read units it finished."""
    all_rows = _list_files(row_group_table)
    (path,) = {row["__path"] for row in all_rows}
    assert sorted(_row_groups(all_rows)) == [0, 1, 2, 3, 4]

    rows = _list_files(
        row_group_table,
        excluded_read_unit_ids={
            _row_group_unit_id(path, 0),
            _row_group_unit_id(path, 3),
        },
    )

    assert sorted(_row_groups(rows)) == [1, 2, 4]


def test_filter_prunes_row_groups(ray_start_regular_shared, row_group_table):
    rows = _list_files(row_group_table, predicate=col("x") >= 80)

    assert _row_groups(rows) == [4]


@pytest.fixture(params=["name", "id"])
def mapped_table(tmp_path, request) -> str:
    """A column-mapped table. In the ``id`` mode its files use other column
    names than the log's physical names, so only the field ids match."""
    path = str(tmp_path / "mapped")
    write_mapped_table(path, request.param)
    return path


def test_mapped_statistics_skip_files(ray_start_regular_shared, mapped_table):
    """The log keys statistics by physical name in both modes, nested fields
    as dotted paths. They're renamed to logical names when loaded, so
    log-statistics skipping finds them."""
    add_actions = load_add_actions(deltalake.DeltaTable(mapped_table))
    assert {"min.id", "max.s.n", "null_count.extra"} <= set(add_actions.column_names)

    rows = _list_files(mapped_table, predicate=col("id") > 5)

    assert {posixpath.dirname(row["__path"]) for row in rows} == {"col-grp=b"}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
