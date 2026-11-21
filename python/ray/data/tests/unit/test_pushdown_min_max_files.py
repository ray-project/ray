"""Unit tests for the footer-summary trust boundary in ``PushdownMinMaxFiles``.

These run without Ray: the rule's batch function is exercised directly with a
reader that answers statistics badly on purpose. Every case must produce the
rows a normal read would, never an exception from a worker.
"""

from typing import Dict, Iterator, Sequence

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.supports_column_statistics import (
    ColumnStatistics,
    ColumnStatisticsState,
    SupportsColumnStatistics,
)
from ray.data._internal.logical.rules.pushdown_min_max_files import (
    _summarize_or_read,
)

_SCHEMA = pa.schema([("x", pa.int64())])


class _BadBoundStatistics(SupportsColumnStatistics, Reader[FileManifest]):
    """A reader whose footer bound cannot be stored in the target schema."""

    def __init__(self, bound):
        self._bound = bound

    def supports_column_type(self, column_type: pa.DataType) -> bool:
        return True

    def read_column_statistics(
        self, file_manifest: FileManifest, columns: Sequence[str]
    ) -> Iterator[Dict[str, ColumnStatistics]]:
        for _ in range(len(file_manifest)):
            yield {
                column: ColumnStatistics(
                    num_rows=1,
                    null_count=0,
                    min=self._bound,
                    max=self._bound,
                    state=ColumnStatisticsState.BOUNDED,
                )
                for column in columns
            }

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """The real rows a normal read would produce."""
        yield pa.table({"x": [7, 3]})


@pytest.mark.parametrize(
    "bound",
    ["not-an-int", 2**80],
    ids=["wrong-type", "overflows-int64"],
)
def test_unstorable_bound_reads_the_batch(bound):
    table = _summarize_or_read(_BadBoundStatistics(bound), _manifest(), ["x"], _SCHEMA)

    assert table.column("x").to_pylist() == [7, 3]


def test_unreadable_statistics_read_the_batch():
    class _RaisingStatistics(_BadBoundStatistics):
        def read_column_statistics(self, file_manifest, columns):
            raise OSError("footer is gone")

    table = _summarize_or_read(_RaisingStatistics(1), _manifest(), ["x"], _SCHEMA)

    assert table.column("x").to_pylist() == [7, 3]


def _manifest() -> FileManifest:
    return FileManifest.construct_manifest(
        paths=["a.parquet"], sizes=[10], chunk_metadatas=[None]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
