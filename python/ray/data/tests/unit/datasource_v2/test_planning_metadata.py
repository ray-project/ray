from unittest.mock import Mock

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from ray.data._internal.datasource_v2.common import schema_inference as inference
from ray.data.context import DataContext


def test_schema_batch_preserves_per_file_metadata_budget():
    schema = pa.schema([("id", pa.int64())])

    def read_metadata(path, filesystem, size, *, retried_io_errors):
        return Mock(num_rows=int(path), schema=Mock(to_arrow_schema=lambda: schema))

    result = inference._read_schema_batch(
        ["1", "2", "3"],
        [None, None, None],
        pafs.LocalFileSystem(),
        None,
        "Parquet",
        DataContext.get_current(),
        2,
        read_metadata=read_metadata,
        row_records=2,
        mtimes=[10, 20, 30],
    )
    assert [position for position, _, _ in result.schemas] == [0]
    metadata = result.metadata
    assert metadata is not None
    assert metadata.to_pylist() == [
        {"num_rows": 1, "mtime_ns": 10},
        {"num_rows": 2, "mtime_ns": 20},
        None,
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
