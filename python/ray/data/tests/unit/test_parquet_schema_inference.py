import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version

from ray.data._internal.datasource.parquet_datasource import _infer_schema
from ray.data._internal.utils.arrow_utils import get_pyarrow_version


def test_read_parquet_memory_growth(tmp_path, monkeypatch):
    """Schema inference should not inspect every fragment on PyArrow >= 22.

    Regression test for a bug where _infer_schema fell back to reading every
    fragment's physical_schema when the sampled fragment had a pa.null() column
    (PyArrow < 22.0), causing O(N) metadata reads and memory usage.
    """
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is None or pyarrow_version < parse_version("22.0.0"):
        pytest.skip("Bounded permissive schema inspection requires PyArrow >= 22.0.0")

    num_cols = 50
    num_files = 1000
    inspect_num_fragments = 1

    def _write_files(directory, n_files):
        directory.mkdir(exist_ok=True)
        for i in range(n_files):
            cols = {f"col_{j}": [0] for j in range(num_cols)}
            # First file has a column of all nulls, which triggers the schema inference fallback.
            if i == 0:
                cols["null_col"] = pa.nulls(1)
            else:
                cols["null_col"] = [1]
            pq.write_table(pa.table(cols), directory / f"part_{i:05d}.parquet")

    _write_files(tmp_path, num_files)

    finish_calls = []
    inspect_calls = []
    real_factory = pds.FileSystemDatasetFactory

    # RSS deltas for this code path are sub-MiB in CI, so check the bounded
    # schema-inspection behavior directly instead of comparing process memory.
    class TrackingFactory:
        def __init__(self, *args, **kwargs):
            self._factory = real_factory(*args, **kwargs)

        def inspect(self, **kwargs):
            inspect_calls.append(kwargs)
            return self._factory.inspect(**kwargs)

        def finish(self, *args, **kwargs):
            finish_calls.append((args, kwargs))
            return self._factory.finish(*args, **kwargs)

    monkeypatch.setattr(pds, "FileSystemDatasetFactory", TrackingFactory)

    schema = _infer_schema(
        [str(path) for path in sorted(tmp_path.iterdir())],
        inspect_num_fragments=inspect_num_fragments,
        filesystem=pafs.LocalFileSystem(),
    )

    assert inspect_calls == [
        {
            "fragments": inspect_num_fragments,
            "promote_options": "permissive",
        }
    ]
    assert not finish_calls
    assert "null_col" in schema.names


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
