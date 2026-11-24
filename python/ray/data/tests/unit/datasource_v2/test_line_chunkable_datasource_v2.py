"""Unit tests for the shared line-chunkable datasource base (CSV, JSONL)."""

import pytest

from ray.data._internal.datasource_v2.formats.csv.csv_datasource_v2 import (
    CsvDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.datasource.partitioning import Partitioning, PartitionStyle


def _write(path, text: str) -> str:
    path.write_text(text)
    return str(path)


def test_discovers_partition_keys_from_later_sampled_files(tmp_path):
    # The first sampled file has no ``key=value`` directories; the shared
    # ``resolve_partitioning`` walks the rest, as Parquet, ORC and Text do.
    plain = _write(tmp_path / "a.csv", "id\n1\n")
    part_dir = tmp_path / "country=US"
    part_dir.mkdir()
    partitioned = _write(part_dir / "b.csv", "id\n2\n")
    datasource = CsvDatasourceV2(
        [plain, partitioned], partitioning=Partitioning(PartitionStyle.HIVE)
    )
    sample = FileManifest.construct_manifest(
        paths=[plain, partitioned],
        sizes=[1, 1],
        chunk_metadatas=[None, None],
    )

    resolved = datasource.resolve_partitioning(sample)

    assert resolved is not None
    assert resolved.field_names == ["country"]


def test_resolves_partitioning_without_a_sample():
    datasource = CsvDatasourceV2(
        ["/tmp/a.csv"], partitioning=Partitioning(PartitionStyle.HIVE)
    )

    resolved = datasource.resolve_partitioning(None)

    assert resolved is not None
    assert resolved.field_names is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
