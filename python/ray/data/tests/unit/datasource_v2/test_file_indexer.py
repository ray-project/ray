import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunker,
    ParquetFileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_pruners import FileExtensionPruner


def _list_all(indexer, paths, **kwargs):
    """Run list_files and flatten all manifests into (path, size) pairs."""
    pa_paths = pa.array(paths)
    fs = LocalFileSystem()
    manifests = list(indexer.list_files(pa_paths, filesystem=fs, **kwargs))
    results = []
    for m in manifests:
        for p, s in zip(m.paths, m.file_sizes):
            results.append((str(p), int(s)))
    return sorted(results)


@pytest.fixture(params=[1, 2], ids=["sequential", "threaded"])
def indexer(request):
    """Yield a NonSamplingFileIndexer using sequential or threaded listing."""
    return NonSamplingFileIndexer(ignore_missing_paths=False, num_workers=request.param)


class TestListFiles:
    def test_single_file(self, tmp_path, indexer):
        f = tmp_path / "data.csv"
        f.write_bytes(b"x" * 42)

        results = _list_all(indexer, [str(f)])
        assert results == [(str(f), 42)]

    def test_directory(self, tmp_path, indexer):
        for name in ["a.csv", "b.csv", "c.csv"]:
            (tmp_path / name).write_bytes(b"x" * 10)

        results = _list_all(indexer, [str(tmp_path)])
        assert len(results) == 3
        assert all(size == 10 for _, size in results)

    def test_nested_directories(self, tmp_path, indexer):
        (tmp_path / "top.csv").write_bytes(b"x" * 100)
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "nested.csv").write_bytes(b"x" * 100)
        (tmp_path / "sub" / "deep").mkdir()
        (tmp_path / "sub" / "deep" / "leaf.csv").write_bytes(b"x" * 100)

        results = _list_all(indexer, [str(tmp_path)])
        assert len(results) == 3
        basenames = sorted(os.path.basename(p) for p, _ in results)
        assert basenames == ["leaf.csv", "nested.csv", "top.csv"]

    def test_multiple_paths(self, tmp_path, indexer):
        f1 = tmp_path / "one.csv"
        f2 = tmp_path / "two.csv"
        f1.write_bytes(b"x" * 10)
        f2.write_bytes(b"x" * 20)

        results = _list_all(indexer, [str(f1), str(f2)])
        assert sorted(results) == [(str(f1), 10), (str(f2), 20)]

    @pytest.mark.parametrize(
        "filename",
        [".hidden", "_metadata", "_SUCCESS", ".gitignore"],
        ids=["dot-prefix", "underscore-prefix", "underscore-upper", "dotfile"],
    )
    def test_excludes_hidden_and_metadata_files(self, tmp_path, indexer, filename):
        (tmp_path / filename).write_bytes(b"x" * 100)
        (tmp_path / "visible.csv").write_bytes(b"x" * 100)

        results = _list_all(indexer, [str(tmp_path)])
        assert len(results) == 1
        assert os.path.basename(results[0][0]) == "visible.csv"

    @pytest.mark.parametrize(
        "filename",
        ["_metadata", "_my_file.csv", ".hidden_data"],
        ids=["underscore-metadata", "underscore-csv", "dot-hidden"],
    )
    def test_includes_excluded_prefix_files_in_subdirectories(
        self, tmp_path, indexer, filename
    ):
        """Files whose names start with _ or . should only be excluded when
        they appear at the top level of the listed directory, not when they
        appear inside a subdirectory. The relative path from the root is
        e.g. "subdir/_metadata" which starts with "s", not "_"."""
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / filename).write_bytes(b"x" * 50)
        (sub / "normal.csv").write_bytes(b"x" * 50)

        results = _list_all(indexer, [str(tmp_path)])
        basenames = sorted(os.path.basename(p) for p, _ in results)
        assert filename in basenames
        assert "normal.csv" in basenames

    def test_skips_zero_size_files(self, tmp_path, indexer):
        (tmp_path / "empty.csv").write_bytes(b"")
        (tmp_path / "real.csv").write_bytes(b"x" * 50)

        results = _list_all(indexer, [str(tmp_path)])
        assert len(results) == 1
        assert os.path.basename(results[0][0]) == "real.csv"

    def test_empty_directory(self, tmp_path, indexer):
        os.makedirs(tmp_path / "empty_dir", exist_ok=True)
        results = _list_all(indexer, [str(tmp_path / "empty_dir")])
        assert results == []


class TestPruners:
    @pytest.fixture
    def indexer(self):
        return NonSamplingFileIndexer(ignore_missing_paths=False, num_workers=1)

    @pytest.mark.parametrize(
        "extensions, expected_basenames",
        [
            (["csv"], ["a.csv"]),
            (["json"], ["b.json"]),
            (["csv", "json"], ["a.csv", "b.json"]),
            (["parquet"], []),
        ],
        ids=["csv-only", "json-only", "csv-and-json", "no-match"],
    )
    def test_extension_pruner(self, tmp_path, indexer, extensions, expected_basenames):
        (tmp_path / "a.csv").write_bytes(b"x" * 100)
        (tmp_path / "b.json").write_bytes(b"x" * 100)
        (tmp_path / "c.txt").write_bytes(b"x" * 100)

        pruner = FileExtensionPruner(extensions)
        results = _list_all(indexer, [str(tmp_path)], pruners=[pruner])
        basenames = sorted(os.path.basename(p) for p, _ in results)
        assert basenames == sorted(expected_basenames)

    def test_multiple_pruners_intersect(self, tmp_path, indexer):
        """Multiple pruners are AND'd — a file must pass all of them."""
        (tmp_path / "a.csv").write_bytes(b"x" * 100)
        (tmp_path / "b.json").write_bytes(b"x" * 100)

        pruner_csv = FileExtensionPruner(["csv", "json"])
        pruner_json = FileExtensionPruner(["json"])
        results = _list_all(indexer, [str(tmp_path)], pruners=[pruner_csv, pruner_json])
        basenames = [os.path.basename(p) for p, _ in results]
        assert basenames == ["b.json"]


class TestMissingPaths:
    def test_raises_on_missing_path(self, tmp_path):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
        missing = str(tmp_path / "nonexistent")

        with pytest.raises(FileNotFoundError):
            _list_all(indexer, [missing])

    def test_ignores_missing_path(self, tmp_path):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=True)
        missing = str(tmp_path / "nonexistent")

        results = _list_all(indexer, [missing])
        assert results == []

    def test_mixed_existing_and_missing(self, tmp_path):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=True)
        real = tmp_path / "real.csv"
        real.write_bytes(b"x" * 10)
        missing = str(tmp_path / "gone")

        results = _list_all(indexer, [str(real), missing])
        assert results == [(str(real), 10)]


class TestManifestBatching:
    def test_splits_into_multiple_manifests(self, tmp_path):
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False, max_paths_per_output=3
        )

        for i in range(7):
            (tmp_path / f"file_{i}.csv").write_bytes(b"x" * 100)

        pa_paths = pa.array([str(tmp_path)])
        fs = LocalFileSystem()
        manifests = list(indexer.list_files(pa_paths, filesystem=fs))

        assert len(manifests) == 3  # ceil(7/3)
        assert len(manifests[0]) == 3
        assert len(manifests[1]) == 3
        assert len(manifests[2]) == 1

        total_files = sum(len(m) for m in manifests)
        assert total_files == 7


class TestFileChunkerIntegration:
    """Cover ``NonSamplingFileIndexer`` interaction with a ``FileChunker``."""

    def test_default_uses_whole_file_chunker(self):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
        assert isinstance(indexer.file_chunker, WholeFileChunker)

    def test_explicit_chunker_is_exposed(self):
        chunker = ParquetFileChunker(target_chunk_size=1024)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False, file_chunker=chunker
        )
        assert indexer.file_chunker is chunker

    def test_whole_file_chunker_yields_none_chunk_metadata(self, tmp_path):
        (tmp_path / "a.csv").write_bytes(b"x" * 100)
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False, num_workers=1)
        fs = LocalFileSystem()
        manifests = list(indexer.list_files(pa.array([str(tmp_path)]), filesystem=fs))
        assert len(manifests) == 1
        manifest = manifests[0]
        assert len(manifest) == 1
        # ``WholeFileChunker`` emits one ``None`` chunk per file.
        assert list(manifest.file_chunk_metadatas) == [None]
        assert list(manifest.file_sizes) == [100]

    def test_parquet_chunker_splits_file_on_row_group_boundaries(self, tmp_path):
        # A real Parquet file with 8 row groups; the chunker reads the footer
        # at listing time and (target < row-group size) emits one chunk per
        # row group with an explicit half-open range.
        table = pa.table({"a": list(range(80))})
        pq.write_table(table, str(tmp_path / "big.parquet"), row_group_size=10)
        chunker = ParquetFileChunker(target_chunk_size=1)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False,
            num_workers=1,
            file_chunker=chunker,
        )
        fs = LocalFileSystem()
        manifests = list(indexer.list_files(pa.array([str(tmp_path)]), filesystem=fs))
        rows = []
        for m in manifests:
            for path, size, md in zip(m.paths, m.file_sizes, m.file_chunk_metadatas):
                rows.append((str(path), int(size), md))

        # 8 row groups → 8 chunks, contiguous half-open ranges.
        assert len(rows) == 8
        ranges = [(md["row_group_start"], md["row_group_end"]) for _, _, md in rows]
        assert ranges == [(i, i + 1) for i in range(8)]

    def test_parquet_chunker_parallel_footer_reads(self, tmp_path):
        # With num_workers > 1 the chunker's footer reads fan across the
        # thread pool (reads_file_metadata=True). Verify correctness is
        # unaffected: every file's row groups are represented exactly once.
        for f in range(4):
            table = pa.table({"a": list(range(30))})
            pq.write_table(table, str(tmp_path / f"f{f}.parquet"), row_group_size=10)
        chunker = ParquetFileChunker(target_chunk_size=1)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False,
            num_workers=4,
            file_chunker=chunker,
        )
        fs = LocalFileSystem()
        manifests = list(indexer.list_files(pa.array([str(tmp_path)]), filesystem=fs))
        per_path_ranges = {}
        for m in manifests:
            for path, _, md in zip(m.paths, m.file_sizes, m.file_chunk_metadatas):
                per_path_ranges.setdefault(str(path), []).append(
                    (md["row_group_start"], md["row_group_end"])
                )
        # 4 files × 3 row groups each.
        assert len(per_path_ranges) == 4
        for ranges in per_path_ranges.values():
            assert sorted(ranges) == [(0, 1), (1, 2), (2, 3)]

    def test_line_delimited_chunker_byte_ranges(self, tmp_path):
        (tmp_path / "a.jsonl").write_bytes(b"x" * 10_000)
        chunker = LineDelimitedFileChunker()
        # Force smaller chunks via a private override so the unit test
        # doesn't need a 256 MB file on disk.
        chunker._CHUNK_BYTE_SIZE = 1024
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False,
            num_workers=1,
            file_chunker=chunker,
        )
        fs = LocalFileSystem()
        manifests = list(indexer.list_files(pa.array([str(tmp_path)]), filesystem=fs))
        rows = []
        for m in manifests:
            for path, size, md in zip(m.paths, m.file_sizes, m.file_chunk_metadatas):
                rows.append((str(path), int(size), md))
        assert len(rows) == 10
        # Byte ranges must tile the file exactly.
        assert rows[0][2]["chunk_byte_start_idx"] == 0
        assert rows[-1][2]["chunk_byte_end_idx"] == 10_000


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
