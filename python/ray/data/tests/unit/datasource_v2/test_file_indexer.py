import os

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
