import os

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_pruners import FileExtensionPruner


@pytest.fixture
def fs():
    return LocalFileSystem()


def _create_file(path, size=100):
    """Write `size` bytes to `path`, creating parent dirs as needed."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(b"x" * size)


def _list_all(indexer, paths, fs, **kwargs):
    """Run list_files and flatten all manifests into (path, size) pairs."""
    pa_paths = pa.array(paths)
    manifests = list(indexer.list_files(pa_paths, filesystem=fs, **kwargs))
    results = []
    for m in manifests:
        for p, s in zip(m.paths, m.file_sizes):
            results.append((str(p), int(s)))
    return sorted(results)


@pytest.fixture(params=[1, 2], ids=["sequential", "threaded"])
def indexer(request, monkeypatch):
    """Yield a NonSamplingFileIndexer using sequential or threaded listing."""
    monkeypatch.setattr(NonSamplingFileIndexer, "_THREADED_NUM_WORKERS", request.param)
    return NonSamplingFileIndexer(ignore_missing_paths=False)


class TestListFiles:
    def test_single_file(self, tmp_path, fs, indexer):
        f = str(tmp_path / "data.csv")
        _create_file(f, size=42)

        results = _list_all(indexer, [f], fs)
        assert results == [(f, 42)]

    def test_directory(self, tmp_path, fs, indexer):
        for name in ["a.csv", "b.csv", "c.csv"]:
            _create_file(str(tmp_path / name), size=10)

        results = _list_all(indexer, [str(tmp_path)], fs)
        assert len(results) == 3
        assert all(size == 10 for _, size in results)

    def test_nested_directories(self, tmp_path, fs, indexer):
        _create_file(str(tmp_path / "top.csv"))
        _create_file(str(tmp_path / "sub" / "nested.csv"))
        _create_file(str(tmp_path / "sub" / "deep" / "leaf.csv"))

        results = _list_all(indexer, [str(tmp_path)], fs)
        assert len(results) == 3
        basenames = sorted(os.path.basename(p) for p, _ in results)
        assert basenames == ["leaf.csv", "nested.csv", "top.csv"]

    def test_multiple_paths(self, tmp_path, fs, indexer):
        f1 = str(tmp_path / "one.csv")
        f2 = str(tmp_path / "two.csv")
        _create_file(f1, size=10)
        _create_file(f2, size=20)

        results = _list_all(indexer, [f1, f2], fs)
        assert sorted(results) == [(f1, 10), (f2, 20)]

    @pytest.mark.parametrize(
        "filename",
        [".hidden", "_metadata", "_SUCCESS", ".gitignore"],
        ids=["dot-prefix", "underscore-prefix", "underscore-upper", "dotfile"],
    )
    def test_excludes_hidden_and_metadata_files(self, tmp_path, fs, indexer, filename):
        _create_file(str(tmp_path / filename))
        _create_file(str(tmp_path / "visible.csv"))

        results = _list_all(indexer, [str(tmp_path)], fs)
        assert len(results) == 1
        assert os.path.basename(results[0][0]) == "visible.csv"

    def test_skips_zero_size_files(self, tmp_path, fs, indexer):
        _create_file(str(tmp_path / "empty.csv"), size=0)
        _create_file(str(tmp_path / "real.csv"), size=50)

        results = _list_all(indexer, [str(tmp_path)], fs)
        assert len(results) == 1
        assert os.path.basename(results[0][0]) == "real.csv"

    def test_empty_directory(self, tmp_path, fs, indexer):
        os.makedirs(tmp_path / "empty_dir", exist_ok=True)
        results = _list_all(indexer, [str(tmp_path / "empty_dir")], fs)
        assert results == []


class TestPruners:
    @pytest.fixture
    def indexer(self, monkeypatch):
        monkeypatch.setattr(NonSamplingFileIndexer, "_THREADED_NUM_WORKERS", 1)
        return NonSamplingFileIndexer(ignore_missing_paths=False)

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
    def test_extension_pruner(
        self, tmp_path, fs, indexer, extensions, expected_basenames
    ):
        _create_file(str(tmp_path / "a.csv"))
        _create_file(str(tmp_path / "b.json"))
        _create_file(str(tmp_path / "c.txt"))

        pruner = FileExtensionPruner(extensions)
        results = _list_all(indexer, [str(tmp_path)], fs, pruners=[pruner])
        basenames = sorted(os.path.basename(p) for p, _ in results)
        assert basenames == sorted(expected_basenames)

    def test_multiple_pruners_intersect(self, tmp_path, fs, indexer):
        """Multiple pruners are AND'd — a file must pass all of them."""
        _create_file(str(tmp_path / "a.csv"))
        _create_file(str(tmp_path / "b.json"))

        pruner_csv = FileExtensionPruner(["csv", "json"])
        pruner_json = FileExtensionPruner(["json"])
        results = _list_all(
            indexer, [str(tmp_path)], fs, pruners=[pruner_csv, pruner_json]
        )
        basenames = [os.path.basename(p) for p, _ in results]
        assert basenames == ["b.json"]


class TestMissingPaths:
    def test_raises_on_missing_path(self, tmp_path, fs):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
        missing = str(tmp_path / "nonexistent")

        with pytest.raises(FileNotFoundError):
            _list_all(indexer, [missing], fs)

    def test_ignores_missing_path(self, tmp_path, fs):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=True)
        missing = str(tmp_path / "nonexistent")

        results = _list_all(indexer, [missing], fs)
        assert results == []

    def test_mixed_existing_and_missing(self, tmp_path, fs):
        indexer = NonSamplingFileIndexer(ignore_missing_paths=True)
        real = str(tmp_path / "real.csv")
        _create_file(real, size=10)
        missing = str(tmp_path / "gone")

        results = _list_all(indexer, [real, missing], fs)
        assert results == [(real, 10)]


class TestManifestBatching:
    def test_splits_into_multiple_manifests(self, tmp_path, fs, monkeypatch):
        monkeypatch.setattr(NonSamplingFileIndexer, "_THREADED_NUM_WORKERS", 1)
        monkeypatch.setattr(
            NonSamplingFileIndexer, "_MAX_PATHS_PER_LIST_FILES_OUTPUT", 3
        )
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False)

        for i in range(7):
            _create_file(str(tmp_path / f"file_{i}.csv"))

        pa_paths = pa.array([str(tmp_path)])
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
