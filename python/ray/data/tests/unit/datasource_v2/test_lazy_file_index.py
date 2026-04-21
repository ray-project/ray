import pickle
from typing import Iterable, List, Optional

import pytest

from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data._internal.datasource_v2.listing.lazy_file_index import LazyFileIndex


class _FakeIndexer(FileIndexer):
    """Indexer that yields pre-canned manifests, one iterator per call."""

    def __init__(self, manifests: List[FileManifest]):
        self._manifests = manifests
        self.call_count = 0
        self.last_pruners: Optional[List[FilePruner]] = None

    def list_files(
        self,
        paths,
        *,
        filesystem,
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileManifest]:
        self.call_count += 1
        self.last_pruners = pruners
        return iter(self._manifests)


class _AcceptAllPruner(FilePruner):
    def should_include(self, path: str) -> bool:
        return True


def _manifest(paths: List[str], sizes: Optional[List[int]] = None) -> FileManifest:
    if sizes is None:
        sizes = [len(p) for p in paths]
    return FileManifest.construct_manifest(paths, sizes)


def _make_index(manifests: List[FileManifest]) -> LazyFileIndex:
    return LazyFileIndex(_FakeIndexer(manifests), paths=[], filesystem=None)


def _collect_paths(manifests_iter) -> List[str]:
    out: List[str] = []
    for m in manifests_iter:
        out.extend(m.paths.tolist())
    return out


class TestFileManifestConcat:
    def test_concat_preserves_order(self):
        a = _manifest(["a", "b"], [1, 2])
        b = _manifest(["c", "d"], [3, 4])
        merged = FileManifest.concat([a, b])
        assert merged.paths.tolist() == ["a", "b", "c", "d"]
        assert merged.file_sizes.tolist() == [1, 2, 3, 4]
        assert len(merged) == 4

    def test_concat_single_returns_same_instance(self):
        a = _manifest(["a"], [1])
        assert FileManifest.concat([a]) is a


class TestLazyFileIndexSampling:
    def test_sample_less_than_first_manifest_truncates(self):
        idx = _make_index([_manifest(["a", "b", "c", "d", "e"]), _manifest(["f", "g"])])
        sampled = idx.list_files(sample=3)
        assert sampled.paths.tolist() == ["a", "b", "c"]
        assert len(sampled) == 3

    def test_sample_crosses_manifest_boundary(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c", "d", "e"])])
        sampled = idx.list_files(sample=4)
        assert sampled.paths.tolist() == ["a", "b", "c", "d"]

    def test_sample_greater_than_total_returns_all(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c"])])
        sampled = idx.list_files(sample=100)
        assert sampled.paths.tolist() == ["a", "b", "c"]
        assert idx.fully_listed is True

    def test_sample_empty_paths_raises(self):
        idx = _make_index([])
        with pytest.raises(ValueError):
            idx.list_files(sample=5)

    def test_sample_non_positive_raises(self):
        idx = _make_index([_manifest(["a"])])
        with pytest.raises(AssertionError):
            idx.list_files(sample=0)
        with pytest.raises(AssertionError):
            idx.list_files(sample=-1)

    def test_repeat_sample_reuses_cache(self):
        indexer = _FakeIndexer([_manifest(["a", "b"]), _manifest(["c", "d"])])
        idx = LazyFileIndex(indexer, paths=[], filesystem=None)

        first = idx.list_files(sample=2)
        assert first.paths.tolist() == ["a", "b"]
        assert indexer.call_count == 1

        second = idx.list_files(sample=2)
        assert second.paths.tolist() == ["a", "b"]
        assert indexer.call_count == 1


class TestLazyFileIndexStreaming:
    def test_full_stream_after_sample_continues_from_cache(self):
        idx = _make_index(
            [_manifest(["a", "b"]), _manifest(["c", "d"]), _manifest(["e"])]
        )
        sampled = idx.list_files(sample=2)
        assert sampled.paths.tolist() == ["a", "b"]

        full = _collect_paths(idx.list_files())
        assert full == ["a", "b", "c", "d", "e"]
        assert idx.fully_listed is True

    def test_full_stream_twice_uses_cache(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c"])])
        first = _collect_paths(idx.list_files())
        assert first == ["a", "b", "c"]
        second = _collect_paths(idx.list_files())
        assert second == ["a", "b", "c"]

    def test_dedup_set_freed_when_fully_listed(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c"])])
        _collect_paths(idx.list_files())
        assert idx.fully_listed is True
        assert idx._cached_paths == set()

    def test_partial_iteration_then_restart(self):
        # Breaking out of an iteration must release the internal ``_iterating``
        # guard so a subsequent call to ``list_files`` is allowed.
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c", "d"])])
        for _ in idx.list_files():
            break
        # Second call must not raise and must still produce all files.
        assert _collect_paths(idx.list_files()) == ["a", "b", "c", "d"]


class TestLazyFileIndexConcurrency:
    def test_overlapping_iteration_raises(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c"])])
        first = iter(idx.list_files())
        next(first)  # start iteration, do not finish
        with pytest.raises(AssertionError):
            list(idx.list_files())

    def test_sample_during_iteration_raises(self):
        idx = _make_index([_manifest(["a", "b"]), _manifest(["c"])])
        first = iter(idx.list_files())
        next(first)
        with pytest.raises(AssertionError):
            idx.list_files(sample=1)


class TestLazyFileIndexPruners:
    def test_pruners_are_passed_through_to_indexer(self):
        pruner = _AcceptAllPruner()
        indexer = _FakeIndexer([_manifest(["a"])])
        idx = LazyFileIndex(indexer, paths=[], filesystem=None, pruners=[pruner])

        _ = idx.list_files(sample=1)
        assert indexer.last_pruners == [pruner]


class TestLazyFileIndexPickle:
    def test_pickle_drops_iterator_preserves_cache(self):
        indexer = _FakeIndexer(
            [_manifest(["a", "b"]), _manifest(["c", "d"]), _manifest(["e"])]
        )
        idx = LazyFileIndex(indexer, paths=[], filesystem=None)
        _ = idx.list_files(sample=2)
        assert idx._listing_iterator is not None
        cached_before = [m.paths.tolist() for m in idx._cached_manifests]

        restored = pickle.loads(pickle.dumps(idx))

        assert restored._listing_iterator is None
        assert [m.paths.tolist() for m in restored._cached_manifests] == cached_before
        assert restored.fully_listed is False

    def test_pickle_when_fully_listed(self):
        indexer = _FakeIndexer([_manifest(["a", "b", "c"])])
        idx = LazyFileIndex(indexer, paths=[], filesystem=None)
        _collect_paths(idx.list_files())
        assert idx.fully_listed is True

        restored = pickle.loads(pickle.dumps(idx))
        assert restored.fully_listed is True
        assert _collect_paths(restored.list_files()) == ["a", "b", "c"]

    def test_worker_resume_does_not_relist_cached_paths(self):
        # Driver samples partially, pickles to worker. On the worker, the
        # indexer iterator is re-created from scratch and would yield paths
        # already cached; the index must dedup so the remainder comes
        # through exactly once.
        manifests = [_manifest(["a", "b"]), _manifest(["c", "d"]), _manifest(["e"])]
        driver_indexer = _FakeIndexer(manifests)
        idx = LazyFileIndex(driver_indexer, paths=[], filesystem=None)
        _ = idx.list_files(sample=2)
        cached_paths = {p for m in idx._cached_manifests for p in m.paths.tolist()}

        restored = pickle.loads(pickle.dumps(idx))
        # Simulate the worker seeing the same source files via a fresh iterator.
        restored._indexer = _FakeIndexer(manifests)

        full = _collect_paths(restored.list_files())
        # Cache is yielded first, then only un-cached files from the re-listed
        # iterator.
        assert len(full) == len(set(full)), f"duplicates in {full}"
        assert set(full) == {"a", "b", "c", "d", "e"}
        # The originally cached paths appear before any newly listed paths.
        cached_indices = [full.index(p) for p in cached_paths]
        new_indices = [full.index(p) for p in set(full) - cached_paths]
        assert max(cached_indices) < min(new_indices)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
