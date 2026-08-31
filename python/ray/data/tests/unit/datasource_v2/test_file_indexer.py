import os

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    LineDelimitedFileChunker,
    ParquetFileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
    _shuffle_file_infos,
)
from ray.data._internal.datasource_v2.listing.file_pruners import FileExtensionPruner
from ray.data.datasource.file_based_datasource import FileShuffleConfig


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


def _list_all_file_infos(indexer, paths, **kwargs):
    """Run list_file_infos and flatten into sorted (path, size) pairs."""
    file_infos = indexer.list_file_infos(
        pa.array(paths), filesystem=LocalFileSystem(), **kwargs
    )
    return sorted((fi.path, fi.size) for fi in file_infos)


def _list_paths_in_order(indexer, paths, **kwargs):
    """Run list_files and flatten paths in yield order (not sorted)."""
    pa_paths = pa.array(paths)
    fs = LocalFileSystem()
    manifests = list(indexer.list_files(pa_paths, filesystem=fs, **kwargs))
    results = []
    for m in manifests:
        for p in m.paths:
            results.append(str(p))
    return results


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


class TestListFileInfos:
    """``list_file_infos`` is the pre-chunk file stream ``list_files`` sits on.

    It owns the zero-size skip and pruner filtering for both paths, so those
    have to hold here directly and not just through ``list_files``.
    """

    def test_yields_path_and_size(self, tmp_path, indexer):
        (tmp_path / "a.csv").write_bytes(b"x" * 10)
        (tmp_path / "b.csv").write_bytes(b"x" * 20)

        assert _list_all_file_infos(indexer, [str(tmp_path)]) == [
            (str(tmp_path / "a.csv"), 10),
            (str(tmp_path / "b.csv"), 20),
        ]

    def test_skips_zero_size_files(self, tmp_path, indexer):
        (tmp_path / "empty.csv").write_bytes(b"")
        (tmp_path / "real.csv").write_bytes(b"x" * 50)

        assert _list_all_file_infos(indexer, [str(tmp_path)]) == [
            (str(tmp_path / "real.csv"), 50)
        ]

    def test_applies_pruners(self, tmp_path, indexer):
        (tmp_path / "keep.csv").write_bytes(b"x" * 10)
        (tmp_path / "drop.json").write_bytes(b"x" * 10)

        results = _list_all_file_infos(
            indexer,
            [str(tmp_path)],
            pruners=[FileExtensionPruner(file_extensions=["csv"])],
        )

        assert results == [(str(tmp_path / "keep.csv"), 10)]

    def test_does_not_chunk(self, tmp_path):
        """One entry per file even when the chunker would split it."""
        (tmp_path / "a.jsonl").write_bytes(b"x" * 10_000)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False,
            num_workers=1,
            file_chunker=LineDelimitedFileChunker(),
        )

        assert _list_all_file_infos(indexer, [str(tmp_path)]) == [
            (str(tmp_path / "a.jsonl"), 10_000)
        ]

    def test_is_lazy(self, tmp_path, indexer):
        """Consumers stop early under a limit, so nothing may be eager."""
        for i in range(5):
            (tmp_path / f"f{i}.csv").write_bytes(b"x" * 10)

        stream = indexer.list_file_infos(
            pa.array([str(tmp_path)]), filesystem=LocalFileSystem()
        )

        assert isinstance(next(iter(stream)).path, str)


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


class TestSkipPaths:
    def test_skips_named_existing_file(self, tmp_path):
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.write_bytes(b"x" * 10)
        b.write_bytes(b"x" * 20)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False, skip_paths={str(b)}
        )

        results = _list_all(indexer, [str(a), str(b)])
        assert results == [(str(a), 10)]

    def test_skips_missing_named_path_without_ignore_missing(self, tmp_path):
        # ``skip_paths`` drops a named path *before* the existence check, so a
        # missing entry is skipped even with ``ignore_missing_paths=False``.
        a = tmp_path / "a.csv"
        a.write_bytes(b"x" * 10)
        missing = str(tmp_path / "gone.csv")
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False, skip_paths={missing}
        )

        results = _list_all(indexer, [str(a), missing])
        assert results == [(str(a), 10)]

    def test_skips_file_discovered_under_directory(self, tmp_path):
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        a.write_bytes(b"x" * 10)
        b.write_bytes(b"x" * 20)
        indexer = NonSamplingFileIndexer(
            ignore_missing_paths=False, skip_paths={str(a)}
        )

        results = _list_all(indexer, [str(tmp_path)])
        assert results == [(str(b), 20)]

    def test_empty_skip_paths_is_noop(self, tmp_path):
        a = tmp_path / "a.csv"
        a.write_bytes(b"x" * 10)
        indexer = NonSamplingFileIndexer(ignore_missing_paths=False, skip_paths=None)

        results = _list_all(indexer, [str(a)])
        assert results == [(str(a), 10)]


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

    def test_parquet_chunker_splits_large_file_into_many_chunks(self, tmp_path):
        # Write a "Parquet" file by name only — the chunker doesn't open it.
        (tmp_path / "big.parquet").write_bytes(b"x" * 10_000)
        chunker = ParquetFileChunker(target_chunk_size=1024)
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

        # 10000 bytes / 1024 target chunk size -> 10 chunks (ceil).
        assert len(rows) == 10
        for i, (_, _, md) in enumerate(rows):
            assert md is not None
            assert md["chunk_idx"] == i
            assert md["total_num_chunks"] == 10

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


class TestAsWholeFileIndexer:
    """``as_whole_file_indexer`` must downgrade to a plain per-file lister.

    ``PushdownCountFiles`` relies on this to count each file exactly once
    without triggering a metadata-aware subclass's listing strategy.
    """

    def test_returns_base_type_from_metadata_aware_subclass(self):
        from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
            FooterFileIndexer,
        )

        downgraded = FooterFileIndexer(
            ignore_missing_paths=False
        ).as_whole_file_indexer()

        # Exact type, not isinstance: FooterFileIndexer subclasses
        # NonSamplingFileIndexer but overrides list_files, so an isinstance
        # check here would not catch a regression.
        assert type(downgraded) is NonSamplingFileIndexer

    @pytest.mark.parametrize(
        "ignore_missing_paths,num_workers,max_paths_per_output",
        [(False, 1, 10), (True, 4, 1000)],
    )
    def test_carries_over_traversal_config(
        self, ignore_missing_paths, num_workers, max_paths_per_output
    ):
        source = NonSamplingFileIndexer(
            ignore_missing_paths=ignore_missing_paths,
            num_workers=num_workers,
            max_paths_per_output=max_paths_per_output,
        )

        downgraded = source.as_whole_file_indexer()

        assert downgraded._ignore_missing_paths == ignore_missing_paths
        assert downgraded._num_workers == num_workers
        assert downgraded._max_paths_per_output == max_paths_per_output
        # Derived in __init__, so rebuilding must recompute it rather than
        # copying a stale value.
        assert downgraded._queue_size_per_thread == max_paths_per_output * 4

    def test_always_uses_whole_file_chunker(self):
        source = NonSamplingFileIndexer(
            ignore_missing_paths=False, file_chunker=LineDelimitedFileChunker()
        )

        assert isinstance(source.as_whole_file_indexer().file_chunker, WholeFileChunker)

    def test_source_indexer_is_not_mutated(self):
        source = NonSamplingFileIndexer(
            ignore_missing_paths=False, file_chunker=LineDelimitedFileChunker()
        )

        source.as_whole_file_indexer()

        # Guards against regressing to in-place mutation of the caller's indexer.
        assert isinstance(source.file_chunker, LineDelimitedFileChunker)

    def test_carries_over_skip_paths(self, tmp_path):
        """A dropped ``skip_paths`` would let excluded files back into the
        listing, inflating a pushed-down ``count()``."""
        kept = tmp_path / "kept.csv"
        skipped = tmp_path / "skipped.csv"
        kept.write_bytes(b"x" * 10)
        skipped.write_bytes(b"x" * 20)
        source = NonSamplingFileIndexer(
            ignore_missing_paths=False, skip_paths={str(skipped)}
        )

        downgraded = source.as_whole_file_indexer()

        assert downgraded._skip_paths == frozenset({str(skipped)})
        assert _list_all(downgraded, [str(tmp_path)]) == [(str(kept), 10)]

    def test_carried_skip_paths_still_tolerate_missing_path(self, tmp_path):
        """``skip_paths`` drops a named path whether or not it exists, so a
        skip-only missing path must not raise once carried over."""
        kept = tmp_path / "kept.csv"
        kept.write_bytes(b"x" * 10)
        missing = str(tmp_path / "gone.csv")
        source = NonSamplingFileIndexer(
            ignore_missing_paths=False, skip_paths={missing}
        )

        downgraded = source.as_whole_file_indexer()

        assert _list_all(downgraded, [str(kept), missing]) == [(str(kept), 10)]


class TestFileShuffle:
    """File shuffle runs after path discovery and before chunking/metadata."""

    def _write_files(self, tmp_path, n=10):
        paths = []
        for i in range(n):
            path = tmp_path / f"f{i:02d}.txt"
            path.write_bytes(b"x" * (i + 1))
            paths.append(str(path))
        return paths

    def test_seeded_shuffle_is_deterministic_and_permutes(self, tmp_path, indexer):
        paths = self._write_files(tmp_path)
        shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)
        listed = list(
            indexer.list_file_infos(
                pa.array(paths),
                filesystem=LocalFileSystem(),
                preserve_order=True,
            )
        )
        expected_paths = [fi.path for fi in _shuffle_file_infos(listed, seed=42)]

        first = _list_paths_in_order(
            indexer, paths, shuffle_config=shuffle_config, preserve_order=True
        )
        second = _list_paths_in_order(
            indexer, paths, shuffle_config=shuffle_config, preserve_order=True
        )
        unshuffled = _list_paths_in_order(indexer, paths, preserve_order=True)

        assert first == second == expected_paths
        assert first != unshuffled
        assert sorted(first) == sorted(unshuffled)

    def test_list_file_infos_is_not_shuffled(self, tmp_path, indexer):
        paths = self._write_files(tmp_path)
        shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)
        shuffled = _list_paths_in_order(
            indexer, paths, shuffle_config=shuffle_config, preserve_order=True
        )
        infos = [
            fi.path
            for fi in indexer.list_file_infos(
                pa.array(paths), filesystem=LocalFileSystem(), preserve_order=True
            )
        ]
        unshuffled = _list_paths_in_order(indexer, paths, preserve_order=True)

        assert infos == unshuffled
        assert infos != shuffled

    def test_unshuffled_listing_matches_file_info_order(self, tmp_path, indexer):
        paths = self._write_files(tmp_path)
        infos = [
            fi.path
            for fi in indexer.list_file_infos(
                pa.array(paths), filesystem=LocalFileSystem(), preserve_order=True
            )
        ]
        assert _list_paths_in_order(indexer, paths, preserve_order=True) == infos


class TestFooterIndexerFileShuffle:
    """Footer indexer shuffles files before footer-read batches."""

    def test_shuffled_file_infos_drive_footer_batches(self, tmp_path):
        from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
            FooterFileIndexer,
        )

        paths = []
        for i in range(8):
            path = tmp_path / f"f{i:02d}.parquet"
            path.write_bytes(b"x")
            paths.append(str(path))

        indexer = FooterFileIndexer(
            ignore_missing_paths=False,
            num_workers=1,
            footer_batch_size=3,
        )
        shuffle_config = FileShuffleConfig(seed=42, reseed_after_execution=False)
        shuffled = list(
            indexer._iter_file_infos_for_list(
                pa.array(paths),
                filesystem=LocalFileSystem(),
                preserve_order=True,
                shuffle_config=shuffle_config,
            )
        )
        unshuffled = list(
            indexer.list_file_infos(
                pa.array(paths),
                filesystem=LocalFileSystem(),
                preserve_order=True,
            )
        )
        assert [fi.path for fi in shuffled] != [fi.path for fi in unshuffled]
        assert [fi.path for fi in shuffled] == [
            fi.path for fi in _shuffle_file_infos(unshuffled, seed=42)
        ]

        batches = list(indexer._batches(iter(shuffled)))
        flattened = [p for batch in batches for p, _ in batch]
        assert flattened == [fi.path for fi in shuffled]
        assert len(batches) > 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
