"""ORC-specific contracts; generic scanner/packer behavior is tested alongside them."""

import io
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from pyarrow import orc
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc import orc_metadata as metadata
from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    _file_tail_message_type,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    OrcStripeIndexer,
    _orc_manifest,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col
from ray.data.tests.datasource.orc_decode_trace import (
    write_orc_stripes,
)


@pytest.fixture
def case(tmp_path):
    path = tmp_path / "year=2024" / "data.orc"
    path.parent.mkdir()
    write_orc_stripes(
        path, pa.table({"id": range(16), "label": [str(i) for i in range(16)]})
    )
    layout = metadata.read_orc_metadata(str(path), None)
    return SimpleNamespace(
        path=path,
        layout=layout,
        indexer=OrcStripeIndexer(ignore_missing_paths=True, min_split_size=0),
    )


def selected(case, ids=(0, 2)):
    rows = tuple((5, 7, 4)[i] for i in ids)
    sizes = tuple(case.layout.stripes[i].size_bytes for i in ids)
    run = FileChunk(ids, sum(rows), sum(sizes), unit_rows=rows, unit_sizes=sizes)
    return _orc_manifest(
        paths=[str(case.path)],
        sizes=[case.layout.size],
        chunk_metadatas=[run.to_metadata()],
    )


def read_ids(case, manifests, **kwargs):
    reader = OrcScanner(
        schema=orc.ORCFile(str(case.path)).schema, batch_size=2, **kwargs
    ).create_reader()
    return [
        row["id"]
        for manifest in manifests
        for table in reader.read(manifest)
        for row in table.to_pylist()
    ]


class Input(io.BytesIO):
    def size(self):
        return len(self.getbuffer())


@pytest.mark.parametrize(
    "filename,nrows,stripes",
    [
        pytest.param("empty.orc", 0, [], id="empty"),
        pytest.param("no-index.orc", 16, [(5, 0), (7, 5), (4, 12)], id="no-index"),
    ],
)
def test_footer_omits_optional_zero_fields(filename, nrows, stripes):
    path = Path(__file__).parent / "data" / filename
    native = orc.ORCFile(str(path))
    tail = cast(
        Any, _file_tail_message_type().FromString(native.reader.serialized_file_tail())
    )
    assert native.nrows == nrows
    assert tail.footer.HasField("number_of_rows") is bool(nrows)
    assert all(not stripe.HasField("index_length") for stripe in tail.footer.stripes)
    layout = metadata.read_orc_metadata(str(path), None)
    assert layout.stripes is not None
    assert [
        (stripe.num_rows, stripe.row_offset) for stripe in layout.stripes
    ] == stripes
    assert sum(stripe.size_bytes for stripe in layout.stripes) == native.content_length
    indexer = OrcStripeIndexer(ignore_missing_paths=False, min_split_size=0)
    manifest = next(
        indexer.list_files(pa.array([str(path)]), filesystem=LocalFileSystem())
    )
    assert len(manifest) == max(1, len(stripes))
    reader = OrcScanner(schema=native.schema).create_reader()
    assert [row for batch in reader.read(manifest) for row in batch.to_pylist()] == [
        {"id": i, "label": str(i)} for i in range(nrows)
    ]


def test_missing_stripe_estimate_falls_back_or_rejects_resume(case, monkeypatch):
    monkeypatch.setattr(
        "ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer.read_orc_metadata",
        lambda *_: replace(case.layout, estimated_max_stripe_bytes=None),
    )
    paths = pa.array([str(case.path)])
    manifest = next(case.indexer.list_files(paths, filesystem=LocalFileSystem()))
    assert manifest.file_chunk_metadatas.tolist() == [None]
    monkeypatch.setattr(orc.ORCFile, "read_stripe", Mock(side_effect=AssertionError))
    assert read_ids(case, [manifest]) == list(range(16))
    with pytest.raises(RuntimeError, match="Cannot safely resume"):
        list(
            case.indexer.list_files(
                paths,
                filesystem=LocalFileSystem(),
                excluded_read_unit_ids={metadata.orc_read_unit_id(str(case.path), 1)},
            )
        )


def test_small_file_skips_footer_and_missing_layout_rejects_exclusion(
    case, monkeypatch
):
    name = "ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer.read_orc_metadata"
    monkeypatch.setattr(name, Mock(side_effect=AssertionError("footer read")))
    indexer = OrcStripeIndexer(ignore_missing_paths=False)
    paths = pa.array([str(case.path)])
    manifest = next(indexer.list_files(paths, filesystem=LocalFileSystem()))
    assert manifest.file_chunk_metadatas.tolist() == [None]
    monkeypatch.setattr(orc.ORCFile, "read_stripe", Mock(side_effect=AssertionError))
    assert read_ids(case, [manifest], predicate=col("id") >= 12, limit=2) == [12, 13]
    monkeypatch.setattr(name, lambda *_: replace(case.layout, stripes=None))
    with pytest.raises(RuntimeError, match="without layout"):
        list(
            indexer.list_files(
                paths,
                filesystem=LocalFileSystem(),
                excluded_read_unit_ids={metadata.orc_read_unit_id(str(case.path), 1)},
            )
        )


def test_unknown_size_rejects_stripe_resume(case):
    info = FileInfo(str(case.path), None)
    excluded = {metadata.orc_read_unit_id(str(case.path), 1)}
    manifest = FileManifest.construct_manifest(
        paths=[info.path], sizes=[None], chunk_metadatas=[None]
    )
    paths = [
        case.indexer.list_files_from_file_infos(
            [info], filesystem=LocalFileSystem(), excluded_read_unit_ids=excluded
        ),
        case.indexer._list_chunks([manifest], LocalFileSystem(), None, None, excluded),
    ]
    for manifests in paths:
        with pytest.raises(
            RuntimeError, match="Cannot resume ORC stripes without layout"
        ):
            list(manifests)


def test_corrupt_footer_fails_and_closes_input(case):
    source = Input(case.path.read_bytes()[:8])
    with pytest.raises((pa.ArrowInvalid, OSError), match="data.orc"):
        metadata.read_orc_metadata(
            str(case.path), SimpleNamespace(open_input_file=lambda _: source)
        )
    assert source.closed

    owner = orc.ORCFile(str(case.path))
    data = owner.reader.serialized_file_tail()
    mutations = (
        lambda footer: footer.stripes[0].ClearField("number_of_rows"),
        lambda footer: setattr(footer.stripes[1], "offset", footer.stripes[0].offset),
    )
    for mutate in mutations:
        tail = cast(Any, _file_tail_message_type().FromString(data))
        mutate(tail.footer)
        reader = cast(
            orc.ORCFile,
            SimpleNamespace(
                nrows=16,
                nstripes=3,
                reader=SimpleNamespace(serialized_file_tail=tail.SerializeToString),
            ),
        )
        with pytest.raises(pa.ArrowInvalid, match="data.orc"):
            metadata.read_orc_layout(reader, str(case.path), case.layout.size)


def test_missing_stripe_api_rejects_assignment(case, monkeypatch):
    assignment = selected(case)
    reader = OrcScanner(schema=orc.ORCFile(str(case.path)).schema).create_reader()
    reader_type = metadata._orc.ORCReader
    for target, attribute, replacement in (
        (metadata._orc, "ORCReader", lambda: reader_type()),
        (orc.ORCFile, "read_stripe", None),
    ):
        with monkeypatch.context() as patch:
            patch.setattr(target, attribute, replacement)
            with pytest.raises(RuntimeError, match="requires Arrow stripe/footer"):
                list(reader.read(assignment))


def test_stripe_partition_only_schema_keeps_root_columns(case, tmp_path):
    root = tmp_path / "root.orc"
    root.write_bytes(case.path.read_bytes())
    source = SimpleNamespace(path=root, layout=case.layout)
    scanner = OrcScanner(
        schema=pa.schema([("year", pa.int32())]),
        batch_size=2,
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
        synthesized_columns=(PathColumn(),),
    )
    batches = list(scanner.create_reader().read(selected(source)))
    assert all(batch.schema.field("year").type == pa.int32() for batch in batches)
    assert [row for batch in batches for row in batch.to_pylist()] == [
        dict(id=i, label=str(i), year=None, path=str(source.path))
        for i in (*range(5), *range(12, 16))
    ]


@pytest.mark.parametrize(
    "limit,expected_chunks",
    [pytest.param(1, [None], id="whole"), pytest.param(6, [(0,), (1,)], id="stripes")],
)
def test_listing_limit_stops_before_later_invalid_footer(case, limit, expected_chunks):
    invalid = case.path.with_name("invalid.orc")
    invalid.write_bytes(b"invalid ORC")
    infos = [
        FileInfo(str(case.path), case.layout.size),
        FileInfo(str(invalid), invalid.stat().st_size),
    ]
    listings = (
        lambda **kwargs: case.indexer.list_files_from_file_infos(infos, **kwargs),
        lambda **kwargs: case.indexer.list_files(
            pa.array([info.path for info in infos]), **kwargs
        ),
    )
    for list_files in listings:
        assert list(list_files(filesystem=LocalFileSystem(), limit=0)) == []
        kwargs = dict(filesystem=LocalFileSystem(), limit=limit)
        manifests = list(list_files(**kwargs))
        assert [
            None if chunk is None else FileChunk.from_metadata(chunk).unit_ids
            for manifest in manifests
            for chunk in manifest.file_chunk_metadatas
        ] == expected_chunks
        with pytest.raises((pa.ArrowInvalid, OSError), match="invalid.orc"):
            list(list_files(**kwargs, predicate=col("id") > 0))


@pytest.mark.parametrize(
    "limit,expected_count",
    [pytest.param(None, 10, id="all"), pytest.param(6, 2, id="limit")],
)
@pytest.mark.parametrize(
    "seed", [pytest.param(0, id="fixed"), pytest.param(None, id="auto")]
)
def test_prelisted_shuffle_excludes_files_before_shuffling(
    case, limit, expected_count, seed
):
    paths = [case.path.parent / f"{name}.orc" for name in "abcd"]
    for path in paths:
        path.write_bytes(case.path.read_bytes())
    inputs = paths + [paths[0]]
    infos = [FileInfo(str(path), case.layout.size) for path in inputs]
    excluded = {str(paths[1]), metadata.orc_read_unit_id(str(paths[0]), 1)}
    kwargs = dict(
        filesystem=LocalFileSystem(),
        shuffle_config=FileShuffleConfig(seed=seed),
        excluded_read_unit_ids=excluded,
        limit=limit,
    )

    def rows(manifests):
        return [
            (str(path), FileChunk.from_metadata(chunk).unit_ids)
            for manifest in manifests
            for path, chunk in zip(manifest.paths, manifest.file_chunk_metadatas)
        ]

    listed = rows(case.indexer.list_files(pa.array([str(p) for p in inputs]), **kwargs))
    prelisted = rows(case.indexer.list_files_from_file_infos(infos, **kwargs))
    assert prelisted == listed
    assert rows(case.indexer.list_files_from_file_infos(infos, **kwargs)) == prelisted
    assert str(paths[1]) not in {path for path, _ in listed}
    expected: list[tuple[str, tuple[int, ...]]] = [
        (str(path), (index,))
        for path in inputs
        if path != paths[1]
        for index in range(3)
        if (path, index) != (paths[0], 1)
    ]
    assert len(listed) == expected_count
    assert not (Counter(listed) - Counter(expected))


def test_unordered_listing_replay_is_stable(case, monkeypatch):
    import threading

    from ray.data._internal.datasource_v2.common import non_sampling_file_indexer

    other = case.path.with_name("other.orc")
    other.write_bytes(case.path.read_bytes())
    paths = [str(case.path), str(other), str(case.path)]
    original = non_sampling_file_indexer._get_path_contents
    ready = threading.Event()
    delayed = [paths[0]]

    def list_path(path, *args, **kwargs):
        if path == delayed[0]:
            assert ready.wait(timeout=3)
        result = original(path, *args, **kwargs)
        if path != delayed[0]:
            ready.set()
        return result

    monkeypatch.setattr(non_sampling_file_indexer, "_get_path_contents", list_path)
    indexer = OrcStripeIndexer(
        ignore_missing_paths=False,
        min_split_size=0,
        num_workers=3,
        max_paths_per_output=1,
    )
    expected = [(path, (stripe,)) for path in paths for stripe in range(3)]
    for path in paths[:2]:
        delayed[0] = path
        ready.clear()
        manifests = indexer.list_files(
            pa.array(paths), filesystem=LocalFileSystem(), preserve_order=False
        )
        assert [
            (str(p), tuple(chunk["unit_ids"]))
            for m in manifests
            for p, chunk in zip(m.paths, m.file_chunk_metadatas)
        ] == expected


def test_stripe_positions_filter_and_retry(case, monkeypatch, restore_data_context):
    predicate = (col("id") == 1) | (col("id") == 3) | (col("id") >= 12)
    scanner = OrcScanner(
        schema=orc.ORCFile(str(case.path)).schema,
        batch_size=2,
        predicate=predicate,
    )
    reader = scanner.create_reader()
    original, failed = reader._iter_stripe_tables, False

    def flaky(*args):
        nonlocal failed
        for i, table in enumerate(original(*args)):
            if i == 1 and not failed:
                failed = True
                raise OSError("transient ORC")
            yield table

    monkeypatch.setattr(reader, "_iter_stripe_tables", flaky)
    restore_data_context.retried_io_errors = ["transient ORC"]
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)
    manifest = FileManifest.concat([selected(case, (0,)), selected(case, (2,))])
    batches = list(
        reader._read_fragment_batches(
            ds.dataset(str(case.path), format="orc"),
            dict(columns=None, filter=predicate.to_pyarrow(), batch_size=2),
            manifest,
        )
    )
    assert failed
    assert [row["id"] for batch, _ in batches for row in batch.to_pylist()] == [
        1,
        3,
        12,
        13,
        14,
        15,
    ]
    assert [
        (pos.unit.index, pos.unit_start_row, pos.rows_before, pos.unit.num_rows)
        for _, pos in batches
    ] == [(0, 0, 0, 5), (0, 0, 1, 5), (2, 12, 0, 4), (2, 12, 2, 4)]
    assert all(
        pos.unit.id == metadata.orc_read_unit_id(str(case.path), pos.unit.index)
        for _, pos in batches
    )


@pytest.fixture
def tracked_stripe_input(case, monkeypatch, restore_data_context):
    restore_data_context.retried_io_errors = ["transient ORC"]
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)

    def create(limit=None, manifest=None):
        source = Input(case.path.read_bytes())
        opened = Mock(return_value=source)
        reader = OrcScanner(
            schema=orc.ORCFile(str(case.path)).schema, batch_size=2, limit=limit
        ).create_reader()
        reader._filesystem = SimpleNamespace(open_input_file=opened)
        batches = reader._read_fragment_batches(
            ds.dataset(str(case.path), format="orc"),
            dict(columns=None, filter=None, batch_size=2),
            selected(case) if manifest is None else manifest,
        )
        return reader, batches, source, opened

    return create


def test_stripe_open_retry_and_reuse(case, tracked_stripe_input):
    manifest = FileManifest.concat([selected(case, (0,)), selected(case, (2,))])
    _, batches, source, opened = tracked_stripe_input(manifest=manifest)
    opened.side_effect = [OSError("transient ORC"), source]
    assert sum(batch.num_rows for batch, _ in batches) == 9
    assert opened.call_count == 2 and source.closed


def test_stripe_limit_and_early_close_close_input(tracked_stripe_input):
    _, batches, source, opened = tracked_stripe_input()
    next(batches)
    batches.close()
    assert opened.call_count == 1 and source.closed
    _, batches, source, opened = tracked_stripe_input(limit=1)
    assert sum(batch.num_rows for batch, _ in batches) == 2
    assert opened.call_count == 1 and source.closed
    _, batches, _, opened = tracked_stripe_input(limit=0)
    assert list(batches) == []
    opened.assert_not_called()


def test_stripe_decoder_error_closes_input(tracked_stripe_input, monkeypatch):
    reader, batches, source, opened = tracked_stripe_input()
    monkeypatch.setattr(
        reader, "_iter_stripe_tables", Mock(side_effect=ValueError("decode"))
    )
    with pytest.raises(ValueError, match="decode"):
        list(batches)
    assert opened.call_count == 1 and source.closed


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-q", __file__]))
