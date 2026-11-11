"""ORC-specific contracts; generic scanner/packer behavior is tested alongside them."""

import io
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
from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    OrcStripeIndexer,
    OrcStripePartitioner,
    _orc_manifest,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data.context import DataContext
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
        indexer=OrcStripeIndexer(ignore_missing_paths=True),
    )


def sample(case, count=1):
    return FileManifest.construct_manifest(
        paths=[str(case.path)] * count,
        sizes=[case.layout.size] * count,
        chunk_metadatas=[None] * count,
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


class Input(io.BytesIO):
    def size(self):
        return len(self.getbuffer())


@pytest.mark.parametrize("no_index", [False, True], ids=["empty", "no-index"])
def test_optional_zero_footer_fields(no_index):
    # These uncompressed fixtures omit zero-valued protobuf fields.
    path = (
        Path(__file__).parent / "data" / ("no-index.orc" if no_index else "empty.orc")
    )
    expected = (
        pa.table({"id": range(16), "label": [str(i) for i in range(16)]})
        if no_index
        else pa.table({"id": pa.array([], type=pa.int64())})
    )
    native = orc.ORCFile(str(path))
    assert native.read().equals(expected)
    tail = cast(
        Any, _file_tail_message_type().FromString(native.reader.serialized_file_tail())
    )
    if no_index:
        assert native.row_index_stride == 0 and native.nstripes == 3
        assert all(
            not stripe.HasField("index_length") for stripe in tail.footer.stripes
        )
    else:
        assert native.nrows == 0 and not tail.footer.HasField("number_of_rows")

    indexer = OrcStripeIndexer(ignore_missing_paths=False)
    manifests = list(
        indexer.list_files(pa.array([str(path)]), filesystem=LocalFileSystem())
    )
    reader = OrcScanner(schema=native.schema).create_reader()
    batches = [batch for manifest in manifests for batch in reader.read(manifest)]
    assert sum(batch.num_rows for batch in batches) == expected.num_rows
    if no_index:
        assert len(manifests[0]) == 3
        assert pa.concat_tables(batches).equals(expected)


def test_layout_is_exact_metadata_only(case, monkeypatch):
    reader = orc.ORCFile(str(case.path))
    oracle = [reader.read_stripe(i).num_rows for i in range(reader.nstripes)]
    data_end = 3 + reader.content_length
    for method in ("read", "read_stripe"):
        monkeypatch.setattr(
            orc.ORCFile,
            method,
            Mock(side_effect=AssertionError("decode")),
            raising=False,
        )
    seed = metadata.read_orc_metadata(str(case.path), None)
    assert seed.stripes is not None
    assert [s.num_rows for s in seed.stripes] == oracle == [5, 7, 4]
    assert [s.row_offset for s in seed.stripes] == [0, 5, 12]
    offset = 3
    for stripe in seed.stripes:
        assert stripe.bytes_offset == offset
        offset += stripe.size_bytes
    assert offset == data_end
    assert seed.num_rows == 16


@pytest.mark.parametrize(
    "fault",
    [
        "gaps",
        "missing",
        "overlap",
        "oversize",
        "rows",
        "stripe_rows",
        "footer",
        "bytes",
    ],
)
def test_footer_layout_validation(case, fault):
    owner = orc.ORCFile(str(case.path))
    tail = _file_tail_message_type().FromString(owner.reader.serialized_file_tail())
    footer = cast(Any, tail).footer
    stripes = footer.stripes
    if fault == "gaps":
        for i, stripe in enumerate(stripes):
            stripe.offset += i * 17
    elif fault == "missing":
        stripes[0].ClearField("offset")
    elif fault == "overlap":
        stripes[1].offset = stripes[0].offset
    elif fault == "oversize":
        stripes[0].data_length = case.layout.size + 1
    elif fault == "rows":
        footer.ClearField("number_of_rows")
    elif fault == "stripe_rows":
        stripes[0].ClearField("number_of_rows")
    elif fault == "footer":
        tail.ClearField("footer")
    fake = SimpleNamespace(
        nrows=16,
        nstripes=3,
        reader=SimpleNamespace(
            serialized_file_tail=lambda: b"invalid"
            if fault == "bytes"
            else tail.SerializeToString()
        ),
    )
    if fault == "gaps":
        layout = metadata.read_orc_layout(
            cast(orc.ORCFile, fake), str(case.path), case.layout.size + 34
        )
        assert layout.stripes is not None
        assert [s.bytes_offset for s in layout.stripes] == [
            s.bytes_offset + i * 17 for i, s in enumerate(case.layout.stripes)
        ]
        assert [s.row_offset for s in layout.stripes] == [0, 5, 12]
    else:
        with pytest.raises(pa.ArrowInvalid, match="data.orc"):
            metadata.read_orc_layout(
                cast(orc.ORCFile, fake),
                str(case.path),
                case.layout.size,
                allow_unsupported=True,
            )


def test_bad_footer_closes_source_and_keeps_path(case):
    source = Input(case.path.read_bytes()[:8])
    with pytest.raises((pa.ArrowInvalid, OSError), match="data.orc"):
        metadata.read_orc_metadata(
            str(case.path), SimpleNamespace(open_input_file=lambda _: source)
        )
    assert source.closed


@pytest.mark.parametrize("missing", ["metadata", "decoder"])
def test_missing_api_freezes_whole_file_and_rejects_assignments(
    case, monkeypatch, missing
):
    assignment = selected(case)
    if missing == "metadata":
        reader_type = metadata._orc.ORCReader
        monkeypatch.setattr(metadata._orc, "ORCReader", lambda: reader_type())
    else:
        monkeypatch.setattr(orc.ORCFile, "read_stripe", None)
    source = OrcDatasourceV2([str(case.path)])
    indexer = source._get_file_indexer()
    partitioner = source.get_file_partitioner(hints=PartitionHints(0, 4096, 4))
    assert type(partitioner).__name__ == "RoundRobinPartitioner"
    reader = OrcScanner(schema=orc.ORCFile(str(case.path)).schema).create_reader()
    with pytest.raises(RuntimeError, match="requires Arrow stripe/footer"):
        list(reader.read(assignment))
    manifest = next(
        iter(
            indexer.list_files(pa.array([str(case.path)]), filesystem=LocalFileSystem())
        )
    )
    result = list(reader.read(manifest))
    assert pa.concat_tables(result)["id"].to_pylist() == list(range(16))


@pytest.mark.parametrize("stripe", [False, True], ids=["whole", "stripe"])
@pytest.mark.parametrize(
    "schema_kind",
    [
        "none",
        "empty",
        "synthesized",
        "partition",
        "partition_root",
        "partition_root_stored",
    ],
)
@pytest.mark.parametrize("empty_projection", [False, True])
def test_schema_fallback_keeps_physical_columns(
    case, monkeypatch, stripe, schema_kind, empty_projection
):
    root = schema_kind.startswith("partition_root")
    if root:
        table = orc.read_table(str(case.path))
        if schema_kind.endswith("stored"):
            table = table.append_column("year", pa.array(["2025"] * 16))
        path = write_orc_stripes(case.path.parent.parent / "root.orc", table)
        case = SimpleNamespace(
            path=path, layout=metadata.read_orc_metadata(str(path), None)
        )
    if not stripe:
        monkeypatch.setattr(orc.ORCFile, "read_stripe", None)
    elif empty_projection:
        monkeypatch.setattr(
            orc.ORCFile, "read_stripe", Mock(side_effect=AssertionError("decoded"))
        )
    schema = {
        "none": None,
        "empty": pa.schema([]),
        "synthesized": pa.schema([("path", pa.string())]),
    }.get(schema_kind, pa.schema([("year", pa.int32() if root else pa.string())]))
    batches = list(
        OrcScanner(
            schema=schema,
            columns=() if empty_projection else None,
            batch_size=2,
            partitioning=Partitioning(
                PartitionStyle.HIVE, field_names=["year"] if root else None
            ),
            synthesized_columns=(PathColumn(),),
        )
        .create_reader()
        .read(selected(case) if stripe else sample(case))
    )
    ids = [0, 1, 2, 3, 4, 12, 13, 14, 15] if stripe else list(range(16))
    assert sum(b.num_rows for b in batches) == len(ids)
    if empty_projection:
        assert all(
            not {"id", "label", "year", "path"}.intersection(b.column_names)
            for b in batches
        )
    else:
        names = {"synthesized": ["path", "id", "label", "year"]}.get(
            schema_kind,
            ["year", "id", "label", "path"]
            if schema_kind.startswith("partition")
            else ["id", "label", "year", "path"],
        )
        assert all(b.column_names == names for b in batches)
        assert all(
            b.schema.field("year").type == (pa.int32() if root else pa.string())
            for b in batches
        )
        year = 2025 if schema_kind.endswith("stored") else None if root else "2024"
        assert [r for b in batches for r in b.to_pylist()] == [
            dict(id=i, label=str(i), year=year, path=str(case.path)) for i in ids
        ]


@pytest.mark.parametrize("stripe", [False, True], ids=["whole", "stripe"])
@pytest.mark.parametrize("include_extra", [False, True])
def test_late_physical_columns_follow_schema(case, stripe, include_extra):
    fields = [("id", pa.int32())]
    if include_extra:
        fields.append(("label", pa.string()))
    fields.append(("missing", pa.string()))
    reader = OrcScanner(
        schema=pa.schema(fields), predicate=col("missing").is_null()
    ).create_reader()
    batches = list(reader.read(selected(case) if stripe else sample(case)))
    rows = [row for batch in batches for row in batch.to_pylist()]
    ids = [0, 1, 2, 3, 4, 12, 13, 14, 15] if stripe else list(range(16))
    assert all(batch.schema.field("id").type == pa.int32() for batch in batches)
    assert rows == [
        {"id": i, **({"label": str(i)} if include_extra else {}), "missing": None}
        for i in ids
    ]


@pytest.mark.parametrize("prelisted", [False, True])
@pytest.mark.parametrize("limit", [1, 6])
def test_listing_limit_stops_before_later_invalid_footer(case, prelisted, limit):
    invalid = case.path.with_name("invalid.orc")
    invalid.write_bytes(b"invalid ORC")
    infos = [
        FileInfo(str(case.path), case.layout.size),
        FileInfo(str(invalid), invalid.stat().st_size),
    ]

    def list_files(**kwargs):
        if prelisted:
            return list(case.indexer.list_files_from_file_infos(infos, **kwargs))
        return list(
            case.indexer.list_files(pa.array([info.path for info in infos]), **kwargs)
        )

    assert list_files(filesystem=LocalFileSystem(), limit=0) == []
    kwargs = dict(filesystem=LocalFileSystem(), limit=limit)
    manifests = list_files(**kwargs)
    assert [
        FileChunk.from_metadata(chunk).unit_ids
        for manifest in manifests
        for chunk in manifest.file_chunk_metadatas
    ] == ([(0,)] if limit == 1 else [(0,), (1,)])
    with pytest.raises((pa.ArrowInvalid, OSError), match="invalid.orc"):
        list_files(**kwargs, predicate=col("id") > 0)


@pytest.mark.parametrize("limit", [None, 6])
@pytest.mark.parametrize("seed", [0, None])
def test_prelisted_shuffle_excludes_files_before_shuffling(case, limit, seed):
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
    if limit is None:
        expected = [
            (str(path), (index,))
            for path in inputs
            if path != paths[1]
            for index in range(3)
            if (path, index) != (paths[0], 1)
        ]
        assert sorted(listed) == sorted(expected)


class Position(SynthesizedColumn):
    name, type, requires_read_unit_boundaries = "position", pa.int64(), True

    def __init__(self):
        self.seen = []

    def compute(self, position, num_rows):
        self.seen.append(position)
        return pa.array(range(position.rows_before, position.rows_before + num_rows))


def test_stripe_partitioner_mixes_whole_files_and_stripes():
    chunks = [
        FileChunk((0,), 5, 50).to_metadata(),
        None,
        FileChunk((1,), 7, 150).to_metadata(),
        FileChunk((2,), 4, 50).to_metadata(),
        None,
    ]
    manifest = _orc_manifest(
        ["a.orc", "b.orc", "a.orc", "a.orc", "c.orc"],
        [1000, 200, 1000, 1000, 30],
        chunks,
    )
    packer = OrcStripePartitioner(100)
    packer.add_input(manifest)
    packer.finalize()
    groups, manifests = [], []
    while packer.has_partition():
        output = packer.next_partition()
        manifests.append(output)
        groups.append(
            [
                (str(path), None if md is None else tuple(md["unit_ids"]))
                for path, md in zip(output.paths, output.file_chunk_metadatas)
            ]
        )
    assert groups == [
        [("b.orc", None)],
        [("a.orc", (1,))],
        [("a.orc", (2,))],
        [("c.orc", None), ("a.orc", (0,))],
    ]
    assert len(FileManifest.concat(manifests)) == len(chunks)


@pytest.mark.parametrize("retry", [False, True])
def test_stripe_positions_filter_and_retry(case, monkeypatch, retry):
    import sys

    monkeypatch.setitem(sys.modules, "pyorc", None)
    observer = Position()
    scanner = OrcScanner(
        schema=orc.ORCFile(str(case.path)).schema,
        batch_size=2,
        target_block_size=1,
        predicate=(col("id") == 1) | (col("id") == 3) | (col("id") >= 12),
        synthesized_columns=(observer,),
    )
    reader = scanner.create_reader()
    original, failed = reader._iter_stripe_tables, False

    def flaky(*args):
        nonlocal failed
        for i, table in enumerate(original(*args)):
            if retry and i == 1 and not failed:
                failed = True
                raise OSError("transient ORC")
            yield table

    monkeypatch.setattr(reader, "_iter_stripe_tables", flaky)
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["transient ORC"]
    )
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)
    packer = OrcStripePartitioner(1 << 30)
    for index in (0, 2):
        packer.add_input(selected(case, (index,)))
    packer.finalize()
    manifest = packer.next_partition()
    assert FileChunk.from_metadata(manifest.file_chunk_metadatas[0]).unit_ids == (0, 2)
    batches = list(reader.read(manifest))
    assert all(b.num_rows <= 2 for b in batches)
    assert reader._sampled_batch_size == 1
    assert [r["id"] for b in batches for r in b.to_pylist()] == [
        1,
        3,
        12,
        13,
        14,
        15,
    ]
    cursors = {0: 0, 2: 0}
    assert len(batches) == len(observer.seen) and failed == retry
    for batch, pos in zip(batches, observer.seen):
        i = pos.unit.index
        assert pos.unit == ReadUnit(
            id=metadata.orc_read_unit_id(str(case.path), i),
            source=str(case.path),
            index=i,
            count=3,
            num_rows={0: 5, 2: 4}[i],
        )
        assert (pos.unit_start_row, pos.rows_before) == (
            {0: 0, 2: 12}[i],
            cursors[i],
        )
        cursors[i] += batch.num_rows


@pytest.mark.parametrize("mode", ["normal", "open", "close", "limit", "error"])
def test_stripe_input_lifetime(case, monkeypatch, mode):
    sources, attempts = [], []

    def opened(path):
        attempts.append(path)
        if mode == "open" and len(attempts) == 1:
            raise OSError("transient ORC")
        source = Input(case.path.read_bytes())
        sources.append(source)
        return source

    reader = OrcScanner(
        schema=orc.ORCFile(str(case.path)).schema,
        batch_size=2,
        limit=1 if mode == "limit" else None,
    ).create_reader()
    reader._filesystem = SimpleNamespace(open_input_file=opened)
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["transient ORC"]
    )
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)
    if mode == "error":
        monkeypatch.setattr(
            reader, "_iter_stripe_tables", Mock(side_effect=ValueError("decode"))
        )
    iterator = reader._read_fragment_batches(
        ds.dataset(str(case.path), format="orc"),
        dict(columns=None, filter=None, batch_size=2),
        selected(case),
    )
    if mode == "error":
        with pytest.raises(ValueError, match="decode"):
            list(iterator)
    elif mode == "close":
        next(iterator)
        iterator.close()
    else:
        assert sum(b.num_rows for b, _ in iterator) == (2 if mode == "limit" else 9)
    assert len(sources) == 1 and sources[0].closed
    assert len(attempts) == (2 if mode == "open" else 1)


@pytest.mark.parametrize("limit", [10, None])
def test_whole_file_boundary_mixed_limit_and_manifest_compatibility(case, limit):
    observer = Position()
    schema = orc.ORCFile(str(case.path)).schema
    other = case.path.parent / "single.orc"
    write_orc_stripes(
        other, pa.table({"id": range(7), "label": [str(i) for i in range(7)]}), (7,)
    )
    whole = _orc_manifest(
        paths=[str(other)], sizes=[other.stat().st_size], chunk_metadatas=[None]
    )
    mixed = FileManifest.concat([selected(case), whole])
    batches = list(
        OrcScanner(
            schema=schema,
            batch_size=2,
            limit=limit,
            synthesized_columns=(observer,),
        )
        .create_reader()
        .read(mixed)
    )
    expected = [0, 1, 2, 3, 4, 12, 13, 14, 15] + list(range(7))
    assert [r["id"] for b in batches for r in b.to_pylist()] == expected[:limit]
    positions = [p for p in observer.seen if p.unit.source == str(other)]
    assert [p.rows_before for p in positions] == ([0] if limit else [0, 2, 4, 6])
    for position in positions:
        assert position.unit == ReadUnit(
            id=str(other), source=str(other), index=0, count=1, num_rows=7
        )
        assert position.unit_start_row == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-q", __file__]))
