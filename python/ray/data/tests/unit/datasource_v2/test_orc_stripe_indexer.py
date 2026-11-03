"""ORC-specific contracts; generic scanner/packer behavior is tested alongside them."""

import io
import pickle
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
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import FileDataSourceV2
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
        indexer=OrcStripeIndexer(ignore_missing_paths=True, seeds=[layout]),
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
    return FileManifest.construct_manifest(
        paths=[str(case.path)],
        sizes=[case.layout.size],
        chunk_metadatas=[run.to_metadata()],
    )


class Input(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.reads = []

    def size(self):
        return len(self.getbuffer())

    def read(self, n=-1):
        self.reads.append((self.tell(), n))
        return super().read(n)


def test_layout_is_exact_metadata_only_and_reuses_owner(case, monkeypatch):
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
    source = Input(case.path.read_bytes())
    original = metadata.read_orc_layout

    def inspect(*args, **kwargs):
        reads = list(source.reads)
        result = original(*args, **kwargs)
        assert source.reads == reads
        return result

    monkeypatch.setattr(metadata, "read_orc_layout", inspect)
    schema, seed = metadata.read_orc_schema_and_layout(
        str(case.path), SimpleNamespace(open_input_file=lambda _: source)
    )
    assert schema.names == ["id", "label"] and source.closed and source.reads
    assert [s.num_rows for s in seed.stripes] == oracle == [5, 7, 4]
    assert [s.row_offset for s in seed.stripes] == [0, 5, 12]
    offset = 3
    for stripe in seed.stripes:
        assert stripe.bytes_offset == offset
        offset += stripe.size_bytes
    assert offset == data_end
    assert seed.num_rows == 16


def test_layout_allows_gaps_between_stripes(case):
    reader = orc.ORCFile(str(case.path))
    tail = _file_tail_message_type().FromString(reader.reader.serialized_file_tail())
    for i, stripe in enumerate(cast(Any, tail).footer.stripes):
        stripe.offset += i * 17
    owner = SimpleNamespace(
        nrows=16,
        nstripes=3,
        reader=SimpleNamespace(serialized_file_tail=tail.SerializeToString),
    )
    layout = metadata.read_orc_layout(
        cast(orc.ORCFile, owner), str(case.path), case.layout.size + 34
    )
    assert layout.stripes is not None
    assert [s.bytes_offset for s in layout.stripes] == [
        s.bytes_offset + i * 17 for i, s in enumerate(case.layout.stripes)
    ]
    assert [s.row_offset for s in layout.stripes] == [0, 5, 12]


@pytest.mark.parametrize("fault", ["missing", "overlap", "rows", "footer", "bytes"])
def test_invalid_footer_is_not_a_capability_fallback(case, fault):
    owner = orc.ORCFile(str(case.path))
    tail = _file_tail_message_type().FromString(owner.reader.serialized_file_tail())
    footer = cast(Any, tail).footer
    stripes = footer.stripes
    if fault == "missing":
        stripes[0].ClearField("offset")
    elif fault == "overlap":
        stripes[1].offset = stripes[0].offset
    elif fault == "rows":
        footer.ClearField("number_of_rows")
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
    with pytest.raises(pa.ArrowInvalid, match="data.orc"):
        metadata.read_orc_layout(
            cast(orc.ORCFile, fake),
            str(case.path),
            case.layout.size,
            allow_unsupported=True,
        )


@pytest.mark.parametrize(
    "read", [metadata.read_orc_metadata, metadata.read_orc_schema_and_layout]
)
def test_bad_footer_closes_source_and_keeps_path(case, read):
    source = Input(case.path.read_bytes()[:8])
    with pytest.raises((pa.ArrowInvalid, OSError), match="data.orc"):
        read(str(case.path), SimpleNamespace(open_input_file=lambda _: source))
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
    _, prepared, partitioner = source.prepare_read(
        sample(case), indexer, PartitionHints(0, 4096, 4)
    )
    assert prepared is indexer and type(partitioner).__name__ == "RoundRobinPartitioner"
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
@pytest.mark.parametrize("schema_kind", ["none", "empty", "synthesized", "partition"])
@pytest.mark.parametrize("empty_projection", [False, True])
def test_schema_fallback_keeps_physical_columns(
    case, monkeypatch, stripe, schema_kind, empty_projection
):
    if not stripe:
        monkeypatch.setattr(orc.ORCFile, "read_stripe", None)
    schema = {
        "none": None,
        "empty": pa.schema([]),
        "synthesized": pa.schema([("path", pa.string())]),
        "partition": pa.schema([("year", pa.string())]),
    }[schema_kind]
    batches = list(
        OrcScanner(
            schema=schema,
            columns=() if empty_projection else None,
            batch_size=2,
            partitioning=Partitioning(PartitionStyle.HIVE),
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
        names = {
            "synthesized": ["path", "id", "label", "year"],
            "partition": ["year", "id", "label", "path"],
        }.get(schema_kind, ["id", "label", "year", "path"])
        assert all(b.column_names == names for b in batches)
        assert [r for b in batches for r in b.to_pylist()] == [
            dict(id=i, label=str(i), year="2024", path=str(case.path)) for i in ids
        ]


@pytest.mark.parametrize("prelisted", [False, True])
@pytest.mark.parametrize("ordered", [False, True])
def test_listing_identity_exclusion_limit_shuffle_and_raw_sample(
    case, prelisted, ordered
):
    kwargs = dict(filesystem=LocalFileSystem(), preserve_order=ordered)
    infos = [FileInfo(str(case.path), case.layout.size)] * 2

    def emit(**extra):
        values = (
            case.indexer.list_files_from_file_infos(infos, **kwargs, **extra)
            if prelisted
            else case.indexer.list_files(
                pa.array([str(case.path.parent), str(case.path)]), **kwargs, **extra
            )
        )
        return list(values)

    def chunk(blocks):
        return [
            FileChunk.from_metadata(m)
            for block in blocks
            for m in block.file_chunk_metadatas
        ]

    assert chunk(emit())[0].unit_ids == (0, 1, 2)
    assert chunk(
        emit(excluded_read_unit_ids={metadata.orc_read_unit_id(str(case.path), 1)})
    )[0].unit_ids == (0, 2)
    assert emit(excluded_read_unit_ids={str(case.path)}) == []
    assert chunk(emit(limit=1))[0].unit_ids == (0,)
    assert chunk(emit(limit=1, predicate=col("id") == 15))[0].unit_ids == (0, 1, 2)
    assert emit(limit=0) == []
    config = FileShuffleConfig(seed=66999)
    assert [b.as_block().to_pylist() for b in emit(shuffle_config=config)] == [
        b.as_block().to_pylist() for b in emit(shuffle_config=config)
    ]
    raw = case.indexer.as_whole_file_indexer()
    assert len(list(raw.list_files(pa.array([str(case.path)]), **kwargs))) == 1


def test_bootstrap_is_bounded_read_local_and_budget_is_frozen(case, monkeypatch):
    import ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 as module

    source = OrcDatasourceV2([str(case.path)])
    before = vars(source).copy()
    hints = PartitionHints(0, 1 << 30, 4)
    budgets = []
    for count in (1, 15, 16, 17):
        first = source.prepare_read(sample(case, count), case.indexer, hints)
        second = source.prepare_read(sample(case, count), case.indexer, hints)
        assert first[1] is not second[1] and vars(source) == before
        assert len(cast(OrcStripeIndexer, first[1])._seeds) == 1
        budgets.append(cast(OrcStripePartitioner, first[2])._budget)
    assert budgets[0] == budgets[1] < budgets[2] == budgets[3]
    monkeypatch.setattr(module, "_MAX_SEED_BYTES", 0)
    assert (
        cast(
            OrcStripeIndexer, source.prepare_read(sample(case), case.indexer, hints)[1]
        )._seeds
        == ()
    )
    stub = SimpleNamespace(infer_schema=Mock(return_value=pa.schema([])))
    assert FileDataSourceV2.prepare_read(
        cast(FileDataSourceV2, stub), None, case.indexer, hints
    )[1:] == (
        case.indexer,
        None,
    )
    stub.infer_schema.assert_called_once_with(None)


class Position(SynthesizedColumn):
    name, type, requires_read_unit_boundaries = "position", pa.int64(), True

    def __init__(self):
        self.seen = []

    def compute(self, position, num_rows):
        self.seen.append(position)
        return pa.array(range(position.rows_before, position.rows_before + num_rows))


@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("split", [False, True])
@pytest.mark.parametrize("retry", [False, True])
def test_stripe_positions_filter_retry_and_group_conservation(
    case, monkeypatch, ordered, split, retry
):
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", ordered
    )
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
    groups = [selected(case)]
    if split:
        packer = OrcStripePartitioner(PartitionHints(0, 1, 1))
        packer.add_input(groups[0])
        packer.finalize()
        groups = []
        while packer.has_partition():
            groups.append(packer.next_partition())
    batches = [b for group in groups for b in reader.read(group)]
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


@pytest.mark.parametrize(
    "columns,predicate,expected",
    [
        (None, col("id") >= 3, [3, 4, 12, 13, 14, 15]),
        (
            ["id", "missing", "year"],
            col("missing").is_null(),
            [0, 1, 2, 3, 4, 12, 13, 14, 15],
        ),
        ([], None, list(range(9))),
        (["path"], None, list(range(9))),
    ],
)
def test_stripe_schema_projection_zero_columns_and_partitions(
    case, monkeypatch, columns, predicate, expected
):
    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("label", pa.string()),
            ("missing", pa.string()),
            ("year", pa.string()),
        ]
    )
    read_stripe = orc.ORCFile.read_stripe
    decoded = []

    def decode(owner, index, columns=None):
        decoded.append((index, columns))
        return read_stripe(owner, index, columns=columns)

    monkeypatch.setattr(orc.ORCFile, "read_stripe", decode)
    result = list(
        OrcScanner(
            schema=schema,
            columns=tuple(columns) if columns is not None else None,
            predicate=predicate,
            batch_size=2,
            partitioning=Partitioning(PartitionStyle.HIVE),
            synthesized_columns=(PathColumn(),),
        )
        .create_reader()
        .read(selected(case))
    )
    assert sum(b.num_rows for b in result) == len(expected)
    rows = [r for b in result for r in b.to_pylist()]
    if columns in ([], ["path"]):
        assert decoded == []
        if columns:
            assert all(r["path"] == str(case.path) for r in rows)
    else:
        assert [index for index, _ in decoded] == [0, 2]
        assert [r["id"] for r in rows] == expected
        assert all(b.schema.field("id").type == pa.int32() for b in result)
        assert all(r["missing"] is None and r["year"] == "2024" for r in rows)


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


@pytest.mark.parametrize("decoded", [False, True])
def test_stripe_pickle_guard_precedes_filter(case, monkeypatch, decoded):
    from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType

    schema = pa.schema([("id", pa.int64()), ("label", ArrowPythonObjectType())])
    unsafe = pa.RecordBatch.from_arrays(
        [
            pa.array([0]),
            pa.ExtensionArray.from_storage(
                schema.field("label").type,
                pa.array([b"must not unpickle"], type=pa.large_binary()),
            ),
        ],
        schema=schema,
    )

    owner = SimpleNamespace(
        schema=orc.ORCFile(str(case.path)).schema if decoded else schema,
        read_stripe=Mock(return_value=unsafe),
    )
    access = Mock(side_effect=AssertionError("unsafe table accessed"))
    monkeypatch.setattr(ds, "dataset", access)
    monkeypatch.setattr(
        "ray.data._internal.object_extensions.arrow.pickle.load", access
    )
    reader = OrcScanner(schema=owner.schema, predicate=col("id") < 0).create_reader()
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            reader._iter_stripe_tables(
                owner,
                str(case.path),
                case.layout.stripes[0],
                dict(columns=None, filter=None, batch_size=2),
            )
        )
    assert owner.read_stripe.call_count == int(decoded)
    access.assert_not_called()


@pytest.mark.parametrize("limit", [10, None])
def test_whole_file_boundary_mixed_limit_and_manifest_compatibility(case, limit):
    observer = Position()
    schema = orc.ORCFile(str(case.path)).schema
    other = case.path.parent / "single.orc"
    write_orc_stripes(
        other, pa.table({"id": range(7), "label": [str(i) for i in range(7)]}), (7,)
    )
    whole = FileManifest.construct_manifest(
        paths=[str(other)], sizes=[other.stat().st_size], chunk_metadatas=[None]
    )
    mixed = FileManifest.concat([selected(case), whole])
    assert pickle.loads(pickle.dumps(mixed)).as_block().equals(mixed.as_block())
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
