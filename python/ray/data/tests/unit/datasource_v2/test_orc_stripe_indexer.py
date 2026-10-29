"""Physical layout, byte reuse, deterministic manifests and stripe contracts."""

import importlib
import io
import pickle
from dataclasses import replace
from functools import reduce
from pathlib import Path
from types import SimpleNamespace
from typing import Generator, NoReturn, Protocol, cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyorc
import pytest
from pyarrow import orc
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
    OrcDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    OrcLayoutSeed,
    _RangeCache,
    orc_read_unit_id,
    pyorc_available,
    read_orc_metadata,
    read_orc_schema_and_layout,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    OrcStripeIndexer,
    OrcStripePartitioner,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.file_partitioner import PartitionHints
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


class _IntermediateFooterWriter(Protocol):
    """The native writer exposes this method, but its stubs omit it."""

    def write_intermediate_footer(self) -> object:
        ...


def _write(path, counts=(5, 7, 4)):
    with path.open("wb") as stream:
        writer = pyorc.Writer(
            stream,
            "struct<id:bigint,label:string>",
            batch_size=1,
            stripe_size=1 << 30,
            compression=pyorc.CompressionKind.NONE,
        )
        row = 0
        for count in counts:
            writer.writerows((i, f"row-{i}") for i in range(row, row + count))
            row += count
            cast(_IntermediateFooterWriter, writer).write_intermediate_footer()
        writer.close()
    # Independent fixture acceptance, not the adapter's own output as oracle.
    with path.open("rb") as stream:
        assert [len(stripe) for stripe in pyorc.Reader(stream).iter_stripes()] == list(
            counts
        )
    assert orc.ORCFile(str(path)).nrows == sum(counts)
    return pa.table(
        {
            "id": list(range(sum(counts))),
            "label": [f"row-{i}" for i in range(sum(counts))],
        }
    )


def _indexer(*paths):
    seeds = [
        OrcLayoutSeed(
            str(path), path.stat().st_size, read_orc_metadata(str(path), None)
        )
        for path in paths
    ]
    return OrcStripeIndexer(ignore_missing_paths=False, seeds=seeds)


def _list(path, **kwargs):
    return list(
        _indexer(path).list_files(
            pa.array([str(path)]), filesystem=LocalFileSystem(), **kwargs
        )
    )


def _chunks(manifests):
    return [
        FileChunk.from_metadata(md)
        for manifest in manifests
        for md in manifest.file_chunk_metadatas
        if md is not None
    ]


def _selected(path, indices=(0, 2)):
    run = _chunks(_list(path))[0]
    chunk = replace(
        run,
        unit_ids=indices,
        num_rows=sum(run.unit_rows[i] for i in indices),
        size_bytes=sum(run.unit_sizes[i] for i in indices),
        unit_sizes=tuple(run.unit_sizes[i] for i in indices),
        unit_rows=tuple(run.unit_rows[i] for i in indices),
        unit_start_rows=tuple(run.unit_start_rows[i] for i in indices),
    )
    return FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[path.stat().st_size],
        chunk_metadatas=[chunk.to_metadata()],
    )


def test_metadata_layout_without_data_iteration(tmp_path, monkeypatch):
    path = tmp_path / "data.orc"
    _write(path)

    class MetadataOnlyReader(pyorc.Reader):
        def read(self, num: int = -1) -> NoReturn:
            raise AssertionError("row decoding")

        def __next__(self):
            raise AssertionError("row iteration")

    monkeypatch.setattr(pyorc, "Reader", MetadataOnlyReader)
    layout = read_orc_metadata(str(path), None)
    assert layout.num_rows == 16
    assert [s.num_rows for s in layout.stripes] == [5, 7, 4]
    assert [s.row_offset for s in layout.stripes] == [0, 5, 12]
    assert [s.index for s in layout.stripes] == [0, 1, 2]
    assert all(s.size_bytes > 0 and s.bytes_offset >= 3 for s in layout.stripes)


class _Input(io.BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.requests = []
        self.short = False

    def size(self):
        return len(self.getbuffer())

    def read(self, length=-1):
        self.requests.append((self.tell(), length))
        return super().read(max(0, length - 1) if self.short else length)


def test_range_cache_partial_hits_eof_cap_and_ownership():
    source = _Input(bytes(range(64)))
    cache = _RangeCache(source, capacity=16)
    a, b = cache.view(), cache.view()
    a.seek(10)
    assert a.read(10) == bytes(range(10, 20))
    b.seek(15)
    assert b.read(10) == bytes(range(15, 25))
    assert source.requests == [(10, 10), (20, 5)]
    a.seek(-4, 2)
    assert a.read(20) == bytes(range(60, 64))
    assert a.read(1) == b""
    assert cache.retained_bytes == 15  # oversize gap read-through
    assert cache.uncached_bytes == 4
    a.close()
    assert not source.closed
    assert b.seek(0, 2) == 64
    with pytest.raises(ValueError):
        a.tell()
    with pytest.raises(ValueError):
        b.seek(-1)
    b.close()
    source.close()


def test_failed_short_read_does_not_poison_cache():
    source = _Input(b"abcdefgh")
    source.short = True
    cache = _RangeCache(source)
    with pytest.raises(OSError, match="short ORC read"):
        cache.read(0, 8)
    assert cache.retained_bytes == 0
    source.short = False
    assert cache.read(0, 8) == b"abcdefgh"


def test_schema_layout_share_actual_ranges_and_close(tmp_path, monkeypatch):
    import ray.data._internal.datasource_v2.formats.orc.orc_metadata as metadata

    path = tmp_path / "data.orc"
    table = _write(path)
    source = _Input(path.read_bytes())
    original = metadata._layout_from_source
    requests_after_schema = []

    def layout(stream, path, size):
        requests_after_schema.extend(source.requests)
        result = original(stream, path, size)
        assert source.requests == requests_after_schema
        return result

    monkeypatch.setattr(metadata, "_layout_from_source", layout)
    schema, seed = read_orc_schema_and_layout(
        str(path), SimpleNamespace(open_input_file=lambda _: source)
    )
    assert schema == table.schema
    assert seed.layout.num_rows == 16
    assert source.closed
    assert requests_after_schema


def test_prepare_read_isolated_and_seeded_without_execution_io(tmp_path, monkeypatch):
    path = tmp_path / "data.orc"
    table = _write(path)
    datasource = OrcDatasourceV2([str(path)])
    before = vars(datasource).copy()
    sample = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    hints = PartitionHints(0, 1 << 30, 3)
    schema, first, partitioner = datasource.prepare_read(
        sample, datasource._get_file_indexer(), hints
    )
    _, second, _ = datasource.prepare_read(
        sample, datasource._get_file_indexer(), hints
    )
    first = cast(OrcStripeIndexer, first)
    second = cast(OrcStripeIndexer, second)
    partitioner = cast(OrcStripePartitioner, partitioner)
    assert schema == table.schema
    assert vars(datasource) == before
    assert first is not second and first._seeds is not second._seeds
    monkeypatch.setattr(
        "ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer.OrcMetadataActor",
        None,
    )
    for _ in range(2):
        assert _chunks(
            list(first.list_files(pa.array([str(path)]), filesystem=LocalFileSystem()))
        )[0].unit_start_rows == (0, 5, 12)
    assert partitioner._budget == max(
        1, (sum(s.size_bytes for s in first._seeds[0].layout.stripes) + 2) // 3
    )


@pytest.mark.parametrize("ordered", [False, True])
def test_indexer_entrances_exclusions_dedup_and_limit(tmp_path, ordered):
    path = tmp_path / "data.orc"
    _write(path)
    indexer = _indexer(path)
    files = [FileInfo(str(path), path.stat().st_size)] * 2
    normal = list(
        indexer.list_files(
            pa.array([str(tmp_path), str(path)]),
            filesystem=LocalFileSystem(),
            preserve_order=ordered,
        )
    )
    prelisted = list(
        indexer.list_files_from_file_infos(
            files, filesystem=LocalFileSystem(), preserve_order=ordered
        )
    )
    assert [cast(pa.Table, m.as_block()).to_pylist() for m in normal] == [
        cast(pa.Table, m.as_block()).to_pylist() for m in prelisted
    ]
    assert _chunks(normal)[0].unit_ids == (0, 1, 2)
    selected = _list(path, excluded_read_unit_ids={orc_read_unit_id(str(path), 1)})
    assert _chunks(selected)[0].unit_start_rows == (0, 12)
    assert _list(path, excluded_read_unit_ids={str(path)}) == []
    assert _chunks(_list(path, limit=1))[0].unit_ids == (0,)
    filtered = _chunks(_list(path, limit=1, predicate=col("id") == 15))[0]
    assert filtered.unit_ids == (0, 1, 2) and not filtered.fully_matched
    assert _list(path, limit=0) == []
    assert (
        len(
            list(
                indexer.as_whole_file_indexer().list_files(
                    pa.array([str(path)]), filesystem=LocalFileSystem()
                )
            )
        )
        == 1
    )


def test_single_stripe_and_empty_use_whole_file(tmp_path, monkeypatch):
    for counts in [(7,), (0,)]:
        path = tmp_path / f"{counts[0]}.orc"
        if counts == (0,):
            orc.write_table(pa.table({"id": pa.array([], type=pa.int64())}), str(path))
        else:
            _write(path, counts)
        manifests = _list(path)
        assert len(manifests) == 1 and manifests[0].file_chunk_metadatas.tolist() == [
            None
        ]
        monkeypatch.setattr(
            orc.ORCFile,
            "read_stripe",
            lambda *args, **kwargs: pytest.fail("whole file must stream"),
        )
        assert (
            sum(
                t.num_rows
                for t in OrcScanner(schema=orc.ORCFile(str(path)).schema, batch_size=2)
                .create_reader()
                .read(manifests[0])
            )
            == counts[0]
        )


@pytest.mark.parametrize("ordered", [False, True])
def test_online_packing_preserves_positions_and_budget(tmp_path, monkeypatch, ordered):
    path = tmp_path / "data.orc"
    _write(path)
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", ordered
    )
    monkeypatch.setenv("RAY_DATA_ORC_BIN_PACKING_BYTES", "1")
    partitioner = OrcStripePartitioner(PartitionHints(0, 1 << 20, 1))
    partitioner.add_input(_list(path)[0])
    assert partitioner.has_partition()  # sealed before finalize
    partitioner.finalize()
    chunks = []
    while partitioner.has_partition():
        chunks.extend(_chunks([partitioner.next_partition()]))
    assert sorted(
        (i, r, s, c.file_num_units)
        for c in chunks
        for i, r, s in zip(c.unit_ids, c.unit_rows, c.unit_start_rows)
    ) == [(0, 5, 0, 3), (1, 7, 5, 3), (2, 4, 12, 3)]
    assert [c.num_rows for c in chunks] == [5, 7, 4]


class _PositionColumn(SynthesizedColumn):
    name = "position"
    type = pa.string()
    requires_read_unit_boundaries = True

    def compute(self, position, num_rows):
        unit = position.unit
        return pa.repeat(
            pa.scalar(
                f"{unit.index},{unit.count},{unit.num_rows},{position.unit_start_row},{position.rows_before}"
            ),
            num_rows,
        )


@pytest.mark.parametrize(
    "split_groups", [False, True], ids=["one-group", "split-groups"]
)
@pytest.mark.parametrize("ordered", [False, True])
@pytest.mark.parametrize("retry", [False, True])
def test_positions_filters_retry_and_no_worker_pyorc(
    tmp_path, monkeypatch, ordered, retry, split_groups
):
    path = tmp_path / "data.orc"
    expected = _write(path)
    manifest = _selected(path)
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", ordered
    )
    original_import = importlib.import_module

    def no_pyorc(name, *args, **kwargs):
        if name == "pyorc":
            pytest.fail("read workers must use manifest positions")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", no_pyorc)
    scanner = OrcScanner(
        schema=expected.schema,
        batch_size=2,
        synthesized_columns=(_PositionColumn(),),
        predicate=reduce(
            lambda a, b: a | b, [col("id") == i for i in [1, 3, 4, 5, 8, 11, 12, 15]]
        ),
    )
    reader = scanner.create_reader()
    original = reader._scan_stripe
    failed = False

    def flaky(*args):
        nonlocal failed
        for i, table in enumerate(original(*args)):
            if retry and i == 1 and not failed:
                failed = True
                raise OSError("ORC transient position probe")
            yield table

    monkeypatch.setattr(reader, "_scan_stripe", flaky)
    monkeypatch.setattr(
        DataContext.get_current(), "retried_io_errors", ["ORC transient position probe"]
    )
    monkeypatch.setattr("ray.data._internal.util.time.sleep", lambda _: None)
    splits = [manifest]
    if split_groups:
        partitioner = OrcStripePartitioner(PartitionHints(0, 1, 1))
        partitioner.add_input(manifest)
        partitioner.finalize()
        splits = []
        while partitioner.has_partition():
            splits.append(partitioner.next_partition())
    batches = [table for split in splits for table in reader.read(split)]
    assert [row["id"] for table in batches for row in table.to_pylist()] == [
        1,
        3,
        4,
        12,
        15,
    ]
    cursors = {0: 0, 2: 0}
    for table in batches:
        index, count, rows, start, cursor = map(
            int, table["position"][0].as_py().split(",")
        )
        assert (count, rows, start, cursor) == (
            3,
            {0: 5, 2: 4}[index],
            {0: 0, 2: 12}[index],
            cursors[index],
        )
        cursors[index] += table.num_rows
    assert failed == retry


def test_all_units_stream_only_without_boundary_consumer(tmp_path, monkeypatch):
    path = tmp_path / "data.orc"
    expected = _write(path)
    manifest = _list(path)[0]
    decoded = []
    original = orc.ORCFile.read_stripe

    def trace(reader, index, columns=None):
        decoded.append(index)
        return original(reader, index, columns)

    monkeypatch.setattr(orc.ORCFile, "read_stripe", trace)
    assert pa.concat_tables(
        list(
            OrcScanner(schema=expected.schema, batch_size=2)
            .create_reader()
            .read(manifest)
        )
    ).equals(expected)
    assert decoded == []
    list(
        OrcScanner(
            schema=expected.schema,
            batch_size=2,
            synthesized_columns=(_PositionColumn(),),
        )
        .create_reader()
        .read(manifest)
    )
    assert decoded == [0, 1, 2]


def test_chunk_missing_positions_is_protocol_error(tmp_path):
    path = tmp_path / "data.orc"
    table = _write(path)
    chunk = FileChunk((0,), 5, 100)
    manifest = FileManifest.construct_manifest(
        paths=[str(path)],
        sizes=[path.stat().st_size],
        chunk_metadatas=[chunk.to_metadata()],
    )
    with pytest.raises(ValueError, match="file_num_units"):
        list(OrcScanner(schema=table.schema).create_reader().read(manifest))


def test_manifest_types_legacy_and_serialization(tmp_path):
    path = tmp_path / "data.orc"
    _write(path)
    whole = FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )
    merged = FileManifest.concat([whole, _selected(path, (0,)), _selected(path)])
    assert pickle.loads(pickle.dumps(merged)).as_block().equals(merged.as_block())
    old = FileChunk((0,), 5, 100).to_metadata()
    old.pop("unit_start_rows")
    old.pop("file_num_units")
    assert FileChunk.from_metadata(old).file_num_units is None


def test_optional_backend_failures_are_not_hidden(tmp_path, monkeypatch):
    path = tmp_path / "bad.orc"
    path.write_bytes(b"not ORC")
    with pytest.raises(pa.ArrowInvalid, match="bad.orc"):
        read_orc_metadata(str(path), None)

    def missing(name):
        raise ModuleNotFoundError(name="pyorc")

    monkeypatch.setattr(importlib, "import_module", missing)
    assert not pyorc_available()
    assert (
        type(OrcDatasourceV2([str(path)])._get_file_indexer()).__name__
        == "NonSamplingFileIndexer"
    )

    def broken(name):
        raise ModuleNotFoundError(name="backend_dependency")

    monkeypatch.setattr(importlib, "import_module", broken)
    with pytest.raises(ModuleNotFoundError):
        pyorc_available()


def test_metadata_window_includes_completed_not_delivered_and_cleans(monkeypatch):
    import ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer as module

    submitted, killed, cancelled, options = [], [], [], []

    class Ref:
        def __init__(self, path):
            self.path = path

    class Actor:
        def __init__(self):
            self.read = SimpleNamespace(remote=self.submit)

        def submit(self, path):
            ref = Ref(path)
            submitted.append(ref)
            return ref

    class Factory:
        def options(self, **kwargs):
            options.append(kwargs)
            return self

        def remote(self, *args):
            return Actor()

    monkeypatch.setattr(module, "OrcMetadataActor", Factory())
    monkeypatch.setattr(module.ray, "get", lambda ref: ref.path)
    monkeypatch.setattr(
        module.ray, "kill", lambda actor, **kwargs: killed.append(actor)
    )
    monkeypatch.setattr(
        module.ray, "cancel", lambda ref, **kwargs: cancelled.append(ref)
    )
    monkeypatch.setattr(
        module.ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "a" * 56),
    )
    monkeypatch.setattr(module.ray, "is_initialized", lambda: True)
    indexer = OrcStripeIndexer(ignore_missing_paths=False, local_only=True)
    # Execution may run on a different coordinator; metadata stays on driver.
    monkeypatch.setattr(
        module.ray,
        "get_runtime_context",
        lambda: SimpleNamespace(get_node_id=lambda: "b" * 56),
    )
    generator = indexer._iter_layouts((FileInfo(f"p{i}", 100) for i in range(40)), None)
    assert next(generator)[1] == "p0"
    assert len(submitted) == 8  # completed slots still occupy their window
    assert len(options) == 4
    assert all(o["scheduling_strategy"].node_id == "a" * 56 for o in options)
    assert next(generator)[1] == "p1"
    assert len(submitted) == 9
    generator.close()
    assert len(killed) == 4 and len(cancelled) == 8


def test_prelisted_shuffle_and_replay_are_repeatable(tmp_path):
    paths = [tmp_path / f"{i}.orc" for i in range(3)]
    for path in paths:
        _write(path)
    indexer = _indexer(*paths)
    files = [FileInfo(str(path), path.stat().st_size) for path in paths]
    config = FileShuffleConfig(seed=66999)
    first = list(
        indexer.list_files_from_file_infos(
            files, filesystem=LocalFileSystem(), shuffle_config=config, execution_idx=2
        )
    )
    second = list(
        indexer.list_files_from_file_infos(
            files, filesystem=LocalFileSystem(), shuffle_config=config, execution_idx=2
        )
    )
    assert [cast(pa.Table, m.as_block()).to_pylist() for m in first] == [
        cast(pa.Table, m.as_block()).to_pylist() for m in second
    ]
    assert len(first) == 3


def test_budget_sampling_boundary_and_seed_cap(tmp_path, monkeypatch):
    import ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 as module

    paths = [tmp_path / f"{i}.orc" for i in range(16)]
    for path in paths:
        _write(path, (2, 2))
    hints = PartitionHints(0, 1 << 30, 100)
    budgets = []
    for n in [15, 16]:
        sample = FileManifest.construct_manifest(
            paths=[str(p) for p in paths[:n]],
            sizes=[p.stat().st_size for p in paths[:n]],
            chunk_metadatas=[None] * n,
        )
        source = OrcDatasourceV2([str(tmp_path)])
        _, indexer, partitioner = source.prepare_read(
            sample, source._get_file_indexer(), hints
        )
        indexer = cast(OrcStripeIndexer, indexer)
        partitioner = cast(OrcStripePartitioner, partitioner)
        assert len(indexer._seeds) == n
        budgets.append(partitioner._budget)
    assert budgets[0] < budgets[1] == 128 * 1024 * 1024
    sample = FileManifest.construct_manifest(
        paths=[str(p) for p in paths[:15]],
        sizes=[p.stat().st_size for p in paths[:15]],
        chunk_metadatas=[None] * 15,
    )
    monkeypatch.setattr(module, "_MAX_SEED_BYTES", 0)
    _, indexer, partitioner = source.prepare_read(
        sample, source._get_file_indexer(), hints
    )
    indexer = cast(OrcStripeIndexer, indexer)
    partitioner = cast(OrcStripePartitioner, partitioner)
    assert indexer._seeds == () and partitioner._budget == budgets[1]
    monkeypatch.setenv("RAY_DATA_ORC_BIN_PACKING_BYTES", "0")
    with pytest.raises(ValueError, match="positive"):
        OrcStripePartitioner(hints)


def test_default_preparation_preserves_existing_order():
    from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
        FileDataSourceV2,
    )

    calls = []
    source = SimpleNamespace(
        infer_schema=lambda sample: calls.append(sample) or pa.schema([])
    )
    indexer = object()
    # The unbound default only calls infer_schema and returns the opaque indexer.
    assert FileDataSourceV2.prepare_read(
        cast(FileDataSourceV2, source),
        None,
        cast(FileIndexer, indexer),
        PartitionHints(0, 100, 1),
    ) == (pa.schema([]), indexer, None)
    assert calls == [None]


@pytest.mark.parametrize(
    "columns", [None, [], ["id"], ["label"], ["year"], ["path"], ["id", "missing"]]
)
def test_stripe_schema_projection_partition_filter_and_empty_columns(tmp_path, columns):
    folder = tmp_path / "year=2024"
    folder.mkdir()
    path = folder / "data.orc"
    with path.open("wb") as stream:
        writer = pyorc.Writer(
            stream,
            "struct<id:bigint,label:string,year:string>",
            batch_size=1,
            stripe_size=1 << 30,
        )
        row = 0
        for count in (5, 7, 4):
            writer.writerows((i, f"row-{i}", "wrong") for i in range(row, row + count))
            row += count
            cast(_IntermediateFooterWriter, writer).write_intermediate_footer()
        writer.close()
    schema = pa.schema(
        [
            ("id", pa.int32()),
            ("label", pa.string()),
            ("year", pa.string()),
            ("missing", pa.string()),
        ]
    )
    scanner = OrcScanner(
        schema=schema,
        columns=tuple(columns) if columns is not None else None,
        batch_size=2,
        partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path)),
        synthesized_columns=(),
    )
    from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn

    scanner = replace(
        scanner,
        synthesized_columns=(PathColumn(),),
        predicate=(col("year") == "2024") & (col("id") >= 3),
    )
    tables = list(scanner.create_reader().read(_selected(path)))
    assert sum(t.num_rows for t in tables) == 6
    for table in tables:
        if "id" in table.column_names:
            assert table.schema.field("id").type == pa.int32()
        for row in table.to_pylist():
            if "year" in row:
                assert row["year"] == "2024"
            if "missing" in row:
                assert row["missing"] is None
            if "path" in row:
                assert row["path"] == str(path)
    if columns == []:
        from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME

        assert all(
            t.column_names == [_BATCH_SIZE_PRESERVING_STUB_COL_NAME] for t in tables
        )


@pytest.mark.parametrize("mode", ["normal", "limit", "close", "error"])
def test_stripe_session_reuse_and_close(tmp_path, monkeypatch, mode):
    from ray.data._internal.datasource_v2.common.file_reader import FileFormat
    from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import (
        OrcFileReader,
    )

    path = tmp_path / "data.orc"
    table = _write(path)
    manifest = _selected(path)
    sources = []

    def opened(path):
        source = _Input(Path(path).read_bytes())
        sources.append(source)
        return source

    reader = OrcFileReader(
        format=FileFormat.ORC,
        schema=table.schema,
        batch_size=2,
        filesystem=SimpleNamespace(open_input_file=opened),
        limit=1 if mode == "limit" else None,
    )
    if mode == "error":
        monkeypatch.setattr(
            reader,
            "_scan_stripe",
            lambda *args: (_ for _ in ()).throw(ValueError("decode probe")),
        )
    dataset = pds.dataset(str(path), format="orc")
    fragments = reader._get_fragments_to_read(dataset, manifest)
    iterator = cast(
        Generator[tuple[pa.Table, ReadUnitPosition], None, None],
        reader._read_fragments_sequential(
            iter(fragments), {"columns": None, "filter": None, "batch_size": 2}
        ),
    )
    if mode == "error":
        with pytest.raises(ValueError, match="decode probe"):
            list(iterator)
    elif mode == "close":
        next(iterator)
        iterator.close()
    else:
        batches = list(iterator)
        assert sum(t.num_rows for t, _ in batches) == (2 if mode == "limit" else 9)
    assert len(sources) == 1 and all(source.closed for source in sources)


def test_stripe_complex_types_and_cast_errors(tmp_path):
    from datetime import datetime, timedelta, timezone
    from decimal import Decimal

    path = tmp_path / "complex.orc"
    values = [
        (
            i,
            (i,),
            Decimal(f"{i}.25"),
            datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=i),
        )
        for i in range(16)
    ]
    with path.open("wb") as stream:
        writer = pyorc.Writer(
            stream,
            "struct<id:bigint,nested:struct<inner:bigint>,amount:decimal(12,2),ts:timestamp>",
            batch_size=1,
            stripe_size=1 << 30,
        )
        start = 0
        for count in (5, 7, 4):
            writer.writerows(values[start : start + count])
            start += count
            cast(_IntermediateFooterWriter, writer).write_intermediate_footer()
        writer.close()
    schema = orc.ORCFile(str(path)).schema
    expected = pa.Table.from_pylist(
        [
            {
                "id": i,
                "nested": {"inner": i},
                "amount": amount,
                "ts": ts.replace(tzinfo=None),
            }
            for i, _, amount, ts in values
        ],
        schema=schema,
    )
    result = pa.concat_tables(
        list(
            OrcScanner(schema=schema, columns=("nested", "amount", "ts"), batch_size=2)
            .create_reader()
            .read(_selected(path))
        )
    )
    assert result.equals(
        expected.take(pa.array([0, 1, 2, 3, 4, 12, 13, 14, 15])).select(
            ["nested", "amount", "ts"]
        )
    )
    bad_schema = pa.schema([("nested", pa.int32())])
    with pytest.raises(
        (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError)
    ):
        list(OrcScanner(schema=bad_schema).create_reader().read(_selected(path)))


def test_stripe_pickle_guard_precedes_empty_filter(tmp_path, monkeypatch):
    from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType

    path = tmp_path / "data.orc"
    _write(path)
    manifest = _selected(path)
    original = orc.ORCFile

    class UnsafeORC:
        nstripes = 3
        nrows = 16
        schema = pa.schema([("id", pa.int64()), ("label", ArrowPythonObjectType())])

        def __init__(self, source):
            self.reader = original(source)

        def read_stripe(self, index, columns=None):
            assert "label" not in cast(list[str], columns)
            return self.reader.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", UnsafeORC)
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            OrcScanner(schema=UnsafeORC.schema, predicate=col("id") < 0)
            .create_reader()
            .read(manifest)
        )
    rows = list(
        OrcScanner(schema=UnsafeORC.schema, columns=("id",))
        .create_reader()
        .read(manifest)
    )
    assert sum(t.num_rows for t in rows) == 9


def test_single_stripe_boundary_consumer_keeps_file_identity(tmp_path, monkeypatch):
    path = tmp_path / "single.orc"
    table = _write(path, (7,))
    manifest = _list(path)[0]
    monkeypatch.setattr(
        orc.ORCFile,
        "read_stripe",
        lambda *args, **kwargs: pytest.fail("single stripe must stream"),
    )
    reader = OrcScanner(
        schema=table.schema, batch_size=2, synthesized_columns=(_PositionColumn(),)
    ).create_reader()
    output = list(reader.read(manifest))
    assert sum(t.num_rows for t in output) == 7
    assert [t["position"][0].as_py() for t in output] == [
        f"0,1,7,0,{cursor}" for cursor in (0, 2, 4, 6)
    ]
    units = reader._get_fragments_to_read(
        pds.dataset(str(path), format="orc"), manifest
    )
    assert units[0].unit.id == str(path)


def test_bootstrap_failure_retains_path_and_closes_source(tmp_path):
    path = tmp_path / "bad.orc"
    path.write_bytes(b"bad ORC")
    source = _Input(path.read_bytes())
    with pytest.raises((pa.ArrowInvalid, OSError), match="bad.orc"):
        read_orc_schema_and_layout(
            str(path), SimpleNamespace(open_input_file=lambda _: source)
        )
    assert source.closed


@pytest.mark.parametrize("ordered", [False, True])
def test_mixed_whole_file_and_stripes_share_one_limit(tmp_path, monkeypatch, ordered):
    first, second = tmp_path / "first.orc", tmp_path / "second.orc"
    schema = _write(first).schema
    _write(second, (7,))
    manifest = FileManifest.concat([_selected(first), _list(second)[0]])
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", ordered
    )
    reader = OrcScanner(schema=schema, batch_size=2).push_limit(10).create_reader()
    result = pa.concat_tables(list(reader.read(manifest)))
    assert result["id"].to_pylist() == [0, 1, 2, 3, 4, 12, 13, 14, 15, 0]


def test_full_schema_keeps_driver_layout_bootstrap_bounded(tmp_path, monkeypatch):
    import ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 as module

    paths = [tmp_path / f"f{i:04d}.orc" for i in range(40)]
    for path in paths:
        _write(path, (2, 2))
    orc.write_table(
        pa.table({"id": [1], "label": ["last"], "late": [True]}), str(paths[-1])
    )
    sample = FileManifest.construct_manifest(
        paths=[str(p) for p in paths],
        sizes=[p.stat().st_size for p in paths],
        chunk_metadatas=[None] * len(paths),
    )
    source = OrcDatasourceV2([str(tmp_path)])
    shared_paths, schema_paths = [], []
    shared = module.read_orc_schema_and_layout
    schema_only = source._read_schema

    def layout(path, filesystem):
        shared_paths.append(path)
        return shared(path, filesystem)

    def schema(path):
        schema_paths.append(path)
        return schema_only(path)

    monkeypatch.setattr(module, "read_orc_schema_and_layout", layout)
    monkeypatch.setattr(source, "_read_schema", schema)
    schema, indexer, partitioner = source.prepare_read(
        sample, source._get_file_indexer(), PartitionHints(0, 1 << 30, 4)
    )
    indexer = cast(OrcStripeIndexer, indexer)
    partitioner = cast(OrcStripePartitioner, partitioner)
    assert "late" in schema.names
    assert set(shared_paths) == {str(p) for p in paths[:16]}
    assert set(schema_paths) == {str(p) for p in paths[16:]}
    assert len(indexer._seeds) == 16
    assert partitioner._budget == 128 * 1024 * 1024


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
