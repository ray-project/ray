"""ORC tail and pruning tests with real writer/decoder and Arrow filter oracles."""

import copy
import itertools
import pickle
import random
from dataclasses import replace
from pathlib import Path
from typing import Generator, cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
import pytest

from ray.data._internal.datasource_v2.common.synthesized_columns import (
    PathColumn,
    RowHashColumn,
)
from ray.data._internal.datasource_v2.formats.orc import (
    orc_file_reader as reader_module,
    orc_metadata as metadata_module,
)
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    ColumnStatistics,
    MetadataUnavailable,
    _decode,
    _decode_block,
    _parse,
    _statistics,
    read_metadata,
    read_stripe_statistics,
)
from ray.data._internal.datasource_v2.formats.orc.orc_predicate import OrcPredicate
from ray.data._internal.datasource_v2.formats.orc.orc_proto import (
    Footer,
    Metadata,
    PostScript,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col, lit

CODECS = ["uncompressed", "zlib", "snappy", "lz4", "zstd"]


def _manifest(path):
    return FileManifest.construct_manifest(
        paths=[str(path)], sizes=[path.stat().st_size], chunk_metadatas=[None]
    )


def _write(path, table, codec="uncompressed"):
    orc.write_table(
        table,
        str(path),
        compression=codec,
        compression_strategy="compression",
        stripe_size=8192,
        batch_size=256,
        compression_block_size=65536,
    )


def _rows(scanner, path):
    return [
        row
        for batch in scanner.create_reader().read(_manifest(path))
        for row in batch.to_pylist()
    ]


@pytest.fixture(params=CODECS)
def striped_file(tmp_path, request):
    # Incompressible payload makes multiple physical stripes for every codec;
    # the sorted key gives each stripe a distinct, provable range.
    rng = random.Random(66995)
    table = pa.table(
        {
            "x": range(4096),
            "flag": [False] * 2048 + [True] * 2048,
            "nullable": pa.array([None] * 2048 + list(range(2048)), type=pa.int64()),
            "payload": [rng.randbytes(128) for _ in range(4096)],
        }
    )
    path = tmp_path / "striped.orc"
    _write(path, table, request.param)
    assert orc.ORCFile(str(path)).nstripes > 2
    return path, table


def test_real_tail_and_stripe_statistics(striped_file, monkeypatch):
    path, table = striped_file
    decoded = []
    native_decode = metadata_module._decode_block

    def decode(data, codec, capacity):
        decoded.append(codec)
        return native_decode(data, codec, capacity)

    monkeypatch.setattr(metadata_module, "_decode_block", decode)
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
        statistics = read_stripe_statistics(source, metadata)
    if metadata.postscript.compression:
        assert metadata.postscript.compression in decoded
    assert metadata.rows == table.num_rows
    assert metadata.statistics["x"].minimum == 0
    assert metadata.statistics["x"].maximum == 4095
    assert metadata.statistics["flag"].true_count == 2048
    assert metadata.statistics["nullable"].values == 2048
    native = orc.ORCFile(str(path))
    for index, stats in enumerate(statistics):
        stripe = native.read_stripe(index)
        values = stripe.column("x").to_pylist()
        assert (stats["x"].minimum, stats["x"].maximum) == (min(values), max(values))


@pytest.mark.parametrize("mode", ["file", "stripe"])
def test_real_pruning_and_exact_filter(striped_file, mode, monkeypatch):
    path, table = striped_file
    scans, stripes = [], []
    native_file = orc.ORCFile

    class TrackingFile:
        def __init__(self, source):
            self._file = native_file(source)

        def read_stripe(self, index, columns=None):
            stripes.append((index, columns))
            return self._file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    native_scan_stripes = reader_module.OrcFileReader._scan_stripes

    def scan_stripes(self, *args, **kwargs):
        scans.append(True)
        yield from native_scan_stripes(self, *args, **kwargs)

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_stripes", scan_stripes)
    scanner = OrcScanner(schema=table.schema, statistics_pruning=mode, batch_size=37)
    # Whole-file exclusion must never enter either the stripe decoder or the
    # original streaming scanner's scan_batches method.
    fragment = pds.dataset(str(path), format="orc").get_fragments().__next__()
    original_scanner = fragment.scanner

    class Fragment:
        def __getattr__(self, name):
            return getattr(fragment, name)

        def scanner(self, **kwargs):
            original_scanner(**kwargs)  # Real binding, but no data decode allowed.

            class Scanner:
                def scan_batches(self):
                    pytest.fail("Excluded file decoded data")

            return Scanner()

    reader = replace(scanner, predicate=col("x") > 5000).create_reader()
    assert (
        list(
            reader._iter_fragment_tables(
                Fragment(),
                {
                    "columns": None,
                    "filter": (col("x") > 5000).to_pyarrow(),
                    "batch_size": 37,
                },
            )
        )
        == []
    )
    assert stripes == []
    predicate = (col("x") >= 3500) & (col("x") < 3800)
    selected = replace(scanner, predicate=predicate).prune_columns(["payload"])
    batches = list(selected.create_reader().read(_manifest(path)))
    assert all(batch.num_rows <= 37 for batch in batches)
    assert pa.concat_tables(batches).equals(
        table.filter(predicate.to_pyarrow()).select(["payload"])
    )
    if mode == "stripe":
        assert scans and 0 < len(stripes) < native_file(str(path)).nstripes
        assert all(
            "x" in columns and "nullable" not in columns for _, columns in stripes
        )
    else:
        assert not scans and not stripes


def test_partial_stripe_filter_only_projection_batch_and_limit(
    striped_file, monkeypatch
):
    path, table = striped_file
    predicate = (col("x") >= 3500) & (col("x") < 3800)
    batch_size, limit = 37, 73
    expected = table.filter(predicate.to_pyarrow()).select(["payload"])
    assert batch_size < limit < expected.num_rows
    native_file = orc.ORCFile
    native = native_file(str(path))
    expected_ids, matched = [], 0
    for index in range(native.nstripes):
        stripe = pa.Table.from_batches([native.read_stripe(index, columns=["x"])])
        count = stripe.filter(predicate.to_pyarrow()).num_rows
        if count:
            expected_ids.append(index)
            matched += count
            if matched >= limit:
                break
    assert 0 < len(expected_ids) < native.nstripes
    decoded = []

    class TrackingFile:
        def __init__(self, source):
            self.file = native_file(source)

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            assert columns is not None
            assert set(columns) == {"x", "payload"}
            return self.file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    scanner = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        columns=("payload",),
        statistics_pruning="stripe",
        batch_size=batch_size,
        limit=limit,
    )
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert len(batches) > 1
    assert all(0 < batch.num_rows <= batch_size for batch in batches)
    assert batches[-1].num_rows < batch_size
    assert pa.concat_tables(batches).equals(expected.slice(0, limit))
    assert decoded == expected_ids


@pytest.mark.parametrize(
    "predicate",
    [
        col("flag") == lit(True),
        col("flag") != lit(False),
        col("nullable").is_null(),
        col("nullable").is_not_null(),
        col("x").is_in([3700, 3900, None]),
        ~col("x").is_in(list(range(1024))),
        ~((col("x") < 3000) | (col("x") >= 3800)),
    ],
)
def test_extended_predicates_prune_real_stripes(striped_file, predicate, monkeypatch):
    path, table = striped_file
    decoded = []
    native_file = orc.ORCFile

    class TrackingFile:
        def __init__(self, source):
            self._file = native_file(source)

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            return self._file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning="stripe"
    )
    assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()
    assert 0 < len(decoded) < native_file(str(path)).nstripes


@pytest.mark.parametrize("codec", [1, 2, 4, 5])
def test_block_capacity_actual_length_and_corruption(codec):
    raw = b"ORC metadata" * 32
    if codec == 1:
        import zlib

        compressor = zlib.compressobj(wbits=-15)
        encoded = compressor.compress(raw) + compressor.flush()
    else:
        encoded = bytes(
            pa.Codec({2: "snappy", 4: "lz4_raw", 5: "zstd"}[codec]).compress(raw)
        )
    assert _decode_block(encoded, codec, 4096) == raw
    with pytest.raises(MetadataUnavailable):
        _decode_block(encoded, codec, 10)
    with pytest.raises(MetadataUnavailable):
        _decode_block(b"\xff\xff", codec, 4096)


def test_lz4_does_not_auto_detect_size_prefix_or_frame():
    import lz4.block
    import lz4.frame

    raw = b"test ORC tail" * 100
    for encoded in (lz4.block.compress(raw, store_size=True), lz4.frame.compress(raw)):
        try:
            result = _decode_block(encoded, 4, 4096)
        except MetadataUnavailable:
            continue
        # A prefixed encoding can coincidentally also be a valid raw block,
        # but it must never be auto-detected to recover the prefixed payload.
        assert result != raw


@pytest.mark.parametrize("data", [b"\x02", b"\x00\x00\x00", b"\x0a\x00\x00x"])
def test_invalid_orc_framing(data):
    with pytest.raises(MetadataUnavailable):
        _decode(data, 1, 4096)


def test_compression_original_blocks_and_budget(monkeypatch):
    raw = b"abc"
    block = ((len(raw) << 1) | 1).to_bytes(3, "little") + raw
    assert _decode(block + block, 4, 4096) == raw * 2
    monkeypatch.setattr(metadata_module, "_MAX_DECODED", 5)
    with pytest.raises(MetadataUnavailable):
        _decode(block + block, 4, 4096)


def test_presence_counts_bounds_and_writer():
    footer = Footer()
    raw = footer.statistics.add()
    columns = {"x": (0, 4)}
    assert _statistics(footer.statistics, 5, columns, False) == {}
    raw.number_of_values = 0
    assert _statistics(footer.statistics, 5, columns, False)["x"].values == 0
    raw.has_null = True
    assert _statistics(footer.statistics, 5, columns, False)["x"].values == 0
    raw.number_of_values = 6
    assert _statistics(footer.statistics, 5, columns, False) == {}
    raw.number_of_values, raw.has_null = 5, False
    raw.int_statistics.minimum = 0
    assert _statistics(footer.statistics, 5, columns, False)["x"].minimum is None
    raw.int_statistics.maximum = -1
    assert _statistics(footer.statistics, 5, columns, False)["x"].minimum is None
    raw.string_statistics.minimum, raw.string_statistics.maximum = "a", "z"
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, False)["x"].minimum is None
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, True)["x"].minimum == "a"
    raw.string_statistics.lower_bound = "a"
    assert _statistics(footer.statistics, 5, {"x": (0, 7)}, True)["x"].minimum is None
    # Presence survives protobuf serialization, including explicitly zero counts.
    parsed = _parse(Footer, footer.SerializeToString())
    assert parsed.statistics[0].HasField("number_of_values")
    assert not parsed.statistics[0].HasField("bucket_statistics")


def _integer_stats(values):
    valid = [value for value in values if value is not None]
    return {
        "x": ColumnStatistics(
            4, len(valid), min(valid) if valid else None, max(valid) if valid else None
        )
    }


def test_possible_values_against_arrow_oracle():
    schema = pa.schema([("x", pa.int64())])
    leaves = [col("x").is_null(), col("x").is_not_null()]
    for value in (-3, 0, 3, None):
        leaves.extend(
            [
                col("x") == value,
                col("x") != value,
                col("x") < value,
                col("x") <= value,
                col("x") > value,
                col("x") >= value,
                lit(value) < col("x"),
            ]
        )
    leaves.extend(col("x").is_in(values) for values in ([], [None], [0], [-3, 3, None]))
    expressions = leaves + [~leaf for leaf in leaves]
    expressions += [a & b for a, b in itertools.product(leaves[::3], leaves[::5])]
    expressions += [~(a | b) for a, b in itertools.product(leaves[::5], leaves[::3])]
    # An unsupported arithmetic leaf remains unknown even under NOT.
    expressions += [~((col("x") + 1) > 5), ((col("x") + 1) > 5) | (col("x") == 0)]
    rng = random.Random(66995)
    cases = [[None], [0], [-3, None, 3], []]
    cases += [
        [rng.choice([None, -3, -1, 0, 2, 3]) for _ in range(rng.randrange(1, 16))]
        for _ in range(40)
    ]
    for values, expr in itertools.product(cases, expressions):
        table = pa.table({"x": pa.array(values, type=pa.int64())})
        bound = OrcPredicate(expr, schema, schema)
        actual = pds.dataset(table).to_table(filter=expr.to_pyarrow()).num_rows
        assert not actual or bound.can_match(_integer_stats(values), len(values)), repr(
            expr
        )
    for expr in [col("x") > 3, col("x") == 3, ~col("x").is_in([0, None])]:
        assert not OrcPredicate(expr, schema, schema).can_match(
            _integer_stats([0, None]), 2
        )


def test_mode_capture_validation_and_serialization(monkeypatch):
    context = DataContext.get_current()
    monkeypatch.setattr(context, "orc_statistics_pruning", "stripe")
    scanner = OrcScanner(schema=pa.schema([("x", pa.int64())]))
    monkeypatch.setattr(context, "orc_statistics_pruning", "off")
    for copy_scanner in (
        copy.deepcopy(scanner),
        pickle.loads(pickle.dumps(scanner)),
        scanner.push_limit(3),
    ):
        assert isinstance(copy_scanner, OrcScanner)
        assert copy_scanner.statistics_pruning == "stripe"
        assert copy_scanner.create_reader()._statistics_pruning == "stripe"
    with pytest.raises(ValueError):
        context.orc_statistics_pruning = "invalid"
    with pytest.raises(ValueError):
        replace(scanner, statistics_pruning="invalid")


@pytest.mark.parametrize("mode", ["off", "file", "stripe"])
def test_statistics_and_batch_sizing_config_survive_pushdowns(
    tmp_path, monkeypatch, mode
):
    from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
        OrcDatasourceV2,
    )

    table = pa.table({"x": range(16), "payload": ["value"] * 16})
    path = tmp_path / "data.orc"
    _write(path, table)
    context = DataContext.get_current()
    monkeypatch.setattr(context, "orc_statistics_pruning", mode)
    monkeypatch.setattr(context, "target_max_block_size", 4096)
    scanner = OrcDatasourceV2([str(path)]).create_scanner(table.schema)
    monkeypatch.setattr(context, "orc_statistics_pruning", "off")
    monkeypatch.setattr(context, "target_max_block_size", 1)
    pushed, residual = scanner.push_filters(col("x") >= 8)
    assert residual is None
    pushed = pushed.prune_columns(["payload"]).push_limit(3)
    for copied in (
        pushed,
        copy.deepcopy(pushed),
        pickle.loads(pickle.dumps(pushed)),
    ):
        assert isinstance(copied, OrcScanner)
        reader = copied.create_reader()
        assert copied.statistics_pruning == reader._statistics_pruning == mode
        assert copied.target_block_size == reader._target_block_size == 4096
        assert reader._explicit_batch_size is None
        assert pa.concat_tables(list(reader.read(_manifest(path)))).equals(
            table.slice(8, 3).select(["payload"])
        )


@pytest.mark.parametrize("mode", ["off", "file", "stripe"])
@pytest.mark.parametrize("batch_size", [None, 37], ids=["estimated", "explicit"])
def test_statistics_pruning_preserves_batch_sizing(
    striped_file, monkeypatch, mode, batch_size
):
    path, table = striped_file
    estimated, decoded = [], []
    original_estimate = reader_module.estimate_batch_size
    original_read_stripe = orc.ORCFile.read_stripe

    def estimate(*args, **kwargs):
        result = original_estimate(*args, **kwargs)
        estimated.append(result)
        return result

    def read_stripe(self, index, **kwargs):
        decoded.append(index)
        return original_read_stripe(self, index, **kwargs)

    monkeypatch.setattr(reader_module, "estimate_batch_size", estimate)
    monkeypatch.setattr(orc.ORCFile, "read_stripe", read_stripe)
    predicate = (col("x") >= 3500) & (col("x") < 3800)
    scanner = OrcScanner(
        schema=table.schema,
        statistics_pruning=mode,
        target_block_size=4096,
        batch_size=batch_size,
    )
    scanner, residual = scanner.push_filters(predicate)
    assert residual is None
    scanner = scanner.prune_columns(["payload"]).push_limit(73)
    batches = list(scanner.create_reader().read(_manifest(path)))
    assert pa.concat_tables(batches).equals(
        table.filter(predicate.to_pyarrow()).slice(0, 73).select(["payload"])
    )
    if batch_size is None:
        assert len(estimated) == 1 and estimated[0] is not None
        initial_size = estimated[0]
        assert 1 < initial_size < 73
    else:
        assert estimated == []
        initial_size = batch_size
    assert len(batches) > 1
    assert all(batch.num_rows <= initial_size for batch in batches)
    if mode == "stripe":
        with pa.OSFile(str(path), "rb") as source:
            metadata = read_metadata(source)
            statistics = read_stripe_statistics(source, metadata)
        selected = [
            index
            for index, stat in enumerate(statistics)
            if cast(int, stat["x"].maximum) >= 3500
            and cast(int, stat["x"].minimum) < 3800
        ]
        assert decoded == selected[: len(decoded)]
        assert 0 < len(decoded) < len(metadata.stripes)
    else:
        assert decoded == []


@pytest.mark.parametrize("mode", ["off", "file", "stripe"])
def test_no_eligible_predicate_has_no_extra_statistics(tmp_path, monkeypatch, mode):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)

    def fail(*args):
        pytest.fail("Unexpected statistics I/O")

    monkeypatch.setattr(reader_module, "read_metadata", fail)
    predicate = (col("x") > 10) if mode == "off" else ((col("x") + 1) > 2)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning=mode
    )
    assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()


@pytest.mark.parametrize("mode", ["file", "stripe"])
def test_unavailable_metadata_falls_back_but_io_errors_propagate(
    tmp_path, monkeypatch, mode
):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [1, 2, 3]})
    _write(path, table)
    scanner = OrcScanner(
        schema=table.schema, predicate=col("x") > 10, statistics_pruning=mode
    )

    def unavailable(source):
        raise MetadataUnavailable("unknown writer")

    monkeypatch.setattr(reader_module, "read_metadata", unavailable)
    assert _rows(scanner, path) == []

    def broken(source):
        raise OSError("object store connection failed")

    monkeypatch.setattr(reader_module, "read_metadata", broken)
    with pytest.raises(OSError, match="object store connection failed"):
        _rows(scanner, path)


def test_partition_shadow_and_missing_columns_in_stripe_path(striped_file):
    path, table = striped_file
    directory = path.parent / "year=2024"
    directory.mkdir()
    partitioned = directory / path.name
    # Keep the original fixture and write a second file; never move test inputs.
    _write(partitioned, table.append_column("year", pa.array([1900] * len(table))))
    schema = table.schema.append(pa.field("year", pa.int64())).append(
        pa.field("missing", pa.string())
    )
    predicate = (col("year") == 2024) & (col("x") > 3500)
    scanner = OrcScanner(
        schema=schema,
        predicate=predicate,
        statistics_pruning="stripe",
        partitioning=Partitioning(PartitionStyle.HIVE, field_types={"year": int}),
    )
    rows = _rows(scanner.prune_columns(["year", "missing", "x"]), partitioned)
    assert rows == [{"year": 2024, "missing": None, "x": x} for x in range(3501, 4096)]


@pytest.mark.parametrize("mode", ["off", "file", "stripe"])
@pytest.mark.parametrize("style", [PartitionStyle.HIVE, PartitionStyle.DIRECTORY])
def test_logical_partition_pruning_and_statistics(tmp_path, monkeypatch, mode, style):
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    rng = random.Random(66995)
    table = pa.table(
        {
            "x": range(4096),
            "payload": [rng.randbytes(128) for _ in range(4096)],
            "year": pa.array([1900] * 4096, type=pa.int32()),
        }
    )
    paths = []
    for year, name, values in (
        (2, "partition-excluded", range(4096)),
        (10, "statistics-excluded", range(-4096, 0)),
        (10, "candidate", range(4096)),
    ):
        parts = (
            [f"year={year}", "unused=not-an-int"]
            if style == PartitionStyle.HIVE
            else [str(year), "not-an-int"]
        )
        directory = tmp_path.joinpath(*parts)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{name}.orc"
        _write(path, table.set_column(0, "x", pa.array(values)), "zlib")
        paths.append(str(path))
    schema = table.schema.append(pa.field("unused", pa.int32()))
    partitioning = Partitioning(
        style, base_dir=str(tmp_path), field_names=["year", "unused"]
    )
    scanner = OrcScanner(
        schema=schema,
        partitioning=partitioning,
        statistics_pruning=mode,
        batch_size=37,
    ).prune_partitions(col("year") > 2)
    scanner, residual = scanner.push_filters(col("x") >= 3500)
    assert residual is None
    scanner = scanner.prune_columns(["payload"]).push_limit(73)
    assert isinstance(scanner, OrcScanner)
    manifest = FileManifest.construct_manifest(
        paths=paths,
        sizes=[Path(path).stat().st_size for path in paths],
        chunk_metadatas=[None] * len(paths),
    )
    pruner = scanner.pushed_partition_pruner()
    assert pruner is not None
    assert [path for path in paths if pruner.should_include(path)] == paths[1:]
    pruned = scanner.prune_input_split(manifest)
    assert pruned.paths.tolist() == paths[1:]
    assert partitioning.field_types == {}

    streamed, decoded = [], []
    native_scan = reader_module.OrcFileReader._scan_with_statistics
    native_read_stripe = orc.ORCFile.read_stripe

    def scan(self, fragment, *args, **kwargs):
        class Fragment:
            def __getattr__(self, name):
                return getattr(fragment, name)

            def scanner(self, **scan_kwargs):
                native = fragment.scanner(**scan_kwargs)

                class Scanner:
                    def scan_batches(self):
                        streamed.append(fragment.path)
                        yield from native.scan_batches()

                return Scanner()

        yield from native_scan(self, Fragment(), *args, **kwargs)

    def read_stripe(self, index, **kwargs):
        decoded.append(index)
        return native_read_stripe(self, index, **kwargs)

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_with_statistics", scan)
    monkeypatch.setattr(orc.ORCFile, "read_stripe", read_stripe)
    batches = list(scanner.create_reader().read(pruned))
    assert pa.concat_tables(batches).equals(table.slice(3500, 73).select(["payload"]))
    assert all(batch.num_rows <= 37 for batch in batches)
    assert sorted(streamed) == sorted(
        paths[1:] if mode == "off" else paths[2:] if mode == "file" else []
    )
    if mode == "stripe":
        assert decoded == sorted(set(decoded))
        assert 0 < len(decoded) < orc.ORCFile(paths[2]).nstripes
    else:
        assert decoded == []


def test_unsafe_output_cast_is_not_hidden_by_pruning(tmp_path):
    path = tmp_path / "data.orc"
    table = pa.table({"x": [0], "text": ["not-an-integer"]})
    _write(path, table)
    schema = pa.schema([("x", pa.int64()), ("text", pa.int64())])
    scanner = OrcScanner(schema=schema, predicate=col("x") > 10)
    # Arrow may filter the bad row before its projection cast. In either case,
    # the optimization must agree with the unmodified streaming path.
    def outcome(mode):
        try:
            return _rows(replace(scanner, statistics_pruning=mode), path)
        except pa.ArrowException as exc:
            return type(exc), str(exc)

    assert outcome("file") == outcome("off")


def test_postscript_proto_presence_and_unknown_codec():
    ps = PostScript()
    assert not ps.HasField("compression")
    ps.compression = 999
    assert _parse(PostScript, ps.SerializeToString()).compression == 999


@pytest.mark.parametrize(
    "predicate",
    [
        col("s") < "b",
        col("s") == "a\x00z",
        col("s") == "😀",
        ~(col("s") < "b"),
        col("s") > "😿",
    ],
)
@pytest.mark.parametrize("mode", ["file", "stripe"])
def test_qualified_java_strings(predicate, mode, monkeypatch):
    path = Path(__file__).parent / "fixtures" / "orc_java_strings.orc"
    native_file = orc.ORCFile
    table = native_file(str(path)).read()
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
        assert metadata.string_statistics_safe
        stats = read_stripe_statistics(source, metadata)
        assert stats[0]["s"].minimum == "a"
        assert stats[2]["s"].minimum is None  # truncated bound, not exact min
    decoded = []

    class TrackingFile:
        def __init__(self, source):
            self._file = native_file(source)

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            return self._file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning=mode, batch_size=2
    )
    assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()
    if mode == "stripe" and table.filter(predicate.to_pyarrow()).num_rows:
        assert 0 < len(decoded) < 3
    else:
        assert not decoded
    if table.filter(predicate.to_pyarrow()).num_rows == 0:
        fragment = next(pds.dataset(str(path), format="orc").get_fragments())

        class GuardFragment:
            def __getattr__(self, name):
                return getattr(fragment, name)

            def scanner(self, **kwargs):
                fragment.scanner(**kwargs)

                class Scanner:
                    def scan_batches(self):
                        pytest.fail("Qualified STRING exclusion decoded data")

                return Scanner()

        assert (
            list(
                scanner.create_reader()._iter_fragment_tables(
                    GuardFragment(),
                    {
                        "columns": None,
                        "filter": predicate.to_pyarrow(),
                        "batch_size": 2,
                    },
                )
            )
            == []
        )


def _tail_bytes(footer, ps=None):
    if ps is None:
        ps = PostScript(compression=0, metadata_length=0, magic="ORC", writer_version=6)
        ps.version.extend((0, 12))
    data = footer.SerializeToString()
    ps.footer_length = len(data)
    script = ps.SerializeToString()
    return b"ORC" + b"x" * 64 + data + script + bytes([len(script)])


def _footer():
    footer = Footer(number_of_rows=5, writer=1, software_version="2.2.1")
    root = footer.types.add(kind=12)
    root.field_names.append("x")
    root.subtypes.append(1)
    footer.types.add(kind=4)
    footer.stripes.add(
        offset=3, index_length=0, data_length=64, footer_length=0, number_of_rows=5
    )
    return footer


@pytest.mark.parametrize(
    "count,counts",
    [(0, [1]), (1, [2]), (2, [3]), (0, [1, 0]), (0, [0, 1])],
    ids=["zero", "one", "two", "multi-first", "multi-second"],
)
def test_contradictory_boolean_bucket_invalidates_count(count, counts):
    footer = Footer()
    raw = footer.statistics.add(number_of_values=count, has_null=count < 2)
    raw.bucket_statistics.count.extend(counts)
    assert _statistics(footer.statistics, 2, {"flag": (0, 0)}, False) == {}
    valid = footer.statistics.add(number_of_values=2, has_null=False)
    valid.int_statistics.minimum, valid.int_statistics.maximum = 0, 1
    stats = _statistics(footer.statistics, 2, {"flag": (0, 0), "x": (1, 4)}, False)
    assert stats == {"x": ColumnStatistics(4, 2, 0, 1)}


@pytest.mark.parametrize(
    "scope,mode", [("file", "file"), ("file", "stripe"), ("stripe", "stripe")]
)
@pytest.mark.parametrize(
    "predicate",
    [col("flag") == lit(True), col("flag").is_not_null()],
    ids=["equal", "not-null"],
)
def test_contradictory_boolean_counts_retain_rows(
    tmp_path, monkeypatch, scope, mode, predicate
):
    path = tmp_path / "contradictory.orc"
    table = pa.table({"flag": [True, None]})
    _write(path, table)
    data = path.read_bytes()
    ps_offset = len(data) - 1 - data[-1]
    ps = _parse(PostScript, data[ps_offset:-1])
    assert ps.compression == 0
    footer_offset = ps_offset - ps.footer_length
    footer = _parse(Footer, data[footer_offset:ps_offset])
    column_id = footer.types[0].subtypes[0]
    if scope == "file":
        offset, end, target = footer_offset, ps_offset, footer
        raw = footer.statistics[column_id]
    else:
        offset, end = footer_offset - ps.metadata_length, footer_offset
        target = _parse(Metadata, data[offset:end])
        assert len(target.stripe_stats) == 1
        raw = target.stripe_stats[0].col_stats[column_id]
    assert raw.number_of_values == raw.bucket_statistics.count[0] == 1
    assert raw.has_null
    # Change only the count. Layout, data and the contradictory true bucket
    # remain intact, so the original decoder can still read the matching row.
    raw.number_of_values = 0
    encoded = target.SerializeToString()
    assert len(encoded) == end - offset
    path.write_bytes(data[:offset] + encoded + data[end:])
    scanner = OrcScanner(schema=table.schema, predicate=predicate)
    expected = table.filter(predicate.to_pyarrow()).to_pylist()
    assert expected == [{"flag": True}]
    assert _rows(replace(scanner, statistics_pruning="off"), path) == expected

    def fail(*args, **kwargs):
        pytest.fail("Contradictory counts must retain the streaming path")

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_stripes", fail)
    assert _rows(replace(scanner, statistics_pruning=mode), path) == expected


@pytest.mark.parametrize(
    "change",
    [
        lambda f: f.ClearField("number_of_rows"),
        lambda f: setattr(f, "writer", 999),
        lambda f: setattr(f, "software_version", "3.0.0"),
        lambda f: setattr(f, "software_version", "9" * 5000 + ".0.0"),
        lambda f: setattr(f, "software_version", "1.0.0"),
        lambda f: setattr(f, "encryption", b""),
        lambda f: setattr(f.stripes[0], "number_of_rows", 6),
        lambda f: setattr(f.stripes[0], "offset", 2),
        lambda f: setattr(f.stripes[0], "data_length", 1000),
        lambda f: f.stripes[0].ClearField("index_length"),
        lambda f: f.types[0].subtypes.append(1),
    ],
)
def test_unqualified_or_inconsistent_tail(change):
    footer = _footer()
    change(footer)
    with pytest.raises(MetadataUnavailable):
        read_metadata(pa.BufferReader(_tail_bytes(footer)))


def test_metadata_requests_do_not_overlap_data():
    data = _tail_bytes(_footer())
    reads = []

    class Source:
        def size(self):
            return len(data)

        def read_at(self, length, offset):
            reads.append((offset, length))
            return data[offset : offset + length]

    assert read_metadata(Source()).rows == 5
    assert len(reads) == 3
    assert all(offset >= 67 for offset, _ in reads)
    assert sum(length for _, length in reads) == len(data) - 67


def test_limit_does_not_decode_next_stripe_and_identity_matches(
    striped_file, monkeypatch
):
    path, table = striped_file
    predicate = col("x") >= 3000
    scanner = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        batch_size=17,
        synthesized_columns=(PathColumn(), RowHashColumn()),
        limit=20,
    )
    expected = _rows(replace(scanner, statistics_pruning="off"), path)
    decoded = []
    native_file = orc.ORCFile

    class TrackingFile:
        def __init__(self, source):
            self._file = native_file(source)

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            return self._file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    assert _rows(replace(scanner, statistics_pruning="stripe"), path) == expected
    assert len(decoded) == 1


def test_retry_retains_post_filter_identity(striped_file, monkeypatch):
    from unittest.mock import Mock

    # Exercise the real retry/skip logic without waiting for wall-clock backoff
    # (unit/conftest.py deliberately forbids sleeps).
    sleep = Mock()
    monkeypatch.setattr("ray.data._internal.util.time.sleep", sleep)
    path, table = striped_file
    predicate = col("x") >= 3000
    scanner = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        batch_size=17,
        synthesized_columns=(PathColumn(), RowHashColumn()),
    )
    expected = _rows(replace(scanner, statistics_pruning="off"), path)
    native_file = orc.ORCFile
    attempts = []
    failed = [False]

    class TrackingFile:
        def __init__(self, source):
            self._file = native_file(source)

        def read_stripe(self, index, columns=None):
            attempts.append(index)
            if len(set(attempts)) == 2 and not failed[0]:
                failed[0] = True
                raise OSError("test transient ORC connection")
            return self._file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    monkeypatch.setattr(
        DataContext.get_current(),
        "retried_io_errors",
        ["test transient ORC connection"],
    )
    assert _rows(replace(scanner, statistics_pruning="stripe"), path) == expected
    assert failed[0]
    sleep.assert_called_once()


@pytest.mark.parametrize(
    "values,predicate",
    [
        ([False, False], col("x") == lit(True)),
        ([True, True], col("x") != lit(True)),
        ([1, 2], col("x").is_null()),
        ([None, None], col("x").is_not_null()),
        ([1, 2], ~(col("x") >= 0)),
        ([1, 2], col("x").is_in([3, None])),
        ([1, 1, None], ~col("x").is_in([1, None])),
    ],
)
def test_extended_predicates_exclude_whole_file(tmp_path, values, predicate):
    path = tmp_path / "data.orc"
    value_type = (
        pa.bool_() if any(type(value) is bool for value in values) else pa.int64()
    )
    table = pa.table({"x": pa.array(values, type=value_type)})
    _write(path, table, "zlib")
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())

    class Fragment:
        def __getattr__(self, name):
            return getattr(fragment, name)

        def scanner(self, **kwargs):
            fragment.scanner(**kwargs)

            class Scanner:
                def scan_batches(self):
                    pytest.fail("Statistics exclusion decoded data")

            return Scanner()

    assert table.filter(predicate.to_pyarrow()).num_rows == 0
    for mode in ("file", "stripe"):
        reader = OrcScanner(
            schema=table.schema, predicate=predicate, statistics_pruning=mode
        ).create_reader()
        assert (
            list(
                reader._iter_fragment_tables(
                    Fragment(),
                    {
                        "columns": None,
                        "filter": predicate.to_pyarrow(),
                        "batch_size": 2,
                    },
                )
            )
            == []
        )


def test_missing_lz4_backend_falls_back(striped_file, monkeypatch):
    import builtins

    path, table = striped_file
    with pa.OSFile(str(path), "rb") as source:
        codec = read_metadata(source).postscript.compression
    native_import = builtins.__import__
    missing = []

    def no_lz4(name, *args, **kwargs):
        if name == "lz4.block":
            missing.append(True)
            raise ImportError("deliberately missing codec backend")
        return native_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_lz4)
    predicate = col("x") >= 3500
    scanner = OrcScanner(
        schema=table.schema, predicate=predicate, statistics_pruning="stripe"
    )
    assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()
    if codec == 4:
        assert missing


def test_all_stripes_kept_uses_streaming_reader(striped_file, monkeypatch):
    path, table = striped_file

    def fail(*args, **kwargs):
        pytest.fail("All-retained stripes must use the streaming reader")

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_stripes", fail)
    scanner = OrcScanner(
        schema=table.schema, predicate=col("x") >= 0, statistics_pruning="stripe"
    )
    assert _rows(scanner, path) == table.to_pylist()


@pytest.mark.parametrize(
    "data_type", [pa.float64(), pa.date32(), pa.timestamp("ns"), pa.decimal128(12, 2)]
)
def test_null_counts_do_not_require_range_support(tmp_path, data_type):
    path = tmp_path / "nulls.orc"
    table = pa.table({"x": pa.nulls(3, type=data_type)})
    _write(path, table, "zlib")
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    predicate = OrcPredicate(col("x").is_not_null(), table.schema, table.schema)
    assert not predicate.can_match(metadata.statistics, 3)


@pytest.mark.parametrize(
    "values", [[1, True], [1.5], ["1"], [1 << 64], list(range(1025))]
)
def test_unsafe_or_over_budget_integer_sets_are_unknown(values):
    schema = pa.schema([("x", pa.int64())])
    for expr in (col("x").is_in(values), ~col("x").is_in(values)):
        bound = OrcPredicate(expr, schema, schema)
        assert not bound.eligible
        assert bound.can_match(_integer_stats([1, None]), 2)


def test_cpp_embedded_nul_string_ranges_stay_unknown(tmp_path, monkeypatch):
    path = tmp_path / "cpp-strings.orc"
    table = pa.table({"s": ["a\x00z", "a\x00b", None]})
    _write(path, table, "zlib")
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
    assert not metadata.string_statistics_safe
    assert metadata.statistics["s"].minimum is None
    assert metadata.statistics["s"].values == 2

    def fail(*args, **kwargs):
        pytest.fail("Unqualified C++ STRING range entered stripe decoding")

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_stripes", fail)
    predicate = col("s") < "a\x00m"
    for mode in ("file", "stripe"):
        scanner = OrcScanner(
            schema=table.schema, predicate=predicate, statistics_pruning=mode
        )
        assert _rows(scanner, path) == table.filter(predicate.to_pyarrow()).to_pylist()


@pytest.mark.parametrize("projection", [["x"], None])
def test_stripe_decoder_pickle_gate_precedes_cast_and_projection(
    monkeypatch, projection
):
    from ray.data._internal.object_extensions.arrow import ArrowPythonObjectType

    storage = pa.array([b"not-deserialized"], type=pa.large_binary())
    extension = pa.ExtensionArray.from_storage(ArrowPythonObjectType(), storage)
    batch = pa.record_batch([pa.array([1]), extension], names=["x", "object"])
    decoded = []

    class Decoder:
        def __init__(self, source):
            pass

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            return batch

    monkeypatch.setattr(orc, "ORCFile", Decoder)
    schema = pa.schema([("x", pa.int64()), ("object", pa.large_binary())])
    predicate = (col("x") > 0) & col("object").is_not_null()
    reader = OrcScanner(
        schema=schema, predicate=predicate, statistics_pruning="stripe"
    ).create_reader()
    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            reader._scan_stripes(
                pa.BufferReader(b""),
                [0],
                {
                    "columns": projection,
                    "filter": predicate.to_pyarrow(),
                    "batch_size": 2,
                },
                schema,
                schema,
                predicate,
                None,
            )
        )
    assert decoded == [0]  # The explicit stripe decoder really ran.


@pytest.mark.parametrize(
    "operation", ["success", "fallback", "init_failure", "early_close", "excluded"]
)
def test_statistics_handle_lifetime(tmp_path, monkeypatch, operation):
    path = tmp_path / "data.orc"
    table = pa.table({"x": range(4096), "payload": [b"p" * 128] * 4096})
    _write(path, table)
    fragment = next(pds.dataset(str(path), format="orc").get_fragments())
    opened = []

    class Filesystem:
        def open_input_file(self, path):
            source = fragment.filesystem.open_input_file(path)
            opened.append(source)
            return source

    class Fragment:
        filesystem = Filesystem()

        def __getattr__(self, name):
            return getattr(fragment, name)

    predicate = col("x") > (5000 if operation == "excluded" else 3500)
    reader = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        statistics_pruning="stripe",
        batch_size=17,
    ).create_reader()
    if operation == "fallback":

        def unavailable(source):
            raise MetadataUnavailable("test fallback")

        monkeypatch.setattr(reader_module, "read_metadata", unavailable)
    elif operation == "init_failure":

        def failed(source):
            raise OSError("test decoder initialization")

        monkeypatch.setattr(orc, "ORCFile", failed)
    iterator = reader._iter_fragment_tables(
        Fragment(),
        {"columns": None, "filter": predicate.to_pyarrow(), "batch_size": 17},
    )
    if operation == "early_close":
        next(iterator)
        cast(Generator[pa.Table, None, None], iterator).close()
    elif operation == "init_failure":
        with pytest.raises(OSError, match="test decoder initialization"):
            list(iterator)
    else:
        list(iterator)
    assert opened and all(source.closed for source in opened)


def test_independent_no_null_flag_and_contradictions():
    footer = Footer()
    raw = footer.statistics.add(has_null=False)
    schema = pa.schema([("x", pa.int64())])
    stats = _statistics(footer.statistics, 5, {"x": (0, 4)}, False)
    assert stats["x"].values == 5
    assert not OrcPredicate(col("x").is_null(), schema, schema).can_match(stats, 5)
    raw.has_null = True
    assert _statistics(footer.statistics, 5, {"x": (0, 4)}, False) == {}
    raw.number_of_values = 5
    assert _statistics(footer.statistics, 5, {"x": (0, 4)}, False) == {}


@pytest.mark.parametrize("kind", [16, 17])
def test_char_varchar_null_proof_does_not_enable_ranges(kind):
    schema = pa.schema([("x", pa.string())])
    stats = {"x": ColumnStatistics(kind, 0)}
    assert not OrcPredicate(col("x").is_not_null(), schema, schema).can_match(stats, 5)
    assert OrcPredicate(col("x") < "z", schema, schema).can_match(stats, 5)


def test_nan_is_not_null_and_float_ranges_stay_unknown(tmp_path):
    path = tmp_path / "float.orc"
    table = pa.table({"x": [float("nan"), float("inf")]})
    _write(path, table, "zlib")
    with pa.OSFile(str(path), "rb") as source:
        stats = read_metadata(source).statistics
    assert not OrcPredicate(col("x").is_null(), table.schema, table.schema).can_match(
        stats, 2
    )
    assert not OrcPredicate(col("x") < 0.0, table.schema, table.schema).eligible


def test_string_comparisons_and_integer_limits_against_arrow():
    for values, data_type, kind, literals in [
        (
            ["", "a\x00z", "é", "中", "😀", None],
            pa.string(),
            7,
            ["", "a\x00b", "é", "😀", "😁"],
        ),
        (
            [-(1 << 63), -1, (1 << 63) - 1, None],
            pa.int64(),
            4,
            [-(1 << 63), 0, (1 << 63) - 1],
        ),
    ]:
        table = pa.table({"x": pa.array(values, type=data_type)})
        valid = [value for value in values if value is not None]
        stats = {"x": ColumnStatistics(kind, len(valid), min(valid), max(valid))}
        for value in literals:
            expressions = [
                col("x") == value,
                col("x") != value,
                col("x") < value,
                col("x") <= value,
                col("x") > value,
                col("x") >= value,
                lit(value) < col("x"),
                lit(value) >= col("x"),
            ]
            for expr in expressions + [~expr for expr in expressions]:
                actual = table.filter(expr.to_pyarrow()).num_rows
                assert not actual or OrcPredicate(
                    expr, table.schema, table.schema
                ).can_match(stats, len(values))


@pytest.mark.parametrize("codec", [1, 2, 4, 5])
def test_original_and_compressed_blocks_can_be_mixed(codec):
    import zlib

    raw = b"metadata short output" * 100
    first, second = raw[:17], raw[17:]
    if codec == 1:
        compressor = zlib.compressobj(wbits=-15)
        encoded = compressor.compress(second) + compressor.flush()
    else:
        encoded = bytes(
            pa.Codec({2: "snappy", 4: "lz4_raw", 5: "zstd"}[codec]).compress(second)
        )
    framed = ((len(first) << 1) | 1).to_bytes(3, "little") + first
    framed += (len(encoded) << 1).to_bytes(3, "little") + encoded
    assert _decode(framed, codec, 4096) == raw


def test_nested_payload_type_ids_and_projection_in_partial_stripe(
    tmp_path, monkeypatch
):
    rng = random.Random(66995)
    payload = pa.array(
        [{"values": [rng.randbytes(32), rng.randbytes(32)]} for _ in range(4096)],
        type=pa.struct([("values", pa.list_(pa.binary()))]),
    )
    # The scalar follows nested types, so its ORC id is not its Arrow ordinal.
    table = pa.table({"payload": payload, "x": range(4096)})
    path = tmp_path / "nested.orc"
    _write(path, table)
    assert orc.ORCFile(str(path)).nstripes > 1
    with pa.OSFile(str(path), "rb") as source:
        metadata = read_metadata(source)
        assert metadata.columns["x"][0] > 2
        assert metadata.statistics["x"].maximum == 4095
    calls = []
    original = reader_module.OrcFileReader._scan_stripes

    def traced(self, *args, **kwargs):
        calls.append(True)
        yield from original(self, *args, **kwargs)

    monkeypatch.setattr(reader_module.OrcFileReader, "_scan_stripes", traced)
    predicate = col("x") >= 3000
    scanner = OrcScanner(
        schema=table.schema,
        predicate=predicate,
        columns=("x", "payload"),
        statistics_pruning="stripe",
        batch_size=37,
    )
    assert (
        _rows(scanner, path)
        == table.filter(predicate.to_pyarrow()).select(["x", "payload"]).to_pylist()
    )
    assert calls


def test_global_limit_closes_before_next_ordered_file(striped_file, monkeypatch):
    path, table = striped_file
    other = path.parent / "other.orc"
    _write(other, table)
    manifest = FileManifest.construct_manifest(
        paths=[str(path), str(other)],
        sizes=[path.stat().st_size, other.stat().st_size],
        chunk_metadatas=[None, None],
    )
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    native_file = orc.ORCFile
    sources, decoded = [], []

    class TrackingFile:
        def __init__(self, source):
            sources.append(source)
            self.file = native_file(source)

        def read_stripe(self, index, columns=None):
            decoded.append(index)
            return self.file.read_stripe(index, columns=columns)

    monkeypatch.setattr(orc, "ORCFile", TrackingFile)
    scanner = OrcScanner(
        schema=table.schema,
        predicate=col("x") >= 3000,
        statistics_pruning="stripe",
        batch_size=17,
        limit=20,
    )
    rows = [
        row
        for batch in scanner.create_reader().read(manifest)
        for row in batch.to_pylist()
    ]
    assert rows == table.slice(3000, 20).to_pylist()
    assert len(sources) == len(decoded) == 1
    assert sources[0].closed


@pytest.mark.parametrize(
    "original,replacement", [(b"2.2.1", b"\xff" * 5), (b"\x1a\x01x", b"\x1a\x01\xff")]
)
def test_invalid_utf8_writer_and_field_names_are_unavailable(original, replacement):
    data = _tail_bytes(_footer()).replace(original, replacement)
    with pytest.raises(MetadataUnavailable):
        read_metadata(pa.BufferReader(data))


def test_invalid_utf8_bounds_are_not_used():
    footer = Footer()
    stat = footer.statistics.add(number_of_values=1, has_null=False)
    stat.string_statistics.minimum = "a"
    stat.string_statistics.maximum = "z"
    encoded = footer.SerializeToString().replace(b"\x0a\x01a", b"\x0a\x01\xff")
    try:
        parsed = _parse(Footer, encoded)
    except MetadataUnavailable:
        # protobuf 3.20 raises UnicodeDecodeError; newer proto2 runtimes retain
        # invalid bytes. Both must fail closed for range pruning.
        return
    stats = _statistics(parsed.statistics, 1, {"x": (0, 7)}, True)
    assert stats["x"].minimum is None
    schema = pa.schema([("x", pa.string())])
    assert OrcPredicate(col("x") < "a", schema, schema).can_match(stats, 1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
