"""Tests for Parquet bloom filter row-group pruning (ray-project/ray#67569).

The format-level pieces -- XXH64, the split-block bloom filter, the thrift
compact header, literal serialization -- are checked against known vectors
and hand-built bytes, so they run on any pyarrow Ray supports. The
end-to-end pruning tests read files pyarrow itself wrote bloom filters
into; seeing the filter offsets off the column chunk metadata needs
pyarrow >= 25.0.0, so on older versions those tests skip and the rest still
guard the pure-Python logic.
"""

import os
import random
import struct
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from packaging.version import parse as parse_version
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.parquet_bloom_filter import (
    _parse_bloom_filter_header,
    _sbbf_check,
    _sbbf_insert,
    _serialize_literal,
    prepare_bloom_probes,
    row_group_may_match,
    xxhash64,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_equality_predicate_values,
)
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col, lit

_PYARROW_VERSION = get_pyarrow_version()
requires_bloom_filter_metadata = pytest.mark.skipif(
    _PYARROW_VERSION is None or _PYARROW_VERSION < parse_version("25.0.0"),
    reason="column chunk bloom_filter_offset/length metadata needs pyarrow >= 25.0.0",
)


class TestXxHash64:
    def test_known_vector_empty_input(self):
        # The XXH64(seed=0) of the empty string, from the xxHash spec.
        assert xxhash64(b"") == 0xEF46DB3751D8E999

    def test_seed_changes_the_hash(self):
        assert xxhash64(b"payload", seed=0) != xxhash64(b"payload", seed=1)

    def test_input_length_boundaries(self):
        # 0/1/4/5/8/9/31/32/33/40 cross every stripe and tail boundary of the
        # algorithm; each must digest without error, and no two distinct
        # inputs may collide.
        lengths = (0, 1, 4, 5, 8, 9, 31, 32, 33, 40)
        hashes = {xxhash64(bytes(range(length))) for length in lengths}
        assert len(hashes) == len(lengths)


class TestSbbf:
    def test_inserted_hashes_are_present(self):
        # No false negatives: everything inserted must check as present.
        bitset = bytearray(32 * 256)
        hashes = [xxhash64(struct.pack("<q", v)) for v in range(64)]
        for h in hashes:
            _sbbf_insert(bitset, h)
        for h in hashes:
            assert _sbbf_check(bytes(bitset), h)

    def test_absent_hashes_are_absent(self):
        # 64 inserts set at most 512 of 65536 bits, so a false positive needs
        # eight lucky hits at once; the assert holds with huge margin.
        bitset = bytearray(32 * 256)
        for v in range(64):
            _sbbf_insert(bitset, xxhash64(struct.pack("<q", v)))
        absent = (xxhash64(struct.pack("<q", v)) for v in range(1000, 1200))
        assert not any(_sbbf_check(bytes(bitset), h) for h in absent)

    def test_empty_bitset_reads_as_may_match(self):
        # A bitless filter is not a valid SBBF; the checker must have no
        # opinion rather than answer "definitely not".
        assert _sbbf_check(b"", xxhash64(b"x"))


def _varint(n):
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _header_bytes(
    num_bytes=32,
    *,
    algorithm=1,
    hash_fn=1,
    compression=1,
    with_num_bytes=True,
):
    """A hand-rolled ``BloomFilterHeader`` in thrift compact protocol.

    All fields sit at consecutive ids, so every field header carries a delta
    of 1 (the first carries 2 when ``numBytes`` is left out and ids start at
    2). Fields 2-4 are unions whose only members -- ``SplitBlockAlgorithm``,
    ``XxHash``, ``Uncompressed`` -- are field id 1 of an empty struct;
    passing a different member id simulates a writer using something the
    checker does not implement.
    """
    out = bytearray()
    last_field_id = 0
    if with_num_bytes:
        out.append(((1 - last_field_id) << 4) | 0x05)  # field 1: i32
        out += _varint(num_bytes * 2)  # zigzag of a positive i32
        last_field_id = 1
    for member in (algorithm, hash_fn, compression):
        field_id = last_field_id + 1
        out.append(((field_id - last_field_id) << 4) | 0x0C)  # union: struct
        out.append((member << 4) | 0x0C)  # member at field id `member`
        out.append(0x00)  # the member struct is empty: STOP
        out.append(0x00)  # union STOP
        last_field_id = field_id
    out.append(0x00)  # header STOP
    return bytes(out)


class TestBloomFilterHeader:
    def test_well_formed_header(self):
        # Two bytes of field 1, three four-byte unions, one STOP.
        assert _parse_bloom_filter_header(_header_bytes(32)) == (32, 15)

    def test_multibyte_num_bytes(self):
        # 3200 zigzags to 6400, a two-byte varint.
        assert _parse_bloom_filter_header(_header_bytes(32 * 100)) == (3200, 16)

    def test_non_split_block_algorithm_is_rejected(self):
        assert _parse_bloom_filter_header(_header_bytes(32, algorithm=2)) is None

    def test_non_xxhash_hash_is_rejected(self):
        assert _parse_bloom_filter_header(_header_bytes(32, hash_fn=2)) is None

    def test_compressed_filter_is_rejected(self):
        assert _parse_bloom_filter_header(_header_bytes(32, compression=2)) is None

    def test_missing_num_bytes_is_rejected(self):
        assert _parse_bloom_filter_header(_header_bytes(with_num_bytes=False)) is None

    def test_bitset_not_whole_blocks_is_rejected(self):
        # A bitset is whole 256-bit blocks; 30 bytes is not one.
        assert _parse_bloom_filter_header(_header_bytes(30)) is None

    def test_truncated_header_is_rejected(self):
        assert _parse_bloom_filter_header(_header_bytes(32)[:-1]) is None

    def test_garbage_is_rejected(self):
        assert _parse_bloom_filter_header(b"\xff" * 8) is None


@pytest.fixture(scope="module")
def typed_schema(tmp_path_factory):
    """A tiny Parquet file with a column per type of interest."""
    table = pa.table(
        {
            "i64": pa.array([1], pa.int64()),
            "i32": pa.array([1], pa.int32()),
            "f32": pa.array([1.5], pa.float32()),
            "f64": pa.array([1.5], pa.float64()),
            "str": pa.array(["a"], pa.string()),
            "bin": pa.array([b"x"], pa.binary()),
            "dec": pa.array([Decimal("1.23")], pa.decimal128(10, 2)),
            "flag": pa.array([True], pa.bool_()),
            "ts": pa.array([datetime(2020, 1, 1)], pa.timestamp("us")),
        }
    )
    path = tmp_path_factory.mktemp("typed") / "typed.parquet"
    pq.write_table(table, str(path))
    return pq.read_metadata(str(path)).schema


def _leaf(schema, path):
    for i in range(len(schema)):
        if schema.column(i).path == path:
            return i
    raise KeyError(path)


class TestSerializeLiteral:
    def test_int64_literal(self, typed_schema):
        assert _serialize_literal(
            5, typed_schema.column(_leaf(typed_schema, "i64"))
        ) == (struct.pack("<q", 5))

    def test_int32_literal(self, typed_schema):
        assert _serialize_literal(
            5, typed_schema.column(_leaf(typed_schema, "i32"))
        ) == (struct.pack("<i", 5))

    def test_int_literal_out_of_int32_range(self, typed_schema):
        column = typed_schema.column(_leaf(typed_schema, "i32"))
        assert _serialize_literal(2**31, column) is None

    def test_int_literal_out_of_int64_range(self, typed_schema):
        column = typed_schema.column(_leaf(typed_schema, "i64"))
        assert _serialize_literal(2**63, column) is None

    def test_float_literals(self, typed_schema):
        f32 = typed_schema.column(_leaf(typed_schema, "f32"))
        f64 = typed_schema.column(_leaf(typed_schema, "f64"))
        assert _serialize_literal(2.5, f32) == struct.pack("<f", 2.5)
        assert _serialize_literal(2.5, f64) == struct.pack("<d", 2.5)

    def test_int_literal_on_float_column(self, typed_schema):
        # An int literal hashes as its float value's bytes.
        f64 = typed_schema.column(_leaf(typed_schema, "f64"))
        assert _serialize_literal(2, f64) == struct.pack("<d", 2.0)

    def test_string_literal(self, typed_schema):
        column = typed_schema.column(_leaf(typed_schema, "str"))
        assert _serialize_literal("hello", column) == b"hello"

    def test_binary_literal(self, typed_schema):
        column = typed_schema.column(_leaf(typed_schema, "bin"))
        assert _serialize_literal(b"\x01\x02", column) == b"\x01\x02"

    def test_string_literal_on_binary_column(self, typed_schema):
        # Without the String logical type, a str's bytes are not the column's.
        column = typed_schema.column(_leaf(typed_schema, "bin"))
        assert _serialize_literal("x", column) is None

    def test_decimal_literal_is_skipped(self, typed_schema):
        # The literal's digits must be scale-shifted to become physical
        # bytes, a conversion the checker does not attempt.
        column = typed_schema.column(_leaf(typed_schema, "dec"))
        assert _serialize_literal(Decimal("1.23"), column) is None

    def test_bool_literal_is_skipped(self, typed_schema):
        # bool is an int to Python but not to Parquet's BOOLEAN encoding.
        column = typed_schema.column(_leaf(typed_schema, "flag"))
        assert _serialize_literal(True, column) is None

    def test_datetime_literal_is_skipped(self, typed_schema):
        # Spelling a datetime in the column's timestamp unit is a conversion.
        column = typed_schema.column(_leaf(typed_schema, "ts"))
        assert _serialize_literal(datetime(2020, 1, 1), column) is None

    def test_wrong_type_literal_is_skipped(self, typed_schema):
        column = typed_schema.column(_leaf(typed_schema, "i64"))
        assert _serialize_literal("5", column) is None


class TestEqualityPredicateExtraction:
    def test_eq(self):
        assert get_equality_predicate_values(col("u") == 5) == {"u": [5]}

    def test_reversed_eq(self):
        assert get_equality_predicate_values(lit(5) == col("u")) == {"u": [5]}

    def test_and_of_conjuncts(self):
        # A non-equality conjunct contributes nothing; the equality still does.
        assert get_equality_predicate_values((col("u") == 5) & (col("x") > 1)) == {
            "u": [5]
        }

    def test_is_in(self):
        assert get_equality_predicate_values(col("u").is_in([1, 2, 3])) == {
            "u": [1, 2, 3]
        }

    def test_is_in_and_eq_intersect_on_same_column(self):
        assert get_equality_predicate_values(
            (col("u") == 5) & col("u").is_in([5, 6])
        ) == {"u": [5]}

    def test_disjoint_eq_on_same_column_yields_empty_probe_set(self):
        # An unsatisfiable conjunction has no literal to probe with; an empty
        # probe set must not be read as "drop every row group".
        assert get_equality_predicate_values((col("a") == 1) & (col("a") == 2)) == {
            "a": []
        }

    def test_or_contributes_nothing(self):
        # Under OR a "definitely not" rules nothing out.
        assert get_equality_predicate_values((col("u") == 5) | (col("x") == 6)) == {}

    def test_not_contributes_nothing(self):
        # NOT flips the polarity: a value the filter rules out makes the
        # negated predicate hold everywhere.
        assert get_equality_predicate_values(~(col("u") == 5)) == {}

    def test_not_in_contributes_nothing(self):
        assert get_equality_predicate_values(col("u").not_in([1, 2])) == {}

    def test_ne_contributes_nothing(self):
        assert get_equality_predicate_values(col("u") != 5) == {}

    def test_arithmetic_equality_contributes_nothing(self):
        # (u + 1) == 5 is an equality of a computed value, not of the column.
        assert get_equality_predicate_values(col("u") + 1 == 5) == {}

    def test_alias_contributes_nothing(self):
        assert get_equality_predicate_values(col("u").alias("v") == 5) == {}

    def test_column_vs_column_contributes_nothing(self):
        assert get_equality_predicate_values(col("u") == col("v")) == {}

    def test_none_expr(self):
        assert get_equality_predicate_values(None) == {}


class TestPrepareBloomProbes:
    def test_single_probe(self, typed_schema):
        probes = prepare_bloom_probes(typed_schema, {"i64": [5]})
        assert probes == [
            (_leaf(typed_schema, "i64"), [xxhash64(struct.pack("<q", 5))])
        ]

    def test_columns_come_in_leaf_order(self, typed_schema):
        probes = prepare_bloom_probes(typed_schema, {"str": ["a"], "i64": [1]})
        assert [leaf for leaf, _ in probes] == [
            _leaf(typed_schema, "i64"),
            _leaf(typed_schema, "str"),
        ]

    def test_column_absent_from_schema(self, typed_schema):
        assert prepare_bloom_probes(typed_schema, {"missing": [5]}) == []

    def test_unserializable_literals_leave_no_probe(self, typed_schema):
        assert prepare_bloom_probes(typed_schema, {"dec": [Decimal("1.23")]}) == []
        assert prepare_bloom_probes(typed_schema, {"flag": [True]}) == []

    def test_no_predicates(self, typed_schema):
        assert prepare_bloom_probes(typed_schema, {}) == []

    def test_mixed_literals_probe_only_the_serializable(self, typed_schema):
        probes = prepare_bloom_probes(typed_schema, {"i64": [1, "not an int"]})
        assert probes == [
            (_leaf(typed_schema, "i64"), [xxhash64(struct.pack("<q", 1))])
        ]


class _FakeColumnChunk:
    def __init__(self, bloom_filter_offset=None, bloom_filter_length=None):
        self.bloom_filter_offset = bloom_filter_offset
        self.bloom_filter_length = bloom_filter_length


class _FakeRowGroup:
    def __init__(self, *chunks):
        self._chunks = chunks

    def column(self, i):
        return self._chunks[i]


@pytest.fixture
def bloom_filter_file(tmp_path):
    """A file holding one bloom filter: header plus a bitset holding 42.

    The four junk bytes up front stand in for the "PAR1" magic a real file
    starts with, keeping the filter's offset nonzero the way real offsets
    are.
    """
    present = xxhash64(struct.pack("<q", 42))
    absent = xxhash64(struct.pack("<q", 999_999))
    bitset = bytearray(32 * 8)
    _sbbf_insert(bitset, present)
    header = _header_bytes(len(bitset))
    path = tmp_path / "filter.bin"
    path.write_bytes(b"junk" + header + bytes(bitset))
    return {
        "path": str(path),
        "offset": 4,
        "length": len(header) + len(bitset),
        "present": present,
        "absent": absent,
    }


class TestRowGroupMayMatch:
    def test_present_value_may_match(self, bloom_filter_file):
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk(f["offset"], f["length"]))
        assert row_group_may_match(
            filesystem=LocalFileSystem(),
            path=f["path"],
            row_group=rg,
            probes=[(0, [f["present"]])],
        )

    def test_absent_value_does_not_match(self, bloom_filter_file):
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk(f["offset"], f["length"]))
        assert not row_group_may_match(
            filesystem=LocalFileSystem(),
            path=f["path"],
            row_group=rg,
            probes=[(0, [f["absent"]])],
        )

    def test_is_in_matches_if_any_value_may_match(self, bloom_filter_file):
        # is_in probes every literal; one "maybe" keeps the row group.
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk(f["offset"], f["length"]))
        assert row_group_may_match(
            filesystem=LocalFileSystem(),
            path=f["path"],
            row_group=rg,
            probes=[(0, [f["absent"], f["present"]])],
        )

    def test_length_unknown_falls_back_to_header_probe(self, bloom_filter_file):
        # Writers predating bloom_filter_length leave it unset; the header
        # then sizes the bitset read.
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk(f["offset"], None))
        kwargs = dict(filesystem=LocalFileSystem(), path=f["path"], row_group=rg)
        assert not row_group_may_match(**kwargs, probes=[(0, [f["absent"]])])
        assert row_group_may_match(**kwargs, probes=[(0, [f["present"]])])

    def test_chunk_without_bloom_filter_keeps_group(self, bloom_filter_file):
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk())
        assert row_group_may_match(
            filesystem=LocalFileSystem(),
            path=f["path"],
            row_group=rg,
            probes=[(0, [f["absent"]])],
        )

    def test_unreadable_file_keeps_group(self, bloom_filter_file):
        f = bloom_filter_file
        rg = _FakeRowGroup(_FakeColumnChunk(f["offset"], f["length"]))
        assert row_group_may_match(
            filesystem=LocalFileSystem(),
            path=f["path"] + ".missing",
            row_group=rg,
            probes=[(0, [f["absent"]])],
        )

    def test_truncated_filter_keeps_group(self, tmp_path):
        # The header promises 8 blocks the file does not hold.
        header = _header_bytes(32 * 8)
        path = tmp_path / "truncated.bin"
        path.write_bytes(b"junk" + header + bytes(32 * 4))
        rg = _FakeRowGroup(_FakeColumnChunk(4, len(header) + 32 * 4))
        assert row_group_may_match(
            filesystem=LocalFileSystem(),
            path=str(path),
            row_group=rg,
            probes=[(0, [xxhash64(struct.pack("<q", 42))])],
        )


_ROWS_PER_GROUP = 50
_NUM_GROUPS = 5


@pytest.fixture
def unsorted_file(tmp_path):
    """The issue #67569 shape: sorted ``x``, shuffled ``u``, bloom on ``u``.

    ``u`` draws 250 distinct values uniformly from one wide range, so every
    row group's min/max statistics span nearly the whole range -- exactly
    why statistics cannot prune a point lookup while the bloom filter can.
    """
    rng = random.Random(42)
    values = rng.sample(range(1_000_000), _ROWS_PER_GROUP * _NUM_GROUPS)
    xs = list(range(_ROWS_PER_GROUP * _NUM_GROUPS))
    table = pa.table({"x": xs, "u": values})
    path = tmp_path / "point_lookup.parquet"
    pq.write_table(
        table,
        str(path),
        row_group_size=_ROWS_PER_GROUP,
        bloom_filter_options={
            "u": {"ndv": _ROWS_PER_GROUP * _NUM_GROUPS, "fpp": 0.001}
        },
    )
    return str(path), values


def _reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


def _kept_row_group_ids(path, filter_expr, **reader_kwargs):
    chunks = _reader(filter_expr=filter_expr, **reader_kwargs)._read_and_chunk(
        FileInfo(path, os.path.getsize(path))
    )
    return [rg.unit_ids[0] for rg in chunks.row_groups]


@requires_bloom_filter_metadata
class TestFooterReaderBloomPruning:
    def _clean_lookup(self, path, values, group):
        """A value of ``group`` that only that group can answer "maybe" for.

        The value must sit inside every group's min/max statistics (so a
        pruned foreign group is the bloom filter's doing alone) and probe as
        "definitely not" in every foreign group's filter (no false positive,
        which fpp=0.001 makes the common case).
        """
        metadata = pq.read_metadata(path)
        u_index = _leaf(metadata.schema, "u")
        stats = [
            metadata.row_group(g).column(u_index).statistics for g in range(_NUM_GROUPS)
        ]
        for v in values[group * _ROWS_PER_GROUP : (group + 1) * _ROWS_PER_GROUP]:
            if not all(s.min <= v <= s.max for s in stats):
                continue
            probes = prepare_bloom_probes(metadata.schema, {"u": [v]})
            if all(
                row_group_may_match(
                    filesystem=LocalFileSystem(),
                    path=path,
                    row_group=metadata.row_group(other),
                    probes=probes,
                )
                == (other == group)
                for other in range(_NUM_GROUPS)
            ):
                return v
        raise AssertionError("fixture has no clean point-lookup value")

    def test_point_lookup_keeps_only_the_owning_group(self, unsorted_file):
        path, values = unsorted_file
        v = self._clean_lookup(path, values, group=2)
        assert _kept_row_group_ids(path, col("u") == v) == [2]

    def test_no_false_negatives(self, unsorted_file):
        # Whatever else pruning does, a value a group really holds must keep
        # that group: bloom filters have no false negatives.
        path, values = unsorted_file
        for group in range(_NUM_GROUPS):
            v = values[group * _ROWS_PER_GROUP]
            assert group in _kept_row_group_ids(path, col("u") == v)

    def test_is_in_keeps_each_owning_group(self, unsorted_file):
        path, values = unsorted_file
        v0 = self._clean_lookup(path, values, group=0)
        v3 = self._clean_lookup(path, values, group=3)
        kept = _kept_row_group_ids(path, col("u").is_in([v0, v3]))
        assert {0, 3} <= set(kept)

    def test_file_without_bloom_filters_is_unchanged(self, tmp_path, unsorted_file):
        path, values = unsorted_file
        v = self._clean_lookup(path, values, group=1)
        plain = str(tmp_path / "plain.parquet")
        pq.write_table(pq.read_table(path), plain, row_group_size=_ROWS_PER_GROUP)
        assert _kept_row_group_ids(plain, col("u") == v) == list(range(_NUM_GROUPS))

    def test_env_var_disables_bloom_pruning(self, unsorted_file, monkeypatch):
        path, values = unsorted_file
        v = self._clean_lookup(path, values, group=2)
        monkeypatch.setenv("RAY_DATA_PARQUET_BLOOM_FILTER_PRUNING", "0")
        assert _kept_row_group_ids(path, col("u") == v) == list(range(_NUM_GROUPS))

    def test_range_predicate_on_bloom_column_is_noop(self, unsorted_file):
        # Bloom filters only answer equality; u > 0 matches every group's
        # statistics and must keep them all.
        path, _ = unsorted_file
        assert _kept_row_group_ids(path, col("u") > 0) == list(range(_NUM_GROUPS))

    def test_predicate_on_column_without_bloom_filter(self, unsorted_file):
        # x carries no bloom filter; its sorted statistics alone pick the
        # group, and the bloom pass must not disturb that.
        path, _ = unsorted_file
        x_value = 2 * _ROWS_PER_GROUP + 7
        assert _kept_row_group_ids(path, col("x") == x_value) == [2]

    def test_pyarrow_header_bytes_round_trip(self, unsorted_file):
        # Validate the thrift parser -- and the claim that the recorded
        # length covers header plus bitset -- against bytes pyarrow itself
        # wrote.
        path, _ = unsorted_file
        metadata = pq.read_metadata(path)
        chunk = metadata.row_group(0).column(_leaf(metadata.schema, "u"))
        offset = chunk.bloom_filter_offset
        length = chunk.bloom_filter_length
        assert offset and length
        with LocalFileSystem().open_input_file(path) as f:
            f.seek(offset)
            data = f.read(length)
        parsed = _parse_bloom_filter_header(data)
        assert parsed is not None
        num_bytes, header_len = parsed
        assert num_bytes % 32 == 0
        assert header_len + num_bytes == length
