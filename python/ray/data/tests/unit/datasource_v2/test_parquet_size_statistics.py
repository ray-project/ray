"""Tests for the targeted Parquet ``SizeStatistics`` Thrift reader.

Hand-decoded binary fails dangerously: a one-byte cursor desync makes the
compact-protocol field-id deltas land on plausible but wrong ids, so the walk
returns numbers rather than raising. Nothing downstream can catch that, because a
decoded size has no second source to be compared against -- which makes this file
the module's actual safety mechanism rather than a regression net.

It attacks correctness from three directions:

- A differential oracle against ``thriftpy2``'s own compact protocol, over a
  matrix of schemas chosen to reach every branch of the walk and the estimator.
- Fault injection that reintroduces the two desync bugs the prototype actually
  hit, asserting that neither can yield a decoded size.
- Hand-built payloads for the shapes the protocol permits but PyArrow never
  writes, which the oracle therefore cannot reach.
"""

import numpy as np
import pytest

import ray.data._internal.datasource_v2.chunkers.parquet_size_statistics as pss
from ray.data._internal.datasource_v2.chunkers.parquet_decoded_size import (
    build_leaf_profiles,
    estimate_row_group_decoded_size,
)
from ray.data._internal.datasource_v2.chunkers.parquet_size_statistics import (
    _CompactReader,
    footer_bytes,
    read_size_statistics,
)

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


# A minimal parquet.thrift covering only the traversal path. Everything else in
# the footer is skipped generically by the protocol, so the oracle needs no more
# than this -- and unlike vendoring the full 1,000-line schema, it cannot drift
# out of sync with upstream in ways that matter here. All fields are optional so
# a missing one is a null rather than a parse error.
_ORACLE_THRIFT = """
struct SizeStatistics {
  1: optional i64 unencoded_byte_array_data_bytes;
  2: optional list<i64> repetition_level_histogram;
  3: optional list<i64> definition_level_histogram;
}
struct ColumnMetaData {
  5: optional i64 num_values;
  6: optional i64 total_uncompressed_size;
  16: optional SizeStatistics size_statistics;
}
struct ColumnChunk {
  3: optional ColumnMetaData meta_data;
}
struct RowGroup {
  1: optional list<ColumnChunk> columns;
}
struct FileMetaData {
  4: optional list<RowGroup> row_groups;
}
"""

N = 500


def _table_cases():
    """Schemas spanning every branch of the decoder and the estimator."""
    return {
        "flat-numeric": pa.table({"a": pa.array(np.arange(N, dtype="int64"))}),
        "strings": pa.table({"a": pa.array([f"v{i % 7}" for i in range(N)])}),
        "nullable-strings": pa.table(
            {"a": pa.array([None if i % 3 == 0 else f"v{i}" for i in range(N)])}
        ),
        "mixed": pa.table(
            {
                "i": pa.array(np.arange(N, dtype="int64")),
                "s": pa.array([f"s{i}" for i in range(N)]),
                "b": pa.array([i % 2 == 0 for i in range(N)]),
                "f": pa.array(np.random.rand(N)),
            }
        ),
        "list-int64": pa.table(
            {"a": pa.array([[1, 2, 3]] * N, type=pa.list_(pa.int64()))}
        ),
        "struct-nested": pa.table(
            {
                "a": pa.array(
                    [{"s": f"v{i % 5}", "l": [1, 2]} for i in range(N)],
                    type=pa.struct([("s", pa.string()), ("l", pa.list_(pa.int64()))]),
                )
            }
        ),
        "decimal-timestamp": pa.table(
            {
                "d": pa.array(range(N), type=pa.decimal128(12, 3)),
                "t": pa.array(np.arange(N, dtype="int64"), type=pa.timestamp("us")),
            }
        ),
        # Many row groups and columns: the widest walk, and the case where a
        # cursor desync has the most chances to show up.
        "wide": pa.table(
            {f"c{c}": pa.array([f"v{c}_{i}" for i in range(N)]) for c in range(12)}
        ),
    }


@pytest.fixture(scope="module")
def written_files(tmp_path_factory):
    """Each case written to disk with several row groups, plus its metadata."""
    out = {}
    directory = tmp_path_factory.mktemp("size_stats")
    for label, table in _table_cases().items():
        path = directory / f"{label}.parquet"
        pq.write_table(table, path, row_group_size=max(1, len(table) // 4))
        out[label] = (str(path), pq.read_metadata(str(path)))
    return out


# ---------------------------------------------------------------------------
# Footer byte recovery
# ---------------------------------------------------------------------------


def test_footer_bytes_matches_serialized_size(written_files):
    _, metadata = written_files["flat-numeric"]
    buf = footer_bytes(metadata)
    assert buf is not None
    # __reduce__ hands back exactly the serialized footer, nothing more.
    assert len(buf) == metadata.serialized_size
    # Signed 'b' would make every byte >= 0x80 negative and corrupt varints.
    assert buf.format == "B"
    assert all(value >= 0 for value in buf)


def test_footer_bytes_returns_none_for_unreducible_object():
    class NoReduce:
        def __reduce__(self):
            raise TypeError("not picklable")

    assert footer_bytes(NoReduce()) is None


# ---------------------------------------------------------------------------
# Differential oracle
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def oracle_module(tmp_path_factory):
    thriftpy2 = pytest.importorskip("thriftpy2")
    path = tmp_path_factory.mktemp("thrift") / "parquet_oracle.thrift"
    path.write_text(_ORACLE_THRIFT)
    return thriftpy2.load(str(path), module_name="parquet_oracle_thrift")


def _oracle_parse(oracle_module, raw):
    """``[row_group][leaf]`` size statistics via thriftpy2's own compact protocol."""
    from thriftpy2.protocol.compact import TCompactProtocol
    from thriftpy2.transport import TMemoryBuffer

    file_metadata = oracle_module.FileMetaData()
    file_metadata.read(TCompactProtocol(TMemoryBuffer(bytes(raw))))
    return [
        [
            None
            if column.meta_data is None or column.meta_data.size_statistics is None
            else (
                column.meta_data.size_statistics.unencoded_byte_array_data_bytes,
                tuple(
                    column.meta_data.size_statistics.repetition_level_histogram or ()
                ),
                tuple(
                    column.meta_data.size_statistics.definition_level_histogram or ()
                ),
            )
            for column in row_group.columns
        ]
        for row_group in file_metadata.row_groups
    ]


@pytest.mark.parametrize("label", list(_table_cases()))
def test_matches_thriftpy2_oracle(written_files, oracle_module, label):
    """Our targeted walk must agree with a full independent parse, chunk for chunk."""
    _, metadata = written_files[label]
    ours = read_size_statistics(metadata)
    assert ours is not None, "expected PyArrow-written files to carry SizeStatistics"

    theirs = _oracle_parse(oracle_module, footer_bytes(metadata))
    assert len(ours) == len(theirs) == metadata.num_row_groups
    for our_row_group, their_row_group in zip(ours, theirs):
        assert len(our_row_group) == len(their_row_group)
        for our_leaf, their_leaf in zip(our_row_group, their_row_group):
            assert our_leaf is not None and their_leaf is not None
            assert (
                our_leaf.unencoded_byte_array_data_bytes,
                our_leaf.repetition_level_histogram,
                our_leaf.definition_level_histogram,
            ) == their_leaf


# ---------------------------------------------------------------------------
# Decoded values against ground truth
# ---------------------------------------------------------------------------


def test_unencoded_bytes_matches_true_character_count(tmp_path):
    values = ["hello", "worldworld", "x"] * 1000
    path = tmp_path / "strings.parquet"
    pq.write_table(pa.table({"a": pa.array(values)}), path, row_group_size=len(values))
    stats = read_size_statistics(pq.read_metadata(str(path)))

    assert stats is not None
    # Exclusive of length prefixes, so exactly the sum of the character bytes.
    assert stats[0][0].unencoded_byte_array_data_bytes == sum(
        len(value) for value in values
    )


def test_definition_histogram_counts_nulls(tmp_path):
    values = [None if i % 3 == 0 else "abc" for i in range(999)]
    path = tmp_path / "nullable.parquet"
    pq.write_table(pa.table({"a": pa.array(values)}), path, row_group_size=len(values))
    stats = read_size_statistics(pq.read_metadata(str(path)))

    assert stats is not None
    # Last bucket is the count at max definition level, i.e. the non-nulls.
    assert stats[0][0].definition_level_histogram == (333, 666)


def test_repetition_histogram_counts_lists(tmp_path):
    path = tmp_path / "lists.parquet"
    pq.write_table(
        pa.table({"a": pa.array([[1, 2, 3]] * 1000, type=pa.list_(pa.int64()))}),
        path,
        row_group_size=1000,
    )
    stats = read_size_statistics(pq.read_metadata(str(path)))

    assert stats is not None
    # Level 0 is the number of top-level lists; level 1 the continuations.
    assert stats[0][0].repetition_level_histogram == (1000, 2000)


def test_fixed_width_leaves_omit_unencoded_bytes(written_files):
    """Sub-field 1 is BYTE_ARRAY-only per spec, so numeric leaves must lack it.

    A gate requiring it on *every* leaf would silently fall back for every flat
    numeric schema while still paying the decode cost, so this asymmetry is
    load-bearing rather than incidental.
    """
    _, metadata = written_files["mixed"]
    stats = read_size_statistics(metadata)
    assert stats is not None

    for rg_idx, leaves in enumerate(stats):
        for leaf_idx, leaf in enumerate(leaves):
            physical_type = metadata.row_group(rg_idx).column(leaf_idx).physical_type
            has_unencoded = leaf.unencoded_byte_array_data_bytes is not None
            assert has_unencoded == (physical_type == "BYTE_ARRAY")


# ---------------------------------------------------------------------------
# Skip paths: the cursor must land exactly on the next field
# ---------------------------------------------------------------------------
#
# Skipping is where a desync originates, and it is only self-evident when the
# cursor stops one byte off. PyArrow-written footers do not contain every
# container shape the protocol allows -- no double or byte lists, rarely a
# long-form field id -- so the widths for those are asserted directly here
# rather than relying on the oracle to stumble over them.


def _reader(*byte_values):
    return _CompactReader(memoryview(bytes(byte_values)).cast("B"))


def _list_header(size, element_type):
    assert size < 0x0F, "sizes >= 15 use the varint escape, not this helper"
    return (size << 4) | element_type


@pytest.mark.parametrize(
    "element_type,payload,label",
    [
        (pss._DOUBLE, [0xFF] * 24, "3 doubles are 8 bytes each"),
        (pss._BYTE, [0x01, 0x02, 0x03], "3 bytes are 1 byte each"),
        # A bool costs a byte inside a container, unlike inside a struct where
        # it lives in the type nibble.
        (pss._BOOL_TRUE, [0x01, 0x02, 0x01], "3 container bools are 1 byte each"),
        # Varints are the one element type whose width is in the data.
        (pss._I64, [0xAC, 0x02, 0x02, 0x80, 0x01], "3 zigzag varints, mixed widths"),
    ],
)
def test_skip_list_consumes_exactly_the_elements(element_type, payload, label):
    size = 3
    reader = _reader(_list_header(size, element_type), *payload)

    reader._skip_list()

    # +1 for the list header itself.
    assert reader._pos == 1 + len(payload), label


def test_skip_list_handles_nested_binary_elements():
    """Binary elements carry their own length prefix, so they cannot be bulk-skipped."""
    reader = _reader(
        _list_header(2, pss._BINARY),
        0x03,
        0xAA,
        0xBB,
        0xCC,  # 3-byte value
        0x01,
        0xDD,  # 1-byte value
    )

    reader._skip_list()

    assert reader._pos == 7


def test_skip_struct_steps_over_a_long_form_field_id():
    """A field id too large for the nibble is a varint the skip path must consume.

    The delta-0 escape is rare in practice, which is exactly why it is worth
    asserting: getting it wrong shifts every subsequent field id in the struct.
    """
    reader = _reader(
        0x06,  # delta 0 (long form), type I64
        0x20,  # zigzag(16) == 32 -> field id 16
        0x0A,  # the i64 value
        0x00,  # STOP
    )

    reader._skip_struct()

    assert reader._pos == 4


def test_skip_struct_recurses_into_nested_structs():
    reader = _reader(
        0x1C,  # delta 1, type STRUCT
        0x16,  # nested: delta 1, type I64
        0x02,  # nested value
        0x00,  # STOP of nested struct
        0x00,  # STOP of outer struct
    )

    reader._skip_struct()

    assert reader._pos == 5


# ---------------------------------------------------------------------------
# Writer quirks the compact protocol permits but PyArrow does not produce
# ---------------------------------------------------------------------------
#
# arrow-rs hit both of these after shipping its own custom decoder, so they are
# known-real rather than hypothetical, and neither is reachable through a
# PyArrow-written file -- meaning the oracle test above cannot catch a
# regression here.


def test_empty_list_with_zero_element_type_is_accepted():
    """An empty list may declare element type 0, which is not a valid type id.

    A decoder that validates the element type before checking the count rejects
    such a footer outright; that was arrow-rs #8826, filed against the release
    that introduced their custom parser. Our guard checks the count first, so
    this pins that ordering -- it currently holds for an unrelated reason (an
    empty list's element type carries no information), which is exactly the kind
    of incidental correctness that a later edit can quietly undo.
    """
    reader = _reader(
        0x16,  # delta 1, I64 -> unencoded_byte_array_data_bytes
        0x0A,  # zigzag(10) == 5
        0x19,  # delta 1, LIST -> repetition_level_histogram
        0x00,  # size 0, element type 0
        0x19,  # delta 1, LIST -> definition_level_histogram
        0x00,  # size 0, element type 0
        0x00,  # STOP
    )

    result = reader._size_statistics()

    assert result == pss.LeafSizeStats(5, (), ())
    assert reader._pos == 7


def test_empty_list_with_zero_element_type_is_skippable():
    """The same shape must also be steppable when it is a field we do not read."""
    reader = _reader(
        0x19,  # delta 1, LIST
        0x00,  # size 0, element type 0
        0x00,  # STOP
    )

    reader._skip_struct()

    assert reader._pos == 3


def test_column_metadata_reads_absolute_field_ids():
    """Field 16 given in long form, i.e. no reliance on ascending deltas.

    arrow-rs took a faster path that assumes ids are always delta-encoded and in
    order, and noted it would have to be reverted if a writer using absolute ids
    turned up (#8190). Our inlined ``_column_metadata`` keeps the general path,
    and this is what holds it to that -- the long-form branch is otherwise
    unexercised by PyArrow footers.
    """
    reader = _reader(
        0x06,  # delta 0 (long form), type I64 -> some field we skip
        0x0A,  # zigzag(10) == 5
        0x02,  # its value
        0x0C,  # delta 0, type STRUCT
        0x20,  # zigzag(32) == 16 -> size_statistics
        0x16,  # delta 1, I64 -> unencoded_byte_array_data_bytes
        0x0A,  # zigzag(10) == 5
        0x00,  # STOP of size_statistics
        0x00,  # STOP of ColumnMetaData
    )

    result = reader._column_metadata()

    assert result == pss.LeafSizeStats(5, (), ())


def test_column_metadata_finds_field_16_after_a_higher_id():
    """A descending id, which only the long-form escape can express.

    A delta is an unsigned nibble and can only move forward, so reaching field 16
    after field 20 requires an absolute id -- and it must still be recognized.
    """
    reader = _reader(
        0x0C,  # long form, STRUCT
        0x28,  # zigzag(40) == 20 -> an unknown field, skipped
        0x00,  # STOP of that struct
        0x0C,  # long form, STRUCT
        0x20,  # zigzag(32) == 16 -> size_statistics, going backwards
        0x16,  # delta 1, I64 -> unencoded_byte_array_data_bytes
        0x0A,  # zigzag(10) == 5
        0x00,  # STOP of size_statistics
        0x00,  # STOP of ColumnMetaData
    )

    result = reader._column_metadata()

    assert result == pss.LeafSizeStats(5, (), ())


# ---------------------------------------------------------------------------
# Fault injection: the cross-check must convert corruption into a fallback
# ---------------------------------------------------------------------------


def test_decoding_is_insensitive_to_view_signedness(written_files, monkeypatch):
    """Dropping the ``.cast("B")`` must not change the decoded values.

    ``memoryview(pyarrow.Buffer)`` is format 'b', so every byte >= 0x80 reads back
    negative. That does *not* corrupt this reader: it only ever consumes bytes
    through masks (``& 0x7F``, ``& 0x80``, ``>> 4 & 0x0F``), and Python's
    arbitrary-precision two's complement makes those extract identical bits from a
    sign-extended value. The cast is defensive, so this pins the equivalence
    rather than asserting a fallback that would never fire.
    """
    _, metadata = written_files["wide"]
    unsigned = read_size_statistics(metadata)
    assert unsigned is not None  # healthy baseline

    monkeypatch.setattr(
        pss, "footer_bytes", lambda md: memoryview(md.__reduce__()[1][0])
    )
    assert read_size_statistics(metadata) == unsigned


def _buggy_binary_skip(original):
    """``self._pos += self._varint()``, the desync this module was built around.

    Python evaluates the left operand first, so the advance ``_varint`` performs
    while reading the length is discarded and every binary field consumes one
    byte too few.
    """

    def skip(self, field_type):
        if field_type == pss._BINARY:
            self._pos += self._varint()  # noqa: B909 - the bug, on purpose
            return
        return original(self, field_type)

    return skip


def _short_varint_skip(self):
    """A skip that stops one byte before the varint terminator."""
    buf, pos = self._buf, self._pos
    while buf[pos] & 0x80:
        pos += 1
    self._pos = pos  # missing the +1


@pytest.mark.parametrize("bug", ["binary-skip", "short-varint"])
@pytest.mark.parametrize("label", list(_table_cases()))
def test_a_desync_never_yields_a_decoded_size(written_files, monkeypatch, bug, label):
    """No cursor desync may produce an exact size, only a fallback.

    This is the whole safety argument for the module, so it is asserted through
    the estimator rather than stopping at the walk: what must never happen is a
    *number* derived from a drifted cursor, and there are two ways to be safe --
    the walk declines outright, or it reports a leaf as ``None`` and the estimator
    declines. Both end at the caller's uncompressed-size fallback.

    Verifying decoded values against PyArrow used to make this trivially true.
    That check is gone, so this pins that the structural guards which remain --
    the type assertions and the row-group/leaf shape check -- are still sufficient
    for the failure modes we have actually hit.
    """
    _, metadata = written_files[label]
    assert read_size_statistics(metadata) is not None, "healthy baseline"

    if bug == "binary-skip":
        monkeypatch.setattr(
            _CompactReader, "_skip", _buggy_binary_skip(_CompactReader._skip)
        )
    else:
        monkeypatch.setattr(_CompactReader, "_skip_varint", _short_varint_skip)

    try:
        stats = read_size_statistics(metadata)
    except Exception:
        return  # raising is a fallback too: the caller catches and degrades

    if stats is None:
        return

    leaf_profiles = build_leaf_profiles(metadata.schema)
    for rg_idx, leaves in enumerate(stats):
        assert (
            estimate_row_group_decoded_size(
                metadata.row_group(rg_idx), leaf_profiles, None, leaves
            )
            is None
        ), f"{label}/{bug} produced a size from a desynced cursor"


def test_truncated_footer_falls_back(written_files, monkeypatch):
    _, metadata = written_files["mixed"]

    def truncated(md):
        return memoryview(md.__reduce__()[1][0]).cast("B")[:20]

    monkeypatch.setattr(pss, "footer_bytes", truncated)
    # An IndexError from running off the end must not escape into the caller.
    assert read_size_statistics(metadata) is None


def test_geospatial_struct_shape_is_rejected():
    """Field 16 must be a SizeStatistics, not any struct that lands there.

    ``ColumnMetaData`` field 17 is ``geospatial_statistics``, also a struct, so a
    cursor that drifted by one field would pass a bare "is it a struct" test. Its
    field 1 is a ``BoundingBox`` struct rather than an i64, which the inner type
    assertion is what catches.
    """
    # field 16 (delta 16 from id 0 via long form), type STRUCT, whose field 1 is
    # itself a STRUCT (empty) -- the geospatial shape.
    payload = bytes(
        [
            0x0C,  # long-form field id, type STRUCT
            0x20,  # zigzag(16) == 32
            0x1C,  # delta 1, type STRUCT -> inner field 1 is a struct, not i64
            0x00,  # STOP of the inner struct
            0x00,  # STOP of size_statistics
            0x00,  # STOP of ColumnMetaData
        ]
    )
    reader = _CompactReader(memoryview(payload).cast("B"))
    with pytest.raises(pss._ThriftDesync):
        reader._column_metadata()


def test_unknown_trailing_field_is_tolerated():
    """A future parquet-format field must be skipped, not treated as corruption."""
    payload = bytes(
        [
            0x56,  # delta 5, type I64 -> num_values, which we no longer read
            0x02,  # zigzag(2) == 1
            0x16,  # delta 1, type I64 -> total_uncompressed_size, also skipped
            0x04,  # zigzag(4) == 2
            0x0C,  # long-form id, STRUCT
            0x20,  # zigzag(16) == 32 -> size_statistics
            0x16,  # delta 1, I64 -> unencoded_byte_array_data_bytes
            0x0A,  # zigzag(10) == 5
            0x00,  # STOP of size_statistics
            0x0C,  # long-form id, STRUCT -> a hypothetical future field
            0x28,  # zigzag(20) == 40
            0x00,  # STOP of that struct
            0x00,  # STOP of ColumnMetaData
        ]
    )
    reader = _CompactReader(memoryview(payload).cast("B"))
    result = reader._column_metadata()

    assert result == pss.LeafSizeStats(5, (), ())
    assert reader._pos == len(payload)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
