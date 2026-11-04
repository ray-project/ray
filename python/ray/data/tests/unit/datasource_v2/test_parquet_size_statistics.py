"""Tests for the hand-written footer and dictionary page header reader.

A desynced cursor returns wrong numbers rather than raising, so the walk is
checked against ``thriftpy2``, against the two desyncs the prototype hit, and on
hand-built payloads PyArrow never writes. Tests taking ``walk`` run on both the
Python reader and the compiled one, when it's built.
"""

import numpy as np
import pytest
from packaging.version import parse as parse_version

import ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics as pss
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
    _CompactReader,
    footer_bytes,
    read_footer_stats,
)

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

# PyArrow 17 writes no SizeStatistics; 23 does (18 to 22 unchecked).
requires_pyarrow_23 = pytest.mark.skipif(
    parse_version(pa.__version__) < parse_version("23.0.0"),
    reason="PyArrow 17 writes no SizeStatistics",
)


# A minimal parquet.thrift covering only the traversal path and the dictionary
# page header. The protocol skips everything else generically, so unlike the
# full 1,000-line schema this cannot drift from upstream in ways that matter
# here. Every field is optional, so a missing one is a null, not an error.
_ORACLE_THRIFT = """
struct SizeStatistics {
  1: optional i64 unencoded_byte_array_data_bytes;
  2: optional list<i64> repetition_level_histogram;
  3: optional list<i64> definition_level_histogram;
}
struct PageEncodingStats {
  1: optional i32 page_type;
  2: optional i32 encoding;
  3: optional i32 count;
}
struct ColumnMetaData {
  13: optional list<PageEncodingStats> encoding_stats;
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
struct DictionaryPageHeader {
  1: optional i32 num_values;
}
struct PageHeader {
  2: optional i32 uncompressed_page_size;
  7: optional DictionaryPageHeader dictionary_page_header;
}
"""

N = 500


def _table_cases():
    """Schemas spanning every branch of the walk."""
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
        # The widest walk, where a desync has the most chances to show.
        "wide": pa.table(
            {f"c{c}": pa.array([f"v{c}_{i}" for i in range(N)]) for c in range(12)}
        ),
    }


@pytest.fixture(scope="module")
def written_files(tmp_path_factory):
    """Each case written with four row groups, and its metadata."""
    out = {}
    directory = tmp_path_factory.mktemp("size_stats")
    for label, table in _table_cases().items():
        path = directory / f"{label}.parquet"
        pq.write_table(table, path, row_group_size=len(table) // 4)
        out[label] = pq.read_metadata(str(path))
    return out


def _reader(*byte_values):
    return _CompactReader(memoryview(bytes(byte_values)).cast("B"))


@pytest.fixture(params=["python", "compiled"])
def walk(request, monkeypatch):
    """A footer walk, also made the one ``read_footer_stats`` runs."""
    if request.param == "python":
        monkeypatch.setattr(pss, "_CompiledCompactReader", None)
        return lambda buf: _CompactReader(buf).read()
    compiled = pss._CompiledCompactReader
    if compiled is None:
        pytest.skip("_compact_reader.pyx isn't built")
    return lambda buf: compiled(buf, pss.LeafSizeStats).read()


def _one_leaf_footer(column_metadata):
    """A footer of one row group of one leaf, around ``ColumnMetaData``'s fields."""
    # row_groups (field 4), a list of one struct; its columns (field 1), a list of
    # one struct; its meta_data (field 3), a struct.
    head = [0x49, 0x1C, 0x19, 0x1C, 0x3C]
    # The ColumnChunk's, RowGroup's and FileMetaData's STOP bytes.
    return memoryview(bytes(head + column_metadata + [0x00] * 3)).cast("B")


# ---------------------------------------------------------------------------
# Footer bytes, and the walk against the oracle
# ---------------------------------------------------------------------------


def test_footer_bytes(written_files):
    metadata = written_files["flat-numeric"]
    buf = footer_bytes(metadata)
    assert buf is not None

    # ``__reduce__`` hands back exactly the serialized footer, unsigned: a
    # signed view would make every byte >= 0x80 negative.
    assert len(buf) == metadata.serialized_size
    assert buf.format == "B"

    class NoReduce:
        def __reduce__(self):
            raise TypeError("not picklable")

    assert footer_bytes(NoReduce()) is None


@pytest.fixture(scope="module")
def oracle(tmp_path_factory):
    thriftpy2 = pytest.importorskip("thriftpy2")
    path = tmp_path_factory.mktemp("thrift") / "parquet_oracle.thrift"
    path.write_text(_ORACLE_THRIFT)
    return thriftpy2.load(str(path), module_name="parquet_oracle_thrift")


def _oracle_read(struct, raw):
    compact = pytest.importorskip("thriftpy2.protocol.compact")
    transport = pytest.importorskip("thriftpy2.transport")

    struct.read(compact.TCompactProtocol(transport.TMemoryBuffer(bytes(raw))))
    return struct


def _oracle_leaf(column):
    stats = column.meta_data.size_statistics
    if stats is None:
        return None
    return pss.LeafSizeStats(
        stats.unencoded_byte_array_data_bytes,
        tuple(stats.repetition_level_histogram or ()),
        tuple(stats.definition_level_histogram or ()),
    )


def _oracle_pages(column):
    # Data pages are PageType 0 and 3; dictionary pages, type 2, aren't counted.
    entries = column.meta_data.encoding_stats or ()
    pages = sum(entry.count for entry in entries if entry.page_type in (0, 3))
    return pages or None


@pytest.mark.usefixtures("walk")
@pytest.mark.parametrize("label", list(_table_cases()))
def test_matches_thriftpy2_oracle(written_files, oracle, label):
    metadata = written_files[label]
    theirs = _oracle_read(oracle.FileMetaData(), footer_bytes(metadata))

    assert read_footer_stats(metadata) == pss.FooterStats(
        [
            [_oracle_leaf(column) for column in row_group.columns]
            for row_group in theirs.row_groups
        ],
        [
            [_oracle_pages(column) for column in row_group.columns]
            for row_group in theirs.row_groups
        ],
    )


def test_dictionary_page_header_matches_thriftpy2_oracle(tmp_path, oracle):
    path = _write_dictionary(tmp_path, write_page_checksum=True)
    raw = _dictionary_page_header_bytes(path)
    theirs = _oracle_read(oracle.PageHeader(), raw)

    assert pss.read_dictionary_page(raw) == pss.DictionaryPage(
        theirs.dictionary_page_header.num_values, theirs.uncompressed_page_size
    )


# ---------------------------------------------------------------------------
# Decoded values against ground truth
# ---------------------------------------------------------------------------


@requires_pyarrow_23
@pytest.mark.usefixtures("walk")
@pytest.mark.parametrize(
    "values, arrow_type, expected",
    [
        # String bytes without their 4-byte length prefixes.
        (["hello", "worldworld", "x"] * 1000, pa.string(), (16_000, (), (0, 3000))),
        # Definition level 0 counts the nulls, the last level the values.
        (
            [None if i % 3 == 0 else "abc" for i in range(999)],
            pa.string(),
            (3 * 666, (), (333, 666)),
        ),
        # Repetition level 0 counts the lists, level 1 the continuations.
        (
            [[1, 2, 3]] * 1000,
            pa.list_(pa.int64()),
            (None, (1000, 2000), (0, 0, 0, 3000)),
        ),
    ],
    ids=["string-bytes", "nulls", "lists"],
)
def test_size_statistics_match_ground_truth(tmp_path, values, arrow_type, expected):
    path = tmp_path / "truth.parquet"
    pq.write_table(pa.table({"a": pa.array(values, arrow_type)}), path)
    stats = read_footer_stats(pq.read_metadata(str(path)))

    assert stats is not None
    assert stats.size_statistics[0] == [pss.LeafSizeStats(*expected)]


@pytest.mark.usefixtures("walk")
@pytest.mark.parametrize("use_dictionary", [False, True])
@pytest.mark.parametrize("data_page_version", ["1.0", "2.0"])
def test_data_pages_match_ground_truth(tmp_path, use_dictionary, data_page_version):
    # A page per 1,000-row write batch; a dictionary page is not a data page.
    path = tmp_path / "pages.parquet"
    table = pa.table(
        {
            "a": pa.array(np.arange(10_000) % 10),
            "b": pa.array([f"v{i % 10}" for i in range(10_000)]),
        }
    )
    pq.write_table(
        table,
        path,
        row_group_size=5_000,
        data_page_size=1,
        write_batch_size=1_000,
        use_dictionary=use_dictionary,
        data_page_version=data_page_version,
    )
    stats = read_footer_stats(pq.read_metadata(path))

    assert stats is not None
    assert stats.data_pages == [[5, 5], [5, 5]]


@requires_pyarrow_23
@pytest.mark.usefixtures("walk")
def test_only_byte_array_leaves_record_unencoded_bytes(written_files):
    """Sub-field 1 is BYTE_ARRAY-only, so numeric leaves must not require it."""
    metadata = written_files["mixed"]
    stats = read_footer_stats(metadata)
    assert stats is not None
    for rg_idx, leaves in enumerate(stats.size_statistics):
        for leaf_idx, leaf in enumerate(leaves):
            physical_type = metadata.row_group(rg_idx).column(leaf_idx).physical_type
            assert leaf is not None
            assert (leaf.unencoded_byte_array_data_bytes is not None) == (
                physical_type == "BYTE_ARRAY"
            )


# ---------------------------------------------------------------------------
# Hand-built payloads: shapes PyArrow never writes
# ---------------------------------------------------------------------------
#
# Skipping is where a desync starts. PyArrow footers hold no double or byte
# lists and rarely a long-form field id, so the oracle would not stumble on a
# wrong width for those; each is pinned here to the byte.


def _list_header(size, element_type):
    return (size << pss._NIBBLE_BITS) | element_type


@pytest.mark.parametrize(
    "skip, payload",
    [
        ("_skip_list", [_list_header(3, pss._DOUBLE)] + [0xFF] * 24),
        ("_skip_list", [_list_header(3, pss._BYTE), 0x01, 0x02, 0x03]),
        # A bool takes a byte in a container, not its type nibble.
        ("_skip_list", [_list_header(3, pss._BOOL_TRUE), 0x01, 0x02, 0x01]),
        # Varints are the one element whose width is in the data.
        ("_skip_list", [_list_header(3, pss._I64), 0xAC, 0x02, 0x02, 0x80, 0x01]),
        # Binary elements carry their own lengths: 3 bytes, then 1.
        ("_skip_list", [_list_header(2, pss._BINARY), 3, 0xA, 0xB, 0xC, 1, 0xD]),
        # Delta 0, I64: the field id follows as a varint (zigzag 32 == 16).
        ("_skip_struct", [0x06, 0x20, 0x0A, 0x00]),
        # A struct field holding a struct with one I64.
        ("_skip_struct", [0x1C, 0x16, 0x02, 0x00, 0x00]),
        # An empty list may declare element type 0, not a valid type id:
        # arrow-rs #8826 rejected such footers by checking the type first.
        ("_skip_struct", [0x19, 0x00, 0x00]),
    ],
    ids=[
        "doubles",
        "bytes",
        "bools",
        "varints",
        "binary",
        "long-form-id",
        "nested-struct",
        "empty-list-type-0",
    ],
)
def test_skip_consumes_exactly_the_payload(walk, skip, payload):
    reader = _reader(*payload)
    getattr(reader, skip)()
    assert reader._pos == len(payload)

    # Through each walk: the payload as field 1, then field 16, which a wrong skip
    # width would garble. Field 16 holds SizeStatistics with field 1 set to 5.
    field_1 = 0x19 if skip == "_skip_list" else 0x1C
    size_statistics_5 = [0xFC, 0x16, 0x0A, 0x00, 0x00]
    footer = _one_leaf_footer([field_1, *payload, *size_statistics_5])
    assert walk(footer) == ([[pss.LeafSizeStats(5, (), ())]], [[None]])


@pytest.mark.parametrize(
    "payload, expected",
    [
        # SizeStatistics given as field 16 in long form, after a long-form
        # field 5: no reliance on ascending deltas (arrow-rs #8190).
        ([0x06, 0x0A, 0x02, 0x0C, 0x20, 0x16, 0x0A, 0x00, 0x00], (5, (), ())),
        # Field 16 after field 20: only an absolute id can go backwards.
        ([0x0C, 0x28, 0x00, 0x0C, 0x20, 0x16, 0x0A, 0x00, 0x00], (5, (), ())),
        # Fields 5 and 6, field 16, then a future field 20 to skip.
        (
            [0x56, 0x02, 0x16, 0x04, 0x0C, 0x20, 0x16, 0x0A, 0x00, 0x0C, 0x28, 0x00]
            + [0x00],
            (5, (), ()),
        ),
        # Both histograms empty lists of element type 0.
        ([0x0C, 0x20, 0x16, 0x0A, 0x19, 0x00, 0x19, 0x00, 0x00, 0x00], (5, (), ())),
        # No field 16: a writer without SizeStatistics.
        ([0x56, 0x02, 0x00], None),
    ],
    ids=[
        "absolute-ids",
        "descending-id",
        "unknown-trailing-field",
        "empty-histograms",
        "absent",
    ],
)
def test_column_metadata_payloads(walk, payload, expected):
    reader = _reader(*payload)
    result = reader._column_metadata()
    leaf = None if expected is None else pss.LeafSizeStats(*expected)

    assert result == (leaf, None)
    assert reader._pos == len(payload)
    assert walk(_one_leaf_footer(payload)) == ([[leaf]], [[None]])


def _page_stats(page_type, count):
    # PageEncodingStats: page_type (field 1), encoding 0 (field 2) and count
    # (field 3), each an i32 field header and a zigzag value, then STOP.
    return [0x15, 2 * page_type, 0x15, 0x00, 0x15, 2 * count, 0x00]


@pytest.mark.parametrize(
    "payload, expected",
    [
        # encoding_stats (field 13) with a dictionary page and 4 data pages.
        (
            [0xD9, _list_header(2, pss._STRUCT)]
            + _page_stats(2, 1)
            + _page_stats(0, 4)
            + [0x00],
            (None, 4),
        ),
        # Version 2 data pages count too.
        ([0xD9, _list_header(1, pss._STRUCT)] + _page_stats(3, 3) + [0x00], (None, 3)),
        # A dictionary page alone counts no data pages.
        ([0xD9, _list_header(1, pss._STRUCT)] + _page_stats(2, 1) + [0x00], None),
        # An empty list of element type 0.
        ([0xD9, 0x00, 0x00], None),
        # Field 13, then SizeStatistics as field 16, three ids on.
        (
            [0xD9, _list_header(1, pss._STRUCT)]
            + _page_stats(0, 4)
            + [0x3C, 0x16, 0x0A, 0x00, 0x00],
            (pss.LeafSizeStats(5, (), ()), 4),
        ),
    ],
    ids=["dictionary-and-data", "data-page-v2", "dictionary-only", "empty", "both"],
)
def test_encoding_stats_payloads(walk, payload, expected):
    leaf, pages = (None, None) if expected is None else expected
    reader = _reader(*payload)

    assert reader._column_metadata() == (leaf, pages)
    assert reader._pos == len(payload)
    assert walk(_one_leaf_footer(payload)) == ([[leaf]], [[pages]])


@pytest.mark.parametrize(
    "payload",
    [
        # Field 17, geospatial statistics, is a struct too: a cursor that drifted
        # onto it as field 16 is caught by its field 1, which isn't an i64.
        [0x0C, 0x20, 0x1C, 0x00, 0x00, 0x00],
        # Field 13 as an i64 rather than a list.
        [0xD6, 0x02, 0x00],
        # Field 13 as a list of i32s rather than structs.
        [0xD9, _list_header(1, pss._I32), 0x02, 0x00],
    ],
    ids=["geospatial", "encoding-stats-not-a-list", "encoding-stats-of-i32s"],
)
def test_wrong_field_types_are_rejected(walk, payload):
    """A field whose type contradicts parquet.thrift means the cursor desynced."""
    with pytest.raises(pss._ThriftDesync):
        _reader(*payload)._column_metadata()
    with pytest.raises(Exception, match="expected"):
        walk(_one_leaf_footer(payload))


def test_a_length_past_the_footer_raises(walk):
    """A corrupt length can't move the reader back to re-read a field forever."""
    # Field 3, a list of one binary whose length is 2**64 - 12: as a signed size,
    # -12, which is this payload's own length.
    payload = [0x39, _list_header(1, pss._BINARY), 0xF4] + [0xFF] * 8 + [0x01]
    with pytest.raises(IndexError):
        walk(_one_leaf_footer(payload))


# ---------------------------------------------------------------------------
# Fault injection: corruption must become ``None``, never other numbers
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("walk")
def test_decoding_is_insensitive_to_view_signedness(written_files, monkeypatch):
    """Masks read a sign-extended byte the same, so ``.cast("B")`` is defensive."""
    metadata = written_files["wide"]
    unsigned = read_footer_stats(metadata)
    monkeypatch.setattr(
        pss, "footer_bytes", lambda md: memoryview(md.__reduce__()[1][0])
    )

    assert read_footer_stats(metadata) == unsigned


def _buggy_binary_skip(original):
    """``self._pos += self._varint()``: every binary field consumes one byte too few."""

    def skip(self, field_type):
        if field_type == pss._BINARY:
            self._pos += self._varint()  # noqa: B909 - the bug, on purpose
            return
        return original(self, field_type)

    return skip


def _short_varint_skip(self):
    """A skip that stops one byte before the varint terminator."""
    buf, pos = self._buf, self._pos
    while buf[pos] & pss._VARINT_CONTINUES:
        pos += 1
    self._pos = pos


@pytest.mark.parametrize("bug", ["binary-skip", "short-varint"])
@pytest.mark.parametrize("label", list(_table_cases()))
def test_a_desync_never_returns_other_numbers(written_files, monkeypatch, bug, label):
    """The prototype's two desyncs give ``None``, caught by type and count checks."""
    # The bugs are injected into the Python reader.
    monkeypatch.setattr(pss, "_CompiledCompactReader", None)
    metadata = written_files[label]
    healthy = read_footer_stats(metadata)
    if bug == "binary-skip":
        monkeypatch.setattr(
            _CompactReader, "_skip", _buggy_binary_skip(_CompactReader._skip)
        )
    else:
        monkeypatch.setattr(_CompactReader, "_skip_varint", _short_varint_skip)

    assert read_footer_stats(metadata) in (None, healthy)


@pytest.mark.usefixtures("walk")
def test_truncated_footer_gives_none(written_files, monkeypatch):
    """An IndexError from running off the end must not reach the caller."""
    monkeypatch.setattr(
        pss,
        "footer_bytes",
        lambda md: memoryview(md.__reduce__()[1][0]).cast("B")[:20],
    )
    assert read_footer_stats(written_files["mixed"]) is None


# ---------------------------------------------------------------------------
# Dictionary page headers
# ---------------------------------------------------------------------------


_DISTINCT = [f"value-{i:03d}" * (1 + i % 3) for i in range(100)]


def _write_dictionary(tmp_path, **write_kwargs):
    path = tmp_path / "dictionary.parquet"
    table = pa.table({"a": pa.array([_DISTINCT[i % 100] for i in range(N)])})
    pq.write_table(table, path, **write_kwargs)
    return path


def _dictionary_page_header_bytes(path, offset=None):
    chunk = pq.read_metadata(str(path)).row_group(0).column(0)
    with open(path, "rb") as f:
        f.seek(chunk.dictionary_page_offset if offset is None else offset)
        return f.read(pss.DICTIONARY_PAGE_HEADER_READ_BYTES)


@pytest.mark.parametrize(
    "write_kwargs",
    [
        {},
        {"write_page_checksum": True},
        {"compression": "zstd"},
        {"compression": "none"},
        {"version": "1.0"},
    ],
    ids=["default", "crc", "zstd", "uncompressed", "format-1.0"],
)
def test_dictionary_page_header_matches_the_values(tmp_path, write_kwargs):
    """One entry per distinct value, each PLAIN behind a 4-byte length."""
    path = _write_dictionary(tmp_path, **write_kwargs)

    assert pss.read_dictionary_page(
        _dictionary_page_header_bytes(path)
    ) == pss.DictionaryPage(
        num_values=100, uncompressed_page_size=sum(4 + len(v) for v in _DISTINCT)
    )


def test_not_a_dictionary_page_header_gives_none(tmp_path):
    path = _write_dictionary(tmp_path)
    raw = _dictionary_page_header_bytes(path)
    chunk = pq.read_metadata(str(path)).row_group(0).column(0)
    data_page = _dictionary_page_header_bytes(path, chunk.data_page_offset)

    for cut in range(10):
        assert pss.read_dictionary_page(raw[:cut]) is None
    assert pss.read_dictionary_page(data_page) is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
