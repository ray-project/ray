"""Tests for the footer and dictionary page header reader.

A desynced cursor returns wrong numbers rather than raising, so hand-built payloads
pin each skip width PyArrow footers never exercise.
"""

import pytest
from packaging.version import parse as parse_version

import ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics as pss
from ray.data._internal.datasource_v2.formats.parquet.parquet_size_statistics import (
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

N = 500
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


def _walk(buf):
    return pss._CompactReader(buf).read()


def _one_leaf_footer(column_metadata):
    """A footer of one row group of one leaf, around ``ColumnMetaData``'s fields."""
    # row_groups (field 4), a list of one struct; its columns (field 1), a list of
    # one struct; its meta_data (field 3), a struct.
    head = [0x49, 0x1C, 0x19, 0x1C, 0x3C]
    # The ColumnChunk's, RowGroup's and FileMetaData's STOP bytes.
    return memoryview(bytes(head + column_metadata + [0x00] * 3)).cast("B")


def test_footer_bytes(tmp_path):
    metadata = pq.read_metadata(str(_write_dictionary(tmp_path)))
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


@requires_pyarrow_23
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
        # Repetition level 0 counts the lists, level 1 the continuations; a
        # numeric leaf records no string bytes.
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
    assert stats[0] == [pss.LeafSizeStats(*expected)]


# Thrift compact type ids.
_BOOL_TRUE, _BYTE, _I64 = 1, 3, 6
_DOUBLE, _BINARY, _LIST, _STRUCT = 7, 8, 9, 12


def _list_header(size, element_type):
    # The count in the high 4 bits, the element type id in the low 4.
    return (size << 4) | element_type


# A field type and its payload, which a wrong skip width would overrun.
_SKIPPED = {
    "doubles": (_LIST, [_list_header(3, _DOUBLE)] + [0xFF] * 24),
    "bytes": (_LIST, [_list_header(3, _BYTE), 0x01, 0x02, 0x03]),
    # A bool takes a byte in a container, not its type nibble.
    "bools": (_LIST, [_list_header(3, _BOOL_TRUE), 0x01, 0x02, 0x01]),
    # Varints are the one element whose width is in the data.
    "varints": (_LIST, [_list_header(3, _I64), 0xAC, 0x02, 0x02, 0x80, 0x01]),
    # Binary elements carry their own lengths: 3 bytes, then 1.
    "binary": (_LIST, [_list_header(2, _BINARY), 3, 0xA, 0xB, 0xC, 1, 0xD]),
    # Delta 0, I64: the field id follows as a varint (zigzag 32 == 16).
    "long-form-id": (_STRUCT, [0x06, 0x20, 0x0A, 0x00]),
    # A struct field holding a struct with one I64.
    "nested-struct": (_STRUCT, [0x1C, 0x16, 0x02, 0x00, 0x00]),
    # An empty list may declare element type 0, not a valid type id:
    # arrow-rs #8826 rejected such footers by checking the type first.
    "empty-list-type-0": (_STRUCT, [0x19, 0x00, 0x00]),
}


@pytest.mark.parametrize("field_type, payload", _SKIPPED.values(), ids=list(_SKIPPED))
def test_skip_consumes_exactly_the_payload(field_type, payload):
    # The payload as field 1, then field 16, which a wrong skip width would
    # garble. Field 16 holds SizeStatistics with field 1 set to 5.
    size_statistics_5 = [0xFC, 0x16, 0x0A, 0x00, 0x00]
    footer = _one_leaf_footer([0x10 | field_type, *payload, *size_statistics_5])
    assert _walk(footer) == [[pss.LeafSizeStats(5, (), ())]]


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
    ids=["long-form", "descending-id", "unknown-field", "empty-histograms", "absent"],
)
def test_column_metadata_payloads(payload, expected):
    leaf = None if expected is None else pss.LeafSizeStats(*expected)
    assert _walk(_one_leaf_footer(payload)) == [[leaf]]


@pytest.mark.parametrize(
    "payload",
    [
        # Field 17, geospatial statistics, is a struct too: a cursor that drifted
        # onto it as field 16 is caught by its field 1, which isn't an i64.
        [0x0C, 0x20, 0x1C, 0x00, 0x00, 0x00],
        # Field 16 as an i64 rather than a struct.
        [0x06, 0x20, 0x02, 0x00],
    ],
    ids=["geospatial", "size-statistics-not-a-struct"],
)
def test_wrong_field_types_are_rejected(payload):
    """A field whose type contradicts parquet.thrift means the cursor desynced."""
    with pytest.raises(pss._ThriftDesync, match="expected"):
        _walk(_one_leaf_footer(payload))


def test_truncated_footer_gives_none(tmp_path, monkeypatch):
    """An IndexError from running off the end must not reach the caller."""
    metadata = pq.read_metadata(str(_write_dictionary(tmp_path)))
    monkeypatch.setattr(
        pss,
        "footer_bytes",
        lambda md: memoryview(md.__reduce__()[1][0]).cast("B")[:20],
    )
    assert read_footer_stats(metadata) is None


@pytest.mark.parametrize("checksum", [False, True])
def test_dictionary_page_header_matches_the_values(tmp_path, checksum):
    """One entry per distinct value, each PLAIN behind a 4-byte length, though
    the page is compressed; a page checksum is skipped."""
    path = _write_dictionary(tmp_path, write_page_checksum=checksum)

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
