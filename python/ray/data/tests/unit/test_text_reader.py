import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.text.text_reader import _iter_line_batches


@pytest.mark.parametrize(
    "text,encoding",
    [
        pytest.param("", "utf-8", id="empty"),
        pytest.param("\n\r\r\n", "utf-8", id="empty_lines"),
        pytest.param("a\r\nb\rc\n", "utf-8", id="newline_endings"),
        pytest.param(
            "a\vb\fc\x1cd\x1de\x1ef\x85g\u2028h\u2029last",
            "utf-8",
            id="unicode_endings",
        ),
        pytest.param("你好\r\n🌍\nfin", "utf-8", id="multibyte"),
        pytest.param("你好\r\n🌍\nfin", "utf-16-le", id="utf16_le"),
        pytest.param("你好\r\n🌍\nfin", "utf-16-be", id="utf16_be"),
        pytest.param("你好\r\n🌍\nfin", "utf-32-le", id="utf32_le"),
        pytest.param("你好\r\n🌍\nfin", "utf-32-be", id="utf32_be"),
        pytest.param("first\r\nlast", "ascii", id="ascii"),
        pytest.param("café\r\nfin", "latin-1", id="latin1"),
        pytest.param("long" * 4096 + "\r\ntail", "utf-8", id="long_line"),
    ],
)
def test_incremental_text_lines_match_splitlines(text, encoding):
    data = text.encode(encoding)
    expected = data.decode(encoding).splitlines()
    for size in (1, 2, 3, 7, 64, 4096):
        with pa.BufferReader(data) as stream:
            assert [
                line
                for batch in _iter_line_batches(stream, encoding, size)
                for line in batch
            ] == expected


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"ok\n\xff", id="invalid_character"),
        pytest.param(b"ok\n\xe2\x82", id="incomplete_at_eof"),
    ],
)
def test_incremental_text_lines_reject_invalid_encoding(data):
    with pa.BufferReader(data) as stream, pytest.raises(
        UnicodeDecodeError, match="utf-8"
    ):
        list(_iter_line_batches(stream, "utf-8", 1))


def test_short_final_chunk_includes_unterminated_line():
    with pa.BufferReader(b"first\nlast") as stream:
        assert list(_iter_line_batches(stream, "utf-8", 64)) == [["first", "last"]]


def test_full_chunk_does_not_read_ahead():
    with pa.BufferReader(b"first\nsecond\n") as stream:
        batches = _iter_line_batches(stream, "utf-8", 6)
        assert next(batches) == ["first"]
        assert stream.tell() == 6
        batches.close()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
