"""Unit tests for reading MCAP summaries and chunks in few requests (``mcap_io``).

Each read of a remote file is one request, so the tests count reads through a
file wrapper and compare what is parsed with mcap's own ``SeekingReader``.
"""

import importlib.util
import os
import struct

import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.formats.mcap.mcap_io import (
    ChunkReads,
    buffered,
    parse_summary_records,
    plan_chunk_reads,
    read_chunk,
    read_chunks,
    read_summary_from,
    read_summary_section_from,
    summary_records,
)
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CHUNKED_FILE_MESSAGES,
    round_robin_messages,
    write_cut_short,
    write_mcap,
    write_mcap_without_summary_schemas,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

# Messages that make a summary larger than the first read (64 KiB): one chunk
# index of about a hundred bytes per message.
MANY_CHUNKS = 3000
FOOTER_AND_MAGIC = 37  # footer record (29 bytes) and the 8 magic bytes
CHUNK_INDEX_OPCODE = 0x08
SUMMARY_FIELDS = (
    "statistics",
    "schemas",
    "channels",
    "chunk_indexes",
    "attachment_indexes",
    "metadata_indexes",
)


class CountingFile:
    """A local file that counts its non-empty reads, one request each remotely."""

    def __init__(self, path: str):
        self._f = LocalFileSystem().open_input_file(path)
        self.reads = 0

    def read(self, nbytes: int) -> bytes:
        data = self._f.read(nbytes)
        self.reads += bool(data)
        return data

    def readinto(self, buffer) -> int:
        size = self._f.readinto(buffer)
        self.reads += bool(size)
        return size

    def __getattr__(self, name):
        return getattr(self._f, name)


def mcaps_own_summary(path: str):
    """The summary mcap's ``SeekingReader`` parses, a few bytes per read."""
    from mcap.reader import SeekingReader

    with open(path, "rb") as f:
        return SeekingReader(f).get_summary()


def write_empty_summary(path: str) -> None:
    """Write messages with nothing repeated in the summary: ``summary_start`` 0."""
    from mcap.writer import IndexType, Writer

    with open(path, "wb") as f:
        writer = Writer(
            f,
            index_types=IndexType.NONE,
            repeat_schemas=False,
            repeat_channels=False,
            use_statistics=False,
            use_summary_offsets=False,
        )
        writer.start(profile="", library="ray-test")
        channel_id = writer.register_channel(
            topic="/a", message_encoding="json", schema_id=0
        )
        writer.add_message(channel_id=channel_id, log_time=0, publish_time=0, data=b"1")
        writer.finish()


def overwrite(path: str, offset: int, data: bytes) -> None:
    with open(path, "r+b") as f:
        f.seek(offset)
        f.write(data)


WRITERS = {
    "chunked": lambda p: write_mcap(p, round_robin_messages(CHUNKED_FILE_MESSAGES)),
    "unindexed": lambda p: write_mcap(p, round_robin_messages(9), index=False),
    "schemas_only_in_chunks": write_mcap_without_summary_schemas,
    "summary_larger_than_first_read": lambda p: write_mcap(
        p, round_robin_messages(MANY_CHUNKS)
    ),
}


@pytest.mark.parametrize("writer", WRITERS.values(), ids=WRITERS.keys())
def test_summary_matches_mcaps_own_reader(tmp_path, writer):
    """The summary parsed from one to three reads holds the same records as the
    one mcap's ``SeekingReader`` parses field by field."""
    path = os.path.join(tmp_path, "file.mcap")
    writer(path)

    f = CountingFile(path)
    summary = read_summary_from(f, path)
    expected = mcaps_own_summary(path)

    assert summary is not None and expected is not None
    for field in SUMMARY_FIELDS:
        assert getattr(summary, field) == getattr(expected, field), field
    assert 1 <= f.reads <= 3


def test_small_file_summary_takes_one_read_and_a_large_one_three(tmp_path):
    """A file shorter than the first read is read once. A larger summary takes
    the tail, the leading magic bytes and the rest of the summary."""
    small = os.path.join(tmp_path, "small.mcap")
    write_mcap(small, round_robin_messages(CHUNKED_FILE_MESSAGES))
    large = os.path.join(tmp_path, "large.mcap")
    write_mcap(large, round_robin_messages(MANY_CHUNKS))

    reads = {}
    for path in (small, large):
        f = CountingFile(path)
        read_summary_from(f, path)
        reads[path] = f.reads

    assert reads == {small: 1, large: 3}


@pytest.mark.parametrize("writer", WRITERS.values(), ids=WRITERS.keys())
def test_records_copied_out_of_a_section_parse_as_the_section_does(tmp_path, writer):
    """A section's records, copied without the footer, parse to the file's
    summary; a run of them parses to just those records."""
    path = os.path.join(tmp_path, "file.mcap")
    writer(path)
    section = read_summary_section_from(CountingFile(path), path)
    expected = read_summary_from(CountingFile(path), path)
    assert section is not None and expected is not None

    records = list(summary_records(section))
    copied = parse_summary_records(b"".join(bytes(r) for _, r in records), path)
    last_chunk_index = [bytes(r) for op, r in records if op == CHUNK_INDEX_OPCODE][-1:]
    alone = parse_summary_records(b"".join(last_chunk_index), path)

    for field in SUMMARY_FIELDS:
        assert getattr(copied, field) == getattr(expected, field), field
    assert alone.chunk_indexes == expected.chunk_indexes[-1:]
    assert not alone.channels and not alone.schemas


def test_no_records_parse_to_an_empty_summary():
    """Empty records, as a read task gets for a file without a summary, parse to
    a summary with nothing in it."""
    summary = parse_summary_records(b"", "file.mcap")

    for field in SUMMARY_FIELDS:
        assert not getattr(summary, field), field


def test_file_without_summary_section_has_no_summary(tmp_path):
    """A footer whose ``summary_start`` is 0 means no summary: ``None``."""
    path = os.path.join(tmp_path, "no_summary.mcap")
    write_empty_summary(path)

    assert read_summary_from(CountingFile(path), path) is None
    assert mcaps_own_summary(path) is None


def _cut_short(path: str) -> None:
    write_cut_short(path, path)


def _not_mcap(path: str) -> None:
    with open(path, "wb") as f:
        f.write(b"not an MCAP file, but long enough to hold a footer" * 4)


def _bad_summary(path: str) -> None:
    size = os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(size - FOOTER_AND_MAGIC + 9)
        (summary_start,) = struct.unpack("<Q", f.read(8))
    overwrite(path, summary_start, b"\xff" * 16)


def _summary_past_the_end(path: str) -> None:
    size = os.path.getsize(path)
    overwrite(path, size - FOOTER_AND_MAGIC + 9, struct.pack("<Q", size + 1))


CORRUPTIONS = {
    "cut_short": (_cut_short, "is not a complete MCAP file"),
    "not_mcap": (_not_mcap, "is not an MCAP file"),
    "bad_summary": (_bad_summary, "Cannot parse the summary section"),
    "summary_past_the_end": (_summary_past_the_end, "past the end of the file"),
}


@pytest.mark.parametrize("corruption", CORRUPTIONS.values(), ids=CORRUPTIONS.keys())
def test_unreadable_summary_raises_naming_the_file(tmp_path, corruption):
    """A file that is not a complete MCAP file raises a ``ValueError`` that names
    it and says what is wrong."""
    corrupt, message = corruption
    path = os.path.join(tmp_path, "recording.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    corrupt(path)

    with pytest.raises(ValueError, match=message) as error:
        read_summary_from(CountingFile(path), path)

    assert path in str(error.value)


def test_chunk_is_read_in_one_read(tmp_path):
    """``read_chunk`` reads a chunk record in one read and parses the chunk mcap
    parses from the file field by field."""
    from mcap.data_stream import ReadDataStream
    from mcap.records import Chunk

    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    summary = mcaps_own_summary(path)
    assert summary is not None
    chunk_indexes = summary.chunk_indexes

    for chunk_index in chunk_indexes:
        f = CountingFile(path)
        chunk = read_chunk(f, chunk_index)
        with open(path, "rb") as raw:
            # Skip the record's opcode (1 byte) and length (8 bytes).
            raw.seek(chunk_index.chunk_start_offset + 1 + 8)
            expected = Chunk.read(ReadDataStream(raw))

        assert chunk == expected
        assert f.reads == 1


def chunk_at(offset: int, length: int = 100):
    """The ``ChunkIndex`` of a chunk record ``length`` bytes long at ``offset``."""
    from mcap.records import ChunkIndex

    return ChunkIndex(
        chunk_length=length,
        chunk_start_offset=offset,
        compression="zstd",
        compressed_size=length,
        message_end_time=0,
        message_index_length=0,
        message_index_offsets={},
        message_start_time=0,
        uncompressed_size=length,
    )


# Chunk records of 100 B, given by offset, in the order they are read.
PLANS = {
    "neighbours_within_the_gap": (
        [0, 110, 220],
        ChunkReads(max_bytes=1000, max_gap_bytes=10, first_bytes=1000),
        [[0, 110, 220]],
    ),
    "gap_past_the_limit": (
        [0, 111],
        ChunkReads(max_bytes=1000, max_gap_bytes=10, first_bytes=1000),
        [[0], [111]],
    ),
    "span_past_max_bytes": (
        [0, 100, 200],
        ChunkReads(max_bytes=250, max_gap_bytes=0, first_bytes=250),
        [[0, 100], [200]],
    ),
    "earlier_in_the_file": (
        [200, 0, 100],
        ChunkReads(max_bytes=1000, max_gap_bytes=1000, first_bytes=1000),
        [[200], [0, 100]],
    ),
    "zero_reads_each_chunk_alone": (
        [0, 100, 200],
        ChunkReads(max_bytes=0, max_gap_bytes=0, first_bytes=0),
        [[0], [100], [200]],
    ),
    # The limit doubles per read: 100, 200, 400, then max_bytes (500).
    "first_reads_grow": (
        list(range(0, 1500, 100)),
        ChunkReads(max_bytes=500, max_gap_bytes=0, first_bytes=100),
        [
            [0],
            [100, 200],
            [300, 400, 500, 600],
            [700, 800, 900, 1000, 1100],
            [1200, 1300, 1400],
        ],
    ),
}


@pytest.mark.parametrize("offsets, reads, expected", PLANS.values(), ids=PLANS.keys())
def test_plan_groups_chunks_within_the_limits(offsets, reads, expected):
    """A chunk joins the read before it only if it lies after that read, within
    the gap limit, and keeps the read within its size limit."""
    planned = plan_chunk_reads([chunk_at(offset) for offset in offsets], reads)

    assert [[c.chunk_start_offset for c in r.chunk_indexes] for r in planned] == (
        expected
    )
    for ranged_read in planned:
        assert ranged_read.start == ranged_read.chunk_indexes[0].chunk_start_offset
        assert ranged_read.end == ranged_read.chunk_indexes[-1].chunk_start_offset + 100


def test_a_chunk_longer_than_max_bytes_is_read_alone():
    """A chunk record longer than ``max_bytes`` is one read of its own."""
    chunks = [chunk_at(0, length=500), chunk_at(500), chunk_at(600)]

    planned = plan_chunk_reads(chunks, ChunkReads(max_bytes=250, max_gap_bytes=0))

    assert [len(r.chunk_indexes) for r in planned] == [1, 2]


@pytest.mark.parametrize(
    "reads, expected_reads",
    [(ChunkReads(), 1), (ChunkReads(max_bytes=0, max_gap_bytes=0, first_bytes=0), 9)],
    ids=["neighbours_together", "one_per_chunk"],
)
def test_read_chunks_returns_what_read_chunk_returns(tmp_path, reads, expected_reads):
    """``read_chunks`` yields the chunks ``read_chunk`` reads one by one, in the
    order asked, fetching all nine in one read unless told to read each alone."""
    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    summary = read_summary_from(CountingFile(path), path)
    assert summary is not None
    chunk_indexes = sorted(summary.chunk_indexes, key=lambda c: c.chunk_start_offset)
    expected = [read_chunk(CountingFile(path), c) for c in chunk_indexes]

    f = CountingFile(path)
    chunks = list(read_chunks(f, chunk_indexes, reads))

    assert [chunk_index for chunk_index, _ in chunks] == chunk_indexes
    assert [chunk for _, chunk in chunks] == expected
    assert f.reads == expected_reads


def test_read_chunks_fetches_a_read_when_its_first_chunk_is_needed(tmp_path):
    """Nothing is fetched until a chunk is asked for, and each read only when
    its first chunk is."""
    path = os.path.join(tmp_path, "chunked.mcap")
    write_mcap(path, round_robin_messages(CHUNKED_FILE_MESSAGES))
    summary = read_summary_from(CountingFile(path), path)
    assert summary is not None
    one_per_chunk = ChunkReads(max_bytes=0, max_gap_bytes=0, first_bytes=0)
    f = CountingFile(path)

    chunks = read_chunks(f, summary.chunk_indexes, one_per_chunk)
    reads = [f.reads]
    for _ in range(3):
        next(chunks)
        reads.append(f.reads)

    assert reads == [0, 1, 2, 3]


def test_buffered_scan_reads_in_large_blocks(tmp_path):
    """A linear scan through ``buffered`` yields the same records with one read
    per buffer's worth of the file, and closing the buffer leaves the file open."""
    from mcap.stream_reader import StreamReader

    path = os.path.join(tmp_path, "unindexed.mcap")
    write_mcap(path, round_robin_messages(500), index=False)
    with open(path, "rb") as raw:
        expected = list(StreamReader(raw).records)

    f = CountingFile(path)
    buffer = buffered(f, 1024 * 1024)
    records = list(StreamReader(buffer).records)
    buffer.close()

    assert records == expected
    # The file fits in the buffer, so one read fetches all of it.
    assert os.path.getsize(path) < 1024 * 1024 and f.reads == 1
    assert not f.closed


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
