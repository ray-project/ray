"""Helpers shared by the MCAP datasource unit tests.

The tests write small MCAP files and drive the indexer, scanner and reader
directly, as planning and the ``ListFiles`` and ``ReadFiles`` tasks do, with no
Ray cluster. The writers import ``mcap`` when called, so the test modules still
collect without it.
"""

import json
import shutil
from collections import defaultdict
from typing import Any, Dict, List, Optional

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import (
    partition_files,
    sample_files,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

BASE_TIME = 1_000_000_000  # log time of the first message, in nanoseconds
STEP = 1_000_000  # 1 ms between consecutive messages
CHUNKED_FILE_MESSAGES = 9  # messages, one per chunk, in the ``chunked_file`` fixture


def write_mcap(
    path,
    messages,
    *,
    chunk_size=1,
    message_encoding="json",
    index=True,
    encodings=None,
):
    """Write ``messages`` (dicts with topic, log_time, data) to ``path``.

    The default ``chunk_size=1`` closes a chunk after every message, so N
    messages make N chunks, each a possible split point. ``encodings``
    overrides ``message_encoding`` per topic.
    """
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=chunk_size,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL if index else IndexType.NONE,
            use_chunking=index,
            use_statistics=index,
            use_summary_offsets=index,
        )
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="test_schema", encoding="jsonschema", data=b'{"type": "object"}'
        )
        channels = {}
        for message in messages:
            topic = message["topic"]
            if topic not in channels:
                channels[topic] = writer.register_channel(
                    schema_id=schema_id,
                    topic=topic,
                    message_encoding=(encodings or {}).get(topic, message_encoding),
                    metadata={"origin": topic},
                )
            data = message["data"]
            if not isinstance(data, bytes):
                data = json.dumps(data).encode()
            writer.add_message(
                channel_id=channels[topic],
                log_time=message["log_time"],
                publish_time=message.get("publish_time", message["log_time"]),
                data=data,
            )
        writer.finish()


def round_robin_messages(n, topics=("/a", "/b", "/c")) -> List[Dict[str, Any]]:
    """``n`` messages ``{"seq": i}`` on ``topics`` in turn, ``STEP`` apart."""
    return [
        {
            "topic": topics[i % len(topics)],
            "data": {"seq": i},
            "log_time": BASE_TIME + i * STEP,
        }
        for i in range(n)
    ]


def mixed_encoding_messages(n=6):
    """``n`` messages alternating a JSON ``/json`` and a ``/cdr`` of raw bytes."""
    messages = round_robin_messages(n, topics=("/json", "/cdr"))
    for message in messages:
        if message["topic"] == "/cdr":
            message["data"] = bytes([message["data"]["seq"]]) * 4
    return messages


def write_mcap_without_summary_schemas(path):
    """Write two JSON channels with two schemas, one message per chunk.

    The schema records are written only inside the chunks
    (``repeat_schemas=False``), so the summary lists the channels but not their
    schemas. ``/a`` uses ``test_schema`` and ``/b`` uses ``other_schema``:

        chunk   0   1   2   3   4   5
        topic   /a  /b  /a  /b  /a  /b
    """
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=1,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL,
            repeat_schemas=False,
        )
        writer.start(profile="", library="ray-test")
        channels = {}
        for topic, name in (("/a", "test_schema"), ("/b", "other_schema")):
            schema_id = writer.register_schema(
                name=name, encoding="jsonschema", data=b'{"type": "object"}'
            )
            channels[topic] = writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="json"
            )
        for i in range(6):
            topic = "/a" if i % 2 == 0 else "/b"
            writer.add_message(
                channel_id=channels[topic],
                log_time=BASE_TIME + i * STEP,
                publish_time=BASE_TIME + i * STEP,
                data=json.dumps({"seq": i}).encode(),
            )
        writer.finish()


MIXED_TOPICS = ("/a", "/b", "/raw", "/ü")  # channels of ``write_mixed_channels``
SHARED_SCHEMA = b"float64 x\n" * 30  # the definition /a and /b both use


def write_mixed_channels(path, num_messages=40, payload_bytes=100):
    """Write CDR messages on four channels in turn, a few messages per chunk.

        topic   schema                      metadata
        /a      A                           {"origin": "/a"}
        /b      B, same bytes as A          {}
        /raw    none (schema id 0)          {}
        /ü      C                           {"ключ": "значение"}

    Message ``i`` has ``payload_bytes`` bytes of value ``i % 256`` and log time
    ``BASE_TIME + i * STEP``.
    """
    from mcap.writer import CompressionType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=4 * (payload_bytes + 31),
            compression=CompressionType.ZSTD,
        )
        writer.start(profile="ros2", library="ray-test")
        a = writer.register_schema("pkg/A", "ros2msg", SHARED_SCHEMA)
        b = writer.register_schema("pkg/B", "ros2msg", SHARED_SCHEMA)
        c = writer.register_schema("pkg/C", "ros2msg", b"uint8 c\n")
        channels = [
            writer.register_channel("/a", "cdr", a, {"origin": "/a"}),
            writer.register_channel("/b", "cdr", b, {}),
            writer.register_channel("/raw", "cdr", 0, {}),
            writer.register_channel("/ü", "cdr", c, {"ключ": "значение"}),
        ]
        for i in range(num_messages):
            log_time = BASE_TIME + i * STEP
            writer.add_message(
                channel_id=channels[i % len(channels)],
                log_time=log_time,
                publish_time=log_time,
                data=bytes([i % 256]) * payload_bytes,
                sequence=i,
            )
        writer.finish()


def list_manifests(datasource, **kwargs):
    """List ``datasource``'s paths as one ``ListFiles`` task does."""
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(
            pa.array(datasource.paths), filesystem=datasource.filesystem, **kwargs
        )
    )


def pack(datasource, paths: Optional[List[str]] = None, **kwargs) -> List[FileManifest]:
    """List ``paths`` (the datasource's by default) and pack the rows into read
    tasks, as one ``ListFiles`` task does. ``kwargs`` go to the listing."""
    indexer = datasource._get_file_indexer()
    partitioner = datasource.get_file_partitioner()
    assert partitioner is not None
    listed = indexer.list_files(
        pa.array(datasource.paths if paths is None else paths),
        filesystem=datasource.filesystem,
        **kwargs,
    )
    return [
        FileManifest(block)
        for block in partition_files(
            [manifest.as_block() for manifest in listed],
            None,  # pyrefly: ignore[bad-argument-type]  (TaskContext unused)
            partitioner,
        )
    ]


def infer_schema(datasource) -> pa.Schema:
    """Plan the read schema from the files planning samples."""
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    return datasource.infer_schema(sample)


def scanner_for(datasource):
    """The scanner planning builds for ``datasource``."""
    return datasource.create_scanner(infer_schema(datasource), datasource.filesystem)


def read_all(datasource, *manifests, scanner=None) -> pa.Table:
    """Read ``manifests`` into one table, with ``scanner`` or the planned one."""
    scanner = scanner or scanner_for(datasource)
    reader = scanner.create_reader()
    tables = [table for manifest in manifests for table in reader.read(manifest)]
    assert tables, "expected at least one table"
    return pa.concat_tables(tables)


def selected_messages(path: str):
    """Every message of the local, indexed file ``path``, as the reader yields them."""
    reader = SelectedMessageReader(MCAPSelection(), log_time_order=True)
    with open(path, "rb") as f:
        yield from reader.iter_chunks(f, path, summary_of(path), None)


def summary_of(path: str):
    """The summary of the local MCAP file at ``path``."""
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    return summary


def write_cut_short(path: str, source: str) -> str:
    """Write the first half of the MCAP file ``source`` to ``path``.

    Like a recording whose writer crashed: no summary, footer or end magic.
    """
    with open(source, "rb") as f:
        data = f.read()
    with open(path, "wb") as f:
        f.write(data[: len(data) // 2])
    return path


def write_corrupt_chunk(path: str, source: str, index: int) -> str:
    """Copy the MCAP file ``source`` to ``path`` with chunk ``index``'s compressed
    bytes zeroed.

    The summary stays readable, so the file is listed as usual, but
    decompressing that chunk fails, as after a bad disk sector or transfer.
    """
    shutil.copyfile(source, path)
    chunk = summary_of(path).chunk_indexes[index]
    start = chunk.chunk_start_offset + chunk.chunk_length - chunk.compressed_size
    with open(path, "r+b") as f:
        f.seek(start)
        f.write(bytes(chunk.compressed_size))
    return path


# Where a chunk record's compression name starts: after the opcode (1 byte),
# the record length (8), the message start and end times (8 each), the
# uncompressed size (8), its CRC (4) and the name's length (4).
CHUNK_COMPRESSION_NAME = 1 + 8 + 8 + 8 + 8 + 4 + 4


def write_corrupt_chunk_header(path: str, source: str, index: int) -> str:
    """Copy the MCAP file ``source`` to ``path`` with chunk ``index``'s
    compression name, ``zstd``, overwritten by bytes that are not UTF-8.

    The summary stays readable, so the file is listed as usual, but that chunk
    record no longer parses, before anything is decompressed.
    """
    shutil.copyfile(source, path)
    chunk = summary_of(path).chunk_indexes[index]
    name_at = chunk.chunk_start_offset + CHUNK_COMPRESSION_NAME
    with open(path, "r+b") as f:
        f.seek(name_at)
        assert f.read(4) == b"zstd"
        f.seek(name_at)
        f.write(b"\xff" * 4)
    return path


def without_summary_records(monkeypatch, *names: str) -> None:
    """Make every later summary read return the record maps ``names`` empty.

    ``names`` are ``Summary`` attributes such as ``"channels"``, as a writer that
    does not repeat those records in the summary leaves them.
    """
    from ray.data._internal.datasource_v2.formats.mcap import mcap_io

    real_parse = mcap_io._collect_summary_records

    def parse(section):
        summary = real_parse(section)
        for name in names:
            setattr(summary, name, {})
        return summary

    monkeypatch.setattr(mcap_io, "_collect_summary_records", parse)


class SeekLog:
    """A filesystem whose files record, per path, every offset they seek to."""

    def __init__(self):
        self._filesystem = LocalFileSystem()
        self.seeks: Dict[str, List[int]] = defaultdict(list)

    def open_input_file(self, path):
        return _SeekLoggingFile(
            self._filesystem.open_input_file(path), self.seeks[path]
        )


class _SeekLoggingFile:
    """A file that appends every absolute seek offset to ``seeks``."""

    def __init__(self, f, seeks: List[int]):
        self._f = f
        self._seeks = seeks

    def seek(self, offset, whence=0):
        if whence == 0:
            self._seeks.append(offset)
        return self._f.seek(offset, whence)

    def __getattr__(self, name):
        return getattr(self._f, name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._f.close()


def chunks_read(seek_log: SeekLog, path: str) -> List[int]:
    """Start offsets of the reads of chunk records in ``path``, in order.

    Each read starts at a chunk record and fetches that chunk, or a run of
    chunks fetched together (``mcap_io.read_chunks``).
    """
    starts = {c.chunk_start_offset for c in summary_of(path).chunk_indexes}
    return [offset for offset in seek_log.seeks[path] if offset in starts]


def record_parsed_chunks(monkeypatch) -> List[int]:
    """Record the offset of every chunk the reader parses out of its reads."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_chunks

    real_read_chunks = mcap_chunks.read_chunks
    offsets: List[int] = []

    def read_chunks(f, chunk_indexes, reads):
        for chunk_index, chunk in real_read_chunks(f, chunk_indexes, reads):
            offsets.append(chunk_index.chunk_start_offset)
            yield chunk_index, chunk

    monkeypatch.setattr(mcap_chunks, "read_chunks", read_chunks)
    return offsets
