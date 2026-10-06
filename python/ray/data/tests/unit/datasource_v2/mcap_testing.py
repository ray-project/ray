"""Helpers shared by the MCAP datasource unit tests.

The tests write small MCAP files and drive the indexer, scanner and reader
directly, as planning and the ``ListFiles`` and ``ReadFiles`` tasks do, with no
Ray cluster. The writers import ``mcap`` when called, so the test modules still
collect without it.
"""

import json
from typing import Any, Dict, List

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary

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


def list_manifests(datasource, **kwargs):
    """List ``datasource``'s paths as one ``ListFiles`` task does."""
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(
            pa.array(datasource.paths), filesystem=datasource.filesystem, **kwargs
        )
    )


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


def summary_of(path: str):
    """The summary of the local MCAP file at ``path``."""
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    return summary
