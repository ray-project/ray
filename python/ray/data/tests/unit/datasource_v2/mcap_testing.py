"""Helpers shared by the MCAP datasource unit tests.

The tests write small MCAP files and drive the indexer, scanner and reader
directly, as planning and the ``ListFiles`` and ``ReadFiles`` tasks do, with no
Ray cluster. The writers import ``mcap`` when called, so the test modules still
collect without it.
"""

import io
import json
import re
import shutil
import struct
from collections import defaultdict
from fractions import Fraction
from typing import Any, Callable, Dict, List, Optional

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import (
    partition_files,
    sample_files,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

SECOND = 1_000_000_000  # one second in nanoseconds
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


def write_mcap_without_summary_schemas(path, b_declared_in_chunk_1=False):
    """Write two JSON channels with two schemas, one message per chunk.

    The schema records are written only inside the chunks
    (``repeat_schemas=False``), so the summary lists the channels but not their
    schemas. ``/a`` uses ``test_schema`` and ``/b`` uses ``other_schema``:

        chunk   0   1   2   3   4   5
        topic   /a  /b  /a  /b  /a  /b

    Both schemas are declared in chunk 0, or with ``b_declared_in_chunk_1``,
    ``other_schema`` only in chunk 1.
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

        def declare(topic, name):
            schema_id = writer.register_schema(
                name=name, encoding="jsonschema", data=b'{"type": "object"}'
            )
            channels[topic] = writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="json"
            )

        declare("/a", "test_schema")
        if not b_declared_in_chunk_1:
            declare("/b", "other_schema")
        for i in range(6):
            topic = "/a" if i % 2 == 0 else "/b"
            if topic not in channels:
                # Chunk 0 closed after its message, so this lands in chunk 1.
                declare(topic, "other_schema")
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


def edit_summaries(monkeypatch, edit: Callable[[Any], None]) -> None:
    """Pass every later summary read through ``edit``, which changes it in place.

    Stands in for a writer whose summary leaves out some records.
    """
    from ray.data._internal.datasource_v2.formats.mcap import mcap_io

    real_parse = mcap_io._collect_summary_records

    def parse(section):
        summary = real_parse(section)
        edit(summary)
        return summary

    monkeypatch.setattr(mcap_io, "_collect_summary_records", parse)


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
    """A filesystem whose files record, per path, every offset they seek to and
    how many reads they serve."""

    def __init__(self):
        self._filesystem = LocalFileSystem()
        self.seeks: Dict[str, List[int]] = defaultdict(list)
        self.reads: Dict[str, int] = defaultdict(int)

    def open_input_file(self, path):
        return _SeekLoggingFile(
            self._filesystem.open_input_file(path), self.seeks[path], self, path
        )


class _SeekLoggingFile:
    """A file that appends every absolute seek offset to ``seeks`` and counts
    its reads in ``log.reads[path]``."""

    def __init__(self, f, seeks: List[int], log: SeekLog, path: str):
        self._f = f
        self._seeks = seeks
        self._log = log
        self._path = path

    def seek(self, offset, whence=0):
        if whence == 0:
            self._seeks.append(offset)
        return self._f.seek(offset, whence)

    def read(self, *args):
        self._log.reads[self._path] += 1
        return self._f.read(*args)

    def readinto(self, buffer):
        self._log.reads[self._path] += 1
        return self._f.readinto(buffer)

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

    def read_chunks(f, chunk_indexes, reads, on_corrupt=None):
        chunks = real_read_chunks(f, chunk_indexes, reads, on_corrupt)
        for chunk_index, chunk in chunks:
            offsets.append(chunk_index.chunk_start_offset)
            yield chunk_index, chunk

    monkeypatch.setattr(mcap_chunks, "read_chunks", read_chunks)
    return offsets


def read_rows(datasource, manifests, columns=None) -> List[Dict[str, Any]]:
    """Read ``manifests`` with the planned scanner into row dicts.

    ``columns`` prunes the read, as a projection does. Every table must match
    the read schema.
    """
    scanner = scanner_for(datasource)
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    reader = scanner.create_reader()
    rows: List[Dict[str, Any]] = []
    for manifest in manifests:
        for table in reader.read(manifest):
            assert table.schema.equals(scanner.read_schema()), table.schema
            rows.extend(table.to_pylist())
    return rows


def read_samples(
    samples: List[Dict[str, Any]],
    topics: Dict[str, Any],
    *,
    columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
    filesystem=None,
    **options,
) -> List[Dict[str, Any]]:
    """Read ``samples`` as one read task of ``read_mcap(samples=...)`` does.

    Each sample is a dict with ``files``, ``clip_start``, ``clip_end`` and
    ``anchor``, and any other columns. ``topics`` maps each topic to its
    ``TopicSpec``. ``columns`` prunes the read and ``limit`` caps its rows, as
    the optimizer's pushdowns do. ``options`` go to the datasource.
    """
    from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
        sample_manifest,
    )

    table = pa.Table.from_pylist(samples)
    if options.get("video") is not None:
        # Planning opens the samples' files to settle the decoded topics.
        options.setdefault("sample_files", [s["files"][0] for s in samples])
    datasource = MCAPDatasourceV2(
        [], topics=topics, samples_schema=table.schema, filesystem=filesystem, **options
    )
    scanner = datasource.create_scanner(datasource.infer_schema(None), filesystem)
    if columns is not None:
        scanner = scanner.prune_columns(columns)
    if limit is not None:
        scanner = scanner.push_limit(limit)
    rows: List[Dict[str, Any]] = []
    for result in scanner.create_reader().read(FileManifest(sample_manifest(table))):
        if columns != []:
            assert result.schema.equals(scanner.read_schema()), result.schema
        rows.extend(result.to_pylist())
    return rows


# -- the camera and IMU recording (the ``recording`` fixture) -----------------

CAM_FPS = 10
IMU_HZ = 50
DURATION_S = 5


def frame_time(k: int) -> int:
    """The log time of camera frame ``k``."""
    return k * SECOND // CAM_FPS


def write_recording(path, *, chunk_size=600, index=True):
    """Five seconds of a 10 fps camera and a 50 Hz IMU, in small chunks.

    Camera frame ``k`` is logged at ``frame_time(k)`` and its payload ends with
    ``k`` as four big-endian bytes. IMU sample ``i`` is ``i`` as eight bytes.
    The ``recording`` fixture's docstring draws the timeline.
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
        writer.start(profile="ros2", library="ray-test")
        cam_schema = writer.register_schema(
            name="test_msgs/msg/Frame",
            encoding="ros2msg",
            data=b"string format\nuint8[] data",
        )
        imu_schema = writer.register_schema(
            name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b"float64 x"
        )
        cam = writer.register_channel(
            schema_id=cam_schema, topic="/cam", message_encoding="cdr"
        )
        imu = writer.register_channel(
            schema_id=imu_schema, topic="/imu", message_encoding="cdr"
        )
        messages = []
        for frame in range(CAM_FPS * DURATION_S):
            messages.append(
                (frame_time(frame), cam, b"frame" + frame.to_bytes(4, "big"))
            )
        for i in range(IMU_HZ * DURATION_S):
            messages.append((i * SECOND // IMU_HZ, imu, i.to_bytes(8, "big")))
        messages.sort()
        for log_time, channel_id, payload in messages:
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=payload,
                sequence=0,
            )
        writer.finish()


# -- codec payloads -----------------------------------------------------------

START_CODE = b"\x00\x00\x00\x01"  # opens each Annex-B NAL unit

# H.264 NAL unit types.
H264_SLICE = 1  # a slice of an inter frame
H264_IDR = 5
H264_SPS = 7
H264_PPS = 8


def h264_nal(nal_type: int, ref_idc: int, payload: bytes) -> bytes:
    """An Annex-B H.264 NAL unit: the start code, a header byte, ``payload``."""
    return START_CODE + bytes([ref_idc << 5 | nal_type]) + payload


H264_KEYFRAME = (
    h264_nal(H264_SPS, 3, bytes([0x42, 0x00, 0x1F]))
    + h264_nal(H264_PPS, 3, bytes([0xCE, 0x38, 0x80]))
    + h264_nal(H264_IDR, 3, bytes([0x88, 0x84, 0x00, 0x33]))
)
H264_PFRAME = h264_nal(H264_SLICE, 2, bytes([0x9A, 0x02, 0x04, 0x33]))


def cdr_video(frame: bytes, fmt: str, stamp_ns: int = 0) -> bytes:
    """A CDR-encoded Foxglove ``CompressedVideo`` around ``frame``: the
    encapsulation header, ``stamp_ns``, a ``frame_id``, ``frame`` as ``data``
    and ``fmt`` as ``format``, in that order."""
    body = struct.pack("<iI", stamp_ns // SECOND, stamp_ns % SECOND)
    body += (4).to_bytes(4, "little") + b"cam\x00"
    body += len(frame).to_bytes(4, "little") + frame
    body += bytes(-len(body) % 4)
    token = fmt.encode() + b"\x00"
    return b"\x00\x01\x00\x00" + body + len(token).to_bytes(4, "little") + token


# A second whose CDR stamp, after the encapsulation header's two zero bytes,
# reads as 00 00 01 25: a start code and an H.264 IDR slice header.
START_CODE_SECOND = 0x2501

# A VP9 frame opens with an uncompressed header: frame_marker 0b10, profile 0,
# show_existing_frame 0, frame_type 0 (key) or 1 (inter), show_frame 1,
# error_resilient 0. A key frame then carries the sync code.
VP9_SYNC_CODE = b"\x49\x83\x42"
VP9_KEYFRAME = b"\x82" + VP9_SYNC_CODE + bytes(16)
VP9_PFRAME = b"\x86" + bytes(16)


GOP = 10  # frames between keyframes of ``encode_h264`` and ``encode_h265``

# -- encoded video (the decode tests) -----------------------------------------

FRAME_NS = 33_000_000  # ``write_payloads`` logs frame i at i * FRAME_NS: about 30 fps
HEIGHT, WIDTH = 48, 64  # the frame size ``encode_h264`` defaults to
FRAME_SHAPE = (HEIGHT, WIDTH, 3)
HALF_SIZE = (HEIGHT // 2, WIDTH // 2)  # a ``resize`` target, or a smaller camera
H264_FILE_FRAMES = 30  # frames in the ``h264_file`` fixture


def encode_h264(num_frames, width=WIDTH, height=HEIGHT, b_frames=0):
    """H.264 Annex-B access units in decode order, one per frame, a keyframe
    every ``GOP``. ``b_frames`` is the most B-frames between two references."""
    import av
    import numpy as np

    container = av.open(io.BytesIO(), mode="w", format="h264")
    # A fixed GOP and no scene-cut keyframes make the keyframe positions
    # predictable. Without a global header, libx264 repeats SPS/PPS before
    # every keyframe, as recorders do.
    options = {"g": str(GOP), "bf": str(b_frames), "sc_threshold": "0"}
    if not b_frames:
        options["tune"] = "zerolatency"
    stream = container.add_stream("libx264", rate=30, options=options)
    stream.width, stream.height, stream.pix_fmt = width, height, "yuv420p"
    packets = []
    for i in range(num_frames):
        array = np.full((height, width, 3), i * 8 % 256, dtype=np.uint8)
        array[:, :, 1] = (i * 3) % 256
        frame = av.VideoFrame.from_ndarray(array, format="rgb24")
        packets.extend(bytes(p) for p in stream.encode(frame))
    packets.extend(bytes(p) for p in stream.encode())
    return packets


def encode_vp9(num_frames, width=WIDTH, height=HEIGHT):
    """VP9 frames from libvpx, one per message, a keyframe every ``GOP``."""
    import av
    import numpy as np

    context = av.CodecContext.create("libvpx-vp9", "w")
    context.width, context.height, context.pix_fmt = width, height, "yuv420p"
    context.time_base = Fraction(1, 30)
    context.framerate = Fraction(30, 1)
    context.gop_size = GOP
    # No look-ahead and no hidden alt-ref frames, so each packet is one shown
    # frame and the keyframes fall every ``GOP`` frames.
    context.options = {
        "lag-in-frames": "0",
        "auto-alt-ref": "0",
        "keyint_min": str(GOP),
    }
    # Noise, so frames are as large as a camera's. A flat frame codes to a few
    # bytes, which can hold an Annex-B start code by chance.
    rng = np.random.default_rng(0)
    packets = []
    for i in range(num_frames):
        array = rng.integers(0, 256, (height, width, 3), dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(array, format="rgb24").reformat(
            format="yuv420p"
        )
        frame.pts = i
        packets.extend(bytes(p) for p in context.encode(frame))
    packets.extend(bytes(p) for p in context.encode(None))
    return packets


def encode_h265(num_frames, width=WIDTH, height=HEIGHT):
    """H.265 Annex-B access units, one per frame, a keyframe every ``GOP``.

    As ``encode_h264``: libx265 repeats VPS, SPS and PPS before every keyframe.
    """
    import av
    import numpy as np

    container = av.open(io.BytesIO(), mode="w", format="hevc")
    params = f"keyint={GOP}:min-keyint={GOP}:scenecut=0:bframes=0:open-gop=0"
    stream = container.add_stream(
        "libx265", rate=30, options={"x265-params": params + ":log-level=error"}
    )
    stream.width, stream.height, stream.pix_fmt = width, height, "yuv420p"
    packets = []
    for i in range(num_frames):
        array = np.full((height, width, 3), i * 8 % 256, dtype=np.uint8)
        array[:, :, 1] = (i * 3) % 256
        frame = av.VideoFrame.from_ndarray(array, format="rgb24")
        packets.extend(bytes(p) for p in stream.encode(frame))
    packets.extend(bytes(p) for p in stream.encode())
    return packets


# NAL unit types of the parameter sets: H.264's SPS and PPS, H.265's VPS, SPS
# and PPS.
PARAMETER_SET_TYPES = {"h264": {H264_SPS, H264_PPS}, "h265": {32, 33, 34}}


def nal_units(access_unit: bytes) -> List[bytes]:
    """The Annex-B NAL units of ``access_unit``, each with its start code."""
    starts = [m.start() for m in re.finditer(b"\x00\x00\x01", access_unit)]
    starts = [i - 1 if i > 0 and access_unit[i - 1] == 0 else i for i in starts]
    return [access_unit[a:b] for a, b in zip(starts, starts[1:] + [len(access_unit)])]


def nal_type(unit: bytes, codec: str) -> int:
    """The type of an Annex-B NAL unit of ``codec`` ("h264" or "h265")."""
    header = unit[unit.index(b"\x00\x00\x01") + 3]
    return header & 0x1F if codec == "h264" else (header >> 1) & 0x3F


def length_prefixed(access_unit: bytes) -> bytes:
    """``access_unit``'s NAL units, each after its length as a 4-byte big-endian
    integer in place of a start code, as MP4 stores them (AVCC)."""
    units = (u[u.index(b"\x00\x00\x01") + 3 :] for u in nal_units(access_unit))
    return b"".join(len(unit).to_bytes(4, "big") + unit for unit in units)


def without_parameter_sets(access_unit: bytes, codec: str) -> bytes:
    """``access_unit`` less its parameter-set NAL units."""
    return b"".join(
        unit
        for unit in nal_units(access_unit)
        if nal_type(unit, codec) not in PARAMETER_SET_TYPES[codec]
    )


def parameter_sets_once(packets, codec: str, keep_first: bool = True):
    """``packets`` with the parameter sets on the first keyframe only, as from an
    encoder that writes them once. ``keep_first=False`` keeps them nowhere."""
    kept = keep_first
    out = []
    for packet in packets:
        stripped = without_parameter_sets(packet, codec)
        out.append(packet if kept or stripped == packet else stripped)
        if stripped != packet:
            kept = False
    return out


class LateContext:
    """A codec context that releases frames ``delay`` packets late, as libdav1d may."""

    def __init__(self, context, delay=1):
        self._context = context
        self._delay = delay
        # The frames of each packet fed and not released yet, oldest first.
        self._held = []

    def decode(self, packet):
        if packet is None:
            frames = [frame for held in self._held for frame in held]
            self._held = []
            return frames + list(self._context.decode(None))
        self._held.append(list(self._context.decode(packet)))
        return self._held.pop(0) if len(self._held) > self._delay else []


class UnstampedAtFlushContext(LateContext):
    """Releases each frame one packet late, and those left at the end unstamped."""

    def decode(self, packet):
        frames = super().decode(packet)
        if packet is None:
            for frame in frames:
                frame.pts = None
        return frames


def write_payloads(
    path,
    payloads,
    *,
    schema_name,
    topic="/camera",
    chunk_size=1 << 20,
    log_times=None,
    extra_topic=None,
    extra_first=0,
    **writer_options,
):
    """Write ``payloads`` on ``topic``, ``FRAME_NS`` apart or at ``log_times``.

    ``extra_topic``, a ``(topic, schema_name, payload)`` triple, adds a channel
    with one message at each of the same times, after ``extra_first`` of its
    messages at time 0 before any payload. ``writer_options`` go to the
    mcap ``Writer``; chunks are zstd-compressed unless they name a compression.
    """
    from mcap.writer import CompressionType, Writer

    writer_options.setdefault("compression", CompressionType.ZSTD)
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=chunk_size, **writer_options)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name=schema_name, encoding="ros2msg", data=b"video\n"
        )
        channel_id = writer.register_channel(
            schema_id=schema_id, topic=topic, message_encoding="cdr"
        )
        extra_channel = None
        extra_payload = b""
        if extra_topic is not None:
            extra_schema = writer.register_schema(
                name=extra_topic[1], encoding="ros2msg", data=b"x\n"
            )
            extra_channel = writer.register_channel(
                schema_id=extra_schema, topic=extra_topic[0], message_encoding="cdr"
            )
            extra_payload = extra_topic[2]
            for _ in range(extra_first):
                writer.add_message(
                    channel_id=extra_channel,
                    log_time=0,
                    publish_time=0,
                    data=extra_payload,
                )
        for i, payload in enumerate(payloads):
            log_time = log_times[i] if log_times is not None else i * FRAME_NS
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=payload,
                sequence=i,
            )
            if extra_channel is not None:
                writer.add_message(
                    channel_id=extra_channel,
                    log_time=log_time,
                    publish_time=log_time,
                    data=extra_payload,
                    sequence=i,
                )
        writer.finish()


def write_two_cameras(path, offset_frames, packets=None):
    """Write the same 30-frame H.264 stream on ``/a`` and ``/b``, one message per
    chunk, ``/b`` running ``offset_frames`` frames behind ``/a``.

        log time / FRAME_NS   0  1  ...  8  9  ...  29  30  ...  37
        /a frame              0  1  ...  8  9  ...  29
        /b frame, offset 8               0  1  ...  21  22  ...  29

    Both streams have keyframes at frames 0, 10 and 20. ``packets`` replaces the
    stream, ``encode_h264(30)`` by default. Returns the channel id of each topic.
    """
    from mcap.writer import CompressionType, Writer

    packets = encode_h264(30) if packets is None else packets
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="foxglove.CompressedVideo", encoding="ros2msg", data=b"video\n"
        )
        channels = {
            topic: writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="cdr"
            )
            for topic in ("/a", "/b")
        }
        for i, payload in enumerate(packets):
            for topic, shift in (("/a", 0), ("/b", offset_frames)):
                log_time = (i + shift) * FRAME_NS
                writer.add_message(
                    channel_id=channels[topic],
                    log_time=log_time,
                    publish_time=log_time,
                    data=payload,
                    sequence=i,
                )
        writer.finish()
    return channels


def manifest_for_frames(datasource, path, channels, offset_frames, frames):
    """Return a manifest that owns exactly ``frames``, and the whole file's manifest.

    ``frames`` holds ``(topic, index)`` pairs. The file has one message per chunk.
    """
    (manifest,) = list_manifests(datasource)
    summary = summary_of(path)
    by_channel = {cid: topic for topic, cid in channels.items()}
    wanted_offsets = set()
    for chunk in summary.chunk_indexes:
        (channel_id,) = chunk.message_index_offsets
        topic = by_channel[channel_id]
        shift = offset_frames if topic == "/b" else 0
        if (topic, chunk.message_start_time // FRAME_NS - shift) in frames:
            wanted_offsets.add(chunk.chunk_start_offset)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    rows = [
        i
        for i, md in enumerate(manifest.file_chunk_metadatas)
        if md is not None and int(md["unit_ids"][0]) in wanted_offsets
    ]
    assert len(rows) == len(frames)
    return FileManifest(block.take(rows)), manifest


def split_at_first_idr(packet):
    """Split an access unit just before the start code of its IDR NAL unit."""
    i = 0
    while True:
        j = packet.find(b"\x00\x00\x01", i)
        assert j >= 0, "no IDR NAL unit"
        if packet[j + 3] & 0x1F == H264_IDR:
            cut = j - 1 if j > 0 and packet[j - 1] == 0 else j
            return packet[:cut], packet[cut:]
        i = j + 3


def listed_if_custom(schema_name):
    """``video_topics`` for the test camera: needed only for a custom schema."""
    return ["/camera"] if schema_name.startswith("custom_msgs/") else None


def split_after(manifest, first):
    """Split a manifest into a task owning its first ``first`` chunks and one
    owning the rest."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return [FileManifest(block.slice(0, first)), FileManifest(block.slice(first))]
