"""Helpers shared by the MCAP datasource unit tests.

The tests write small MCAP files and drive the indexer, scanner and reader
directly, as planning and the ``ListFiles`` and ``ReadFiles`` tasks do, with no
Ray cluster. The writers import ``mcap`` and ``av`` when called, so the test
modules still collect without them.
"""

import io
import json
from typing import Any, Dict, List

import pyarrow as pa
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    VideoOptions,
    WindowSpec,
)
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


def summary_of(path: str):
    """The summary of the local MCAP file at ``path``."""
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    return summary


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

# A VP9 frame opens with an uncompressed header: frame_marker 0b10, profile 0,
# show_existing_frame 0, frame_type 0 (key) or 1 (inter), show_frame 1,
# error_resilient 0. A key frame then carries the sync code.
VP9_SYNC_CODE = b"\x49\x83\x42"
VP9_KEYFRAME = b"\x82" + VP9_SYNC_CODE + bytes(16)
VP9_PFRAME = b"\x86" + bytes(16)


# -- the camera and IMU recording (the ``recording`` fixture) -----------------

CAM_SCHEMA = "foxglove_msgs/msg/CompressedVideo"
CAM_FPS = 10
GOP = 10  # frames between keyframes
IMU_HZ = 50
DURATION_S = 5
CAM_FRAMES = CAM_FPS * DURATION_S
IMU_SAMPLES = IMU_HZ * DURATION_S
# A window opening 0.55 s into a second starts mid-GOP. Its lead-in is the six
# camera frames from the keyframe at the whole second: 0.0, 0.1, ..., 0.5 s.
MID_GOP_NS = 550_000_000
LEAD_IN_FRAMES = 6


def frame_time(k: int) -> int:
    """The log time of camera frame ``k``."""
    return k * SECOND // CAM_FPS


def cam_payload(frame: int, first_keyframe: int = 0) -> bytes:
    """Frame ``frame`` of the test camera, with a keyframe every ``GOP`` frames
    from ``first_keyframe``. The frame number follows the NAL units so tests can
    tell frames apart."""
    is_key = frame >= first_keyframe and (frame - first_keyframe) % GOP == 0
    return (H264_KEYFRAME if is_key else H264_PFRAME) + frame.to_bytes(4, "big")


def write_recording(
    path,
    *,
    first_keyframe=0,
    chunk_size=600,
    cam_payloads=None,
    index=True,
    cam_schema=CAM_SCHEMA,
):
    """Five seconds of a 10 fps camera and a 50 Hz IMU, in small chunks.

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
            name=cam_schema, encoding="ros2msg", data=b"string format\nuint8[] data"
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
        for frame in range(CAM_FRAMES):
            payload = (
                cam_payloads[frame]
                if cam_payloads is not None
                else cam_payload(frame, first_keyframe)
            )
            messages.append((frame_time(frame), cam, payload))
        for i in range(IMU_SAMPLES):
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


# -- encoded video (the decode tests) -----------------------------------------

FRAME_NS = 33_000_000  # ``write_payloads`` logs frame i at i * FRAME_NS: about 30 fps
HEIGHT, WIDTH = 48, 64  # the frame size ``encode_h264`` defaults to
FRAME_SHAPE = (HEIGHT, WIDTH, 3)
HALF_SIZE = (HEIGHT // 2, WIDTH // 2)  # a ``resize`` target, or a smaller camera
H264_FILE_FRAMES = 30  # frames in the ``h264_file`` fixture


def encode_h264(num_frames, width=WIDTH, height=HEIGHT):
    """H.264 Annex-B access units, one per frame, a keyframe every ``GOP``."""
    import av
    import numpy as np

    container = av.open(io.BytesIO(), mode="w", format="h264")
    # A fixed GOP, no B-frames and no scene-cut keyframes make the keyframe
    # positions predictable. Without a global header, libx264 repeats SPS/PPS
    # before every keyframe, as recorders do.
    stream = container.add_stream(
        "libx264",
        rate=30,
        options={"g": str(GOP), "bf": "0", "sc_threshold": "0", "tune": "zerolatency"},
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


def write_payloads(
    path,
    payloads,
    *,
    schema_name,
    topic="/camera",
    chunk_size=1 << 20,
    log_times=None,
    extra_topic=None,
    **writer_options,
):
    """Write ``payloads`` on ``topic``, ``FRAME_NS`` apart or at ``log_times``.

    ``extra_topic``, a ``(topic, schema_name, payload)`` triple, adds a channel
    with one message at each of the same times. ``writer_options`` go to the
    mcap ``Writer``.
    """
    from mcap.writer import CompressionType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=chunk_size,
            compression=CompressionType.ZSTD,
            **writer_options,
        )
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


def write_two_cameras(path, offset_frames):
    """Write the same 30-frame H.264 stream on ``/a`` and ``/b``, one message per
    chunk, ``/b`` running ``offset_frames`` frames behind ``/a``.

        log time / FRAME_NS   0  1  ...  8  9  ...  29  30  ...  37
        /a frame              0  1  ...  8  9  ...  29
        /b frame, offset 8               0  1  ...  21  22  ...  29

    Both streams have keyframes at frames 0, 10 and 20. Returns the channel id
    of each topic.
    """
    from mcap.writer import CompressionType, Writer

    packets = encode_h264(30)
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


def write_stills(path, seconds, **writer_options):
    """Write one JPEG at each of ``seconds`` of log time, one message per chunk."""
    import numpy as np
    from PIL import Image

    buffer = io.BytesIO()
    Image.fromarray(np.zeros(FRAME_SHAPE, dtype=np.uint8)).save(buffer, format="JPEG")
    write_payloads(
        path,
        [buffer.getvalue()] * len(seconds),
        schema_name="sensor_msgs/msg/CompressedImage",
        chunk_size=1,
        log_times=[round(s * SECOND) for s in seconds],
        **writer_options,
    )


def split_after(manifest, first):
    """Split a manifest into a task owning its first ``first`` chunks and one
    owning the rest."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    return [FileManifest(block.slice(0, first)), FileManifest(block.slice(first))]


def window_datasource(path, length_s, stride_s=None, video_topics=None, **video):
    """A window-granularity datasource over ``path`` that decodes video with the
    ``video`` options."""
    return MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=length_s, stride_s=stride_s),
        video=VideoOptions(**video),
        video_topics=video_topics,
    )
