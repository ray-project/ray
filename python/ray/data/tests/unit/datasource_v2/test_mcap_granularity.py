"""Unit tests for the window, topic and file granularities of the V2 MCAP reader.

No Ray cluster: the indexer, scanner and reader are driven directly. The test
recording holds an H.264-like camera topic (synthetic Annex-B payloads with a
keyframe every ten frames) and an IMU topic, written in small chunks so a file
spans many chunks and many read tasks.
"""

import importlib.util
import os
from typing import Any, Dict, List

import pyarrow as pa
import pytest
from pyarrow.fs import LocalFileSystem

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import (
    VideoCodec,
    detect_codec,
    is_keyframe,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_windows import (
    owner_offsets,
    place_windows,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

S = 1_000_000_000  # one second in nanoseconds
START = b"\x00\x00\x00\x01"
# H.264: SPS (type 7), PPS (type 8), IDR slice (type 5), non-IDR slice (type 1).
H264_KEYFRAME = (
    (START + bytes([0x67, 0x42, 0x00, 0x1F]) + START + bytes([0x68, 0xCE, 0x38, 0x80]))
    + START
    + bytes([0x65, 0x88, 0x84, 0x00, 0x33])
)
H264_PFRAME = START + bytes([0x41, 0x9A, 0x02, 0x04, 0x33])
# H.265: VPS (32), SPS (33), PPS (34), IDR_W_RADL (19), TRAIL_R (1).
H265_KEYFRAME = (
    (START + bytes([0x40, 0x01, 0x0C]) + START + bytes([0x42, 0x01, 0x01]))
    + START
    + bytes([0x44, 0x01, 0xC1])
    + START
    + bytes([0x26, 0x01, 0xAF])
)
H265_PFRAME = START + bytes([0x02, 0x01, 0xD0])
JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF" + bytes(32)
PNG = b"\x89PNG\r\n\x1a\n" + bytes(32)

CAM_SCHEMA = "foxglove_msgs/msg/CompressedVideo"
CAM_FPS = 10
GOP = 10  # frames between keyframes
IMU_HZ = 50
DURATION_S = 5


def cam_payload(frame: int, first_keyframe: int = 0) -> bytes:
    """Frame ``frame`` of the test camera: a keyframe every ``GOP`` frames
    starting at ``first_keyframe``; the frame number trails the NAL units so a
    test can tell frames apart."""
    is_key = frame >= first_keyframe and (frame - first_keyframe) % GOP == 0
    return (H264_KEYFRAME if is_key else H264_PFRAME) + frame.to_bytes(4, "big")


def write_recording(
    path,
    *,
    first_keyframe=0,
    chunk_size=600,
    cam_payloads=None,
    index=True,
):
    """Five seconds of a 10 fps camera and a 50 Hz IMU, in small chunks."""
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
            name=CAM_SCHEMA, encoding="ros2msg", data=b"string format\nuint8[] data"
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
            payload = (
                cam_payloads[frame]
                if cam_payloads is not None
                else cam_payload(frame, first_keyframe)
            )
            messages.append((frame * S // CAM_FPS, cam, payload))
        for i in range(IMU_HZ * DURATION_S):
            messages.append((i * S // IMU_HZ, imu, i.to_bytes(8, "big")))
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


def list_manifests(datasource, **kwargs) -> List[FileManifest]:
    indexer = datasource._get_file_indexer()
    return list(
        indexer.list_files(
            pa.array(datasource.paths), filesystem=datasource.filesystem, **kwargs
        )
    )


def infer_schema(datasource) -> pa.Schema:
    indexer = datasource._get_file_indexer()
    sample = sample_files(indexer, datasource.paths, datasource.filesystem, [])
    return datasource.infer_schema(sample)


def read_rows(datasource, manifests) -> List[Dict[str, Any]]:
    scanner = datasource.create_scanner(infer_schema(datasource), datasource.filesystem)
    reader = scanner.create_reader()
    rows: List[Dict[str, Any]] = []
    for manifest in manifests:
        for table in reader.read(manifest):
            assert table.schema.equals(scanner.read_schema()), table.schema
            rows.extend(table.to_pylist())
    return rows


@pytest.fixture
def recording(tmp_path):
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path)
    summary = read_summary(LocalFileSystem(), path)
    assert summary is not None
    assert len(summary.chunk_indexes) > 8, len(summary.chunk_indexes)
    return path


# -- pure helpers -------------------------------------------------------------


def test_place_windows_from_file_start():
    spec = WindowSpec(length_s=1.0)
    assert place_windows(spec, 0, 5 * S - 1) == [(k * S, (k + 1) * S) for k in range(5)]
    # A span shorter than one stride is one window.
    assert place_windows(spec, 7 * S, 7 * S + 10) == [(7 * S, 8 * S)]
    assert place_windows(spec, 10, 5) == []


def test_place_windows_drop_partial_and_stride():
    spec = WindowSpec(length_s=1.0, drop_partial=True)
    # The last message is at 4.5 s, so the window [4 s, 5 s) runs past it.
    assert place_windows(spec, 0, 4 * S + S // 2) == [
        (k * S, (k + 1) * S) for k in range(4)
    ]
    overlapping = WindowSpec(length_s=1.0, stride_s=0.5)
    starts = [start for start, _ in place_windows(overlapping, 0, S)]
    assert starts == [0, S // 2, S]


def test_place_windows_epoch_and_absolute_anchor():
    epoch = WindowSpec(length_s=1.0, anchor="epoch")
    # A file starting at 2.3 s gets windows aligned to whole seconds.
    assert place_windows(epoch, 2 * S + 3 * S // 10, 3 * S) == [
        (2 * S, 3 * S),
        (3 * S, 4 * S),
    ]
    absolute = WindowSpec(length_s=1.0, anchor=10 * S)
    # Windows are aligned to the anchor both before and after it.
    assert place_windows(absolute, 8 * S + S // 2, 11 * S) == [
        (8 * S, 9 * S),
        (9 * S, 10 * S),
        (10 * S, 11 * S),
        (11 * S, 12 * S),
    ]


def test_window_spec_validation():
    with pytest.raises(ValueError, match="length_s"):
        WindowSpec(length_s=0)
    with pytest.raises(ValueError, match="stride_s"):
        WindowSpec(length_s=1, stride_s=-1)
    with pytest.raises(ValueError, match="anchor"):
        WindowSpec(length_s=1, anchor="middle")  # pyrefly: ignore[bad-argument-type]
    with pytest.raises(ValueError, match="lead_in_s"):
        VideoOptions(lead_in_s=-1)
    assert VideoOptions(topics=["/a"]).topics == ("/a",)
    assert WindowSpec(length_s=0.5).stride_ns == S // 2


def test_owner_offsets(recording):
    summary = read_summary(LocalFileSystem(), recording)
    assert summary is not None
    chunks = sorted(summary.chunk_indexes, key=lambda c: c.message_start_time)
    starts = [c.message_start_time for c in chunks]
    # A window starting inside chunk i belongs to chunk i; before every chunk,
    # to the first; at a chunk's exact start, to that chunk.
    assert owner_offsets(chunks, [starts[3] + 1]) == [chunks[3].chunk_start_offset]
    assert owner_offsets(chunks, [starts[3]]) == [chunks[3].chunk_start_offset]
    assert owner_offsets(chunks, [-5]) == [chunks[0].chunk_start_offset]
    assert owner_offsets(chunks, [starts[-1] + 10**12]) == [
        chunks[-1].chunk_start_offset
    ]


@pytest.mark.parametrize(
    "payload, codec, keyframe",
    [
        (H264_KEYFRAME, VideoCodec.H264, True),
        (H264_PFRAME, VideoCodec.H264, False),
        (H265_KEYFRAME, VideoCodec.H265, True),
        (H265_PFRAME, VideoCodec.H265, False),
        (b"\x00\x00\x00\x08cdr-head" + JPEG, VideoCodec.JPEG, True),
        (PNG, VideoCodec.PNG, True),
    ],
)
def test_detect_codec_and_keyframes(payload, codec, keyframe):
    assert detect_codec(payload) is codec
    assert is_keyframe(payload, codec) is keyframe


def test_detect_codec_unknown():
    assert detect_codec(b"\x01\x02\x03\x04" * 8) is None
    assert detect_codec(b"") is None


def test_selection_digest_is_stable_and_selection_sensitive():
    a = MCAPSelection.create(["/b", "/a"], None, None).digest()
    assert a == MCAPSelection.create(["/a", "/b"], None, None).digest()
    assert len(a) == 8
    assert a != MCAPSelection.create(["/a"], None, None).digest()
    assert a != MCAPSelection.create(["/a", "/b"], TimeRange(1, 2), None).digest()
    assert MCAPSelection().digest() == MCAPSelection.create(None, None, None).digest()


# -- window rows ----------------------------------------------------------------


def test_window_rows_carry_the_lead_in_of_video_topics(recording):
    # Windows start 0.55 s into every second, mid-GOP: the lead-in is the six
    # frames from the keyframe at the whole second (0.0, 0.1, ..., 0.5 s).
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        include_row_id=True,
    )
    rows = read_rows(datasource, list_manifests(datasource))
    starts = [row["window_start"] for row in rows]
    assert starts == [k * S + 550_000_000 for k in range(-1, 5)]

    first = rows[0]  # [-0.45 s, 0.55 s): nothing precedes the recording
    assert first["num_lead_in"] == 0
    assert first["window_end"] == 550_000_000

    second = rows[1]  # [0.55 s, 1.55 s)
    assert second["num_lead_in"] == 6
    lead_times = second["log_time"][: second["num_lead_in"]]
    assert lead_times == [k * S // CAM_FPS for k in range(6)]
    assert all(topic == "/cam" for topic in second["topic"][:6])
    body_times = second["log_time"][second["num_lead_in"] :]
    assert body_times[0] >= second["window_start"]
    assert body_times[-1] < second["window_end"]
    assert body_times == sorted(body_times)
    # Ten camera frames and fifty IMU samples fall inside one second.
    assert second["num_messages"] == 6 + 10 + 50
    assert second["row_id"] == (
        f"{recording}#[550000000,1550000000)@{datasource.selection.digest()}"
    )
    channels = {c["topic"]: c for c in second["channels"]}
    assert set(channels) == {"/cam", "/imu"}
    assert channels["/cam"]["schema_name"] == CAM_SCHEMA
    # The first lead-in entry is a keyframe and the payload is still encoded.
    assert second["data"][0].startswith(H264_KEYFRAME)


def test_window_opening_on_a_keyframe_has_no_lead_in(recording):
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="window", window=WindowSpec(length_s=1.0)
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert [row["window_start"] for row in rows] == [k * S for k in range(5)]
    assert all(row["num_lead_in"] == 0 for row in rows)
    assert all(row["num_messages"] == 10 + 50 for row in rows)


def test_window_rows_are_emitted_exactly_once_across_tasks(recording):
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=0.7, stride_s=0.4, anchor=120_000_000),
        include_row_id=True,
    )
    (manifest,) = list_manifests(datasource)
    whole = read_rows(datasource, [manifest])

    # Cut the chunk rows into three tasks, as the packer might.
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    n = len(manifest)
    parts = [
        FileManifest(block.slice(0, n // 3)),
        FileManifest(block.slice(n // 3, n // 3)),
        FileManifest(block.slice(2 * (n // 3))),
    ]
    split = read_rows(datasource, parts)

    assert sorted(r["row_id"] for r in split) == sorted(r["row_id"] for r in whole)
    assert len({r["row_id"] for r in whole}) == len(whole)
    by_id = {r["row_id"]: r for r in whole}
    for row in split:
        assert row["log_time"] == by_id[row["row_id"]]["log_time"]
        assert row["num_lead_in"] == by_id[row["row_id"]]["num_lead_in"]


def test_window_rows_respect_topics_and_time_range(recording):
    datasource = MCAPDatasourceV2(
        [recording],
        topics=["/imu"],
        time_range=TimeRange(start_time=S, end_time=3 * S),
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor="epoch"),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert [row["window_start"] for row in rows] == [S, 2 * S]
    for row in rows:
        assert set(row["topic"]) == {"/imu"}
        assert row["num_lead_in"] == 0
        assert row["num_messages"] == IMU_HZ
        assert all(S <= t < 3 * S for t in row["log_time"])


def test_window_lead_in_reaches_before_the_time_range(recording):
    """A range starting mid-GOP: the first window's lead-in lies before the range,
    in chunks the listing never considered."""
    datasource = MCAPDatasourceV2(
        [recording],
        time_range=TimeRange(start_time=S + 550_000_000, end_time=3 * S),
        read_granularity="window",
        window=WindowSpec(length_s=1.0),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert [row["window_start"] for row in rows] == [
        S + 550_000_000,
        2 * S + 550_000_000,
    ]
    first = rows[0]
    assert first["num_lead_in"] == 6
    assert first["log_time"][:6] == [S + k * S // CAM_FPS for k in range(6)]
    assert first["data"][0].startswith(H264_KEYFRAME)
    body_times = first["log_time"][6:]
    assert all(S + 550_000_000 <= t < 2 * S + 550_000_000 for t in body_times)
    assert len(body_times) == 10 + 50


def test_window_opening_before_the_range_keeps_the_keyframe(recording):
    """An epoch-anchored window opens at 1.0 s but the range starts at 1.25 s:
    the keyframe at 1.0 s is outside the range yet needed by the frames inside."""
    datasource = MCAPDatasourceV2(
        [recording],
        time_range=TimeRange(start_time=S + 250_000_000, end_time=3 * S),
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor="epoch"),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert [row["window_start"] for row in rows] == [S, 2 * S]
    first = rows[0]
    assert first["num_lead_in"] == 3
    assert first["log_time"][:3] == [S, S + S // 10, S + 2 * S // 10]
    assert first["data"][0].startswith(H264_KEYFRAME)
    assert all(t >= S + 250_000_000 for t in first["log_time"][3:])
    # The second window opens on the keyframe at 2.0 s, inside the range.
    assert rows[1]["num_lead_in"] == 0


def write_two_topics(path, *, index):
    """A 2.5 s file whose ``/late`` topic starts 0.35 s after ``/early``."""
    from mcap.writer import CompressionType, IndexType, Writer

    with open(path, "wb") as stream:
        writer = Writer(
            stream,
            chunk_size=200,
            compression=CompressionType.ZSTD,
            index_types=IndexType.ALL if index else IndexType.NONE,
            use_chunking=index,
            use_statistics=index,
            use_summary_offsets=index,
        )
        writer.start(profile="", library="ray-test")
        schema = writer.register_schema(name="t", encoding="ros2msg", data=b"")
        early = writer.register_channel(
            schema_id=schema, topic="/early", message_encoding="cdr"
        )
        late = writer.register_channel(
            schema_id=schema, topic="/late", message_encoding="cdr"
        )
        for i in range(25):
            t = i * S // 10
            writer.add_message(channel_id=early, log_time=t, publish_time=t, data=b"e")
            if t >= 350_000_000:
                writer.add_message(
                    channel_id=late, log_time=t, publish_time=t, data=b"l"
                )
        writer.finish()


def test_window_grid_does_not_depend_on_the_selection_or_the_index(tmp_path):
    """``file_start`` is the file's first message, not the selected topic's, for
    indexed and unindexed files alike."""
    windows = {}
    for index in (True, False):
        path = os.path.join(tmp_path, f"two-{index}.mcap")
        write_two_topics(path, index=index)
        datasource = MCAPDatasourceV2(
            [path],
            topics=["/late"],
            read_granularity="window",
            window=WindowSpec(length_s=1.0),
        )
        rows = read_rows(datasource, list_manifests(datasource))
        windows[index] = [(row["window_start"], row["window_end"]) for row in rows]
        assert all(set(row["topic"]) == {"/late"} for row in rows)
    assert windows[True] == windows[False] == [(0, S), (S, 2 * S), (2 * S, 3 * S)]


def test_fixed_lead_in_replaces_keyframe_detection(recording):
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        video=VideoOptions(lead_in_s=0.25),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    # Frames at 0.3, 0.4 and 0.5 s precede the 0.55 s window start by less
    # than 0.25 s.
    assert rows[1]["num_lead_in"] == 3


def test_max_lead_in_bounds_the_keyframe_search(tmp_path):
    path = os.path.join(tmp_path, "sparse.mcap")
    # Only one keyframe, at the very start.
    write_recording(
        path,
        cam_payloads=[cam_payload(0) if f == 0 else cam_payload(1) for f in range(50)],
    )
    datasource = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        video=VideoOptions(max_lead_in_s=0.3),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert all(row["num_lead_in"] == 0 for row in rows)

    reaching = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        video=VideoOptions(max_lead_in_s=10.0),
    )
    rows = read_rows(reaching, list_manifests(reaching))
    # Every window reaches back to the single keyframe at 0 s.
    assert rows[1]["num_lead_in"] == 6
    assert rows[2]["num_lead_in"] == 16


def test_unindexed_file_still_yields_windows(tmp_path):
    path = os.path.join(tmp_path, "noindex.mcap")
    write_recording(path, index=False)
    datasource = MCAPDatasourceV2(
        [path], read_granularity="window", window=WindowSpec(length_s=1.0)
    )
    manifests = list_manifests(datasource)
    assert len(manifests) == 1 and manifests[0].file_chunk_metadatas[0] is None
    rows = read_rows(datasource, manifests)
    assert [row["window_start"] for row in rows] == [k * S for k in range(5)]
    assert all(row["num_messages"] == 60 for row in rows)


def test_undetectable_video_topic_fails_at_planning(tmp_path):
    path = os.path.join(tmp_path, "opaque.mcap")
    write_recording(path, cam_payloads=[b"\x01\x02\x03\x04" * 8] * 50)
    datasource = MCAPDatasourceV2(
        [path], read_granularity="window", window=WindowSpec(length_s=1.0)
    )
    with pytest.raises(ValueError, match="'/cam'.*cannot be detected"):
        infer_schema(datasource)

    # A fixed lead-in is the way around it.
    with_lead_in = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=550_000_000),
        video=VideoOptions(lead_in_s=0.25),
    )
    rows = read_rows(with_lead_in, list_manifests(with_lead_in))
    assert rows[1]["num_lead_in"] == 3


# -- topic and file rows ------------------------------------------------------


def test_topic_rows_one_task_per_topic_starting_at_a_keyframe(tmp_path):
    path = os.path.join(tmp_path, "run.mcap")
    write_recording(path, first_keyframe=2)  # frames 0 and 1 are not decodable
    datasource = MCAPDatasourceV2([path], read_granularity="topic", include_row_id=True)
    manifests = list_manifests(datasource)
    assert [m.file_chunk_metadatas[0]["topic"] for m in manifests] == ["/cam", "/imu"]
    assert datasource.get_file_partitioner() is None

    rows = read_rows(datasource, manifests)
    assert [row["topic"] for row in rows] == ["/cam", "/imu"]
    cam, imu = rows
    assert cam["num_messages"] == 48
    assert cam["start_time"] == 2 * S // CAM_FPS
    assert cam["data"][0].startswith(H264_KEYFRAME)
    assert cam["row_id"] == f"{path}#/cam@{datasource.selection.digest()}"
    assert imu["num_messages"] == IMU_HZ * DURATION_S
    assert imu["channel_id"] == [imu["channels"][0]["channel_id"]] * imu["num_messages"]
    assert imu["log_time"] == sorted(imu["log_time"])


def test_topic_rows_honor_topics_and_excluded_units(recording):
    datasource = MCAPDatasourceV2(
        [recording], topics=["/imu"], read_granularity="topic"
    )
    manifests = list_manifests(datasource)
    assert len(manifests) == 1

    from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import (
        topic_unit_id,
    )

    assert (
        list_manifests(
            datasource, excluded_read_unit_ids={topic_unit_id(recording, "/imu")}
        )
        == []
    )


def test_file_rows(recording):
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="file", include_row_id=True
    )
    manifests = list_manifests(datasource)
    assert len(manifests) == 1 and manifests[0].file_chunk_metadatas[0] is None
    assert datasource.get_file_partitioner() is None

    (row,) = read_rows(datasource, manifests)
    assert row["path"] == recording
    assert row["num_messages"] == 50 + 250
    assert row["start_time"] == 0
    assert row["end_time"] == (IMU_HZ * DURATION_S - 1) * S // IMU_HZ
    assert set(row["topic"]) == {"/cam", "/imu"}
    assert len(row["channels"]) == 2
    assert row["row_id"] == f"{recording}@{datasource.selection.digest()}"
    assert row["log_time"] == sorted(row["log_time"])


def test_oversized_row_is_refused(recording, tmp_path, monkeypatch):
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", "100")
    # The limit is enforced while the payloads are collected, not after the
    # whole topic or file has been held in memory.
    datasource = MCAPDatasourceV2([recording], read_granularity="file")
    with pytest.raises(ValueError, match="at least [0-9]+ bytes.*over the 100-byte"):
        read_rows(datasource, list_manifests(datasource))
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="topic", topics=["/cam"]
    )
    with pytest.raises(ValueError, match="topic '/cam'.*at least [0-9]+ bytes"):
        read_rows(datasource, list_manifests(datasource))
    unindexed = os.path.join(tmp_path, "noindex.mcap")
    write_recording(unindexed, index=False)
    datasource = MCAPDatasourceV2([unindexed], read_granularity="topic")
    with pytest.raises(ValueError, match="topic '/(cam|imu)'.*at least [0-9]+ bytes"):
        read_rows(datasource, list_manifests(datasource))


@pytest.mark.parametrize("granularity", ["window", "topic", "file"])
def test_coarse_rows_without_metadata(recording, granularity):
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity=granularity,
        window=WindowSpec(length_s=1.0) if granularity == "window" else None,
        include_metadata=False,
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert rows
    assert "channels" not in rows[0]
    assert "path" in rows[0] and "data" in rows[0]


def test_granularity_option_validation(recording):
    with pytest.raises(ValueError, match="read_granularity must be one of"):
        MCAPDatasourceV2([recording], read_granularity="clip")
    with pytest.raises(ValueError, match="needs a WindowSpec"):
        MCAPDatasourceV2([recording], read_granularity="window")
    with pytest.raises(ValueError, match="window applies to"):
        MCAPDatasourceV2([recording], window=WindowSpec(length_s=1))
    with pytest.raises(ValueError, match="video applies to"):
        MCAPDatasourceV2([recording], video=VideoOptions())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
