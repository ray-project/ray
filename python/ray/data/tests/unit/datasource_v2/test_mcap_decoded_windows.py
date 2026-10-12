"""Unit tests for decoded MCAP window rows.

With ``VideoOptions``, a window row holds each video topic's decoded frames in
``frames:<topic>`` and ``frame_times:<topic>``, and the other topics' messages
in the lists. No Ray cluster: the indexer, scanner and reader are driven
directly.
"""

import dataclasses
import importlib.util
import os
from typing import Dict, List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_decode,
    mcap_decoded_windows,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import (
    FrameDecoder,
    FrameThinner,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    VideoOptions,
    WindowSpec,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import VideoCodec
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    FRAME_NS,
    FRAME_SHAPE,
    GOP,
    H264_FILE_FRAMES,
    H264_PFRAME,
    HALF_SIZE,
    HEIGHT,
    SECOND,
    VP9_KEYFRAME,
    VP9_PFRAME,
    WIDTH,
    encode_h264,
    infer_schema,
    list_manifests,
    listed_if_custom,
    manifest_for_frames,
    read_rows,
    scanner_for,
    split_after,
    window_datasource,
    write_payloads,
    write_stills,
    write_two_cameras,
)

pytestmark = [
    pytest.mark.skipif(
        importlib.util.find_spec("mcap") is None,
        reason="mcap module not available. Install with: pip install mcap",
    ),
    pytest.mark.skipif(
        importlib.util.find_spec("av") is None,
        reason="av not available. Install with: pip install av",
    ),
]

# A 0.33 s window holds ten frames, FRAME_NS apart, so 30 frames fill three.
WINDOW_S = 0.33
WINDOW_NS = 330_000_000
WINDOW_STARTS = [0, WINDOW_NS, 2 * WINDOW_NS]


def split_manifest(manifest, parts):
    """Cut a manifest's chunk rows into ``parts`` consecutive tasks."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    n = len(manifest)
    assert n >= parts
    bounds = [round(i * n / parts) for i in range(parts + 1)]
    return [
        FileManifest(block.slice(lo, hi - lo)) for lo, hi in zip(bounds, bounds[1:])
    ]


def frames_by_sequence(path):
    """Ground truth: every frame of the file decoded at message granularity."""
    datasource = MCAPDatasourceV2([path], video=VideoOptions())
    rows = read_rows(datasource, list_manifests(datasource))
    return {row["sequence"]: row["frame"] for row in rows}


def by_window(rows):
    """Window rows keyed by ``window_start``."""
    return {row["window_start"]: row for row in rows}


@pytest.mark.parametrize(
    "schema_name",
    ["foxglove.CompressedVideo", "custom_msgs/msg/Frame"],
    ids=["video-schema", "custom-schema"],
)
def test_window_planning_tells_video_from_the_stream_head(tmp_path, schema_name):
    """Window planning takes a VP9 stream that opens on inter frames, with a known
    schema or listed in ``video_topics``, and gives it frame columns. Only the
    plan is checked, since a real decoder rejects the synthetic payloads."""
    payloads = [VP9_KEYFRAME if i % GOP == 2 else VP9_PFRAME for i in range(30)]
    path = os.path.join(tmp_path, "midgop_window.mcap")
    write_payloads(path, payloads, schema_name=schema_name, chunk_size=1)
    datasource = window_datasource(
        path, length_s=WINDOW_S, video_topics=listed_if_custom(schema_name)
    )

    schema = infer_schema(datasource)

    assert schema.names[-2:] == ["frames:/camera", "frame_times:/camera"]


def test_decoded_window_topics_are_the_sampled_and_the_listed_ones(tmp_path, caplog):
    """Window rows decode the video topics of the files planning samples and every
    ``video_topics`` entry. A video topic seen only in another file stays in the
    message lists, with one warning; listed, it is decoded there too."""
    from ray.util.debug import reset_log_once

    packets = encode_h264(30)
    sampled = os.path.join(tmp_path, "a.mcap")
    write_payloads(sampled, packets, schema_name="foxglove.CompressedVideo", topic="/a")
    other = os.path.join(tmp_path, "b.mcap")
    write_payloads(other, packets, schema_name="foxglove.CompressedVideo", topic="/b")
    (manifest,) = list_manifests(window_datasource(other, length_s=WINDOW_S))
    reset_log_once("mcap_unplanned_video:/b")

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_decoded_windows.logger.addHandler(caplog.handler)
    try:
        for listed, decoded in ((None, ["/a"]), (["/b"], ["/a", "/b"])):
            planner = window_datasource(sampled, length_s=WINDOW_S, video_topics=listed)
            schema = infer_schema(planner)
            assert [n for n in schema.names if n.startswith("frames:")] == [
                f"frames:{topic}" for topic in decoded
            ]
            scanner = planner.create_scanner(schema, planner.filesystem)
            rows = [
                row
                for table in scanner.create_reader().read(manifest)
                for row in table.to_pylist()
            ]
            if listed is None:
                assert sum(row["num_messages"] for row in rows) == 30
            else:
                assert sum(len(row["frame_times:/b"]) for row in rows) == 30
                assert all(row["num_messages"] == 0 for row in rows)
    finally:
        mcap_decoded_windows.logger.removeHandler(caplog.handler)

    warnings = [
        record.message
        for record in caplog.records
        if "not in the files planning sampled" in record.message
    ]
    assert len(warnings) == 1
    assert "'/b'" in warnings[0] and "video_topics" in warnings[0]


def test_decoded_windows_keep_the_lead_in_of_video_topics_left_encoded(tmp_path):
    """A video topic planning did not sample stays encoded in decoded window rows
    with the lead-in a read without ``video`` gives it, so it still decodes on its
    own."""
    packets = encode_h264(30)
    sampled = os.path.join(tmp_path, "a.mcap")
    write_payloads(sampled, packets, schema_name="foxglove.CompressedVideo", topic="/a")
    other = os.path.join(tmp_path, "b.mcap")
    write_payloads(other, packets, schema_name="foxglove.CompressedVideo", topic="/b")
    encoded = MCAPDatasourceV2(
        [other], read_granularity="window", window=WindowSpec(length_s=0.2)
    )
    (manifest,) = list_manifests(encoded)
    expected = [
        (row["window_start"], row["num_lead_in"], row["log_time"])
        for row in read_rows(encoded, [manifest])
    ]
    assert any(num_lead_in > 0 for _, num_lead_in, _ in expected)

    planner = window_datasource(sampled, length_s=0.2)
    scanner = scanner_for(planner)
    rows = [
        row
        for table in scanner.create_reader().read(manifest)
        for row in table.to_pylist()
    ]

    assert [
        (row["window_start"], row["num_lead_in"], row["log_time"]) for row in rows
    ] == expected


def test_decoded_window_row_ids_change_with_the_video_options(h264_file):
    """Decoding, and the ``fps`` or ``resize`` it uses, change what a window row
    holds, so they change the row's id."""

    def row_ids(video):
        """The row ids of a window read with ``video``."""
        datasource = MCAPDatasourceV2(
            [h264_file],
            read_granularity="window",
            window=WindowSpec(length_s=WINDOW_S),
            include_row_id=True,
            video=video,
        )
        rows = read_rows(datasource, list_manifests(datasource))
        return tuple(row["row_id"] for row in rows)

    ids = {
        row_ids(None),
        row_ids(VideoOptions()),
        row_ids(VideoOptions(fps=5)),
        row_ids(VideoOptions(resize=HALF_SIZE)),
    }

    assert len(ids) == 4


def test_decoded_windows_match_the_whole_file_read(h264_file):
    """Window rows hold each window's decoded frames. Splitting the read into
    three tasks gives the same frames as one task and as a message-granularity
    read."""
    import numpy as np

    datasource = window_datasource(h264_file, length_s=WINDOW_S)
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    whole = read_rows(datasource, [manifest])
    split = read_rows(datasource, split_manifest(manifest, 3))

    names = scanner.read_schema().names
    assert names[-2:] == ["frames:/camera", "frame_times:/camera"]
    assert str(scanner.read_schema().field("frames:/camera").type).startswith(
        "ArrowVariableShapedTensorType"
    )
    assert sorted(by_window(whole)) == WINDOW_STARTS
    assert sorted(by_window(split)) == sorted(by_window(whole))
    truth = frames_by_sequence(h264_file)
    for start, row in by_window(whole).items():
        other = by_window(split)[start]
        times = row["frame_times:/camera"]
        assert (
            times
            == other["frame_times:/camera"]
            == [
                t
                for t in range(0, H264_FILE_FRAMES * FRAME_NS, FRAME_NS)
                if start <= t < start + WINDOW_NS
            ]
        )
        frames = row["frames:/camera"]
        assert frames.shape == (10, *FRAME_SHAPE) and frames.dtype == np.uint8
        assert np.array_equal(frames, other["frames:/camera"])
        for t, frame in zip(times, frames):
            assert np.array_equal(frame, truth[t // FRAME_NS])
        # The camera is decoded, so its messages and lead-in stay out of the lists.
        assert row["num_messages"] == 0 and row["num_lead_in"] == 0
        assert row["topic"] == [] and row["data"] == []


def test_decoded_window_opening_mid_gop_starts_on_its_own_first_frame(h264_file):
    """A window that opens at frame 16, inside the second GOP, is decoded from
    the keyframe at frame 10 by whichever task owns it."""
    import numpy as np

    datasource = window_datasource(h264_file, length_s=0.5)
    (manifest,) = list_manifests(datasource)
    truth = frames_by_sequence(h264_file)

    for manifests in ([manifest], split_manifest(manifest, 3)):
        rows = read_rows(datasource, manifests)
        second = by_window(rows)[500_000_000]
        assert second["frame_times:/camera"][0] == 16 * FRAME_NS
        assert np.array_equal(second["frames:/camera"][0], truth[16])
        assert len(second["frame_times:/camera"]) == 14


def test_decoded_windows_overlap_and_bound_their_frames(h264_file):
    """Overlapping windows duplicate frames on purpose, and no window holds a
    frame outside its span."""
    datasource = window_datasource(h264_file, length_s=WINDOW_S, stride_s=0.165)
    (manifest,) = list_manifests(datasource)

    rows = read_rows(datasource, split_manifest(manifest, 2))

    holding = {}
    for row in rows:
        start, end = row["window_start"], row["window_end"]
        for t in row["frame_times:/camera"]:
            assert start <= t < end
            holding.setdefault(t, []).append(start)
    # Frame 6 (198 ms) sits in [0, 330) and [165, 495); frame 0 only in the first.
    assert sorted(holding[6 * FRAME_NS]) == [0, 165_000_000]
    assert holding[0] == [0]
    assert sorted(by_window(rows)) == [
        0,
        165_000_000,
        330_000_000,
        495_000_000,
        660_000_000,
        825_000_000,
    ]


def write_two_cameras_and_an_imu(path):
    """Write two cameras and an IMU in ~400-byte chunks, one message each per frame.

        log time / FRAME_NS   0  1  ...  29
        /cam_a, /cam_b        the same 30-frame H.264 stream
        /imu                  16 bytes per frame time

    Every message at frame ``i`` has sequence ``i``.
    """
    from mcap.writer import CompressionType, Writer

    packets = encode_h264(30)
    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=400, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        video_schema = writer.register_schema(
            name="foxglove.CompressedVideo", encoding="ros2msg", data=b"video\n"
        )
        imu_schema = writer.register_schema(
            name="sensor_msgs/msg/Imu", encoding="ros2msg", data=b"imu\n"
        )
        cameras = {
            topic: writer.register_channel(
                schema_id=video_schema, topic=topic, message_encoding="cdr"
            )
            for topic in ("/cam_a", "/cam_b")
        }
        imu = writer.register_channel(
            schema_id=imu_schema, topic="/imu", message_encoding="cdr"
        )
        for i, payload in enumerate(packets):
            log_time = i * FRAME_NS
            for channel in cameras.values():
                writer.add_message(
                    channel_id=channel,
                    log_time=log_time,
                    publish_time=log_time,
                    data=payload,
                    sequence=i,
                )
            writer.add_message(
                channel_id=imu,
                log_time=log_time,
                publish_time=log_time,
                data=bytes([i % 256]) * 16,
                sequence=i,
            )
        writer.finish()


def test_decoded_windows_keep_the_other_topics_in_the_lists(tmp_path):
    """Two cameras get two pairs of frame columns. The IMU stays in the message
    lists and is the only channel in ``channels``."""
    path = os.path.join(tmp_path, "rig.mcap")
    write_two_cameras_and_an_imu(path)
    datasource = window_datasource(path, length_s=WINDOW_S)
    (manifest,) = list_manifests(datasource)
    scanner = scanner_for(datasource)

    rows = read_rows(datasource, split_manifest(manifest, 3))

    assert scanner.read_schema().names[-4:] == [
        "frames:/cam_a",
        "frame_times:/cam_a",
        "frames:/cam_b",
        "frame_times:/cam_b",
    ]
    assert len(rows) == 3
    for row in rows:
        assert (
            row["frames:/cam_a"].shape
            == row["frames:/cam_b"].shape
            == (10, *FRAME_SHAPE)
        )
        assert row["topic"] == ["/imu"] * 10 and row["num_messages"] == 10
        assert len(row["log_time"]) == 10 and len(row["data"]) == 10
        assert [c["topic"] for c in row["channels"]] == ["/imu"]


def test_decoded_windows_fps_and_resize_do_not_depend_on_the_split(h264_file):
    """With ``fps`` and ``resize``, a split read keeps the same frames, at the
    same size, as a whole-file read."""
    import numpy as np

    datasource = window_datasource(
        h264_file, length_s=WINDOW_S, fps=10, resize=HALF_SIZE
    )
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest])
    split = read_rows(datasource, split_manifest(manifest, 3))

    kept = []
    for start, row in sorted(by_window(whole).items()):
        other = by_window(split)[start]
        assert row["frame_times:/camera"] == other["frame_times:/camera"]
        assert np.array_equal(row["frames:/camera"], other["frames:/camera"])
        assert row["frames:/camera"].shape[1:] == (*HALF_SIZE, 3)
        kept.extend(row["frame_times:/camera"])
    # One frame per 100 ms interval of the epoch grid, over 990 ms of video.
    assert kept == sorted(kept) and len(kept) == 10
    assert len({t // 100_000_000 for t in kept}) == 10


@pytest.mark.parametrize(
    "columns",
    [None, ["window_start", "frame_times:/camera"]],
    ids=["decoded", "pruned"],
)
def test_decoded_windows_fps_interval_longer_than_the_look_back(tmp_path, columns):
    """With 20 s ``fps`` intervals and the default 10 s look-back, the task owning
    the windows after a 14 s pause learns from the message index that the frame
    at 1 s already took their interval."""
    path = os.path.join(tmp_path, "pause.mcap")
    write_stills(path, [1] + list(range(15, 23)))
    datasource = window_datasource(path, length_s=2, fps=0.05)
    (manifest,) = list_manifests(datasource)

    whole = read_rows(datasource, [manifest], columns)
    split = read_rows(datasource, split_after(manifest, 1), columns)

    def frame_times(rows):
        """Each window's frame times, keyed by its start."""
        return {r["window_start"]: r["frame_times:/camera"] for r in rows}

    assert frame_times(whole) == {1 * SECOND: [1 * SECOND], 19 * SECOND: [20 * SECOND]}
    assert frame_times(split) == frame_times(whole)


def test_decoded_windows_pruned_frames_skip_the_decoder(h264_file, monkeypatch):
    """With ``frames:`` pruned no decoder is built, yet ``frame_times:`` still
    lists the frames a decoder would keep."""

    class NoDecoder(FrameDecoder):
        """A decoder that fails if built."""

        def __init__(self, *args, **kwargs):
            raise AssertionError("frames were pruned; nothing should decode")

    monkeypatch.setattr(mcap_decoded_windows, "FrameDecoder", NoDecoder)
    datasource = window_datasource(h264_file, length_s=WINDOW_S, fps=10)
    (manifest,) = list_manifests(datasource)

    rows = read_rows(
        datasource,
        split_manifest(manifest, 3),
        columns=["window_start", "frame_times:/camera"],
    )

    assert sorted(by_window(rows)) == WINDOW_STARTS
    assert sum(len(row["frame_times:/camera"]) for row in rows) == 10
    assert set(rows[0]) == {"window_start", "frame_times:/camera"}


def test_decoded_window_over_the_row_limit_fails_early(h264_file, monkeypatch):
    """A window whose decoded frames pass ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails,
    naming the window and ``fps``."""
    # Room for two decoded frames, where a window holds ten.
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", str(HEIGHT * WIDTH * 3 * 2))
    datasource = window_datasource(h264_file, length_s=WINDOW_S)

    with pytest.raises(ValueError, match=r"window \[0, 330000000\).*fps"):
        read_rows(datasource, list_manifests(datasource))


def test_emitted_windows_release_their_frames(h264_file, monkeypatch):
    """An emitted window drops its frames and messages, so a task holds only the
    windows still in flight."""
    created = []

    class Tracking(mcap_decoded_windows._PendingWindow):
        """A pending window that records itself when created."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            created.append(self)

    monkeypatch.setattr(mcap_decoded_windows, "_PendingWindow", Tracking)
    datasource = window_datasource(h264_file, length_s=WINDOW_S)

    rows = read_rows(datasource, list_manifests(datasource))

    assert sorted(by_window(rows)) == WINDOW_STARTS
    assert all(len(row["frame_times:/camera"]) == 10 for row in rows)
    assert len(created) == 3
    assert all(not window.messages and not window.frames for window in created)
    assert all(window.decoded_bytes == 0 for window in created)


def write_cameras(
    path: str,
    log_times: Dict[str, List[int]],
    packets: Optional[Dict[str, List[bytes]]] = None,
) -> None:
    """Write an H.264 stream on each topic of ``log_times``, a frame per time.

    ``packets`` gives the payloads of the topics it names instead.
    """
    from mcap.writer import CompressionType, Writer

    with open(path, "wb") as stream:
        writer = Writer(stream, chunk_size=1 << 12, compression=CompressionType.ZSTD)
        writer.start(profile="", library="ray-test")
        schema_id = writer.register_schema(
            name="foxglove.CompressedVideo", encoding="ros2msg", data=b"video\n"
        )
        messages = []
        for topic, times in log_times.items():
            channel_id = writer.register_channel(
                schema_id=schema_id, topic=topic, message_encoding="cdr"
            )
            payloads = (packets or {}).get(topic) or encode_h264(len(times))
            messages += [(t, channel_id, i, payloads[i]) for i, t in enumerate(times)]
        for log_time, channel_id, sequence, payload in sorted(messages):
            writer.add_message(
                channel_id=channel_id,
                log_time=log_time,
                publish_time=log_time,
                data=payload,
                sequence=sequence,
            )
        writer.finish()


@pytest.fixture
def held_windows(monkeypatch):
    """How many filled windows the task still holds after each emit.

        windows   [w0][w1][w2][w3][w4]
        emitted   [w0][w1]
        held                [w2][w3]     filled, not emitted yet: 2

    A window is emitted once it is complete, so this stays small.
    """
    held = []

    class Tracking(mcap_decoded_windows._PendingWindows):
        def ready_rows(self, up_to):
            yield from super().ready_rows(up_to)
            windows = self._windows[self._next :]
            held.append(sum(1 for w in windows if w.messages or w.frames))

    monkeypatch.setattr(mcap_decoded_windows, "_PendingWindows", Tracking)
    return held


@pytest.mark.parametrize(
    "b_frames",
    [list(range(20, 30)), list(range(5)) + list(range(25, 30))],
    ids=["starts-late", "pauses"],
)
def test_a_silent_camera_does_not_hold_windows_back(tmp_path, held_windows, b_frames):
    """While ``/b`` has no frames, before it starts or in a pause, the windows
    ``/a`` fills are emitted rather than held until ``/b``'s next frame."""
    path = os.path.join(tmp_path, "cameras.mcap")
    tenth = SECOND // 10
    write_cameras(
        path,
        {"/a": [i * tenth for i in range(30)], "/b": [i * tenth for i in b_frames]},
    )
    datasource = window_datasource(path, length_s=0.5)

    rows = read_rows(datasource, list_manifests(datasource))

    assert sum(len(row["frame_times:/a"]) for row in rows) == 30
    assert sum(len(row["frame_times:/b"]) for row in rows) == len(b_frames)
    assert max(held_windows) <= 2


def test_a_rejected_frame_does_not_hold_windows_back(tmp_path, held_windows):
    """``/b`` pauses right after a frame its decoder rejects. That frame never
    comes out, so the windows ``/a`` fills in the pause are emitted rather than
    held until ``/b``'s next frame."""
    tenth = SECOND // 10
    b_times = [i * tenth for i in list(range(6)) + list(range(25, 30))]
    # Frame 5 is rejected, and a new stream opens on a keyframe after the pause.
    b_packets = encode_h264(5) + [H264_PFRAME] + encode_h264(5)
    path = os.path.join(tmp_path, "cameras.mcap")
    write_cameras(
        path,
        {"/a": [i * tenth for i in range(30)], "/b": b_times},
        packets={"/b": b_packets},
    )
    datasource = window_datasource(path, length_s=0.5)

    rows = read_rows(datasource, list_manifests(datasource))

    assert sum(len(row["frame_times:/a"]) for row in rows) == 30
    b_frame_times = [t for row in rows for t in row["frame_times:/b"]]
    assert b_frame_times == b_times[:5] + b_times[6:]
    assert max(held_windows) <= 2


def test_cold_channel_warns_once_per_topic(tmp_path, monkeypatch, caplog):
    """A channel with no keyframe within the look-back cap warns once per topic,
    naming the topic and the cap, for message and window rows. A stream that
    begins mid-GOP at the start of the recording does not warn."""
    from ray.util.debug import reset_log_once

    path = os.path.join(tmp_path, "coldwarn.mcap")
    channels = write_two_cameras(path, offset_frames=0)
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    owned = {("/a", i) for i in range(15, 30)}

    def cold_warnings():
        """The cold-channel warnings logged so far."""
        return [
            record.message
            for record in caplog.records
            if "skipped until the next keyframe" in record.message
        ]

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_decode.logger.addHandler(caplog.handler)
    try:
        # Message rows: the 100 ms lead-in of frames 15-29 holds no keyframe.
        reset_log_once("mcap_cold_channel:/a")
        datasource = MCAPDatasourceV2([path], topics=["/a"], video=VideoOptions())
        part, _ = manifest_for_frames(datasource, path, channels, 0, owned)
        rows = read_rows(datasource, [part])
        assert [r["sequence"] for r in rows] == list(range(20, 30))
        warnings = cold_warnings()
        assert len(warnings) == 1
        assert "'/a'" in warnings[0] and "0.1 s" in warnings[0]
        assert "RAY_DATA_MCAP_MAX_LEAD_IN_S" in warnings[0]

        # Window rows, same split: the window at 0.66 s has no keyframe in reach.
        caplog.clear()
        reset_log_once("mcap_cold_channel:/a")
        windows = MCAPDatasourceV2(
            [path],
            topics=["/a"],
            read_granularity="window",
            window=WindowSpec(length_s=WINDOW_S),
            video=VideoOptions(),
        )
        part, _ = manifest_for_frames(windows, path, channels, 0, owned)
        rows = read_rows(windows, [part])
        assert [row["window_start"] for row in rows] == [2 * WINDOW_NS]
        assert len(rows[0]["frame_times:/a"]) == 10
        assert len(cold_warnings()) == 1

        # A stream that opens on a P-frame has nothing to look back into.
        caplog.clear()
        reset_log_once("mcap_cold_channel:/camera")
        head = os.path.join(tmp_path, "midgop_start.mcap")
        write_payloads(
            head, encode_h264(30)[5:], schema_name="foxglove.CompressedVideo"
        )
        from_start = MCAPDatasourceV2([head], video=VideoOptions())
        rows = read_rows(from_start, list_manifests(from_start))
        assert [r["sequence"] for r in rows] == list(range(5, 25))
        assert cold_warnings() == []
    finally:
        mcap_decode.logger.removeHandler(caplog.handler)


def test_empty_window_frames_keep_the_frame_shape(tmp_path):
    """A window with messages but no kept frame still gets a
    ``(0, height, width, 3)`` tensor, sized from ``resize`` or from an earlier
    frame in the task."""
    path = os.path.join(tmp_path, "sparse.mcap")
    write_two_cameras_and_an_imu(path)

    for video, shape in (
        (VideoOptions(fps=1), (HEIGHT, WIDTH)),
        (VideoOptions(fps=1, resize=HALF_SIZE), HALF_SIZE),
    ):
        datasource = MCAPDatasourceV2(
            [path],
            topics=["/cam_a", "/imu"],
            read_granularity="window",
            window=WindowSpec(length_s=WINDOW_S),
            video=video,
        )
        rows = read_rows(datasource, list_manifests(datasource))
        rows = by_window(rows)
        assert sorted(rows) == WINDOW_STARTS
        height, width = shape
        assert rows[0]["frames:/cam_a"].shape == (1, height, width, 3)
        for start in (WINDOW_NS, 2 * WINDOW_NS):
            assert rows[start]["frames:/cam_a"].shape == (0, height, width, 3)
            assert rows[start]["frame_times:/cam_a"] == []
            assert rows[start]["num_messages"] > 0


def test_decoded_windows_are_counted_without_their_frame_columns(h264_file):
    """In a video-only read, a projection without the frame columns, as in
    ``count()``, still yields every window that holds a frame."""
    datasource = window_datasource(h264_file, length_s=WINDOW_S)
    (manifest,) = list_manifests(datasource)

    for columns in (["window_start"], ["path", "window_start", "window_end"]):
        rows = read_rows(datasource, split_manifest(manifest, 3), columns=columns)
        assert sorted(row["window_start"] for row in rows) == WINDOW_STARTS
        assert all(set(row) == set(columns) for row in rows)


def test_thinned_frames_move_a_window_channel_on():
    """A window channel moves past the frames ``fps`` thinned out, so a window
    is not held until the next kept frame."""
    from mcap.records import Message

    # One frame per 10 s: of 30 frames 33 ms apart, only the first is kept.
    thinner = FrameThinner(10 * SECOND)
    decoder = FrameDecoder(VideoCodec.H264, thinner=thinner)
    channel = mcap_decoded_windows._WindowChannel(
        topic="/camera",
        codec=VideoCodec.H264,
        thinner=thinner,
        decoder=decoder,
        remaining=30,
        decodable=True,
    )
    kept = []

    for i, payload in enumerate(encode_h264(30)):
        message = Message(
            channel_id=1,
            sequence=i,
            log_time=i * FRAME_NS,
            publish_time=i * FRAME_NS,
            data=payload,
        )
        kept += channel.feed(message)
    decoder.close()

    assert [log_time for log_time, _ in kept] == [0]
    assert channel.released is not None and channel.released > 0
    assert channel.past(channel.released)


def record_codec_contexts(monkeypatch):
    """Record every codec context built, each noting whether it was drained."""
    contexts = []
    real_context = mcap_decode._codec_context

    class RecordingContext:
        """Wraps a codec context and notes when it is drained."""

        def __init__(self, context):
            self.context = context
            self.drained = False

        def decode(self, packet):
            if packet is None:
                self.drained = True
            return self.context.decode(packet)

    def recording_context(codec):
        contexts.append(RecordingContext(real_context(codec)))
        return contexts[-1]

    monkeypatch.setattr(mcap_decode, "_codec_context", recording_context)
    return contexts


@pytest.mark.parametrize("granularity", ["message", "window"])
def test_a_read_that_stops_early_drains_its_decoders(
    h264_file, monkeypatch, granularity
):
    """A read that stops early drains every codec context before dropping it.

    Freeing a libdav1d (AV1) context that still holds frames can hang the
    process.
    """
    contexts = record_codec_contexts(monkeypatch)
    if granularity == "message":
        datasource = MCAPDatasourceV2([h264_file], video=VideoOptions())
    else:
        datasource = window_datasource(h264_file, length_s=0.1)
    scanner = scanner_for(datasource)
    scanner = dataclasses.replace(scanner, target_block_size=1)
    manifest = list_manifests(datasource)[0]

    tables = scanner.create_reader().read(manifest)
    next(tables)
    tables.close()

    assert contexts
    assert all(context.drained for context in contexts)


def test_a_window_task_that_fails_while_priming_drains_its_decoders(
    tmp_path, monkeypatch
):
    """A window task whose second camera fails to prime drains both decoders."""

    class PrimingFailed(Exception):
        """Raised in place of priming the second camera."""

    path = os.path.join(tmp_path, "two_cameras.mcap")
    write_two_cameras(path, offset_frames=0)
    datasource = window_datasource(path, length_s=0.1)
    scanner = scanner_for(datasource)
    real_prime = mcap_decoded_windows.DecodedWindowRows._prime_window_channel
    primed = []

    def fail_second_prime(self, state, *args):
        primed.append(state.topic)
        if len(primed) == 2:
            raise PrimingFailed(state.topic)
        return real_prime(self, state, *args)

    contexts = record_codec_contexts(monkeypatch)
    monkeypatch.setattr(
        mcap_decoded_windows.DecodedWindowRows,
        "_prime_window_channel",
        fail_second_prime,
    )

    with pytest.raises(PrimingFailed):
        for _ in scanner.create_reader().read(list_manifests(datasource)[0]):
            pass

    assert len(contexts) == 2
    assert all(context.drained for context in contexts)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
