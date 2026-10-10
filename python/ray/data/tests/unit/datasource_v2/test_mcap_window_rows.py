"""Unit tests for MCAP window rows: windows of messages with a video lead-in.

Most tests read the ``recording`` fixture, whose docstring draws its timeline.
No Ray cluster: the indexer, scanner and reader are driven directly.
"""

import importlib.util
import os

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
    WindowSpec,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    CAM_FPS,
    CAM_FRAMES,
    CAM_SCHEMA,
    DURATION_S,
    GOP,
    H264_KEYFRAME,
    IMU_HZ,
    LEAD_IN_FRAMES,
    MID_GOP_NS,
    SECOND,
    VP9_KEYFRAME,
    VP9_PFRAME,
    cam_payload,
    frame_time,
    list_manifests,
    read_rows,
    write_recording,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)


def test_coarse_row_ids_change_with_the_options_that_change_lead_in(
    recording, monkeypatch
):
    """Listing video topics or changing the look-back cap changes what a window row
    holds, so it changes the row's id. The defaults keep the selection digest."""

    def first_row_id(**kwargs):
        datasource = MCAPDatasourceV2(
            [recording],
            read_granularity="window",
            window=WindowSpec(length_s=1.0),
            include_row_id=True,
            **kwargs,
        )
        rows = read_rows(datasource, list_manifests(datasource))
        return rows[0]["row_id"], datasource.selection.digest()

    row_id, digest = first_row_id()
    assert row_id.endswith(f"@{digest}")
    listed, _ = first_row_id(video_topics=["/imu"])
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.3")
    capped, _ = first_row_id()
    assert len({row_id, listed, capped}) == 3


def test_selection_digest_is_stable_and_selection_sensitive():
    """The selection digest ignores the order of ``topics`` and changes with the
    topics or the time range."""
    a = MCAPSelection.create(["/b", "/a"], None, None).digest()
    assert a == MCAPSelection.create(["/a", "/b"], None, None).digest()
    assert len(a) == 8
    assert a != MCAPSelection.create(["/a"], None, None).digest()
    assert a != MCAPSelection.create(["/a", "/b"], TimeRange(1, 2), None).digest()
    assert MCAPSelection().digest() == MCAPSelection.create(None, None, None).digest()


def test_window_rows_carry_the_lead_in_of_video_topics(recording):
    """A window opening mid-GOP carries the camera frames from the keyframe before
    it as lead-in, still encoded, ahead of its own messages."""
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
        include_row_id=True,
    )

    rows = read_rows(datasource, list_manifests(datasource))

    starts = [row["window_start"] for row in rows]
    assert starts == [k * SECOND + MID_GOP_NS for k in range(-1, 5)]

    first = rows[0]  # [-0.45 s, 0.55 s): nothing precedes the recording
    assert first["num_lead_in"] == 0
    assert first["window_end"] == MID_GOP_NS

    second = rows[1]  # [0.55 s, 1.55 s)
    assert second["num_lead_in"] == LEAD_IN_FRAMES
    lead_times = second["log_time"][: second["num_lead_in"]]
    assert lead_times == [frame_time(k) for k in range(LEAD_IN_FRAMES)]
    assert all(topic == "/cam" for topic in second["topic"][:LEAD_IN_FRAMES])
    body_times = second["log_time"][second["num_lead_in"] :]
    assert body_times[0] >= second["window_start"]
    assert body_times[-1] < second["window_end"]
    assert body_times == sorted(body_times)
    assert second["num_messages"] == LEAD_IN_FRAMES + CAM_FPS + IMU_HZ
    assert second["row_id"] == (
        f"{recording}#[550000000,1550000000)@{datasource.selection.digest()}"
    )
    channels = {c["topic"]: c for c in second["channels"]}
    assert set(channels) == {"/cam", "/imu"}
    assert channels["/cam"]["schema_name"] == CAM_SCHEMA
    assert second["data"][0].startswith(H264_KEYFRAME)


def test_window_opening_on_a_keyframe_has_no_lead_in(recording):
    """Windows opening on the keyframe at each whole second carry no lead-in."""
    datasource = MCAPDatasourceV2(
        [recording], read_granularity="window", window=WindowSpec(length_s=1.0)
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["window_start"] for row in rows] == [
        k * SECOND for k in range(DURATION_S)
    ]
    assert all(row["num_lead_in"] == 0 for row in rows)
    assert all(row["num_messages"] == CAM_FPS + IMU_HZ for row in rows)


def test_window_without_a_camera_frame_has_no_lead_in(recording):
    """A window holding no message of a video channel gets no lead-in for it.

    In 50 ms windows the 10 fps camera fills every other window and the 50 Hz
    IMU every one.
    """
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=0.05, anchor=25_000_000),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    with_cam = [r for r in rows if "/cam" in r["topic"][r["num_lead_in"] :]]
    without_cam = [r for r in rows if "/cam" not in r["topic"][r["num_lead_in"] :]]
    assert len(with_cam) == CAM_FRAMES and len(without_cam) == 51
    assert all(r["num_lead_in"] == 0 for r in without_cam)
    # Every window whose frame is not a keyframe reaches back to one.
    assert sum(r["num_lead_in"] > 0 for r in with_cam) == CAM_FRAMES - CAM_FRAMES // GOP


def test_window_rows_are_emitted_exactly_once_across_tasks(recording):
    """Three tasks splitting the chunk rows emit each window once, with the
    messages and lead-in of a whole-file read."""
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
    """``topics`` and ``time_range`` limit both the windows and their messages."""
    datasource = MCAPDatasourceV2(
        [recording],
        topics=["/imu"],
        time_range=TimeRange(start_time=SECOND, end_time=3 * SECOND),
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor="epoch"),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["window_start"] for row in rows] == [SECOND, 2 * SECOND]
    for row in rows:
        assert set(row["topic"]) == {"/imu"}
        assert row["num_lead_in"] == 0
        assert row["num_messages"] == IMU_HZ
        assert all(SECOND <= t < 3 * SECOND for t in row["log_time"])


def test_window_lead_in_reaches_before_the_time_range(recording):
    """A range starting mid-GOP: the first window's lead-in comes from before the
    range, in chunks the listing left out."""
    datasource = MCAPDatasourceV2(
        [recording],
        time_range=TimeRange(start_time=SECOND + MID_GOP_NS, end_time=3 * SECOND),
        read_granularity="window",
        window=WindowSpec(length_s=1.0),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["window_start"] for row in rows] == [
        SECOND + MID_GOP_NS,
        2 * SECOND + MID_GOP_NS,
    ]
    first = rows[0]
    assert first["num_lead_in"] == LEAD_IN_FRAMES
    assert first["log_time"][:LEAD_IN_FRAMES] == [
        SECOND + frame_time(k) for k in range(LEAD_IN_FRAMES)
    ]
    assert first["data"][0].startswith(H264_KEYFRAME)
    body_times = first["log_time"][LEAD_IN_FRAMES:]
    assert all(SECOND + MID_GOP_NS <= t < 2 * SECOND + MID_GOP_NS for t in body_times)
    assert len(body_times) == CAM_FPS + IMU_HZ


def test_window_opening_before_the_range_keeps_the_keyframe(recording):
    """An epoch-anchored window opens at 1.0 s but the range starts at 1.25 s:
    the keyframe at 1.0 s is outside the range yet needed by the frames inside."""
    datasource = MCAPDatasourceV2(
        [recording],
        time_range=TimeRange(start_time=SECOND + 250_000_000, end_time=3 * SECOND),
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor="epoch"),
    )

    rows = read_rows(datasource, list_manifests(datasource))

    assert [row["window_start"] for row in rows] == [SECOND, 2 * SECOND]
    first = rows[0]
    assert first["num_lead_in"] == 3
    assert first["log_time"][:3] == [SECOND + frame_time(k) for k in range(3)]
    assert first["data"][0].startswith(H264_KEYFRAME)
    assert all(t >= SECOND + 250_000_000 for t in first["log_time"][3:])
    # The second window opens on the keyframe at 2.0 s, inside the range.
    assert rows[1]["num_lead_in"] == 0


def write_two_topics(path, *, index):
    """A 2.5 s file whose ``/late`` topic starts 0.4 s after ``/early``."""
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
            t = i * SECOND // 10
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

    assert (
        windows[True]
        == windows[False]
        == [(0, SECOND), (SECOND, 2 * SECOND), (2 * SECOND, 3 * SECOND)]
    )


def test_coarse_rows_scan_a_file_whose_summary_lists_no_channels(
    recording, tmp_path, monkeypatch
):
    """A file whose summary lists no channels is listed as one whole-file row and
    scanned, giving the same window and topic rows as an unindexed file."""
    from mcap.reader import SeekingReader

    unindexed = os.path.join(tmp_path, "noindex.mcap")
    write_recording(unindexed, index=False)
    real_get_summary = SeekingReader.get_summary

    def without_channels(self):
        summary = real_get_summary(self)
        if summary is not None:
            summary.channels = {}
        return summary

    for granularity in ("window", "topic"):
        window = WindowSpec(length_s=1.0) if granularity == "window" else None
        reference = MCAPDatasourceV2(
            [unindexed], read_granularity=granularity, window=window
        )
        expected = read_rows(reference, list_manifests(reference))
        monkeypatch.setattr(SeekingReader, "get_summary", without_channels)
        try:
            datasource = MCAPDatasourceV2(
                [recording], read_granularity=granularity, window=window
            )
            (manifest,) = list_manifests(datasource)
            assert manifest.file_chunk_metadatas[0] is None
            rows = read_rows(datasource, [manifest])
        finally:
            monkeypatch.setattr(SeekingReader, "get_summary", real_get_summary)
        key = "window_start" if granularity == "window" else "topic"
        assert [(r[key], r["num_messages"], r["num_lead_in"]) for r in rows] == [
            (r[key], r["num_messages"], r["num_lead_in"]) for r in expected
        ]
        assert rows


@pytest.mark.parametrize("listed", [None, ["/cam"]], ids=["unlisted", "listed"])
def test_custom_schema_topic_is_video_only_when_listed(tmp_path, listed):
    """A topic whose schema read_mcap does not know gets no lead-in and a topic
    row from its first message, unless ``video_topics`` lists it. Listed, its
    codec is told from its bytes."""
    path = os.path.join(tmp_path, "custom.mcap")
    write_recording(path, cam_schema="my_msgs/msg/Frame", first_keyframe=2)
    windows = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
        video_topics=listed,
    )

    rows = read_rows(windows, list_manifests(windows))

    # The window opening at 0.55 s reaches back to the keyframe at 0.2 s.
    assert rows[1]["num_lead_in"] == (4 if listed else 0)
    if listed:
        assert rows[1]["data"][0].startswith(H264_KEYFRAME)

    topics = MCAPDatasourceV2(
        [path], read_granularity="topic", topics=["/cam"], video_topics=listed
    )
    (row,) = read_rows(topics, list_manifests(topics))
    assert row["log_time"][0] == (frame_time(2) if listed else 0)


def test_listed_vp9_topic_finds_its_codec_past_inter_frames(tmp_path):
    """A listed VP9 topic that opens on inter frames takes its codec from its first
    key frame, so its windows and topic rows start on a keyframe, also when
    ``time_range`` holds only inter frames."""
    path = os.path.join(tmp_path, "custom_vp9.mcap")
    write_recording(
        path,
        cam_schema="my_msgs/msg/Frame",
        cam_payloads=[
            VP9_KEYFRAME if f % GOP == 5 else VP9_PFRAME for f in range(CAM_FRAMES)
        ],
    )
    windows = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
        video_topics=["/cam"],
    )

    rows = read_rows(windows, list_manifests(windows))

    # The key frame at 1.5 s is the only lead-in entry before the 1.55 s start.
    assert rows[1]["num_lead_in"] == 1
    assert rows[1]["data"][0] == VP9_KEYFRAME

    for time_range in (None, TimeRange(frame_time(6), frame_time(10))):
        topics = MCAPDatasourceV2(
            [path],
            read_granularity="topic",
            topics=["/cam"],
            time_range=time_range,
            video_topics=["/cam"],
        )
        (row,) = read_rows(topics, list_manifests(topics))
        assert row["log_time"][0] == frame_time(5)
        assert row["data"][0] == VP9_KEYFRAME
        assert row["num_lead_in"] == (0 if time_range is None else 1)


def test_video_topics_must_be_selected(recording):
    """With ``topics``, every ``video_topics`` entry must be one of them. An empty
    list means none."""
    with pytest.raises(ValueError, match=r"video_topics.*\['/other'\]"):
        MCAPDatasourceV2([recording], topics=["/cam"], video_topics=["/cam", "/other"])
    assert MCAPDatasourceV2(
        [recording], topics=["/cam"], video_topics=["/cam"]
    )._video_topics == {"/cam"}
    assert MCAPDatasourceV2([recording], video_topics=["/elsewhere"])._video_topics == {
        "/elsewhere"
    }
    assert MCAPDatasourceV2(
        [recording], topics=["/cam"], video_topics=[]
    )._video_topics == (frozenset())


def test_window_lead_in_when_the_summary_omits_schema_records(recording, monkeypatch):
    """A summary that lists a channel without its schema record still gives every
    task the lead-in: the look-back is read in case a chunk names a video
    schema, so the rows do not depend on how the read is split."""
    from mcap.reader import SeekingReader

    real_get_summary = SeekingReader.get_summary

    def without_schemas(self):
        summary = real_get_summary(self)
        if summary is not None:
            summary.schemas = {}
        return summary

    monkeypatch.setattr(SeekingReader, "get_summary", without_schemas)
    datasource = MCAPDatasourceV2(
        [recording],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
    )
    (manifest,) = list_manifests(datasource)
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    n = len(manifest)
    parts = [FileManifest(block.slice(k, 1)) for k in range(n)]

    rows = sorted(read_rows(datasource, parts), key=lambda r: r["window_start"])

    assert [row["num_lead_in"] for row in rows] == [0] + [LEAD_IN_FRAMES] * DURATION_S


def test_unparseable_video_codec_carries_the_capped_lead_in(
    tmp_path, monkeypatch, caplog
):
    """A video topic whose format and bytes name no codec logs a warning, its
    window rows carry the whole look-back span as lead-in, and its topic row
    keeps every frame."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_lead_in
    from ray.util.debug import reset_log_once

    path = os.path.join(tmp_path, "opaque.mcap")
    write_recording(path, cam_payloads=[b"\x01\x02\x03\x04" * 8] * CAM_FRAMES)
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.25")
    datasource = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
    )
    reset_log_once("mcap_capped_lead_in:/cam")

    # Ray's loggers do not propagate to the root logger caplog listens on.
    mcap_lead_in.logger.addHandler(caplog.handler)
    try:
        rows = read_rows(datasource, list_manifests(datasource))
    finally:
        mcap_lead_in.logger.removeHandler(caplog.handler)

    assert any(
        "'/cam'" in record.message and "look-back" in record.message
        for record in caplog.records
    )
    # The 0.25 s look-back holds the frames at 0.3, 0.4 and 0.5 s.
    assert rows[1]["num_lead_in"] == 3
    assert rows[1]["topic"][:3] == ["/cam"] * 3

    topics = MCAPDatasourceV2([path], read_granularity="topic", topics=["/cam"])
    rows = read_rows(topics, list_manifests(topics))
    assert rows[0]["num_messages"] == CAM_FRAMES  # no keyframe to start at


def test_vp9_lead_in_is_found_past_inter_frames(tmp_path, caplog):
    """VP9 inter frames name no codec, but the codec is found at a later key frame,
    so window and topic rows starting mid-GOP get an exact lead-in and no warning."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_lead_in

    path = os.path.join(tmp_path, "vp9.mcap")
    write_recording(
        path,
        cam_payloads=[
            VP9_KEYFRAME if f % GOP == 0 else VP9_PFRAME for f in range(CAM_FRAMES)
        ],
    )

    mcap_lead_in.logger.addHandler(caplog.handler)
    try:
        windows = MCAPDatasourceV2(
            [path],
            read_granularity="window",
            window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
        )
        rows = read_rows(windows, list_manifests(windows))
        assert rows[1]["num_lead_in"] == LEAD_IN_FRAMES
        assert rows[1]["data"][0] == VP9_KEYFRAME

        topics = MCAPDatasourceV2(
            [path],
            topics=["/cam"],
            time_range=TimeRange(start_time=SECOND + MID_GOP_NS, end_time=3 * SECOND),
            read_granularity="topic",
        )
        (row,) = read_rows(topics, list_manifests(topics))
        assert row["num_lead_in"] == LEAD_IN_FRAMES
        assert row["data"][0] == VP9_KEYFRAME
    finally:
        mcap_lead_in.logger.removeHandler(caplog.handler)

    assert not any("look-back" in record.message for record in caplog.records)


def test_max_lead_in_bounds_the_keyframe_search(tmp_path, monkeypatch):
    """The look-back cap bounds the search for a keyframe: with the only keyframe
    out of reach no window gets a lead-in, and in reach every window does."""
    path = os.path.join(tmp_path, "sparse.mcap")
    # Only one keyframe, at the very start.
    write_recording(
        path,
        cam_payloads=[
            cam_payload(0) if f == 0 else cam_payload(1) for f in range(CAM_FRAMES)
        ],
    )
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.3")
    datasource = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
    )
    rows = read_rows(datasource, list_manifests(datasource))
    assert all(row["num_lead_in"] == 0 for row in rows)

    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "10")
    reaching = MCAPDatasourceV2(
        [path],
        read_granularity="window",
        window=WindowSpec(length_s=1.0, anchor=MID_GOP_NS),
    )
    rows = read_rows(reaching, list_manifests(reaching))
    # Every window reaches back to the single keyframe at 0 s.
    assert rows[1]["num_lead_in"] == LEAD_IN_FRAMES
    assert rows[2]["num_lead_in"] == CAM_FPS + LEAD_IN_FRAMES


def test_unindexed_file_still_yields_windows(tmp_path):
    """A file without a chunk index is listed whole and still cut into windows."""
    path = os.path.join(tmp_path, "noindex.mcap")
    write_recording(path, index=False)
    datasource = MCAPDatasourceV2(
        [path], read_granularity="window", window=WindowSpec(length_s=1.0)
    )

    manifests = list_manifests(datasource)
    assert len(manifests) == 1 and manifests[0].file_chunk_metadatas[0] is None
    rows = read_rows(datasource, manifests)

    assert [row["window_start"] for row in rows] == [
        k * SECOND for k in range(DURATION_S)
    ]
    assert all(row["num_messages"] == CAM_FPS + IMU_HZ for row in rows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
