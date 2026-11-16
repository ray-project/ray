"""Unit tests for decoded sample rows: a camera topic's steps as RGB frames.

The tests read 30 H.264 frames, a keyframe every 10, with ``video`` and compare
each decoded frame with the same frame from a decode of the whole stream in
order. They drive the scanner and reader as a read task does, with no Ray
cluster.
"""

import importlib.util
import logging
import os
import sys
from typing import Dict

import numpy as np
import pytest

from ray.data._internal.datasource_v2.formats.mcap import (
    mcap_decoded_samples,
    mcap_sample_tasks,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameDecoder
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import VideoOptions
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import VideoCodec
from ray.data._internal.tensor_extensions.arrow import FixedShapeTensorFormat
from ray.data.context import DataContext
from ray.data.datasource.mcap import match, steps
from ray.data.read_api import _estimate_samples_per_task
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    FRAME_NS,
    FRAME_SHAPE,
    H264_FILE_FRAMES,
    HALF_SIZE,
    PARAMETER_SET_TYPES,
    START_CODE,
    cdr_video,
    encode_h264,
    encode_vp9,
    length_prefixed,
    nal_type,
    nal_units,
    parameter_sets_once,
    read_samples,
    without_parameter_sets,
    write_payloads,
)
from ray.util.debug import reset_log_once

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None or importlib.util.find_spec("av") is None,
    reason="needs mcap and av",
)

TOPIC = "/camera"
CLIP_END = H264_FILE_FRAMES * FRAME_NS  # the clip holds every frame


@pytest.fixture(scope="module")
def packets():
    """The 30 H.264 access units of the camera; keyframes are 0, 10 and 20."""
    return encode_h264(H264_FILE_FRAMES)


@pytest.fixture(scope="module")
def reference(packets):
    """Every frame of the stream, decoded in order from its start, by log time."""
    from mcap.records import Message

    decoder = FrameDecoder(VideoCodec.H264)
    frames: Dict[int, np.ndarray] = {}
    for i, packet in enumerate(packets):
        message = Message(
            channel_id=1, sequence=i, log_time=i * FRAME_NS, publish_time=0, data=packet
        )
        frames.update(dict(decoder.decode(message)))
    frames.update(dict(decoder.flush()))
    return frames


def write_camera(tmp_path, payloads, schema_name="foxglove.CompressedVideo"):
    """Write ``payloads`` as the camera, frame ``i`` at ``i * FRAME_NS``, in
    ~400-byte chunks so chunk boundaries fall inside GOPs."""
    path = os.path.join(tmp_path, "camera.mcap")
    write_payloads(path, payloads, schema_name=schema_name, chunk_size=400)
    return path


def sample(path, frame, sample_id="s"):
    """A sample anchored at camera frame ``frame``, its clip the whole stream."""
    return {
        "sample_id": sample_id,
        "files": [path],
        "clip_start": 0,
        "clip_end": CLIP_END,
        "anchor": frame * FRAME_NS,
    }


def read(path, frames, spec, **video):
    """The rows of samples anchored at ``frames``, the camera decoded."""
    samples = [sample(path, f, f"s{f}") for f in frames]
    return read_samples(samples, {TOPIC: spec}, video=VideoOptions(**video))


def step_frames(row):
    """The frame number of each step, from its log time."""
    return [t // FRAME_NS for t in row[f"times:{TOPIC}"]]


@pytest.fixture
def fed(monkeypatch):
    """The log times of the messages fed to decoders, in feeding order."""
    log = []
    real = FrameDecoder.decode

    def decode(self, message, wanted=None):
        log.append(message.log_time // FRAME_NS)
        return real(self, message, wanted)

    monkeypatch.setattr(FrameDecoder, "decode", decode)
    return log


@pytest.mark.parametrize(
    "spec, anchors, expected",
    [
        # 10 steps a second of a camera logged every 33 ms: the frames nearest
        # 0.1 s apart up to the anchor, back across the keyframe at frame 10.
        (TopicSpec(steps.before(frames=3, fps=10)), [15], [[9, 12, 15]]),
        (TopicSpec(steps.last(4)), [12, 27], [[9, 10, 11, 12], [24, 25, 26, 27]]),
    ],
)
def test_frames_match_a_decode_of_the_whole_stream(
    tmp_path, packets, reference, spec, anchors, expected
):
    """Each step's frame is the frame a decode of the whole stream gives,
    whichever keyframe its decode starts at."""
    path = write_camera(tmp_path, packets)

    rows = read(path, anchors, spec)

    assert [step_frames(row) for row in rows] == expected
    for row in rows:
        assert np.asarray(row[f"frames:{TOPIC}"]).shape == (
            len(row[f"times:{TOPIC}"]),
            *FRAME_SHAPE,
        )
        for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
            assert np.array_equal(frame, reference[time])


def test_a_file_without_a_chunk_index_decodes_the_same_frames(
    tmp_path, packets, reference
):
    """A file without a chunk index is scanned, and the scan keeps every camera
    message: frame 9's decode starts at the keyframe at frame 0, far before
    the steps' reach."""
    from mcap.writer import IndexType

    path = os.path.join(tmp_path, "unindexed.mcap")
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        index_types=IndexType.NONE,
        use_chunking=False,
        use_statistics=False,
        use_summary_offsets=False,
    )

    (row,) = read(path, [15], TopicSpec(steps.before(frames=3, fps=10)))

    assert step_frames(row) == [9, 12, 15]
    for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
        assert np.array_equal(frame, reference[time])


def test_padded_steps_repeat_a_frame_or_are_black(tmp_path, packets, reference):
    """``edge`` repeats the nearest real frame, and ``zeros`` fills a black
    frame of the row's frame size."""
    path = write_camera(tmp_path, packets)
    # The steps of frame 1 at 10 fps reach 0.13 s before the stream starts.
    steps_before = steps.before(frames=3, fps=10)

    (edge,) = read(path, [1], TopicSpec(steps_before, pad="edge"))
    (zeros,) = read(path, [1], TopicSpec(steps_before, pad="zeros"))

    assert edge[f"is_pad:{TOPIC}"] == [True, True, False]
    assert all(np.array_equal(f, reference[FRAME_NS]) for f in edge[f"frames:{TOPIC}"])
    assert zeros[f"is_pad:{TOPIC}"] == [True, True, False]
    assert not np.asarray(zeros[f"frames:{TOPIC}"])[:2].any()
    assert np.array_equal(zeros[f"frames:{TOPIC}"][2], reference[FRAME_NS])


@pytest.mark.parametrize(
    "anchor, spec, lead_in_s, resize",
    [
        # The first two steps reach before the stream starts and are black.
        (1, TopicSpec(steps.before(frames=3, fps=10), pad="zeros"), None, None),
        # With no look-back, neither inter frame decodes, so ``edge`` has no
        # frame to repeat and both steps are black frames of ``resize``.
        (15, TopicSpec(steps.last(2)), "0", HALF_SIZE),
    ],
    ids=["zeros_before_the_stream", "edge_with_nothing_decoded"],
)
def test_black_frames_count_toward_the_row_limit(
    tmp_path, packets, monkeypatch, anchor, spec, lead_in_s, resize
):
    """A row whose frames, black ones included, pass
    ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails the read."""
    if lead_in_s is not None:
        monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", lead_in_s)
    path = write_camera(tmp_path, packets)
    # A read with no limit gives the size of the row's frames tensor.
    (row,) = read(path, [anchor], spec, resize=resize)
    assert True in row[f"is_pad:{TOPIC}"]
    limit = np.asarray(row[f"frames:{TOPIC}"]).nbytes - 1
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", str(limit))

    with pytest.raises(ValueError, match="RAY_DATA_MCAP_MAX_ROW_BYTES"):
        read(path, [anchor], spec, resize=resize)


# Anchors of three samples whose steps (``steps.last(3)``) meet: they take
# frames 11 to 15, after the keyframe at frame 10.
MEETING = [13, 14, 15]
# Anchors of two samples whose two steps each do not meet. No keyframe lies
# between the samples of BACK_TO_BACK; the one at frame 20 lies between those
# of ACROSS_A_KEYFRAME.
BACK_TO_BACK = [13, 16]
ACROSS_A_KEYFRAME = [13, 22]


@pytest.mark.parametrize(
    "anchors, last, resize, block_frames, expected_fed",
    [
        # Fed from the keyframe at or before the first frame a step takes, not
        # from the stream's start.
        ([15], 2, None, None, list(range(10, 16))),
        # Samples whose steps meet share one decode. With ``resize``, the frame
        # size is known before anything decodes.
        (MEETING, 3, HALF_SIZE, None, list(range(10, 16))),
        # A block of two frames gives a group two blocks, four frames. Frames
        # 11-14 fit, so the sample at 15 starts a new group, decoded afresh
        # from the keyframe at frame 10.
        (MEETING, 3, HALF_SIZE, 2, [10, 11, 12, 13, 14] + list(range(10, 16))),
        # Until a frame decodes, the frame size is unknown, so the first
        # sample is a group of its own.
        (MEETING, 3, None, None, [10, 11, 12, 13] + list(range(10, 16))),
        # No keyframe between two samples: one running decoder feeds each
        # frame from the keyframe on once.
        (BACK_TO_BACK, 2, None, None, list(range(10, 17))),
        # A keyframe between them: the second is decoded from that keyframe.
        (ACROSS_A_KEYFRAME, 2, None, None, [10, 11, 12, 13, 20, 21, 22]),
    ],
    ids=[
        "from_the_keyframe",
        "group_decodes_once",
        "group_ends_at_the_budget",
        "frame_size_unknown",
        "running_decoder",
        "keyframe_between",
    ],
)
def test_messages_fed_to_the_decoder(
    tmp_path,
    packets,
    reference,
    fed,
    monkeypatch,
    anchors,
    last,
    resize,
    block_frames,
    expected_fed,
):
    """A camera is decoded once per group of samples whose steps meet, from the
    keyframe before the group's first frame, or on from a decoder still
    running. A group ends before its decoded frames would pass twice
    ``target_max_block_size``. Every frame matches a decode of the whole
    stream."""
    path = write_camera(tmp_path, packets)
    if block_frames is not None:
        height, width = resize
        monkeypatch.setattr(
            DataContext.get_current(),
            "target_max_block_size",
            block_frames * height * width * 3,
        )

    rows = read(path, anchors, TopicSpec(steps.last(last)), resize=resize)

    assert fed == expected_fed
    assert not any(any(row[f"is_pad:{TOPIC}"]) for row in rows)
    if resize is None:
        for row in rows:
            for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
                assert np.array_equal(frame, reference[time])


def pictures_by_message(packets):
    """The picture of each packet, by its index, from FFmpeg's decoder alone:
    the frames ``read`` should give, even when the decoder reorders them."""
    import av  # pyrefly: ignore[missing-import]

    context = av.CodecContext.create("h264", "r")
    pictures = {}
    for i, payload in enumerate(packets + [None]):
        packet = None if payload is None else av.Packet(payload)
        if packet is not None:
            packet.pts = i
        for frame in context.decode(packet):
            pictures[frame.pts] = frame.to_ndarray(format="rgb24")
    return pictures


def test_b_frames_keep_the_picture_of_their_message(tmp_path):
    """A decoder gives B-frames out of feeding order, so each frame is stamped
    with the log time of the message that held it: a step gets the picture of
    its own message."""
    packets = encode_h264(H264_FILE_FRAMES, b_frames=2)
    path = write_camera(tmp_path, packets)
    expected = pictures_by_message(packets)

    rows = read(path, [15, 25], TopicSpec(steps.last(4)))

    for row in rows:
        assert row[f"is_pad:{TOPIC}"] == [False] * 4
        for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
            assert np.array_equal(frame, expected[time // FRAME_NS])


@pytest.mark.parametrize(
    "resize, expected_fed",
    [
        # The row's size is known from ``resize``, so nothing is fed.
        (HALF_SIZE, []),
        # The first frame decoded, the keyframe, shows the row's size.
        (None, [10]),
    ],
    ids=["resize", "first_frame"],
)
def test_a_row_over_the_limit_fails_before_its_frames_decode(
    tmp_path, packets, fed, monkeypatch, resize, expected_fed
):
    """A row whose frames pass ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails as soon as
    their size is known, rather than after they decode."""
    path = write_camera(tmp_path, packets)
    height, width = resize or FRAME_SHAPE[:2]
    # Room for one of the row's two frames.
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", str(height * width * 3))

    with pytest.raises(ValueError, match="Sample 's15'.*RAY_DATA_MCAP_MAX_ROW_BYTES"):
        read(path, [15], TopicSpec(steps.last(2)), resize=resize)

    assert fed == expected_fed


def test_a_frame_size_change_within_a_row_names_the_sample(tmp_path):
    """Without ``resize``, a sample whose steps span a change of the camera's
    frame size fails, and the error names it."""
    # Ten frames of the default size, then ten of half the size.
    path = write_camera(
        tmp_path, encode_h264(10) + encode_h264(10, *reversed(HALF_SIZE))
    )

    with pytest.raises(ValueError, match="Sample 's11'.*changes frame size"):
        read(path, [11], TopicSpec(steps.last(4)))


def test_a_decoder_that_holds_a_frame_back_is_not_kept(
    tmp_path, packets, reference, fed, monkeypatch
):
    """A decoder that may still hold a frame after a sample's last step is
    flushed, as one whose codec delays its output would be: the next sample
    starts a fresh decoder and still gets the right frames."""
    path = write_camera(tmp_path, packets)
    decode = FrameDecoder.decode

    def lagging(self, message, wanted=None):
        yield from decode(self, message, wanted)
        # As if the frame of this message were still inside the codec.
        self.last_output = None

    monkeypatch.setattr(FrameDecoder, "decode", lagging)

    rows = read(path, BACK_TO_BACK, TopicSpec(steps.last(2)))

    assert fed == [10, 11, 12, 13, 10, 11, 12, 13, 14, 15, 16]
    for row in rows:
        for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
            assert np.array_equal(frame, reference[time])


def test_a_running_decoder_keeps_the_look_back_cap(tmp_path, packets, monkeypatch):
    """A sample whose keyframe lies past the look-back cap does not decode,
    even when the decoder that read the sample before it could go on: rows do
    not depend on which samples share a read task."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    path = write_camera(tmp_path, packets)
    spec = TopicSpec(steps.last(2), pad="zeros")

    together = read(path, BACK_TO_BACK, spec)
    (alone,) = read(path, BACK_TO_BACK[1:], spec)

    # The keyframe at frame 10 is within 0.1 s of frame 12, not of frame 15.
    assert together[0][f"is_pad:{TOPIC}"] == [False, False]
    assert together[1][f"is_pad:{TOPIC}"] == alone[f"is_pad:{TOPIC}"] == [True, True]


@pytest.mark.parametrize(
    "failing, anchors",
    [
        # Going on from the decoder kept running after the first sample.
        ("_fed_on", BACK_TO_BACK),
        # Looking back for the keyframe, after a fresh decoder is made.
        ("_look_back", [15]),
    ],
)
def test_a_failed_read_closes_every_decoder(
    tmp_path, packets, monkeypatch, failing, anchors
):
    """A read that fails while it reads the messages to feed still closes every
    decoder it made, the running one included."""
    path = write_camera(tmp_path, packets)
    made = []
    init = FrameDecoder.__init__

    def tracked(self, *args, **kwargs):
        init(self, *args, **kwargs)
        made.append(self)

    def fail(*args, **kwargs):
        raise OSError("chunk read failed")

    monkeypatch.setattr(FrameDecoder, "__init__", tracked)
    monkeypatch.setattr(mcap_decoded_samples.RunDecoder, failing, fail)

    with pytest.raises(OSError, match="chunk read failed"):
        read(path, anchors, TopicSpec(steps.last(2)))

    # close() drops the codec context; one left open can hang the worker on GC.
    assert made and all(decoder._context is None for decoder in made)


# Anchors of three samples of three steps each: each sample's first step is the
# previous sample's last, so the samples decode as one group.
OVERLAPPING = [12, 14, 16]


def test_a_group_keeps_each_samples_look_back_cap(tmp_path, packets, monkeypatch):
    """Samples whose steps meet decode together, yet each keeps only the frames
    a read of it alone decodes: rows do not depend on which samples share a
    read task."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    path = write_camera(tmp_path, packets)
    spec = TopicSpec(steps.last(3), pad="zeros")

    together = read(path, OVERLAPPING, spec)
    alone = [row for frame in OVERLAPPING for row in read(path, [frame], spec)]

    # The keyframe at frame 10 is within 0.1 s of frames 10 and 12, not of 14.
    expected = [[False] * 3, [False] * 3, [True] * 3]
    assert [row[f"is_pad:{TOPIC}"] for row in together] == expected
    assert [row[f"is_pad:{TOPIC}"] for row in alone] == expected


def test_pictures_at_one_log_time_leave_their_steps_empty(tmp_path, packets, reference):
    """Two pictures logged at one time cannot be told apart once decoded, so the
    steps that take them are padded rather than given the wrong picture."""
    times = [k * FRAME_NS for k in range(H264_FILE_FRAMES)]
    times[11] = times[10]  # frames 10 and 11 share a log time
    path = os.path.join(tmp_path, "camera.mcap")
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        log_times=times,
        chunk_size=400,
    )

    (row,) = read(path, [12], TopicSpec(steps.last(3), pad="zeros"))

    assert row[f"is_pad:{TOPIC}"] == [True, True, False]
    assert not np.asarray(row[f"frames:{TOPIC}"])[:2].any()
    assert np.array_equal(row[f"frames:{TOPIC}"][2], reference[12 * FRAME_NS])


def test_no_keyframe_in_reach_leaves_steps_empty(tmp_path, packets, monkeypatch):
    """A frame with no keyframe within the look-back cap does not decode: its
    step is padded, or fails the read with ``raise``."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    path = write_camera(tmp_path, packets)

    (row,) = read(path, [15], TopicSpec(steps.last(2), pad="zeros"))

    assert row[f"is_pad:{TOPIC}"] == [True, True]
    assert not np.asarray(row[f"frames:{TOPIC}"]).any()
    with pytest.raises(ValueError, match="does not decode"):
        read(path, [15], TopicSpec(steps.last(2), pad="raise"))


def test_parameter_sets_after_chunks_without_the_camera(tmp_path, packets, reference):
    """In a file whose chunks carry no message indexes, the stream start is the
    first chunk with a camera message, not the first chunk, which holds only
    another topic."""
    from mcap.writer import IndexType

    path = os.path.join(tmp_path, "camera.mcap")
    write_payloads(
        path,
        parameter_sets_once(packets, "h264"),
        schema_name="foxglove.CompressedVideo",
        chunk_size=400,
        # One message bigger than a chunk fills the first chunk on its own.
        extra_topic=("/imu", "sensor_msgs/msg/Imu", bytes(500)),
        extra_first=1,
        index_types=IndexType.CHUNK,
    )

    (row,) = read(path, [25], TopicSpec(steps.last(2)))

    assert row[f"is_pad:{TOPIC}"] == [False, False]
    for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
        assert np.array_equal(frame, reference[time])


def test_length_prefixed_payloads(tmp_path, packets, reference):
    """Length-prefixed (AVCC) payloads decode as their Annex-B form does. Their
    ``format`` field names the codec, which their bytes cannot."""
    path = write_camera(
        tmp_path,
        [cdr_video(length_prefixed(p), "h264") for p in packets],
        schema_name="foxglove_msgs/msg/CompressedVideo",
    )

    (row,) = read(path, [17], TopicSpec(steps.before(frames=2, fps=10)))

    for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
        assert np.array_equal(frame, reference[time])


def test_projection_without_frames_decodes_nothing(tmp_path, packets, monkeypatch):
    """A projection that drops ``frames:`` still gives the steps' times and
    padding flags, and decodes no frame."""
    path = write_camera(tmp_path, packets)
    monkeypatch.setattr(mcap_decoded_samples, "FrameDecoder", None)

    rows = read_samples(
        [sample(path, 15)],
        {TOPIC: TopicSpec(steps.last(2))},
        video=VideoOptions(),
        columns=[f"is_pad:{TOPIC}", f"times:{TOPIC}"],
    )

    assert rows == [
        {
            f"is_pad:{TOPIC}": [False, False],
            f"times:{TOPIC}": [14 * FRAME_NS, 15 * FRAME_NS],
        }
    ]


def test_projecting_away_one_cameras_frames_keeps_its_padding_flags(tmp_path, packets):
    """A projection that keeps one camera's frames and drops another's decodes
    only the first. The other camera's steps stay unpadded, as they do when every
    ``frames:`` column is dropped."""
    path = os.path.join(tmp_path, "two_cameras.mcap")
    # A second camera whose every frame is the first keyframe.
    write_payloads(
        path,
        packets,
        schema_name="foxglove.CompressedVideo",
        extra_topic=("/b", "foxglove.CompressedVideo", packets[0]),
    )
    spec = TopicSpec(steps.last(2), pad="raise")

    (row,) = read_samples(
        [sample(path, 15)],
        {TOPIC: spec, "/b": spec},
        video=VideoOptions(),
        columns=[f"frames:{TOPIC}", f"is_pad:{TOPIC}", "is_pad:/b"],
    )

    assert row[f"is_pad:{TOPIC}"] == [False, False]
    assert row["is_pad:/b"] == [False, False]


def test_vp9_without_a_format_finds_its_codec_from_the_look_back(tmp_path):
    """Only a VP9 keyframe names its codec, so a sample whose frames are all
    inter frames finds it from the keyframe its decoding starts at."""
    path = os.path.join(tmp_path, "vp9.mcap")
    # Bare VP9 frames under a schema read_mcap does not know, so no format field
    # names the codec. Keyframes are 0, 10 and 20.
    write_payloads(path, encode_vp9(H264_FILE_FRAMES), schema_name="custom/Video")

    (row,) = read_samples(
        [sample(path, 15)],
        {TOPIC: TopicSpec(steps.last(2))},
        video=VideoOptions(),
        video_topics=[TOPIC],
    )

    assert row[f"is_pad:{TOPIC}"] == [False, False]
    assert row[f"frames:{TOPIC}"].shape == (2, *FRAME_SHAPE)


def test_video_topics_without_video_is_refused(tmp_path, packets):
    """``video_topics`` names topics to decode, so a sample read that lists
    them without ``video`` fails rather than leaving them encoded."""
    path = write_camera(tmp_path, packets, schema_name="custom/Video")

    with pytest.raises(ValueError, match="video_topics.*VideoOptions"):
        read_samples(
            [sample(path, 15)], {TOPIC: TopicSpec(steps.last(2))}, video_topics=[TOPIC]
        )


def test_the_estimate_reuses_the_summaries_planning_read(
    tmp_path, packets, monkeypatch
):
    """With ``video``, the samples-per-task estimate takes the summaries the
    video planner read, so planning reads each sampled file's summary once."""
    import pyarrow as pa

    path = write_camera(tmp_path, packets)
    probe = pa.Table.from_pylist([sample(path, 15)])
    datasource = MCAPDatasourceV2(
        [],
        topics={TOPIC: TopicSpec(steps.last(2))},
        samples_schema=probe.schema,
        video=VideoOptions(),
        sample_files=[path],
    )
    datasource.infer_schema(None)
    # The estimate that reads the summary itself, for comparison.
    expected = _estimate_samples_per_task(
        probe,
        MCAPDatasourceV2(
            [], topics={TOPIC: TopicSpec(steps.last(2))}, samples_schema=probe.schema
        ),
        None,
    )
    reads = []
    monkeypatch.setattr(
        mcap_sample_tasks, "read_summary", lambda fs, p: reads.append(p)
    )

    count = _estimate_samples_per_task(probe, datasource, None)

    assert reads == []
    assert count == expected


def test_all_in_step_is_refused_on_a_decoded_topic(tmp_path, packets):
    """A step of ``match.all_in_step`` holds a list of messages, which cannot be
    one decoded frame, so planning a decoded read of it fails and names the
    topic."""
    path = write_camera(tmp_path, packets)

    with pytest.raises(ValueError, match="all_in_step.*'/camera'"):
        read(path, [15], TopicSpec(steps.before(0.2, fps=10), match.all_in_step()))


@pytest.mark.parametrize("tensor_format", list(FixedShapeTensorFormat))
def test_planned_frame_type_matches_every_block(
    tmp_path, packets, monkeypatch, tensor_format
):
    """With fixed steps and ``resize``, the planned ``frames:`` type is the one
    the blocks carry, whichever fixed-shape tensor format the context names."""
    monkeypatch.setattr(
        DataContext.get_current(), "arrow_fixed_shape_tensor_format", tensor_format
    )
    path = write_camera(tmp_path, packets)

    # ``read_samples`` checks each block's schema against the planned one.
    rows = read(path, [15, 25], TopicSpec(steps.last(3)), resize=HALF_SIZE)

    assert len(rows) == 2


def test_black_frames_take_the_size_the_camera_decodes_to(
    tmp_path, packets, monkeypatch
):
    """A row with no decoded frame gets black frames of the size its camera
    has decoded to in the file when the row is added. A row before any frame
    decodes has 0x0 frames."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0")
    path = write_camera(tmp_path, packets)
    spec = TopicSpec(steps.last(1), pad="zeros")

    rows = read(path, [5, 10, 15], spec)
    (alone,) = read(path, [15], spec)

    # With no look-back, only the keyframe at frame 10 decodes.
    assert [row[f"is_pad:{TOPIC}"] for row in rows] == [[True], [False], [True]]
    shapes = [np.asarray(row[f"frames:{TOPIC}"]).shape for row in rows]
    assert shapes == [(1, 0, 0, 3), (1, *FRAME_SHAPE), (1, *FRAME_SHAPE)]
    assert np.asarray(alone[f"frames:{TOPIC}"]).shape == (1, 0, 0, 3)


def test_the_row_limit_checks_black_frames_as_built(tmp_path, packets, monkeypatch):
    """A row of black frames added before its camera decodes is built at the
    size known then, so no row the limit passes is larger once built, even
    after a later row in the block decodes the camera."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0")
    path = write_camera(tmp_path, packets)
    # Eight steps over frames 1-8, none a keyframe, then the keyframe at 10.
    clips = {"black": (1, 9), "keyframe": (10, 11)}
    samples = [
        {
            "sample_id": name,
            "files": [path],
            "clip_start": start * FRAME_NS,
            "clip_end": end * FRAME_NS,
        }
        for name, (start, end) in clips.items()
    ]
    spec = TopicSpec(steps.across(fps=30), pad="zeros")
    # Eight black frames at the camera's size would pass this limit.
    limit = 8 * np.prod(FRAME_SHAPE) - 1
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", str(limit))

    rows = read_samples(samples, {TOPIC: spec}, video=VideoOptions())

    assert [row[f"is_pad:{TOPIC}"] for row in rows] == [[True] * 8, [False]]
    assert all(np.asarray(row[f"frames:{TOPIC}"]).nbytes <= limit for row in rows)


# Log time of the message that holds only the parameter sets of a keyframe:
# 1 ms before the keyframe, as a recorder that logs them apart writes them.
PARAMETER_SETS_LEAD_NS = 1_000_000
H264_PARAMETER_SETS = PARAMETER_SET_TYPES["h264"]


def parameter_sets_apart(packets):
    """``packets`` with each keyframe's parameter sets in a message of their
    own, logged ``PARAMETER_SETS_LEAD_NS`` before it. Returns the payloads and
    their log times, all shifted by one frame so none is negative."""
    payloads, times = [], []
    for i, packet in enumerate(packets):
        sets = b"".join(
            u for u in nal_units(packet) if nal_type(u, "h264") in H264_PARAMETER_SETS
        )
        if sets:
            payloads.append(sets)
            times.append((i + 1) * FRAME_NS - PARAMETER_SETS_LEAD_NS)
        payloads.append(without_parameter_sets(packet, "h264"))
        times.append((i + 1) * FRAME_NS)
    return payloads, times


@pytest.mark.parametrize(
    "rule, anchor, expected",
    [
        # Back from the keyframe at frame 10, shifted by one frame as
        # ``parameter_sets_apart`` logs it.
        (steps.last(3), 11, [8, 9, 10]),
        # On from frame 9, past the parameter sets before frame 10.
        (steps.next(3), 9, [9, 10, 11]),
        # Steps 0.1 s apart back from frame 13: the middle one falls on the
        # parameter sets of frame 10, so it takes frame 10, the nearest picture.
        (steps.before(frames=3, fps=10), 14, [7, 10, 13]),
    ],
    ids=["last", "next", "nearest"],
)
def test_steps_skip_messages_that_hold_only_parameter_sets(
    tmp_path, packets, rule, anchor, expected
):
    """A message that holds only parameter sets has no picture, so steps take
    the pictures around it, whether they read back or on: three steps across a
    keyframe are three decoded frames."""
    payloads, times = parameter_sets_apart(packets)
    path = os.path.join(tmp_path, "camera.mcap")
    write_payloads(
        path, payloads, schema_name="foxglove.CompressedVideo", log_times=times
    )

    (row,) = read(path, [anchor], TopicSpec(rule))

    assert [t // FRAME_NS - 1 for t in row[f"times:{TOPIC}"]] == expected
    assert row[f"is_pad:{TOPIC}"] == [False, False, False]


def test_the_stream_start_is_read_once_per_camera(tmp_path, packets, monkeypatch):
    """A camera whose parameter sets are only at the stream start has them
    read once per file, though each sample past a keyframe starts a fresh
    decoder."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_video_source

    path = write_camera(tmp_path, parameter_sets_once(packets, "h264"))
    reads = []
    real = mcap_video_source.StreamStart.read

    def counted(self, *args):
        reads.append(args)
        return real(self, *args)

    monkeypatch.setattr(mcap_video_source.StreamStart, "read", counted)

    rows = read(path, ACROSS_A_KEYFRAME, TopicSpec(steps.last(2)))

    assert [row[f"is_pad:{TOPIC}"] for row in rows] == [[False, False]] * 2
    assert len(reads) == 1


def test_a_row_limit_closes_the_decoders_before_the_file(
    tmp_path, packets, monkeypatch
):
    """A read cut short by a row limit ends its decoders while their file is
    still open, rather than when the reader is garbage collected."""
    path = write_camera(tmp_path, packets)
    # One row per block, so the limit is reached while rows are still built.
    monkeypatch.setattr(DataContext.get_current(), "target_max_block_size", 1)
    file_closed_at_close = []
    close = mcap_decoded_samples.RunDecoder.close

    def recorded(self):
        file_closed_at_close.append(self._stream_start.f.closed)
        close(self)

    monkeypatch.setattr(mcap_decoded_samples.RunDecoder, "close", recorded)

    rows = read_samples(
        [sample(path, f, f"s{f}") for f in BACK_TO_BACK],
        {TOPIC: TopicSpec(steps.last(2))},
        video=VideoOptions(),
        limit=1,
    )

    assert len(rows) == 1
    assert file_closed_at_close == [False]


def test_close_drops_held_frames_without_decoding_them(packets):
    """Closing a decoder flushes its buffers instead of draining them, so the
    frames it still holds are not decoded only to be dropped."""
    from mcap.records import Message

    decoder = FrameDecoder(VideoCodec.H264)
    for i, packet in enumerate(packets[:5]):
        message = Message(
            channel_id=1, sequence=i, log_time=i, publish_time=0, data=packet
        )
        list(decoder.decode(message))
    calls = []
    context = decoder._context

    class Spy:
        """Records which of the context's teardown calls ``close`` makes."""

        def decode(self, packet):
            calls.append("decode")
            return context.decode(packet)

        def flush_buffers(self):
            calls.append("flush_buffers")
            context.flush_buffers()

    decoder._context = Spy()

    decoder.close()

    assert calls == ["flush_buffers"]
    assert decoder._context is None


def test_a_failed_flush_still_drops_held_frames():
    """A flush whose drain fails still flushes the context's buffers, so the
    frames it holds are dropped before the context is let go."""
    calls = []

    class FailingDrain:
        """A codec context whose drain raises, as a codec may at teardown."""

        def decode(self, packet):
            calls.append("decode")
            raise RuntimeError("drain failed")

        def flush_buffers(self):
            calls.append("flush_buffers")

    decoder = FrameDecoder(VideoCodec.H264)
    decoder._context = FailingDrain()

    frames = list(decoder.flush())

    assert frames == []
    assert calls == ["decode", "flush_buffers"]
    assert decoder._context is None


def test_a_decoder_uses_one_thread():
    """A read task reserves one CPU, so a decoder runs on one thread rather
    than FFmpeg's pool of one per core."""
    decoder = FrameDecoder(VideoCodec.H264)

    assert decoder._context.thread_count == 1


@pytest.mark.parametrize(
    "codec, module, package",
    [(VideoCodec.H264, "av", "av"), (VideoCodec.JPEG, "PIL", "Pillow")],
)
def test_a_decoder_without_its_library_says_what_to_install(
    monkeypatch, codec, module, package
):
    """A worker that cannot import the codec's library fails at once, saying
    which package to install, rather than skipping every frame."""
    # A None entry makes ``import`` of the module raise ImportError.
    monkeypatch.setitem(sys.modules, module, None)

    with pytest.raises(ImportError, match=f"pip install {package}"):
        FrameDecoder(codec)


# A slice NAL unit of zeros, which the H.264 decoder rejects.
REJECTED_ACCESS_UNIT = START_CODE + bytes([0x41]) + bytes(50)


def test_an_undecodable_frame_warns_once_per_topic(tmp_path, packets, caplog):
    """A frame the codec rejects is skipped with a warning that names its topic
    and file. Each topic warns once, so a second camera's warning is not lost
    behind the first's."""
    corrupt = list(packets)
    corrupt[12] = REJECTED_ACCESS_UNIT
    paths = {}
    for topic in ("/rejecting/a", "/rejecting/b"):
        paths[topic] = os.path.join(tmp_path, f"{topic.rsplit('/', 1)[1]}.mcap")
        write_payloads(
            paths[topic], corrupt, schema_name="foxglove.CompressedVideo", topic=topic
        )
    samples = [sample(p, 13, topic) for topic, p in paths.items()]
    spec = TopicSpec(steps.last(3), pad="zeros")
    decode_logger = logging.getLogger(FrameDecoder.__module__)
    # Ray's loggers do not propagate to the root logger caplog listens on.
    decode_logger.addHandler(caplog.handler)
    reset_log_once()
    try:
        rows = read_samples(
            samples, {t: spec for t in paths}, video=VideoOptions(resize=HALF_SIZE)
        )
    finally:
        decode_logger.removeHandler(caplog.handler)

    assert [row[f"is_pad:{row['sample_id']}"] for row in rows] == [
        [False, True, False]
    ] * 2
    warnings = [r.getMessage() for r in caplog.records if "undecodable" in r.message]
    assert len(warnings) == 2
    for (topic, path), warning in zip(paths.items(), warnings):
        assert repr(topic) in warning and repr(path) in warning


def write_cameras_by_file(tmp_path, packets, topics):
    """One camera file per entry of ``topics``, recording that topic."""
    paths = []
    for i, topic in enumerate(topics):
        path = os.path.join(tmp_path, f"f{i}.mcap")
        write_payloads(
            path, packets, schema_name="foxglove.CompressedVideo", topic=topic
        )
        paths.append(path)
    return paths


# Five files: a camera that the first four record, then one only the last does.
LATE_CAMERA = "/cam/late"
FILE_TOPICS = ["/cam/early"] * 4 + [LATE_CAMERA]


def test_planning_finds_a_camera_past_the_first_files(tmp_path, packets):
    """A known video topic that only a later file records is decoded: planning
    reads the summaries of further sampled files until each named topic is
    found."""
    paths = write_cameras_by_file(tmp_path, packets, FILE_TOPICS)
    spec = TopicSpec(steps.last(2), pad="zeros")

    rows = read_samples(
        [sample(p, 15, f"s{i}") for i, p in enumerate(paths)],
        {"/cam/early": spec, LATE_CAMERA: spec},
        video=VideoOptions(resize=HALF_SIZE),
    )

    assert f"frames:{LATE_CAMERA}" in rows[0]
    assert rows[-1][f"is_pad:{LATE_CAMERA}"] == [False, False]


def test_a_video_topic_planning_never_sees_warns(tmp_path, packets, caplog):
    """A known video topic in none of the files planning samples comes out as
    raw payloads, with a warning that names it, its file, and ``video_topics``."""
    paths = write_cameras_by_file(tmp_path, packets, FILE_TOPICS)
    spec = TopicSpec(steps.last(2), pad="zeros")
    rows_logger = logging.getLogger(mcap_decoded_samples.__name__)
    rows_logger.addHandler(caplog.handler)
    reset_log_once()
    try:
        rows = read_samples(
            [sample(p, 15, f"s{i}") for i, p in enumerate(paths)],
            {"/cam/early": spec, LATE_CAMERA: spec},
            video=VideoOptions(resize=HALF_SIZE),
            # As if planning sampled only the first four files.
            sample_files=paths[:4],
        )
    finally:
        rows_logger.removeHandler(caplog.handler)

    assert f"data:{LATE_CAMERA}" in rows[0]
    warnings = [r.getMessage() for r in caplog.records if LATE_CAMERA in r.message]
    assert len(warnings) == 1
    assert repr(paths[-1]) in warnings[0] and "video_topics" in warnings[0]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
