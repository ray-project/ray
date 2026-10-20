"""Unit tests for decoded sample rows: a camera topic's steps as RGB frames.

The tests read 30 H.264 frames, a keyframe every 10, with ``video`` and compare
each decoded frame with the same frame from a decode of the whole stream in
order. They drive the scanner and reader as a read task does, with no Ray
cluster.
"""

import importlib.util
import os
from typing import Dict

import numpy as np
import pytest

from ray.data._internal.datasource_v2.formats.mcap import mcap_decoded_samples
from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameDecoder
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import VideoOptions
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data._internal.datasource_v2.formats.mcap.mcap_video import VideoCodec
from ray.data.datasource.mcap import match, steps
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    FRAME_NS,
    FRAME_SHAPE,
    H264_FILE_FRAMES,
    HALF_SIZE,
    cdr_video,
    encode_h264,
    encode_vp9,
    length_prefixed,
    parameter_sets_once,
    read_samples,
    write_payloads,
)

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
        (TopicSpec(steps.after(0.2, fps=10)), [5], [[8, 11]]),
        # Whole-clip steps decode the same way, whatever the anchor: 4 steps
        # spread from the clip's first frame to its last, and the clip's end.
        (TopicSpec(steps.across(4)), [15], [[0, 10, 20, 29]]),
        (TopicSpec(steps.at("clip_end")), [15], [[29]]),
        # The last frame at or before each step. Frames are 33 ms apart, so the
        # step 0.1 s before frame 15 lands 1 ms before frame 12: frame 11.
        (
            TopicSpec(steps.before(frames=3, fps=10), match.backward()),
            [15],
            [[8, 11, 15]],
        ),
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


def test_resize_gives_one_shape_per_row(tmp_path, packets):
    """With ``resize`` and fixed steps, every row's frames have one shape, which
    the planned schema fixes."""
    path = write_camera(tmp_path, packets)

    rows = read(
        path, [15, 25], TopicSpec(steps.before(frames=3, fps=10)), resize=HALF_SIZE
    )

    assert [np.asarray(row[f"frames:{TOPIC}"]).shape for row in rows] == [
        (3, *HALF_SIZE, 3)
    ] * 2


def test_padded_steps_repeat_a_frame_or_are_black(tmp_path, packets, reference):
    """``edge`` repeats the nearest real frame, and ``zeros`` fills a black
    frame of the row's frame size."""
    path = write_camera(tmp_path, packets)
    # The steps of frame 1 at 10 fps reach 0.13 s before the stream starts.
    steps_before = steps.before(frames=3, fps=10)

    (edge,) = read(path, [1], TopicSpec(steps_before, pad="edge"))
    (zeros,) = read(path, [1], TopicSpec(steps_before, pad="zeros"))

    assert edge[f"valid:{TOPIC}"] == [False, False, True]
    assert all(np.array_equal(f, reference[FRAME_NS]) for f in edge[f"frames:{TOPIC}"])
    assert zeros[f"valid:{TOPIC}"] == [False, False, True]
    assert not np.asarray(zeros[f"frames:{TOPIC}"])[:2].any()
    assert np.array_equal(zeros[f"frames:{TOPIC}"][2], reference[FRAME_NS])


def test_decoding_starts_at_the_keyframe_before_the_first_frame(tmp_path, packets, fed):
    """A camera is fed from the last keyframe at or before the first frame a
    step takes, not from the stream's start."""
    path = write_camera(tmp_path, packets)

    read(path, [15], TopicSpec(steps.last(2)))

    assert fed == list(range(10, 16))


def test_a_run_decodes_each_camera_once(tmp_path, packets, fed):
    """Samples whose windows meet share one decode: each frame of their span
    is fed once."""
    path = write_camera(tmp_path, packets)

    rows = read(path, [13, 14, 15], TopicSpec(steps.last(3)))

    assert [step_frames(row) for row in rows] == [
        [11, 12, 13],
        [12, 13, 14],
        [13, 14, 15],
    ]
    assert fed == list(range(10, 16))


# Anchors of two samples whose two steps each do not meet. No keyframe lies
# between the samples of BACK_TO_BACK; the one at frame 20 lies between those
# of ACROSS_A_KEYFRAME.
BACK_TO_BACK = [13, 16]
ACROSS_A_KEYFRAME = [13, 22]


def test_back_to_back_samples_keep_the_decoder_running(
    tmp_path, packets, reference, fed
):
    """Samples whose steps do not meet, with no keyframe between them, are
    decoded by one running decoder: each frame from the keyframe on is fed
    once, and every frame matches a decode of the whole stream."""
    path = write_camera(tmp_path, packets)

    rows = read(path, BACK_TO_BACK, TopicSpec(steps.last(2)))

    assert [step_frames(row) for row in rows] == [[12, 13], [15, 16]]
    assert fed == list(range(10, 17))
    for row in rows:
        for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
            assert np.array_equal(frame, reference[time])


def test_a_keyframe_between_samples_starts_a_fresh_decoder(tmp_path, packets, fed):
    """With a keyframe between two samples' steps, the second is decoded from
    that keyframe rather than fed every frame since the first."""
    path = write_camera(tmp_path, packets)

    rows = read(path, ACROSS_A_KEYFRAME, TopicSpec(steps.last(2)))

    assert [step_frames(row) for row in rows] == [[12, 13], [21, 22]]
    assert fed == [10, 11, 12, 13, 20, 21, 22]


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
    assert together[0][f"valid:{TOPIC}"] == [True, True]
    assert together[1][f"valid:{TOPIC}"] == alone[f"valid:{TOPIC}"] == [False, False]


def test_no_keyframe_in_reach_leaves_steps_empty(tmp_path, packets, monkeypatch):
    """A frame with no keyframe within the look-back cap does not decode: its
    step loses its validity and is padded, or fails the read with ``raise``."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_LEAD_IN_S", "0.1")
    path = write_camera(tmp_path, packets)

    (row,) = read(path, [15], TopicSpec(steps.last(2), pad="zeros"))

    assert row[f"valid:{TOPIC}"] == [False, False]
    assert not np.asarray(row[f"frames:{TOPIC}"]).any()
    with pytest.raises(ValueError, match="does not decode"):
        read(path, [15], TopicSpec(steps.last(2), pad="raise"))


def test_parameter_sets_only_at_the_stream_start(tmp_path, packets, reference):
    """A keyframe without its parameter sets decodes with those read where the
    stream starts."""
    path = write_camera(tmp_path, parameter_sets_once(packets, "h264"))

    (row,) = read(path, [25], TopicSpec(steps.last(2)))

    assert row[f"valid:{TOPIC}"] == [True, True]
    for frame, time in zip(row[f"frames:{TOPIC}"], row[f"times:{TOPIC}"]):
        assert np.array_equal(frame, reference[time])


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

    assert row[f"valid:{TOPIC}"] == [True, True]
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
    validity, and decodes no frame."""
    path = write_camera(tmp_path, packets)
    monkeypatch.setattr(mcap_decoded_samples, "FrameDecoder", None)

    rows = read_samples(
        [sample(path, 15)],
        {TOPIC: TopicSpec(steps.last(2))},
        video=VideoOptions(),
        columns=[f"valid:{TOPIC}", f"times:{TOPIC}"],
    )

    assert rows == [
        {
            f"valid:{TOPIC}": [True, True],
            f"times:{TOPIC}": [14 * FRAME_NS, 15 * FRAME_NS],
        }
    ]


def test_projecting_away_one_cameras_frames_keeps_its_validity(tmp_path, packets):
    """A projection that keeps one camera's frames and drops another's decodes
    only the first. The other camera's steps stay valid, as they do when every
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
        columns=[f"frames:{TOPIC}", f"valid:{TOPIC}", "valid:/b"],
    )

    assert row[f"valid:{TOPIC}"] == [True, True]
    assert row["valid:/b"] == [True, True]


def test_planning_finds_a_camera_in_a_later_file(tmp_path, packets):
    """Planning opens every sample file it is given, so a camera that only the
    second file records is still decoded."""
    no_camera = os.path.join(tmp_path, "no_camera.mcap")
    write_payloads(no_camera, [b"x"] * 3, schema_name="std_msgs/String", topic="/a")
    camera = write_camera(tmp_path, packets)

    rows = read_samples(
        [sample(no_camera, 1, "a"), sample(camera, 15, "b")],
        {TOPIC: TopicSpec(steps.last(2))},
        video=VideoOptions(),
    )

    assert [sorted(k for k in row if k.endswith(TOPIC)) for row in rows] == [
        [f"frames:{TOPIC}", f"times:{TOPIC}", f"valid:{TOPIC}"]
    ] * 2
    assert rows[1][f"frames:{TOPIC}"].shape == (2, *FRAME_SHAPE)


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

    assert row[f"valid:{TOPIC}"] == [True, True]
    assert row[f"frames:{TOPIC}"].shape == (2, *FRAME_SHAPE)


def test_all_in_step_is_refused_on_a_decoded_topic(tmp_path, packets):
    """A step of ``match.all_in_step`` holds a list of messages, which cannot be
    one decoded frame, so planning a decoded read of it fails and names the
    topic."""
    path = write_camera(tmp_path, packets)

    with pytest.raises(ValueError, match="all_in_step.*'/camera'"):
        read(path, [15], TopicSpec(steps.before(0.2, fps=10), match.all_in_step()))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
