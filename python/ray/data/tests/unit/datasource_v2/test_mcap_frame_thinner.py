"""Unit tests for ``fps`` thinning: the frame nearest each slot's centre is kept.

``FrameThinner`` is checked against ``centred_nearest``, the rule written
plainly, on cameras at several rates, phases and jitters. Reads split into tasks
are checked the way the reader splits them: each task notes its own frames, the
picture before them from the message index, and the picture after them when its
last frame waits for its slot. No MCAP file and no Ray cluster.
"""

import random
from fractions import Fraction
from typing import List, Sequence

import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_decode import FrameThinner
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    SECOND,
    camera_times,
    centred_nearest,
)

MS = 1_000_000  # one millisecond in nanoseconds
NTSC = Fraction(30000, 1001)
RATES = [Fraction(30), NTSC, Fraction(25), Fraction(60)]
# Camera frames per kept frame.
RATIOS = [1, 2, 3, 4]
# 2026-10-10T00:00:00Z, a whole second, and one day, in nanoseconds.
OCT_10_2026 = 1_791_590_400 * SECOND
DAY = 86_400 * SECOND
JITTER_NS = 1 * MS  # capture jitter, one standard deviation
STREAMS_PER_CASE = 50


def thin(times: Sequence[int], fps: Fraction) -> List[int]:
    """The indexes into ``times`` that one task keeps, noting and offering each
    frame in order."""
    thinner = FrameThinner(fps)
    kept: List[int] = []
    for index, log_time in enumerate(times):
        thinner.note(log_time)
        thinner.offer(log_time, index)
        kept += [i for _, i, is_kept in thinner.release() if is_kept]
    thinner.settle()
    kept += [i for _, i, is_kept in thinner.release() if is_kept]
    return kept


def thin_split(times: Sequence[int], fps: Fraction, cuts: Sequence[int]) -> List[int]:
    """The indexes into ``times`` kept when tasks start at the indexes ``cuts``.

    As ``FpsSeed`` does from the message index, each task first notes the last
    picture before its own in its first frame's slot. After its last frame, it
    notes the first picture in the look-ahead span, if that frame waits.
    """
    kept: List[int] = []
    bounds = [0, *cuts, len(times)]
    for low, high in zip(bounds, bounds[1:]):
        thinner = FrameThinner(fps)
        start = thinner.slot_start(times[low])
        before = [t for t in times[:low] if start is not None and t >= start]
        if before:
            thinner.note(before[-1])
        for index in range(low, high):
            thinner.note(times[index])
            thinner.offer(times[index], index)
        span = thinner.look_ahead()
        after = [t for t in times[high:] if span and span[0] <= t < span[1]]
        if after:
            thinner.note(after[0])
        thinner.settle()
        kept += [i for _, i, is_kept in thinner.release() if is_kept]
    return sorted(kept)


@pytest.mark.parametrize(
    "times_ms, kept",
    [
        ([60, 90, 120], [1]),
        ([80, 120], [0]),  # equally near: the earlier one
        ([100, 100], [0]),  # one log time twice: the first
        ([51, 149], [0]),  # both 49 ms from the centre
        ([149, 150], [0, 1]),  # 150 ms opens the next slot
    ],
)
def test_the_frame_nearest_the_slot_centre_is_kept(times_ms, kept):
    """At 10 fps, slot 1 holds ``[50, 150)`` ms and is centred on 100 ms. It
    keeps its frame nearest the centre, the earlier one on a tie."""
    times = [t * MS for t in times_ms]

    assert thin(times, Fraction(10)) == kept
    assert centred_nearest(times, Fraction(10)) == kept


def test_a_slot_starts_halfway_between_two_centres():
    """``slot_start`` is halfway between a slot's centre and the one before it,
    rounded up to the nanosecond: a frame exactly halfway opens the later slot."""
    thinner = FrameThinner(Fraction(30))

    # Slot 1 of 30 fps is centred on 33,333,333.3 ns and starts at 16,666,666.7.
    assert thinner.slot_start(33_333_333) == 16_666_667
    assert thinner.slot_start(16_666_667) == 16_666_667
    assert thinner.slot_start(16_666_666) == -16_666_666
    assert FrameThinner(None).slot_start(5) is None


def test_a_frame_before_its_slot_centre_waits_for_the_next_picture():
    """A frame before its slot's centre is held until the next picture decides
    it, which may be nearer. A frame at or past the centre is decided at once."""
    thinner = FrameThinner(Fraction(10))

    thinner.note(90 * MS)
    thinner.offer(90 * MS, "a")
    waiting = list(thinner.release())
    held, span = thinner.held_from, thinner.look_ahead()
    # 105 ms is nearer the 100 ms centre than 90 ms, and at or past it.
    thinner.note(105 * MS)
    thinner.offer(105 * MS, "b")
    decided = list(thinner.release())

    assert waiting == [] and held == 90 * MS
    # A later picture is nearer while it lies before 110 ms, exclusive.
    assert span == (90 * MS + 1, 110 * MS)
    assert decided == [(90 * MS, "a", False), (105 * MS, "b", True)]
    assert thinner.look_ahead() is None and thinner.held_from is None


def test_a_held_frame_keeps_its_decision_across_a_long_gap():
    """A frame still in the decoder when a long gap is noted keeps its decision:
    noting the gap's pictures does not push it out."""
    thinner = FrameThinner(Fraction(30))
    # On its slot's centre, so kept at once.
    thinner.note(0)
    # Ten seconds of a 30 fps camera that another task owns, noted at the gap.
    for k in range(1, 301):
        thinner.note(round(k * SECOND / 30))

    thinner.offer(0, "held")
    released = list(thinner.release())

    assert released == [(0, "held", True)]


def test_a_frame_released_after_a_later_one_is_not_kept():
    """A frame the decoder releases after a later frame finds its decision passed:
    it is dropped, and the frames after it keep theirs."""
    thinner = FrameThinner(Fraction(10))
    for log_time in (0, 100 * MS, 200 * MS):
        thinner.note(log_time)

    thinner.offer(100 * MS, "on time")
    thinner.offer(0, "late")
    thinner.offer(200 * MS, "next")
    released = list(thinner.release())

    assert released == [
        (100 * MS, "on time", True),
        (0, "late", False),
        (200 * MS, "next", True),
    ]


def test_without_fps_every_frame_is_kept_at_once():
    """Without ``fps`` nothing waits and every offered frame is kept."""
    thinner = FrameThinner(None)

    thinner.note(5)
    thinner.offer(5, "a")
    thinner.offer(5, "b")

    assert list(thinner.release()) == [(5, "a", True), (5, "b", True)]
    assert thinner.look_ahead() is None


@pytest.mark.parametrize("jitter_ns", [0, JITTER_NS], ids=["grid", "jitter"])
@pytest.mark.parametrize("ratio", RATIOS)
@pytest.mark.parametrize("rate", RATES, ids=str)
def test_kept_frames_match_the_rule_written_plainly(rate, ratio, jitter_ns):
    """On cameras that start at random times, on a regular grid or with capture
    jitter, the thinner keeps exactly the frames ``centred_nearest`` keeps."""
    fps = rate / ratio
    rng = random.Random(f"{rate}:{ratio}:{jitter_ns}")

    for seed in range(STREAMS_PER_CASE):
        start = OCT_10_2026 + rng.randrange(400 * DAY)
        times = camera_times(rate, rng.randrange(1, 200), start, jitter_ns, seed)

        assert thin(times, fps) == centred_nearest(times, fps)


@pytest.mark.parametrize("ratio", RATIOS)
@pytest.mark.parametrize("rate", [Fraction(30), NTSC], ids=str)
def test_whole_ratios_keep_evenly_spaced_frames(rate, ratio):
    """A camera on a regular grid thinned by a whole ratio keeps one frame in
    every ``ratio``, whatever its phase. The first and last slots can be cut
    short by the recording, so only the spacing between them is checked."""
    rng = random.Random(f"{rate}:{ratio}")

    for _ in range(STREAMS_PER_CASE):
        start = OCT_10_2026 + rng.randrange(400 * DAY)
        kept = thin(camera_times(rate, 300, start), rate / ratio)

        assert {b - a for a, b in zip(kept[1:], kept[2:-1])} == {ratio}


def test_fps_10_keeps_every_third_frame_of_a_30_fps_camera():
    """A 30 fps camera that starts on a whole second keeps the frames on the
    100 ms grid at ``fps=10``: frames 0, 3, 6 and so on."""
    # 301 frames end on a slot's centre, so the last slot is whole.
    times = camera_times(Fraction(30), 301, OCT_10_2026)

    assert thin(times, Fraction(10)) == list(range(0, 301, 3))


@pytest.mark.parametrize("jitter_ns", [0, JITTER_NS], ids=["grid", "jitter"])
def test_a_30_fps_camera_keeps_every_frame_at_fps_30_on_any_date(jitter_ns):
    """The same 60 s of a 30 fps camera, recorded from midnight on each of 60
    days, keeps all 1,800 frames at ``fps=30``. The kept frames do not depend
    on the date, with or without capture jitter."""
    offsets = camera_times(Fraction(30), 1800, 0, jitter_ns, seed=7)

    for day in range(60):
        times = [OCT_10_2026 + day * DAY + offset for offset in offsets]

        assert thin(times, Fraction(30)) == list(range(1800))


@pytest.mark.parametrize("rate", [Fraction(30), NTSC], ids=str)
def test_two_second_windows_hold_sixty_frames_at_30_fps(rate):
    """Slots tile windows aligned to the epoch: a camera on the grid, thinned at
    its own rate, keeps exactly 60 frames in every window of 60 slots, which is
    2 s at 30 fps and 2.002 s at 30000/1001."""
    window_ns = round(60 * SECOND / rate)
    # A window boundary near 2026-10-10, on both grids.
    start = OCT_10_2026 // window_ns * window_ns
    times = camera_times(rate, 600, start)

    kept = [times[i] for i in thin(times, rate)]
    per_window = [
        sum(start + w * window_ns <= t < start + (w + 1) * window_ns for t in kept)
        for w in range(10)
    ]

    assert per_window == [60] * 10


@pytest.mark.parametrize("jitter_ns", [0, 5 * MS], ids=["grid", "jitter"])
@pytest.mark.parametrize("ratio", RATIOS)
def test_split_reads_keep_the_frames_of_a_whole_read(ratio, jitter_ns):
    """Cut into tasks anywhere, including one task per frame, a read keeps the
    frames a whole read keeps: a frame's slot depends only on the pictures
    just before and after it."""
    rng = random.Random(f"{ratio}:{jitter_ns}")

    for seed in range(STREAMS_PER_CASE):
        rate = rng.choice(RATES)
        start = OCT_10_2026 + rng.randrange(400 * DAY)
        times = camera_times(rate, rng.randrange(2, 80), start, jitter_ns, seed)
        whole = centred_nearest(times, rate / ratio)
        n = len(times)

        assert thin_split(times, rate / ratio, list(range(1, n))) == whole
        for _ in range(5):
            cuts = sorted(rng.sample(range(1, n), rng.randrange(min(4, n - 1) + 1)))
            assert thin_split(times, rate / ratio, cuts) == whole


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
