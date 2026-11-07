"""Unit tests for the step rules of ``read_mcap(samples=...)`` (``mcap_samples``).

The rules need no recording: each test resolves a window over message times
held in a list, a 10 Hz topic logged at every tenth of a second.
"""

from fractions import Fraction

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    SampleTimes,
    SortedTimes,
    TopicPlan,
    check_samples_schema,
    resolve_slots,
    resolve_steps,
    sample_manifest,
    sample_row_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.context import DataContext
from ray.data.datasource.mcap import match, steps

SECOND = 1_000_000_000
TENTH = SECOND // 10  # the topic's message period
# Clip [1.0 s, 4.0 s); the default anchor lies between two messages.
CLIP_START, CLIP_END = 1 * SECOND, 4 * SECOND
ANCHOR = 2 * SECOND + 5 * SECOND // 100  # 2.05 s
WHERE = "sample 's0'"
# The latest log time a sample row's int64 ``times:`` column holds.
INT64_MAX = 2**63 - 1


def tenths(*ks):
    """The log times of the messages logged ``k`` tenths of a second in."""
    return [k * TENTH for k in ks]


def topic(times=None):
    """A ``TopicTimes`` whose messages are their own log times, 10 Hz from 0 s to
    4.9 s unless ``times`` lists others."""
    if times is None:
        times = tenths(*range(50))
    return SortedTimes([(t, t) for t in times])


def resolve(spec, *, anchor=ANCHOR, times=None, clip=(CLIP_START, CLIP_END)):
    """The steps of ``spec`` for one sample, as ``(time, is_pad)`` pairs and the
    messages that fill them."""
    plan = TopicPlan.from_spec("/t", spec)
    resolved = resolve_steps(
        plan, topic(times), SampleTimes(clip[0], clip[1], anchor), WHERE
    )
    return [(s.time, s.is_pad) for s in resolved], [s.message for s in resolved]


# -- steps on a recorder's clock ----------------------------------------------


def test_steps_on_a_rounded_clock_take_their_own_frame():
    """Steps that fall on the frames of a 30 fps clock, which rounds each frame
    to the nanosecond, take those frames and not their neighbours."""
    frames = [round(k * Fraction(SECOND, 30)) for k in range(120)]
    anchor = frames[90]

    resolved, messages = resolve(
        TopicSpec(steps.before(frames=30, fps=30)), anchor=anchor, times=frames
    )

    assert messages == frames[61:91]
    assert not any(is_pad for _, is_pad in resolved)


def test_steps_take_evenly_spaced_frames_despite_jitter():
    """With the anchor on a frame of a jittery 30 fps camera, 10 steps a
    second take every third frame back from it: each step lands within the
    jitter of its frame, and the nearest message is that frame."""
    # Frames 1 ms early or late in turn, as a recorder's log times are. From
    # an early frame, every other step lands 2 ms before a late frame.
    frames = [k * SECOND // 30 + (1 if k % 2 else -1) * 1_000_000 for k in range(120)]

    _, messages = resolve(
        TopicSpec(steps.before(frames=5, fps=10)), anchor=frames[90], times=frames
    )

    assert messages == [frames[k] for k in (78, 81, 84, 87, 90)]


# -- windows with fps ---------------------------------------------------------


def test_steps_take_the_nearest_message_up_to_the_anchor():
    """Each step of a before-window takes the message logged nearest to it,
    the earlier one on a tie, but never one logged after the anchor."""
    anchor = 20 * TENTH + 8 * SECOND // 100  # 2.08 s

    _, ties = resolve(TopicSpec(steps.before(0.3, fps=10)))
    resolved, messages = resolve(TopicSpec(steps.before(0.3, fps=10)), anchor=anchor)

    # Steps at 1.85, 1.95 and 2.05 s lie halfway between messages.
    assert ties == tenths(18, 19, 20)
    # Steps at 1.88 and 1.98 s take the later message; the anchor's step takes
    # 2.0 s, since 2.1 s lies past the anchor.
    assert messages == tenths(19, 20, 20)
    assert [is_pad for _, is_pad in resolved] == [False] * 3


def test_after_window_takes_only_messages_after_the_anchor():
    """An after-window's steps take messages logged after the anchor: with the
    anchor on a message, no step takes the anchor's message, however near."""
    anchor = 20 * TENTH

    _, messages = resolve(TopicSpec(steps.after(0.2, fps=10)), anchor=anchor)
    _, far = resolve(
        TopicSpec(steps.after(frames=2, fps=10)),
        anchor=anchor,
        times=tenths(15, 20, 35),
    )
    _, none = resolve(
        TopicSpec(steps.after(frames=2, fps=10), pad="zeros"),
        anchor=anchor,
        times=tenths(15, 20),
    )

    assert messages == tenths(21, 22)
    # The only message after the anchor, 1.5 s later, is the nearest one.
    assert far == tenths(35, 35)
    assert none == [None, None]


# -- match.backward and within --------------------------------------------------


def test_backward_takes_the_last_message_at_or_before_each_step():
    """With ``match.backward`` each step takes the last message logged at or
    before it, so a frame logged just after its step goes to the next step."""
    # The jittery 30 fps camera of the nearest test, anchored on an early
    # frame: every other step lands on an early frame, and the others 2 ms
    # before a late one, so they take the frame before it.
    frames = [k * SECOND // 30 + (1 if k % 2 else -1) * 1_000_000 for k in range(120)]
    spec = TopicSpec(steps.before(frames=5, fps=10), match.backward())

    _, messages = resolve(spec, anchor=frames[90], times=frames)

    assert messages == [frames[k] for k in (78, 80, 84, 86, 90)]


def test_backward_never_reaches_back_across_the_anchor():
    """An after-window step takes only messages logged after the anchor: with
    the anchor on a message and the next one past the step, the step is empty,
    where nearest would take the later message."""
    anchor = 20 * TENTH
    one_step = steps.after(frames=1, fps=20)  # a step at 2.05 s

    resolved, backward = resolve(
        TopicSpec(one_step, match.backward(), pad="zeros"), anchor=anchor
    )
    _, nearest = resolve(TopicSpec(one_step), anchor=anchor)

    assert backward == [None]
    assert resolved == [(anchor + SECOND // 20, True)]
    assert nearest == tenths(21)


def test_backward_reads_back_through_a_gap():
    """A step after a gap takes the last message before it, however far back,
    where nearest takes the message after the gap."""
    times = tenths(10, 11, 12, 30, 31)
    spec = steps.before(frames=3, fps=10)

    _, backward = resolve(
        TopicSpec(spec, match.backward()), anchor=30 * TENTH, times=times
    )
    _, nearest = resolve(TopicSpec(spec), anchor=30 * TENTH, times=times)

    assert backward == tenths(12, 12, 30)
    assert nearest == tenths(30, 30, 30)


@pytest.mark.parametrize(
    "spec, anchor, in_clip, in_recording",
    [
        # Steps at 4.05, 4.15 and 4.25 s lie past the clip's end at 4.0 s.
        (
            steps.after(0.3, fps=10),
            39 * TENTH + TENTH // 2,
            [None] * 3,
            tenths(40, 41, 42),
        ),
        # The last three messages up to 1.05 s: the clip from 1.0 s holds one.
        (
            steps.last(3),
            10 * TENTH + TENTH // 2,
            [None, None, 10 * TENTH],
            tenths(8, 9, 10),
        ),
        # Every message in the 0.25 s up to 1.05 s.
        (
            steps.every(before_s=0.25),
            10 * TENTH + TENTH // 2,
            tenths(10),
            tenths(9, 10),
        ),
    ],
)
def test_within_recording_reaches_past_the_clip(spec, anchor, in_clip, in_recording):
    """With ``within="recording"`` steps take messages past the clip's edges;
    inside the clip those steps stay empty."""
    import dataclasses

    _, clip = resolve(TopicSpec(spec, pad="zeros"), anchor=anchor)
    recording_spec = dataclasses.replace(spec, within="recording")
    resolved, recording = resolve(TopicSpec(recording_spec, pad="zeros"), anchor=anchor)

    assert clip == in_clip
    assert recording == in_recording
    assert not any(is_pad for _, is_pad in resolved)


@pytest.mark.parametrize(
    "pad, messages, is_pad",
    [
        # Steps at 0.85 and 0.95 s lie before the clip; 1.05 s onwards inside.
        ("edge", tenths(10, 10, 10, 11, 12), [True, True, False, False, False]),
        ("zeros", [None, None] + tenths(10, 11, 12), [True, True, False, False, False]),
    ],
)
def test_steps_before_the_clip_are_padded(pad, messages, is_pad):
    """Steps before the clip's start are padded: ``edge`` repeats the first
    real step, and ``zeros`` leaves them empty at their own times."""
    anchor = 12 * TENTH + 5 * SECOND // 100  # 1.25 s

    resolved, got = resolve(
        TopicSpec(steps.before(0.5, fps=10), pad=pad), anchor=anchor
    )

    assert got == messages
    assert [v for _, v in resolved] == is_pad
    if pad == "zeros":
        # An empty step keeps its own time.
        assert [t for t, _ in resolved[:2]] == [anchor - 4 * TENTH, anchor - 3 * TENTH]


def test_steps_inside_the_clip_never_take_a_message_before_it():
    """A step inside the clip takes the clip's message even when one logged
    just before the clip's start lies nearer. The step before the clip repeats
    that message under ``edge``."""
    # The clip starts at 1.05 s, after the message at 1.0 s, so its only
    # message up to the anchor is the one at 1.4 s.
    times = tenths(10, 14, 30)
    clip = (10 * TENTH + 5 * SECOND // 100, CLIP_END)

    resolved, messages = resolve(
        TopicSpec(steps.before(frames=6, fps=10)),
        times=times,
        clip=clip,
        anchor=15 * TENTH,
    )

    # Steps at 1.0 s (outside the clip), 1.1, 1.2, 1.3, 1.4 and 1.5 s.
    assert messages == tenths(14, 14, 14, 14, 14, 14)
    assert [is_pad for _, is_pad in resolved] == [True] + [False] * 5


@pytest.mark.parametrize("backward", [False, True])
def test_edge_without_real_steps_repeats_the_edge_message_on_its_side(backward):
    """An after-window whose steps all lie past the clip's end repeats the
    clip's last message after the anchor, with either match."""
    # Steps at 4.025 and 4.125 s lie past the clip; the clip's last message
    # after the anchor at 3.925 s is at 3.95 s.
    anchor, last = 39 * TENTH + TENTH // 4, 39 * TENTH + TENTH // 2
    match_ = match.backward() if backward else None

    resolved, messages = resolve(
        TopicSpec(steps.after(frames=2, fps=10), match_),
        anchor=anchor,
        times=tenths(30, 39) + [last],
    )

    assert messages == [last, last]
    assert [v for _, v in resolved] == [True, True]


@pytest.mark.parametrize(
    "window, anchor",
    [
        # The clip's messages from 2.5 s on lie past the anchor at 2.05 s.
        (steps.before(frames=2, fps=10), ANCHOR),
        # The clip's messages up to 3.0 s lie before the anchor at 3.5 s.
        (steps.after(frames=2, fps=10), 35 * TENTH),
    ],
)
def test_edge_never_repeats_a_message_from_across_the_anchor(window, anchor):
    """A window with no message on its side of the anchor stays empty under
    ``edge``, as ``zeros`` leaves it, rather than repeat a message from the
    other side."""
    times = tenths(25, 26, 30)

    edge = resolve(TopicSpec(window), anchor=anchor, times=times)
    zeros = resolve(TopicSpec(window, pad="zeros"), anchor=anchor, times=times)

    assert edge == zeros
    assert edge[1] == [None, None]


@pytest.mark.parametrize(
    "spec, times, messages, is_pad",
    [
        # Steps at 1.85, 1.95 and 2.05 s; the first message is at 1.9 s.
        (
            TopicSpec(steps.before(0.3, fps=10), match.backward()),
            tenths(19, 20),
            [None] + tenths(19, 20),
            [True, False, False],
        ),
        # Steps at 1.0, 2.0, 3.0 and 3.999 s; the first message is at 2.5 s.
        (
            TopicSpec(steps.across(4), match.backward()),
            tenths(25, 35),
            [None, None] + tenths(25, 35),
            [True, True, False, False],
        ),
    ],
)
def test_backward_edge_never_fills_a_step_with_a_later_message(
    spec, times, messages, is_pad
):
    """With ``match.backward()``, ``edge`` repeats only an earlier step, so a
    step before the topic's first message stays empty rather than hold a
    message logged after it."""
    resolved, got = resolve(spec, times=times)

    assert got == messages
    assert [v for _, v in resolved] == is_pad


def test_backward_edge_repeats_an_earlier_step_past_the_clip():
    """With ``match.backward()``, a step past the clip's end repeats the step
    before it, which holds a message logged before the step."""
    anchor = 37 * TENTH + TENTH // 2  # 3.75 s

    resolved, messages = resolve(
        TopicSpec(steps.after(frames=3, fps=10), match.backward()), anchor=anchor
    )

    # Steps at 3.85, 3.95 and 4.05 s; the last lies past the clip's end.
    assert messages == tenths(38, 39, 39)
    assert [v for _, v in resolved] == [False, False, True]


def test_raise_names_the_sample_and_the_overreach():
    """``raise`` fails on the first sample with an empty step, saying how far
    past the clip the window reaches."""
    anchor = 11 * TENTH

    with pytest.raises(ValueError, match=r"sample 's0': 3 of 5 steps .* 0.3 s before"):
        resolve(TopicSpec(steps.before(0.5, fps=10), pad="raise"), anchor=anchor)
    with pytest.raises(ValueError, match="0.1 s past the clip's end"):
        resolve(
            TopicSpec(steps.after(frames=3, fps=10), pad="raise"), anchor=38 * TENTH
        )
    with pytest.raises(ValueError, match="No message on the topic .* up to the anchor"):
        # The clip's only message, at 2.5 s, lies past the anchor.
        resolve(
            TopicSpec(steps.before(frames=2, fps=10), pad="raise"), times=tenths(25)
        )


# -- frame counts and spans without fps ---------------------------------------


def test_frame_count_without_fps_takes_recorded_messages():
    """``before_frames`` without ``fps`` takes the messages logged up to the
    anchor and ``after_frames`` those after it, whatever their spacing."""
    times = tenths(10, 13, 19, 20, 21, 27, 39)

    _, before = resolve(TopicSpec(steps.last(3)), times=times)
    _, after = resolve(TopicSpec(steps.next(2)), times=times)

    assert before == tenths(13, 19, 20)
    assert after == tenths(21, 27)


def test_frame_count_short_of_messages_pads_the_far_end():
    """A count the clip cannot fill pads its far end, at the clip's edge."""
    times = tenths(5, 12, 20, 38, 41)

    before_steps, before = resolve(TopicSpec(steps.last(4)), times=times)
    after_steps, after = resolve(TopicSpec(steps.next(3), pad="zeros"), times=times)

    # The message at 0.5 s lies before the clip, so two steps are padded.
    assert before == tenths(12, 12, 12, 20)
    assert [v for _, v in before_steps] == [True, True, False, False]
    assert after == tenths(38) + [None, None]
    assert after_steps[1:] == [(CLIP_END - 1, True)] * 2


def test_counted_steps_within_the_recording_take_its_edges_when_short():
    """With ``within="recording"``, the steps ``steps.last`` and ``steps.next``
    cannot fill take the recording's first or last log time, not the clip's
    edges. The last is the latest log time the int64 ``times:`` column holds."""
    times = tenths(20, 21)

    before, _ = resolve(
        TopicSpec(steps.last(3, within="recording"), pad="zeros"), times=times
    )
    after, _ = resolve(
        TopicSpec(steps.next(2, within="recording"), pad="zeros"), times=times
    )

    assert before == [(0, True), (0, True), (20 * TENTH, False)]
    assert after == [(21 * TENTH, False), (INT64_MAX, True)]


def test_span_without_fps_takes_every_message_and_cuts_at_the_clip():
    """``before_s`` without ``fps`` holds every message in ``(anchor - span,
    anchor]``. It is cut at the clip's edge, or fails with ``raise``."""
    _, inside = resolve(TopicSpec(steps.every(before_s=0.3)))
    _, cut = resolve(TopicSpec(steps.every(before_s=2.0)))

    assert inside == tenths(18, 19, 20)
    assert cut == tenths(*range(10, 21))
    with pytest.raises(ValueError, match="0.95 s before the clip"):
        resolve(TopicSpec(steps.every(before_s=2.0), pad="raise"))


# -- clip steps and reference points -------------------------------------------

CLIP_LAST = CLIP_END - 1  # the clip's last nanosecond


@pytest.mark.parametrize(
    "spec, expected",
    [
        # Five steps from 1.0 s to the clip's last nanosecond. The steps at
        # 1.75 and 3.25 s tie between two messages and take the earlier.
        (TopicSpec(steps.across(5)), tenths(10, 17, 25, 32, 39)),
        # One step every half second from 1.0 s while inside the clip.
        (TopicSpec(steps.across(fps=2)), tenths(10, 15, 20, 25, 30, 35)),
        (TopicSpec(steps.across(1)), tenths(10)),
        (TopicSpec(steps.at("clip_start")), tenths(10)),
        # The clip ends before 4.0 s, so its last message is at 3.9 s.
        (TopicSpec(steps.at("clip_end")), tenths(39)),
        # A clip step looks on both sides: 2.05 s ties between 2.0 and 2.1 s.
        (TopicSpec(steps.at("anchor")), tenths(20)),
        (TopicSpec(steps.every()), tenths(*range(10, 40))),
    ],
)
def test_clip_steps_take_the_nearest_message_in_the_clip(spec, expected):
    """Clip steps lie inside the clip, whatever its length, and each takes the
    message logged nearest to it there. ``steps.every()`` takes every message
    of the clip."""
    resolved, messages = resolve(spec)

    assert messages == expected
    assert not any(is_pad for _, is_pad in resolved)


@pytest.mark.parametrize(
    "spec, expected",
    [
        (TopicSpec(steps.last(3, at="clip_end")), tenths(37, 38, 39)),
        (TopicSpec(steps.after(0.2, fps=10, at="clip_start")), tenths(11, 12)),
    ],
)
def test_windows_measured_from_the_clips_edges(spec, expected):
    """A window measured from ``clip_start`` or ``clip_end`` reads as if that
    point were the anchor."""
    _, messages = resolve(spec)

    assert messages == expected


def test_clip_steps_of_an_empty_topic_raise_under_raise():
    """With no message on the topic in the clip, ``pad="raise"`` names the
    sample and says the clip holds none."""
    with pytest.raises(
        ValueError, match="No message on the topic is logged in the clip"
    ):
        resolve(TopicSpec(steps.across(3), pad="raise"), times=tenths(45))


def test_backward_raise_names_the_steps_before_the_first_message():
    """With ``match.backward()``, steps before the first message take none
    while later steps do: ``pad="raise"`` says so, not that the clip holds no
    message."""
    spec = TopicSpec(steps.across(4), match.backward(), pad="raise")

    # Steps at 1.0, 2.0, 3.0 and 3.999 s; messages only at 2.5 and 3.5 s.
    with pytest.raises(ValueError, match="logged after the first"):
        resolve(spec, times=tenths(25, 35))


def test_backward_raise_names_messages_after_the_last_step():
    """With ``match.backward()``, no step takes a message when the topic's
    messages all come after the last step: ``pad="raise"`` says so, not that
    the clip holds no message."""
    spec = TopicSpec(steps.after(0.2, fps=10), match.backward(), pad="raise")

    # Steps at 2.15 and 2.25 s; the only message is at 3.0 s.
    with pytest.raises(ValueError, match="logged after the last step"):
        resolve(spec, times=tenths(30))


def test_backward_raise_ignores_messages_across_the_anchor():
    """An after-window's steps take no message logged before the anchor, so a
    message there does not make ``pad="raise"`` blame the steps' order."""
    spec = TopicSpec(steps.after(0.2, fps=10), match.backward(), pad="raise")

    # Steps at 2.15 and 2.25 s; the only message is at 1.5 s, before the anchor.
    with pytest.raises(ValueError, match="No message on the topic is logged"):
        resolve(spec, times=tenths(15))


@pytest.mark.parametrize(
    "spec, reach",
    [
        (TopicSpec(steps.across(4)), (CLIP_START, CLIP_LAST)),
        (TopicSpec(steps.at("clip_end")), (CLIP_LAST, CLIP_LAST)),
        # How far a count reaches depends on the recording.
        (TopicSpec(steps.last(3)), (ANCHOR, ANCHOR)),
        (TopicSpec(steps.next(2)), (ANCHOR + 1, ANCHOR + 1)),
        (
            TopicSpec(steps.before(0.2, fps=10, at="clip_end")),
            (CLIP_LAST - TENTH, CLIP_LAST),
        ),
        (TopicSpec(steps.after(0.2, fps=10)), (ANCHOR + 1, ANCHOR + 2 * TENTH)),
        # The first slot starts one step period before the first step.
        (
            TopicSpec(steps.before(0.3, fps=10), match.all_in_step()),
            (ANCHOR - 3 * TENTH + 1, ANCHOR),
        ),
        (TopicSpec(steps.every(before_s=0.3)), (ANCHOR - 3 * TENTH + 1, ANCHOR)),
        (TopicSpec(steps.every(after_s=0.3)), (ANCHOR + 1, ANCHOR + 3 * TENTH)),
    ],
)
def test_reach_spans_the_steps(spec, reach):
    """Clip steps reach the whole clip and ``steps.at`` its one point. A window
    reaches from its first step to its last, from the first slot's start with
    ``match.all_in_step``, and over the horizon of ``steps.every``.
    ``steps.last`` and ``steps.next`` reach only the first nanosecond on their
    side of the anchor."""
    plan = TopicPlan.from_spec("/t", spec)

    assert plan.reach(SampleTimes(CLIP_START, CLIP_END, ANCHOR)) == reach


# -- every message of each step's slot ---------------------------------------


def slots(spec, *, anchor=ANCHOR, times=None, clip=(CLIP_START, CLIP_END)):
    """The steps of an ``all_in_step`` ``spec`` for one sample, as each slot's
    message times and whether it is padded."""
    plan = TopicPlan.from_spec("/t", spec)
    resolved = resolve_slots(
        plan, topic(times), SampleTimes(clip[0], clip[1], anchor), WHERE
    )
    return [(s.times, s.is_pad) for s in resolved]


def test_each_step_takes_every_message_of_its_slot():
    """A slot runs from just after the previous step up to and including its
    own, so a message logged on a step belongs to that step, not the next. The
    first slot reaches one step period back."""
    taken = slots(
        TopicSpec(steps.before(0.6, fps=5), match.all_in_step()), anchor=20 * TENTH
    )

    # Steps at 1.6, 1.8 and 2.0 s; slots (1.4, 1.6], (1.6, 1.8], (1.8, 2.0].
    assert taken == [
        (tenths(15, 16), False),
        (tenths(17, 18), False),
        (tenths(19, 20), False),
    ]


def test_after_window_slots_start_just_after_the_anchor():
    """An after-window's first slot starts just after the anchor, so a message
    logged on the anchor belongs to no slot."""
    taken = slots(
        TopicSpec(steps.after(0.2, fps=10), match.all_in_step()), anchor=20 * TENTH
    )

    assert taken == [([21 * TENTH], False), ([22 * TENTH], False)]


@pytest.mark.parametrize(
    "pad, middle",
    [
        # Both neighbours held messages; the earlier one is repeated.
        ("edge", (tenths(18), True)),
        ("zeros", ([], True)),
    ],
)
def test_an_empty_slot_is_padded(pad, middle):
    """A slot with no message, such as one over a gap in the recording, is
    padded as ``pad`` says, and marked as padded."""
    taken = slots(
        TopicSpec(steps.before(0.3, fps=10), match.all_in_step(), pad=pad),
        anchor=20 * TENTH,
        times=tenths(18, 20),
    )

    # Slots (1.7, 1.8], (1.8, 1.9] and (1.9, 2.0]: the middle one is empty.
    assert taken == [(tenths(18), False), middle, (tenths(20), False)]


@pytest.mark.parametrize(
    "window, anchor, times, nearest",
    [
        # Window (1.7, 2.0] s; of 1.2 and 1.5 s before it, 1.5 s is nearest.
        (steps.before(0.3, fps=10), 20 * TENTH, tenths(12, 15, 25), 15),
        # Window (2.0, 2.3] s; of 2.6 and 3.5 s after it, 2.6 s is nearest.
        (steps.after(0.3, fps=10), 20 * TENTH, tenths(15, 26, 35), 26),
        # Window (0.9, 1.2] s, past the clip's start at 1.0 s.
        (
            steps.before(0.3, fps=10, within="recording"),
            CLIP_START + 2 * TENTH,
            tenths(3, 6, 15),
            6,
        ),
        # Window (3.8, 4.1] s, past the clip's end at 4.0 s.
        (
            steps.after(0.3, fps=10, within="recording"),
            38 * TENTH,
            tenths(30, 45, 48),
            45,
        ),
    ],
)
def test_a_window_of_empty_slots_repeats_the_nearest_message_on_its_side(
    window, anchor, times, nearest
):
    """With ``pad="edge"``, a window whose slots are all empty repeats the
    message nearest the window on its side of the anchor in every step, and
    its steps are marked as padded."""
    taken = slots(TopicSpec(window, match.all_in_step()), anchor=anchor, times=times)

    assert taken == [(tenths(nearest), True)] * 3


def test_a_window_of_empty_slots_never_repeats_a_message_across_the_anchor():
    """With ``pad="edge"``, a window whose slots are all empty and with no
    message on its side of the anchor stays empty rather than repeat one
    logged after the anchor."""
    spec = TopicSpec(steps.before(0.3, fps=10), match.all_in_step())

    # The window (0.9, 1.2] s; the clip's only message, at 1.5 s, lies past
    # the anchor.
    taken = slots(spec, anchor=CLIP_START + 2 * TENTH, times=tenths(15))

    assert taken == [([], True)] * 3


def test_an_empty_slot_fails_the_read_with_raise():
    """With ``pad="raise"``, an empty slot fails the read, naming the sample."""
    with pytest.raises(ValueError, match="sample 's0'.*hold no message"):
        slots(
            TopicSpec(steps.before(0.3, fps=10), match.all_in_step(), pad="raise"),
            anchor=20 * TENTH,
            times=tenths(20),
        )


# -- the samples dataset and the row layout -----------------------------------

SAMPLES_SCHEMA = pa.schema(
    [
        ("sample_id", pa.string()),
        ("files", pa.list_(pa.string())),
        ("clip_start", pa.int64()),
        ("clip_end", pa.int64()),
        ("anchor", pa.int64()),
        ("instruction", pa.string()),
    ]
)


@pytest.mark.parametrize(
    "change, error",
    [
        (lambda s: s.remove(s.get_field_index("clip_end")), "lacks the columns"),
        (lambda s: s.set(1, pa.field("files", pa.string())), "list of paths"),
        (lambda s: s.set(2, pa.field("clip_start", pa.float64())), "integer"),
        (lambda s: s.append(pa.field("is_pad:/cam", pa.bool_())), "the read adds"),
        (lambda s: s.append(pa.field("__path", pa.string())), "the read adds"),
    ],
)
def test_check_samples_schema(change, error):
    """A samples dataset needs its files and clip columns with their types, and
    no column the read adds."""
    check_samples_schema(SAMPLES_SCHEMA, ["/cam"])

    with pytest.raises(ValueError, match=error):
        check_samples_schema(change(SAMPLES_SCHEMA), ["/cam"])


def test_check_samples_schema_needs_an_anchor_only_when_a_topic_measures_from_it():
    """Without an ``anchor`` column, a read whose topics all measure from the
    clip passes, and one with a topic that measures from the anchor fails,
    naming the topic."""
    no_anchor = SAMPLES_SCHEMA.remove(SAMPLES_SCHEMA.get_field_index("anchor"))

    check_samples_schema(no_anchor, ["/cam"], anchor_topics=[])
    with pytest.raises(ValueError, match=r"topics \['/cam'\] measure from it"):
        check_samples_schema(no_anchor, ["/cam"], anchor_topics=["/cam"])


def samples_batch(**changes):
    """Two valid samples as a batch of the samples dataset, with ``changes``
    replacing whole columns."""
    columns = {
        "sample_id": ["a", "b"],
        "files": [["x.mcap"], ["y.mcap"]],
        "clip_start": [0, 10],
        "clip_end": [100, 20],
        "anchor": [5, 15],
    }
    columns.update(changes)
    return pa.table(columns)


def manifest_of(batch, **options):
    """The one table ``sample_manifest`` yields for ``batch``."""
    (manifest,) = sample_manifest(batch, **options)
    return manifest


def test_sample_manifest_names_each_samples_file():
    """Each sample becomes a manifest row: its file as the path, no size and no
    chunks, with its own columns kept."""
    manifest = manifest_of(samples_batch())

    assert manifest.column("__path").to_pylist() == ["x.mcap", "y.mcap"]
    assert manifest.column("__file_size").null_count == 2
    assert manifest.column("__file_chunk_metadata").null_count == 2
    assert manifest.column("sample_id").to_pylist() == ["a", "b"]


@pytest.mark.parametrize(
    "changes, error, message",
    [
        ({"files": [["x.mcap"], ["y.mcap", "z.mcap"]]}, NotImplementedError, "2 files"),
        ({"files": [["x.mcap"], []]}, ValueError, "Sample 'b' lists no file"),
        (
            {"anchor": [5, 20]},
            ValueError,
            r"Sample 'b' has clip \[10, 20\) and anchor 20",
        ),
        ({"clip_end": [0, 20]}, ValueError, "Sample 'a' has clip"),
        ({"clip_start": [0, None]}, ValueError, "Sample 'b' has clip"),
    ],
)
def test_sample_manifest_rejects_bad_samples(changes, error, message):
    """A sample must list exactly one file, and its anchor must lie inside a
    clip that is not empty. The error names the first bad sample."""
    with pytest.raises(error, match=message):
        manifest_of(samples_batch(**changes))


def test_sample_manifest_without_anchors():
    """A sample may have no anchor when no topic measures from it, whether the
    column is missing or the value is null. When a topic does, a missing or null
    anchor fails, naming the sample."""
    without_column = samples_batch().drop(["anchor"])
    with_null = samples_batch(anchor=[5, None])

    assert manifest_of(without_column, needs_anchor=False).num_rows == 2
    assert manifest_of(with_null, needs_anchor=False).num_rows == 2
    with pytest.raises(ValueError, match="Sample 'b' has no anchor"):
        manifest_of(with_null, needs_anchor=True)
    with pytest.raises(ValueError, match="Sample 'a' has no anchor"):
        manifest_of(without_column, needs_anchor=True)


def test_sample_manifest_skips_only_the_bad_samples(monkeypatch):
    """When the job skips errors, a bad sample costs only its own row. The good
    rows come first, then one error counts the bad samples and names the first."""
    monkeypatch.setattr(DataContext.get_current(), "max_errored_blocks", 1)
    # 'b' lists no file and 'c' has its anchor at its clip's end. 'b' fails the
    # first check, so the later checks see only 'a', 'c' and 'd'.
    samples = samples_batch(
        sample_id=["a", "b", "c", "d"],
        files=[["x.mcap"], [], ["y.mcap"], ["z.mcap"]],
        clip_start=[0, 0, 10, 10],
        clip_end=[100, 100, 20, 20],
        anchor=[5, 5, 20, 15],
    )

    manifests = sample_manifest(samples)
    kept = next(manifests)
    with pytest.raises(ValueError) as error:
        next(manifests)

    assert kept.column("sample_id").to_pylist() == ["a", "d"]
    assert kept.column("__path").to_pylist() == ["x.mcap", "z.mcap"]
    assert str(error.value).startswith(
        "Skipped 2 bad samples of 4 in this batch, and kept the others. The "
        "first: ValueError: Sample 'b' lists no file"
    )


def test_sample_row_schema():
    """A row keeps the samples' columns, then each topic's in the order given:
    the payloads, log times and padding flags of its steps. With
    ``match.all_in_step``, a step's payloads and log times are lists of its
    slot's messages; its padding flag stays one per step."""
    plans = [
        TopicPlan.from_spec("/cam", TopicSpec(steps.before(frames=4, fps=10))),
        TopicPlan.from_spec("/joints", TopicSpec(steps.every(before_s=0.5))),
        TopicPlan.from_spec(
            "/lidar", TopicSpec(steps.before(0.5, fps=10), match.all_in_step())
        ),
    ]

    schema = sample_row_schema(SAMPLES_SCHEMA, plans)

    assert schema.names == SAMPLES_SCHEMA.names + [
        "data:/cam",
        "times:/cam",
        "is_pad:/cam",
        "data:/joints",
        "times:/joints",
        "is_pad:/joints",
        "data:/lidar",
        "times:/lidar",
        "is_pad:/lidar",
    ]
    assert schema.field("data:/joints").type == pa.list_(pa.large_binary())
    assert schema.field("times:/cam").type == pa.list_(pa.int64())
    assert schema.field("is_pad:/joints").type == pa.list_(pa.bool_())
    assert schema.field("data:/lidar").type == pa.list_(pa.list_(pa.large_binary()))
    assert schema.field("times:/lidar").type == pa.list_(pa.list_(pa.int64()))
    assert schema.field("is_pad:/lidar").type == pa.list_(pa.bool_())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
