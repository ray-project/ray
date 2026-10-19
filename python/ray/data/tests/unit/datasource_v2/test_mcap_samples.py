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
    resolve_steps,
    sample_manifest,
    sample_row_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.datasource.mcap import steps

SECOND = 1_000_000_000
TENTH = SECOND // 10  # the topic's message period
# Clip [1.0 s, 4.0 s); the default anchor lies between two messages.
CLIP_START, CLIP_END = 1 * SECOND, 4 * SECOND
ANCHOR = 2 * SECOND + 5 * SECOND // 100  # 2.05 s
WHERE = "sample 's0'"


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
    """The steps of ``spec`` for one sample, as ``(time, valid)`` pairs and the
    messages that fill them."""
    plan = TopicPlan.from_spec("/t", spec)
    resolved = resolve_steps(
        plan, topic(times), SampleTimes(clip[0], clip[1], anchor), WHERE
    )
    return [(s.time, s.valid) for s in resolved], [s.message for s in resolved]


# -- TopicPlan ----------------------------------------------------------------


@pytest.mark.parametrize(
    "spec, before, num_steps, span_ns",
    [
        (TopicSpec(steps.before(0.5, fps=10)), True, 5, None),
        (TopicSpec(steps.after(1, fps=Fraction(5))), False, 5, None),
        (TopicSpec(steps.before(frames=3, fps=10)), True, 3, None),
        (TopicSpec(steps.next(4)), False, 4, None),
        (TopicSpec(steps.every(before_s=0.25)), True, None, SECOND // 4),
    ],
)
def test_topic_plan_from_spec(spec, before, num_steps, span_ns):
    """Time steps and ``steps.last``/``steps.next`` have a fixed number of
    steps; ``steps.every`` is a span whose number of steps varies."""
    plan = TopicPlan.from_spec("/t", spec)

    assert (plan.before, plan.num_steps, plan.span_ns) == (before, num_steps, span_ns)


def test_step_times_count_from_the_anchor():
    """A before-window's last step is the anchor, and an after-window's first
    step is one step after it."""
    before = TopicPlan.from_spec("/t", TopicSpec(steps.before(0.3, fps=10)))
    after = TopicPlan.from_spec("/t", TopicSpec(steps.after(0.2, fps=10)))

    assert before.step_times(ANCHOR) == [ANCHOR - 2 * TENTH, ANCHOR - TENTH, ANCHOR]
    assert after.step_times(ANCHOR) == [ANCHOR + TENTH, ANCHOR + 2 * TENTH]


def test_steps_on_a_rounded_clock_take_their_own_frame():
    """Steps that fall on the frames of a 30 fps clock, which rounds each frame
    to the nanosecond, take those frames and not their neighbours."""
    frames = [round(k * Fraction(SECOND, 30)) for k in range(120)]
    anchor = frames[90]

    resolved, messages = resolve(
        TopicSpec(steps.before(frames=30, fps=30)), anchor=anchor, times=frames
    )

    assert messages == frames[61:91]
    assert all(valid for _, valid in resolved)


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
    assert [valid for _, valid in resolved] == [True] * 3


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


def test_gap_takes_the_nearest_message_on_its_side_of_the_anchor():
    """A step inside a gap in the recording takes the nearest message on its
    side of the anchor, however far, and its time shows how far."""
    times = tenths(10, 11, 12, 30, 31)

    resolved, messages = resolve(TopicSpec(steps.before(1.0, fps=5)), times=times)

    # Steps at 1.25, 1.45, 1.65, 1.85 and 2.05 s all fall in the gap, and the
    # messages after it lie past the anchor.
    assert messages == tenths(12, 12, 12, 12, 12)
    assert [valid for _, valid in resolved] == [True] * 5


@pytest.mark.parametrize(
    "pad, messages, valid",
    [
        # Steps at 0.85 and 0.95 s lie before the clip; 1.05 s onwards inside.
        ("edge", tenths(10, 10, 10, 11, 12), [False, False, True, True, True]),
        ("zeros", [None, None] + tenths(10, 11, 12), [False, False, True, True, True]),
    ],
)
def test_steps_before_the_clip_are_padded(pad, messages, valid):
    """Steps before the clip's start are padded: ``edge`` repeats the first
    real step, and ``zeros`` leaves them empty at their own times."""
    anchor = 12 * TENTH + 5 * SECOND // 100  # 1.25 s

    resolved, got = resolve(
        TopicSpec(steps.before(0.5, fps=10), pad=pad), anchor=anchor
    )

    assert got == messages
    assert [v for _, v in resolved] == valid
    if pad == "zeros":
        # An empty step keeps its own time.
        assert [t for t, _ in resolved[:2]] == [anchor - 4 * TENTH, anchor - 3 * TENTH]


def test_edge_repeats_the_nearest_real_step():
    """``edge`` fills a step outside the clip from the nearest step that took a
    message, and the steps inside take the clip's one message."""
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
    assert [valid for _, valid in resolved] == [False] + [True] * 5


def test_edge_without_real_steps_repeats_the_clips_edge_message():
    """A window where no step took a message repeats the clip's first message
    for a before-window and its last for an after-window."""
    times = tenths(25, 26, 30)

    before_steps, before = resolve(
        TopicSpec(steps.before(frames=2, fps=10)), times=times
    )
    _, after = resolve(
        TopicSpec(steps.after(frames=2, fps=10)), times=times, anchor=35 * TENTH
    )
    _, none = resolve(TopicSpec(steps.before(frames=2, fps=10)), times=tenths(45))

    assert before == tenths(25, 25)
    assert [v for _, v in before_steps] == [False, False]
    assert after == tenths(30, 30)
    # No message on the topic inside the clip: the steps stay empty.
    assert none == [None, None]


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
    assert [v for _, v in before_steps] == [False, False, True, True]
    assert after == tenths(38) + [None, None]
    assert after_steps[1:] == [(CLIP_END - 1, False)] * 2


def test_span_without_fps_takes_every_message_and_cuts_at_the_clip():
    """``before_s`` without ``fps`` holds every message in ``(anchor - span,
    anchor]``. It is cut at the clip's edge, or fails with ``raise``."""
    _, inside = resolve(TopicSpec(steps.every(before_s=0.3)))
    _, cut = resolve(TopicSpec(steps.every(before_s=2.0)))

    assert inside == tenths(18, 19, 20)
    assert cut == tenths(*range(10, 21))
    with pytest.raises(ValueError, match="0.95 s before the clip"):
        resolve(TopicSpec(steps.every(before_s=2.0), pad="raise"))


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
        (lambda s: s.remove(s.get_field_index("anchor")), "lacks the columns"),
        (lambda s: s.set(1, pa.field("files", pa.string())), "list of paths"),
        (lambda s: s.set(2, pa.field("clip_start", pa.float64())), "integer"),
        (lambda s: s.append(pa.field("valid:/cam", pa.bool_())), "the read adds"),
        (lambda s: s.append(pa.field("__path", pa.string())), "the read adds"),
    ],
)
def test_check_samples_schema(change, error):
    """A samples dataset needs its four columns with their types, and no column
    the read adds."""
    check_samples_schema(SAMPLES_SCHEMA, ["/cam"])

    with pytest.raises(ValueError, match=error):
        check_samples_schema(change(SAMPLES_SCHEMA), ["/cam"])


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


def test_sample_manifest_names_each_samples_file():
    """Each sample becomes a manifest row: its file as the path, no size and no
    chunks, with its own columns kept."""
    manifest = sample_manifest(samples_batch())

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
        sample_manifest(samples_batch(**changes))


def test_sample_row_schema():
    """A row keeps the samples' columns, then each topic's in the order given:
    the payloads, log times and validity of its steps."""
    plans = [
        TopicPlan.from_spec("/cam", TopicSpec(steps.before(frames=4, fps=10))),
        TopicPlan.from_spec("/joints", TopicSpec(steps.every(before_s=0.5))),
    ]

    schema = sample_row_schema(SAMPLES_SCHEMA, plans)

    assert schema.names == SAMPLES_SCHEMA.names + [
        "data:/cam",
        "times:/cam",
        "valid:/cam",
        "data:/joints",
        "times:/joints",
        "valid:/joints",
    ]
    assert schema.field("data:/joints").type == pa.list_(pa.large_binary())
    assert schema.field("times:/cam").type == pa.list_(pa.int64())
    assert schema.field("valid:/joints").type == pa.list_(pa.bool_())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
