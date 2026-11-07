"""Unit tests for sample rows: reading each topic's steps around a sample's anchor.

The tests drive the scanner and reader as a read task of
``read_mcap(samples=...)`` does, with no Ray cluster. Most read the
``recording`` fixture: a 10 fps camera whose payloads end with their frame
number, and a 50 Hz IMU whose payloads are their sample number.
"""

import importlib.util
import json
import os

import pytest

from ray.data._internal.datasource_v2.formats.mcap import mcap_datasource_v2
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_datasource_v2 import (
    MCAPDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_io import is_corrupt_data_error
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_rows import (
    ChunkTimeline,
    _ScanKeep,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.context import DataContext
from ray.data.datasource.mcap import match, steps
from ray.data.tests.unit.datasource_v2.mcap_testing import (
    SECOND,
    frame_time,
    iter_sample_rows,
    pack,
    read_all,
    read_samples,
    record_parsed_chunks,
    summary_of,
    write_corrupt_chunk,
    write_mcap,
    write_recording,
)

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("mcap") is None,
    reason="mcap module not available. Install with: pip install mcap",
)

TENTH = SECOND // 10
CLIP_START, CLIP_END = 1 * SECOND, 4 * SECOND
IMU_NS = SECOND // 50  # the IMU's sample period


def sample(path, anchor, sample_id="s", **columns):
    """A sample of ``path`` with the default clip, anchored at ``anchor``."""
    return {
        "sample_id": sample_id,
        "files": [path],
        "clip_start": CLIP_START,
        "clip_end": CLIP_END,
        "anchor": anchor,
        **columns,
    }


def frames(row, topic="/cam"):
    """The camera frame each step holds, ``None`` for an empty step."""
    return [int.from_bytes(d[-4:], "big") if d else None for d in row[f"data:{topic}"]]


def imu_samples(row):
    """The IMU sample each step holds, ``None`` for an empty step."""
    return [int.from_bytes(d, "big") if d else None for d in row["data:/imu"]]


def json_values(row, topic):
    """The ``t`` value of each step's JSON payload, ``None`` for an empty step."""
    return [json.loads(d)["t"] if d else None for d in row[f"data:{topic}"]]


def write_gap_file(path, index=True):
    """Write the ``gap_file`` fixture's messages to ``path``, with or without a
    chunk index; return ``path``."""
    messages = []
    for k in range(10, 31):
        if k <= 12 or k == 30:
            messages.append({"topic": "/a", "log_time": k * TENTH, "data": {"t": k}})
        messages.append({"topic": "/b", "log_time": k * TENTH, "data": {"t": k}})
    write_mcap(path, messages, index=index)
    return path


@pytest.fixture
def gap_file(tmp_path):
    """Two JSON topics, one message per chunk, ``/a`` with a gap::

        seconds  1.0  1.1  1.2  1.3 ... 2.9  3.0
        /a       a    a    a                 a
        /b       b    b    b    b   ... b    b

    Each message's ``t`` is its log time in tenths of a second.
    """
    return write_gap_file(os.path.join(tmp_path, "gap.mcap"))


def chunks_of(path, topic):
    """The offsets of the chunks that hold ``topic``, one message per chunk."""
    summary = summary_of(path)
    ids = {cid for cid, c in summary.channels.items() if c.topic == topic}
    return {
        c.chunk_start_offset
        for c in summary.chunk_indexes
        if not ids.isdisjoint(c.message_index_offsets)
    }


def test_rows_hold_each_topics_steps(recording):
    """Each row keeps its sample's columns, and per topic the payload, log time and
    padding flag of every step: the camera's frames at 10 fps up to the anchor, and
    the IMU's next two samples after it."""
    topics = {
        "/cam": TopicSpec(steps.before(0.3, fps=10)),
        "/imu": TopicSpec(steps.next(2)),
    }
    anchors = [frame_time(15), frame_time(25) + 5_000_000]

    rows = read_samples(
        [sample(recording, a, f"s{i}", label=i) for i, a in enumerate(anchors)], topics
    )

    assert [(row["sample_id"], row["label"]) for row in rows] == [("s0", 0), ("s1", 1)]
    assert [frames(row) for row in rows] == [[13, 14, 15], [23, 24, 25]]
    assert rows[0]["times:/cam"] == [frame_time(k) for k in (13, 14, 15)]
    assert rows[0]["is_pad:/cam"] == [False, False, False]
    # The IMU logs every 20 ms, so the samples after 1.5 s are 76 and 77.
    assert [imu_samples(row) for row in rows] == [[76, 77], [126, 127]]
    assert rows[1]["times:/imu"] == [126 * IMU_NS, 127 * IMU_NS]


def test_files_in_manifest_order_samples_in_anchor_order(tmp_path):
    """A task reads its files in the order the manifest first names them, and
    each file's samples in anchor order."""
    a, b = (os.path.join(tmp_path, f"{name}.mcap") for name in "ab")
    write_recording(a)
    write_recording(b)
    samples = [
        sample(b, 3 * SECOND, "b3"),
        sample(a, 2 * SECOND, "a2"),
        sample(b, frame_time(15), "b1.5"),
    ]

    rows = read_samples(samples, {"/cam": TopicSpec(steps.before(frames=1, fps=10))})

    assert [row["sample_id"] for row in rows] == ["b1.5", "b3", "a2"]


def test_raise_names_the_sample_and_its_file(recording):
    """``pad="raise"`` fails the read on a sample whose window reaches past its
    clip, naming the sample and its file."""
    topics = {"/cam": TopicSpec(steps.before(0.5, fps=10), pad="raise")}

    with pytest.raises(ValueError, match=r"Sample 'early' of .*run\.mcap.*3 of 5"):
        read_samples([sample(recording, frame_time(11), "early")], topics)


@pytest.mark.parametrize(
    "topics, look_back",
    [
        # The camera's window starts 0.4 s before the anchor.
        (
            {
                "/cam": TopicSpec(steps.before(0.5, fps=10)),
                "/imu": TopicSpec(steps.every(before_s=0.2)),
            },
            4 * TENTH,
        ),
        # ``steps.last(5)`` is read over five of the camera's mean frame gaps.
        ({"/cam": TopicSpec(steps.last(5))}, 5 * TENTH),
    ],
    ids=["time_steps", "last"],
)
def test_nearby_samples_share_their_chunks(recording, monkeypatch, topics, look_back):
    """Samples whose windows meet are read in one pass: no chunk is parsed
    twice, and none outside their windows is parsed."""
    parsed = record_parsed_chunks(monkeypatch)
    anchors = [frame_time(k) for k in range(15, 26)]

    rows = read_samples([sample(recording, a) for a in anchors], topics)

    assert [frames(row)[-1] for row in rows] == list(range(15, 26))
    assert len(parsed) == len(set(parsed))
    # The first window starts ``look_back`` before the first anchor, the last
    # ends at the last anchor.
    low, high = anchors[0] - look_back, anchors[-1]
    near = {
        c.chunk_start_offset
        for c in summary_of(recording).chunk_indexes
        if c.message_end_time >= low and c.message_start_time <= high
    }
    assert set(parsed) <= near


@pytest.mark.parametrize(
    "spec, anchor, expected",
    [
        (TopicSpec(steps.before(frames=2, fps=10)), 25 * TENTH, [12, 12]),
        # Steps at 2.8 and 2.9 s take 1.2 s, though the window's span starts
        # after the gap; nearest would take 3.0 s.
        (
            TopicSpec(steps.before(frames=3, fps=10), match.backward()),
            30 * TENTH,
            [12, 12, 30],
        ),
    ],
    ids=["nearest", "backward"],
)
def test_a_step_in_a_gap_reads_back_to_the_last_message(
    gap_file, monkeypatch, spec, anchor, expected
):
    """A step in a gap takes the topic's last message before the gap, read
    back chunk by chunk through the chunks that hold the topic alone."""
    parsed = record_parsed_chunks(monkeypatch)

    (row,) = read_samples([sample(gap_file, anchor)], {"/a": spec})

    assert json_values(row, "/a") == expected
    assert not any(row["is_pad:/a"])
    # Only the chunks of /a are read, not those of /b.
    assert set(parsed) <= chunks_of(gap_file, "/a")


@pytest.mark.parametrize(
    "spec, anchor, expected",
    [
        # The last three /a up to 2.5 s lie before the gap.
        (TopicSpec(steps.last(3)), 25 * TENTH, [10, 11, 12]),
        # The next two /a after 1.15 s lie on both sides of the gap.
        (TopicSpec(steps.next(2)), 11 * TENTH + 1, [12, 30]),
        # Both slots before 2.5 s are empty, so ``edge`` repeats the last /a
        # before them, at 1.2 s.
        (
            TopicSpec(steps.before(frames=2, fps=10), match.all_in_step(), pad="edge"),
            25 * TENTH,
            [[12], [12]],
        ),
        (TopicSpec(steps.every(before_s=1.0)), 30 * TENTH, [30]),
    ],
    ids=["last_across_gap", "next_across_gap", "edge_before_the_gap", "every"],
)
def test_a_file_without_a_chunk_index_gives_the_same_rows(
    tmp_path, spec, anchor, expected
):
    """A file without a chunk index is scanned once, keeping only what the
    steps can take, and gives the same rows as the file read by its index.
    That holds for steps that reach across a gap and for an edge pad."""
    indexed = write_gap_file(os.path.join(tmp_path, "i.mcap"))
    unindexed = write_gap_file(os.path.join(tmp_path, "u.mcap"), index=False)

    rows = {
        path: read_samples([sample(path, anchor)], {"/a": spec})[0]
        for path in (indexed, unindexed)
    }

    for column in ("data:/a", "times:/a", "is_pad:/a"):
        assert rows[unindexed][column] == rows[indexed][column]
    data = rows[unindexed]["data:/a"]
    if isinstance(data[0], list):
        assert [[json.loads(d)["t"] for d in step] for step in data] == expected
    else:
        assert json_values(rows[unindexed], "/a") == expected


def test_a_file_without_a_chunk_index_keeps_only_what_steps_take(tmp_path, monkeypatch):
    """The scan of a file without a chunk index keeps a sample's frames, the
    nearest frame on each side of them, and the first and last frames of its
    clip, not the whole file."""
    path = os.path.join(tmp_path, "u.mcap")
    write_recording(path, index=False)
    real_kept = _ScanKeep.kept
    kept_times = []

    def kept(self):
        timed = real_kept(self)
        kept_times.extend(time for time, _ in timed)
        return timed

    monkeypatch.setattr(_ScanKeep, "kept", kept)

    (row,) = read_samples(
        [sample(path, frame_time(20))],
        {"/cam": TopicSpec(steps.before(frames=2, fps=10))},
    )

    assert frames(row) == [19, 20]
    # The clip runs from frame 10 to just before frame 40.
    assert kept_times == [frame_time(k) for k in (10, 18, 19, 20, 21, 39)]


@pytest.mark.parametrize(
    "spec, long_frames",
    [
        (TopicSpec(steps.every()), list(range(50))),
        # Ten steps from 0 s to the clip's last nanosecond, 5/9 s apart.
        (TopicSpec(steps.across(10)), [0, 6, 11, 17, 22, 28, 33, 39, 44, 49]),
    ],
    ids=["every", "across"],
)
def test_a_file_without_a_chunk_index_reads_a_long_clip_after_short_ones(
    tmp_path, spec, long_frames
):
    """A clip of the whole recording, anchored after three short clips, joins
    the last short clip's run and stretches it back over the other two. The
    scan still keeps every frame of it, so the rows equal the indexed file's."""
    indexed = os.path.join(tmp_path, "i.mcap")
    unindexed = os.path.join(tmp_path, "u.mcap")
    write_recording(indexed)
    write_recording(unindexed, index=False)
    # Three short clips far apart, then the whole recording, anchored last.
    clips = [(5, 8), (15, 18), (25, 28), (0, 50)]
    catalog = [
        {
            "sample_id": f"s{i}",
            "clip_start": frame_time(start),
            "clip_end": frame_time(end),
            "anchor": frame_time(end) - 1,
        }
        for i, (start, end) in enumerate(clips)
    ]

    rows = {
        path: read_samples([dict(s, files=[path]) for s in catalog], {"/cam": spec})
        for path in (indexed, unindexed)
    }

    for column in ("sample_id", "data:/cam", "times:/cam", "is_pad:/cam"):
        assert [r[column] for r in rows[unindexed]] == [
            r[column] for r in rows[indexed]
        ]
    assert frames(rows[unindexed][-1]) == long_frames


def test_topic_missing_from_the_file(recording):
    """A topic the file lacks leaves every step empty and padded, at the clip's
    edge for a frame count without ``fps``."""
    (row,) = read_samples(
        [sample(recording, 2 * SECOND)], {"/nope": TopicSpec(steps.next(2))}
    )

    assert row["data:/nope"] == [b"", b""]
    assert row["is_pad:/nope"] == [True, True]
    assert row["times:/nope"] == [CLIP_END - 1, CLIP_END - 1]


def test_projection_reads_only_the_wanted_topics(gap_file, monkeypatch):
    """A projection that keeps one topic's columns reads only that topic's
    chunks and builds only its kept columns."""
    parsed = record_parsed_chunks(monkeypatch)
    topics = {
        "/a": TopicSpec(steps.before(frames=1, fps=10)),
        "/b": TopicSpec(steps.before(frames=1, fps=10)),
    }

    rows = read_samples(
        [sample(gap_file, 11 * TENTH)], topics, columns=["is_pad:/b", "sample_id"]
    )

    assert rows == [{"is_pad:/b": [False], "sample_id": "s"}]
    assert set(parsed) <= chunks_of(gap_file, "/b")


def test_projection_to_the_samples_columns_reads_no_file(tmp_path):
    """A projection that keeps only the samples' own columns, as ``count()``
    does, needs no message: the files are never opened."""
    missing = os.path.join(tmp_path, "never-written.mcap")
    samples = [sample(missing, 2 * SECOND, f"s{i}") for i in range(3)]
    topics = {"/cam": TopicSpec(steps.last(1))}

    rows = read_samples(samples, topics, columns=["sample_id"])
    counted = read_samples(samples, topics, columns=[])

    assert [row["sample_id"] for row in rows] == ["s0", "s1", "s2"]
    assert len(counted) == 3


def chunk_index_at(path, topic, time):
    """The index, in the summary, of the chunk of ``topic`` logged over ``time``."""
    summary = summary_of(path)
    ids = {cid for cid, c in summary.channels.items() if c.topic == topic}
    return next(
        i
        for i, c in enumerate(summary.chunk_indexes)
        if c.message_start_time <= time <= c.message_end_time
        and not ids.isdisjoint(c.message_index_offsets)
    )


@pytest.mark.parametrize("max_errored_blocks", [0, 1])
def test_a_corrupt_chunk_fails_a_sample_read(
    recording, tmp_path, monkeypatch, max_errored_blocks
):
    """A sample whose steps need a corrupt chunk fails the read, even when
    errors can be skipped, and the file's other samples get no row. Ray's
    ``max_errored_blocks`` then decides whether the task's block is dropped or
    the dataset fails."""
    monkeypatch.setattr(
        DataContext.get_current(), "max_errored_blocks", max_errored_blocks
    )
    index = chunk_index_at(recording, "/cam", frame_time(25))
    path = write_corrupt_chunk(os.path.join(tmp_path, "bad.mcap"), recording, index)
    samples = [sample(path, frame_time(12), "good"), sample(path, frame_time(25))]

    rows = []
    with pytest.raises(Exception) as error:
        for row in iter_sample_rows(samples, {"/cam": TopicSpec(steps.last(2))}):
            rows.append(row)

    assert rows == []
    assert is_corrupt_data_error(error.value)


def chunk_bytes(path, topic):
    """The bytes a run counts for each chunk of ``topic`` once read, by offset."""
    reader = SelectedMessageReader(MCAPSelection.create([topic], None, None), False)
    timeline = ChunkTimeline(None, path, summary_of(path), reader, [topic])
    return timeline.chunk_bytes(topic, 0, 10 * SECOND)


def record_held_chunks(monkeypatch):
    """Record the offsets of the chunks a ``ChunkTimeline`` holds after each read."""
    real_read = ChunkTimeline._read
    held = []

    def _read(self, chunks):
        real_read(self, chunks)
        held.append(set(self._loaded))

    monkeypatch.setattr(ChunkTimeline, "_read", _read)
    return held


def test_a_run_lets_its_chunks_go_at_the_budget(recording, monkeypatch):
    """Back-to-back windows across the clip meet, so their windows alone make
    one run. With a budget of two chunks, the run ends whenever its chunks
    reach the budget and lets them go, so the chunks held never pass it."""
    sizes = chunk_bytes(recording, "/cam")
    budget = 2 * max(sizes.values())
    monkeypatch.setattr(mcap_datasource_v2, "_BIN_PACKING_BYTES", budget)
    held = record_held_chunks(monkeypatch)
    # Each window is 0.1 s and ends where the next one starts.
    anchors = [frame_time(k) for k in range(11, 40)]

    rows = read_samples(
        [sample(recording, a) for a in anchors],
        {"/cam": TopicSpec(steps.before(frames=2, fps=10))},
    )

    assert [frames(row) for row in rows] == [[k - 1, k] for k in range(11, 40)]
    assert max(sum(sizes[offset] for offset in chunks) for chunks in held) <= budget
    # The clip's chunks are read, so the budget was what kept them few.
    assert len(set().union(*held)) > 4


def test_limit_reads_only_the_chunks_of_its_rows(recording, monkeypatch):
    """With a pushed-down limit, the task reads only the chunks of the samples
    whose rows it returns, not those of the samples after them."""
    parsed = record_parsed_chunks(monkeypatch)
    samples = [sample(recording, frame_time(k)) for k in range(12, 40, 4)]
    spec = {"/cam": TopicSpec(steps.last(1))}

    read_samples(samples[:1], spec)
    first_only = set(parsed)
    parsed.clear()
    rows = read_samples(samples, spec, limit=1)

    assert [frames(row) for row in rows] == [[12]]
    assert set(parsed) == first_only


def test_row_over_the_row_limit_fails(recording, monkeypatch):
    """A row whose payloads pass ``RAY_DATA_MCAP_MAX_ROW_BYTES`` fails the read."""
    monkeypatch.setenv("RAY_DATA_MCAP_MAX_ROW_BYTES", "100")

    with pytest.raises(ValueError, match="RAY_DATA_MCAP_MAX_ROW_BYTES"):
        read_samples(
            [sample(recording, 3 * SECOND)],
            {"/imu": TopicSpec(steps.every(before_s=1.0))},
        )


def test_file_uris_resolve_in_the_task(recording):
    """A sample may name its file by URI: the task resolves its filesystem."""
    (row,) = read_samples(
        [sample(f"file://{recording}", frame_time(20))],
        {"/cam": TopicSpec(steps.before(frames=2, fps=10))},
    )

    assert frames(row) == [19, 20]


def test_a_path_that_does_not_resolve_is_named(recording):
    """A sample whose path has no filesystem fails the read with an error
    naming that path, not an unpacking error."""
    unresolvable = "nosuchscheme://bucket/recording.mcap"

    with pytest.raises(ValueError, match="nosuchscheme://bucket/recording.mcap"):
        read_samples(
            [sample(recording, frame_time(20)), sample(unresolvable, frame_time(20))],
            {"/cam": TopicSpec(steps.before(frames=2, fps=10))},
        )


def test_tiled_windows_hold_the_message_rows(recording):
    """Samples that tile the recording into back-to-back windows, each taking
    every message of its window, hold exactly the message rows of the same file
    grouped by window, in log-time order.

    This is the samples table a fixed-window read amounts to.
    """
    topics = ["/cam", "/imu"]
    window_ns = SECOND // 2
    statistics = summary_of(recording).statistics
    assert statistics is not None
    start, end = statistics.message_start_time, statistics.message_end_time + 1
    # The anchor sits on a window's last nanosecond, so ``steps.every`` over the
    # window's length covers it exactly.
    windows = [(t, min(t + window_ns, end)) for t in range(start, end, window_ns)]
    samples = [
        {"files": [recording], "clip_start": a, "clip_end": b, "anchor": b - 1}
        for a, b in windows
    ]
    datasource = MCAPDatasourceV2([recording], topics=topics, include_metadata=False)

    rows = read_samples(
        samples,
        {t: TopicSpec(steps.every(before_s=window_ns / SECOND)) for t in topics},
    )
    messages = read_all(datasource, *pack(datasource)).to_pylist()

    assert len(rows) == len(windows)
    for (a, b), row in zip(windows, sorted(rows, key=lambda r: r["clip_start"])):
        for t in topics:
            expected = [
                m for m in messages if m["topic"] == t and a <= m["log_time"] < b
            ]
            assert row[f"times:{t}"] == [m["log_time"] for m in expected]
            assert row[f"data:{t}"] == [m["data"] for m in expected]
            assert not any(row[f"is_pad:{t}"])


def test_back_to_back_clips_are_read_in_one_pass(recording, monkeypatch):
    """Clips that tile the recording touch without overlapping. They form one
    run, so their chunks come in as few ranged reads as one long window's."""
    from ray.data._internal.datasource_v2.formats.mcap import mcap_io

    reads = []
    real = mcap_io._read_range
    monkeypatch.setattr(
        mcap_io, "_read_range", lambda f, a, b: reads.append((a, b)) or real(f, a, b)
    )
    half = SECOND // 2
    tiled = [
        {
            "files": [recording],
            "clip_start": t,
            "clip_end": t + half,
            "anchor": t + half - 1,
        }
        for t in range(0, 5 * SECOND, half)
    ]
    topics = {"/imu": TopicSpec(steps.every(before_s=0.5))}

    read_samples(tiled, topics)
    tiled_reads = len(reads)
    reads.clear()
    read_samples(
        [dict(tiled[0], clip_end=5 * SECOND, anchor=5 * SECOND - 1)],
        {"/imu": TopicSpec(steps.every(before_s=5))},
    )

    assert tiled_reads == len(reads)


def test_samples_without_an_anchor_column_read(recording):
    """Samples with no anchor column read when every topic measures from the
    clip: each row holds the steps across its own clip."""
    # Clips of 1 s and 3 s, as a labeling catalog holds them.
    short = {
        "sample_id": "short",
        "files": [recording],
        "clip_start": frame_time(10),
        "clip_end": frame_time(20),
    }
    long = {
        "sample_id": "long",
        "files": [recording],
        "clip_start": frame_time(10),
        "clip_end": frame_time(40),
    }

    rows = read_samples([short, long], {"/cam": TopicSpec(steps.across(4))})

    # Four evenly spaced steps from the clip's first nanosecond to its last,
    # each taking the nearest frame.
    assert {row["sample_id"]: frames(row) for row in rows} == {
        "short": [10, 13, 17, 19],
        "long": [10, 20, 30, 39],
    }


def test_all_in_step_rows_hold_a_list_per_step(recording):
    """With ``match.all_in_step``, each step of a row holds every IMU sample of
    its slot, payloads and log times alike, while other topics keep one message
    per step."""
    topics = {
        "/imu": TopicSpec(steps.before(0.1, fps=20), match.all_in_step()),
        "/cam": TopicSpec(steps.before(frames=1, fps=10)),
    }

    (row,) = read_samples([sample(recording, 2 * SECOND)], topics)

    # Steps at 1.95 and 2.0 s; slots (1.9, 1.95] and (1.95, 2.0] of a 50 Hz IMU.
    expected = [[1_920, 1_940], [1_960, 1_980, 2_000]]
    assert row["times:/imu"] == [[ms * SECOND // 1_000 for ms in s] for s in expected]
    assert row["is_pad:/imu"] == [False, False]
    assert [[int.from_bytes(d, "big") for d in s] for s in row["data:/imu"]] == [
        [ms // 20 for ms in s] for s in expected
    ]
    assert frames(row) == [20]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
