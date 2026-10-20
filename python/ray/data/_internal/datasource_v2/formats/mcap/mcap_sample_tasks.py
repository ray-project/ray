"""How many samples one read task of ``read_mcap(samples=...)`` takes.

A read task should hold about ``RAY_DATA_MCAP_BIN_PACKING_BYTES`` (128 MiB by
default) of decompressed chunks, as a read of paths does. Planning estimates
what the first samples read: for each, the bytes of the chunks that its topics'
steps reach, from the file's chunk index. The budget divided by their median
is the number of samples per task::

    budget 128 MiB / median sample 2 MiB  ->  64 samples per task

The estimate reads the summary of each file the probed samples name, once.
``RAY_DATA_MCAP_SAMPLES_PER_TASK`` fixes the count instead.
"""

import statistics
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Dict, Optional, Sequence, Set, Union

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.mcap.mcap_samples import (
    CLIP_END_COLUMN,
    CLIP_START_COLUMN,
    FILES_COLUMN,
    SampleTimes,
    TopicPlan,
    sample_anchors,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import read_summary

if TYPE_CHECKING:
    from mcap.summary import Summary
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.formats.mcap.mcap_sample_rows import (
        SamplePaths,
    )

# How many samples planning estimates, from the start of the samples dataset.
PROBE_SAMPLES = 256
# The most samples one task takes, however small each one is.
MAX_SAMPLES_PER_TASK = 1024
# The samples per task when none of the probed samples can be estimated.
DEFAULT_SAMPLES_PER_TASK = 64
# How many file summaries the estimate reads at once.
_SUMMARY_READ_THREADS = 16


def samples_per_task(
    probe: pa.Table,
    plans: Sequence[TopicPlan],
    paths: "SamplePaths",
    budget: int,
) -> int:
    """The number of samples per read task, so a task holds about ``budget``
    bytes of chunks.

    ``probe`` holds the first samples. A sample whose file has no chunk index
    weighs its whole file. A file that cannot be opened is left out of the
    estimate: the read reports it, unless a checkpoint skips its samples.
    Without any sample to estimate, a task takes ``DEFAULT_SAMPLES_PER_TASK``.
    """
    files = probe.column(FILES_COLUMN).to_pylist()
    starts = probe.column(CLIP_START_COLUMN).to_pylist()
    ends = probe.column(CLIP_END_COLUMN).to_pylist()
    anchors = sample_anchors(probe)
    named = sorted({f[0] for f in files if f})
    resolved = [paths.resolve(path) for path in named]
    # The summaries are read at once: each is a round trip to storage.
    with ThreadPoolExecutor(max_workers=_SUMMARY_READ_THREADS) as pool:
        weights = pool.map(lambda r: _summary_or_size(r[1], r[0]), resolved)
        summaries = dict(zip(named, weights))
    sizes = []
    for paths_of_sample, start, end, anchor in zip(files, starts, ends, anchors):
        if not paths_of_sample:
            continue
        weight = summaries[paths_of_sample[0]]
        if weight is not None:
            sizes.append(sample_bytes(weight, SampleTimes(start, end, anchor), plans))
    if not sizes:
        return DEFAULT_SAMPLES_PER_TASK
    median = max(1, int(statistics.median(sizes)))
    return max(1, min(MAX_SAMPLES_PER_TASK, budget // median))


def sample_bytes(
    summary: Union["Summary", int], sample: SampleTimes, plans: Sequence[TopicPlan]
) -> int:
    """The decompressed bytes of the chunks one sample's steps reach.

    ``summary`` is the file's summary, or its size in bytes when it has no
    chunk index. Each topic counts the chunks that hold it and overlap the span
    its steps fall in (``TopicPlan.reach``). A chunk two topics share counts
    once.
    """
    if isinstance(summary, int):
        return summary
    channels_by_topic: Dict[str, Set[int]] = {}
    for channel_id, channel in summary.channels.items():
        channels_by_topic.setdefault(channel.topic, set()).add(channel_id)
    reached: Dict[int, int] = {}
    for plan in plans:
        channels = channels_by_topic.get(plan.topic, set())
        low, high = plan.reach(sample)
        for chunk in summary.chunk_indexes:
            if chunk.message_end_time < low or chunk.message_start_time > high:
                continue
            held = chunk.message_index_offsets.keys()
            if held and channels.isdisjoint(held):
                continue
            reached[chunk.chunk_start_offset] = chunk.uncompressed_size
    return sum(reached.values())


def _summary_or_size(
    filesystem: "FileSystem", path: str
) -> Optional[Union["Summary", int]]:
    """The file's summary when it has a chunk index, else its size in bytes, or
    ``None`` when it cannot be opened."""
    try:
        summary = read_summary(filesystem, path)
    except OSError:
        return None
    if summary is not None and summary.chunk_indexes:
        return summary
    return filesystem.get_file_info(path).size
