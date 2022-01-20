from contextlib import contextmanager
from typing import List, Optional, Dict, Set, Tuple, Union
import time
import collections
import numpy as np

import ray
from ray.data.block import BlockMetadata
from ray.data.impl.block_list import BlockList


def fmt(seconds: float) -> str:
    if seconds > 1:
        return str(round(seconds, 2)) + "s"
    elif seconds > 0.001:
        return str(round(seconds * 1000, 2)) + "ms"
    else:
        return str(round(seconds * 1000 * 1000, 2)) + "us"


class Timer:
    """Helper class for tracking accumulated time (in seconds)."""

    def __init__(self):
        self._value: float = 0

    @contextmanager
    def timer(self) -> None:
        time_start = time.perf_counter()
        try:
            yield
        finally:
            self._value += time.perf_counter() - time_start

    def add(self, value: float) -> None:
        self._value += value

    def get(self) -> float:
        return self._value


class _DatasetStatsBuilder:
    """Helper class for building dataset stats.

    When this class is created, we record the start time. When build() is
    called with the final blocks of the new dataset, the time delta is
    saved as part of the stats."""

    def __init__(self, stage_name: str, parent: "DatasetStats"):
        self.stage_name = stage_name
        self.parent = parent
        self.start_time = time.perf_counter()

    def build_multistage(
            self, stages: Dict[str, List[BlockMetadata]]) -> "DatasetStats":
        stats = DatasetStats(
            stages={self.stage_name + "_" + k: v
                    for k, v in stages.items()},
            parent=self.parent)
        stats.time_total_s = time.perf_counter() - self.start_time
        return stats

    def build(self, final_blocks: BlockList) -> "DatasetStats":
        stats = DatasetStats(
            stages={self.stage_name: final_blocks.get_metadata()},
            parent=self.parent)
        stats.time_total_s = time.perf_counter() - self.start_time
        return stats


@ray.remote(num_cpus=0)
class _StatsActor:
    """Actor holding stats for blocks created by LazyBlockList.

    This actor is shared across all datasets created by the same process.
    The stats data is small so we don't worry about clean up for now.

    TODO(ekl) we should consider refactoring LazyBlockList so stats can be
    extracted without using an out-of-band actor."""

    def __init__(self):
        # Mapping from uuid -> dataset-specific stats.
        self.metadata = collections.defaultdict(dict)
        self.last_time = {}
        self.start_time = {}

    def record_start(self, stats_uuid):
        self.start_time[stats_uuid] = time.perf_counter()

    def record_task(self, stats_uuid, i, metadata):
        self.metadata[stats_uuid][i] = metadata
        self.last_time[stats_uuid] = time.perf_counter()

    def get(self, stats_uuid):
        if stats_uuid not in self.metadata:
            return {}, 0.0
        return self.metadata[stats_uuid], \
            self.last_time[stats_uuid] - self.start_time[stats_uuid]


# Actor handle, job id the actor was created for.
_stats_actor = [None, None]


def get_or_create_stats_actor():
    # Need to re-create it if Ray restarts (mostly for unit tests).
    if (not _stats_actor[0] or not ray.is_initialized()
            or _stats_actor[1] != ray.get_runtime_context().job_id.hex()):
        _stats_actor[0] = _StatsActor.remote()
        _stats_actor[1] = ray.get_runtime_context().job_id.hex()

    # Clear the actor handle after Ray reinits since it's no longer
    # valid.
    def clear_actor():
        _stats_actor[0] = None

    ray.worker._post_init_hooks.append(clear_actor)
    return _stats_actor[0]


class DatasetStats:
    """Holds the execution times for a given Dataset.

    This object contains a reference to the parent Dataset's stats as well,
    but not the Dataset object itself, to allow its blocks to be dropped from
    memory."""

    def __init__(self,
                 *,
                 stages: Dict[str, List[BlockMetadata]],
                 parent: Union[Optional["DatasetStats"], List["DatasetStats"]],
                 stats_actor=None,
                 stats_uuid=None):
        """Create dataset stats.

        Args:
            stages: Dict of stages used to create this Dataset from the
                previous one. Typically one entry, e.g., {"map": [...]}.
            parent: Reference to parent Dataset's stats, or a list of parents
                if there are multiple.
            stats_actor: Reference to actor where stats should be pulled
                from. This is only used for Datasets using LazyBlockList.
            stats_uuid: The uuid for the stats, used to fetch the right stats
                from the stats actor.
        """

        self.stages: Dict[str, List[BlockMetadata]] = stages
        self.parents: List["DatasetStats"] = []
        if parent:
            if isinstance(parent, list):
                self.parents.extend(parent)
            else:
                self.parents.append(parent)
        self.number: int = (0 if not self.parents else
                            max(p.number for p in self.parents) + 1)
        self.dataset_uuid: str = None
        self.time_total_s: float = 0
        self.stats_actor = stats_actor
        self.stats_uuid = stats_uuid

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: Timer = Timer()
        self.iter_get_s: Timer = Timer()
        self.iter_format_batch_s: Timer = Timer()
        self.iter_user_s: Timer = Timer()
        self.iter_total_s: Timer = Timer()

    def child_builder(self, name: str) -> _DatasetStatsBuilder:
        """Start recording stats for an op of the given name (e.g., map)."""
        return _DatasetStatsBuilder(name, self)

    def child_TODO(self, name: str) -> "DatasetStats":
        """Placeholder for child ops not yet instrumented."""
        return DatasetStats(stages={name + "_TODO": []}, parent=self)

    @staticmethod
    def TODO():
        """Placeholder for ops not yet instrumented."""
        return DatasetStats(stages={"TODO": []}, parent=None)

    def summary_string(self, already_printed: Set[str] = None) -> str:
        """Return a human-readable summary of this Dataset's stats."""
        if already_printed is None:
            already_printed = set()

        if self.stats_actor:
            # XXX this is a super hack, clean it up.
            stats_map, self.time_total_s = ray.get(
                self.stats_actor.get.remote(self.stats_uuid))
            for i, metadata in stats_map.items():
                self.stages["read"][i] = metadata
        out = ""
        if self.parents:
            for p in self.parents:
                out += p.summary_string(already_printed)
                out += "\n"
        first = True
        for stage_name, metadata in sorted(self.stages.items()):
            stage_uuid = self.dataset_uuid + stage_name
            if first:
                first = False
            else:
                out += "\n"
            out += "Stage {} {}: ".format(self.number, stage_name)
            if stage_uuid in already_printed:
                out += "[execution cached]"
            else:
                already_printed.add(stage_uuid)
                out += self._summarize_blocks(metadata)
        out += self._summarize_iter()
        return out

    def _summarize_iter(self) -> str:
        out = ""
        if (self.iter_total_s.get() or self.iter_wait_s.get()
                or self.iter_format_batch_s.get() or self.iter_get_s.get()):
            out += "\nDataset iterator time breakdown:\n"
            out += "* In ray.wait(): {}\n".format(fmt(self.iter_wait_s.get()))
            out += "* In ray.get(): {}\n".format(fmt(self.iter_get_s.get()))
            out += "* In format_batch(): {}\n".format(
                fmt(self.iter_format_batch_s.get()))
            out += "* In user code: {}\n".format(fmt(self.iter_user_s.get()))
            out += "* Total time: {}\n".format(fmt(self.iter_total_s.get()))
        return out

    def _summarize_blocks(self, blocks: List[BlockMetadata]) -> str:
        exec_stats = [m.exec_stats for m in blocks if m.exec_stats is not None]
        out = "{}/{} blocks executed in {}s\n".format(
            len(exec_stats), len(blocks), round(self.time_total_s, 2))

        if exec_stats:
            out += ("* Remote wall time: {} min, {} max, {} mean, {} total\n".
                    format(
                        fmt(min([e.wall_time_s for e in exec_stats])),
                        fmt(max([e.wall_time_s for e in exec_stats])),
                        fmt(np.mean([e.wall_time_s for e in exec_stats])),
                        fmt(sum([e.wall_time_s for e in exec_stats]))))

            out += ("* Remote cpu time: {} min, {} max, {} mean, {} total\n".
                    format(
                        fmt(min([e.cpu_time_s for e in exec_stats])),
                        fmt(max([e.cpu_time_s for e in exec_stats])),
                        fmt(np.mean([e.cpu_time_s for e in exec_stats])),
                        fmt(sum([e.cpu_time_s for e in exec_stats]))))

        output_num_rows = [
            m.num_rows for m in blocks if m.num_rows is not None
        ]
        if output_num_rows:
            out += ("* Output num rows: {} min, {} max, {} mean, {} total\n".
                    format(
                        min(output_num_rows), max(output_num_rows),
                        int(np.mean(output_num_rows)), sum(output_num_rows)))

        output_size_bytes = [
            m.size_bytes for m in blocks if m.size_bytes is not None
        ]
        if output_size_bytes:
            out += ("* Output size bytes: {} min, {} max, {} mean, {} total\n".
                    format(
                        min(output_size_bytes), max(output_size_bytes),
                        int(np.mean(output_size_bytes)),
                        sum(output_size_bytes)))

        if exec_stats:
            node_counts = collections.defaultdict(int)
            for s in exec_stats:
                node_counts[s.node_id] += 1
            out += (
                "* Tasks per node: {} min, {} max, {} mean; {} nodes used\n".
                format(
                    min(node_counts.values()), max(node_counts.values()),
                    int(np.mean(list(node_counts.values()))),
                    len(node_counts)))

        return out


class DatasetPipelineStats:
    """Holds the execution times for a pipeline of Datasets."""

    def __init__(self, *, max_history: int = 3):
        """Create a dataset pipeline stats object.

        Args:
            max_history: The max number of dataset window stats to track.
        """
        self.max_history: int = max_history
        self.history_buffer: List[Tuple[int, DatasetStats]] = []
        self.count = 0
        self.wait_time_s = []

        # Iteration stats, filled out if the user iterates over the pipeline.
        self.iter_wait_s: Timer = Timer()
        self.iter_user_s: Timer = Timer()
        self.iter_total_s: Timer = Timer()

    def add(self, stats: DatasetStats) -> None:
        """Called to add stats for a newly computed window."""
        self.history_buffer.append((self.count, stats))
        if len(self.history_buffer) > self.max_history:
            self.history_buffer.pop(0)
        self.count += 1

    def summary_string(self, exclude_first_window: bool = True) -> str:
        """Return a human-readable summary of this pipeline's stats."""
        already_printed = set()
        out = ""
        for i, stats in self.history_buffer:
            out += "== Pipeline Window {} ==\n".format(i)
            out += stats.summary_string(already_printed)
            out += "\n"
        out += "##### Overall Pipeline Time Breakdown #####\n"
        # Drop the first sample since there's no pipelining there.
        wait_time_s = self.wait_time_s[1 if exclude_first_window else 0:]
        if wait_time_s:
            out += ("* Time stalled waiting for next dataset: "
                    "{} min, {} max, {} mean, {} total\n".format(
                        fmt(min(wait_time_s)), fmt(max(wait_time_s)),
                        fmt(np.mean(wait_time_s)), fmt(sum(wait_time_s))))
        out += "* Time in dataset iterator: {}\n".format(
            fmt(self.iter_wait_s.get()))
        out += "* Time in user code: {}\n".format(fmt(self.iter_user_s.get()))
        out += "* Total time: {}\n".format(fmt(self.iter_total_s.get()))
        return out
