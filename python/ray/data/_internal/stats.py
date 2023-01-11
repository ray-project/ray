import collections
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Set, Tuple, Union

import numpy as np

import ray
from ray.data._internal.block_list import BlockList
from ray.data.block import BlockMetadata
from ray.data.context import DatasetContext
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

STATS_ACTOR_NAME = "datasets_stats_actor"
STATS_ACTOR_NAMESPACE = "_dataset_stats_actor"

StatsDict = Dict[str, List[BlockMetadata]]


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

    def build_multistage(self, stages: StatsDict) -> "DatasetStats":
        stage_infos = {}
        for i, (k, v) in enumerate(stages.items()):
            if len(stages) > 1:
                if i == 0:
                    stage_infos[self.stage_name + "_" + k] = v
                else:
                    stage_infos[self.stage_name.split("->")[-1] + "_" + k] = v
            else:
                stage_infos[self.stage_name] = v
        stats = DatasetStats(
            stages=stage_infos,
            parent=self.parent,
            base_name=self.stage_name,
        )
        stats.time_total_s = time.perf_counter() - self.start_time
        return stats

    def build(self, final_blocks: BlockList) -> "DatasetStats":
        stats = DatasetStats(
            stages={self.stage_name: final_blocks.get_metadata()},
            parent=self.parent,
        )
        stats.time_total_s = time.perf_counter() - self.start_time
        return stats


@ray.remote(num_cpus=0)
class _StatsActor:
    """Actor holding stats for blocks created by LazyBlockList.

    This actor is shared across all datasets created in the same cluster.
    In order to cap memory usage, we set a max number of stats to keep
    in the actor. When this limit is exceeded, the stats will be garbage
    collected in FIFO order.

    TODO(ekl) we should consider refactoring LazyBlockList so stats can be
    extracted without using an out-of-band actor."""

    def __init__(self, max_stats=1000):
        # Mapping from uuid -> (task_id -> list of blocks statistics).
        self.metadata = collections.defaultdict(dict)
        self.last_time = {}
        self.start_time = {}
        self.max_stats = max_stats
        self.fifo_queue = []

    def record_start(self, stats_uuid):
        self.start_time[stats_uuid] = time.perf_counter()
        self.fifo_queue.append(stats_uuid)
        # Purge the oldest stats if the limit is exceeded.
        if len(self.fifo_queue) > self.max_stats:
            uuid = self.fifo_queue.pop(0)
            if uuid in self.start_time:
                del self.start_time[uuid]
            if uuid in self.last_time:
                del self.last_time[uuid]
            if uuid in self.metadata:
                del self.metadata[uuid]

    def record_task(
        self, stats_uuid: str, task_idx: int, blocks_metadata: List[BlockMetadata]
    ):
        # Null out the schema to keep the stats size small.
        # TODO(chengsu): ideally schema should be null out on caller side.
        for metadata in blocks_metadata:
            metadata.schema = None
        if stats_uuid in self.start_time:
            self.metadata[stats_uuid][task_idx] = blocks_metadata
            self.last_time[stats_uuid] = time.perf_counter()

    def get(self, stats_uuid):
        if stats_uuid not in self.metadata:
            return {}, 0.0
        return (
            self.metadata[stats_uuid],
            self.last_time[stats_uuid] - self.start_time[stats_uuid],
        )

    def _get_stats_dict_size(self):
        return len(self.start_time), len(self.last_time), len(self.metadata)


def _get_or_create_stats_actor():
    ctx = DatasetContext.get_current()
    scheduling_strategy = ctx.scheduling_strategy
    if not ray.util.client.ray.is_connected():
        # Pin the stats actor to the local node
        # so it fate-shares with the driver.
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    return _StatsActor.options(
        name=STATS_ACTOR_NAME,
        namespace=STATS_ACTOR_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
    ).remote()


class DatasetStats:
    """Holds the execution times for a given Dataset.

    This object contains a reference to the parent Dataset's stats as well,
    but not the Dataset object itself, to allow its blocks to be dropped from
    memory."""

    def __init__(
        self,
        *,
        stages: StatsDict,
        parent: Union[Optional["DatasetStats"], List["DatasetStats"]],
        needs_stats_actor: bool = False,
        stats_uuid: str = None,
        base_name: str = None,
    ):
        """Create dataset stats.

        Args:
            stages: Dict of stages used to create this Dataset from the
                previous one. Typically one entry, e.g., {"map": [...]}.
            parent: Reference to parent Dataset's stats, or a list of parents
                if there are multiple.
            needs_stats_actor: Whether this Dataset's stats needs a stats actor for
                stats collection. This is currently only used for Datasets using a lazy
                datasource (i.e. a LazyBlockList).
            stats_uuid: The uuid for the stats, used to fetch the right stats
                from the stats actor.
            base_name: The name of the base operation for a multi-stage operation.
        """

        self.stages: StatsDict = stages
        if parent is not None and not isinstance(parent, list):
            parent = [parent]
        self.parents: List["DatasetStats"] = parent
        self.number: int = (
            0 if not self.parents else max(p.number for p in self.parents) + 1
        )
        self.base_name = base_name
        self.dataset_uuid: str = None
        self.time_total_s: float = 0
        self.needs_stats_actor = needs_stats_actor
        self.stats_uuid = stats_uuid

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: Timer = Timer()
        self.iter_get_s: Timer = Timer()
        self.iter_next_batch_s: Timer = Timer()
        self.iter_format_batch_s: Timer = Timer()
        self.iter_user_s: Timer = Timer()
        self.iter_total_s: Timer = Timer()
        self.extra_metrics = {}

    @property
    def stats_actor(self):
        return _get_or_create_stats_actor()

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

    def summary_string(
        self, already_printed: Set[str] = None, include_parent: bool = True
    ) -> str:
        """Return a human-readable summary of this Dataset's stats.

        Args:
        already_printed: Set of stage IDs that have already had its stats printed out.
        include_parent: If true, also include parent stats summary; otherwise, only
        log stats of the latest stage.
        """
        if already_printed is None:
            already_printed = set()

        if self.needs_stats_actor:
            ac = self.stats_actor
            # TODO(chengsu): this is a super hack, clean it up.
            stats_map, self.time_total_s = ray.get(ac.get.remote(self.stats_uuid))
            if DatasetContext.get_current().block_splitting_enabled:
                # Only populate stats when stats from all read tasks are ready at
                # stats actor.
                if len(stats_map.items()) == len(self.stages["read"]):
                    self.stages["read"] = []
                    for _, blocks_metadata in sorted(stats_map.items()):
                        self.stages["read"] += blocks_metadata
            else:
                for i, metadata in stats_map.items():
                    self.stages["read"][i] = metadata[0]
        out = ""
        if self.parents and include_parent:
            for p in self.parents:
                parent_sum = p.summary_string(already_printed)
                if parent_sum:
                    out += parent_sum
                    out += "\n"
        if len(self.stages) == 1:
            stage_name, metadata = next(iter(self.stages.items()))
            # TODO(ekl) deprecate and remove the notion of dataset UUID once we move
            # fully to streaming execution.
            stage_uuid = (self.dataset_uuid or "unknown_uuid") + stage_name
            out += "Stage {} {}: ".format(self.number, stage_name)
            if stage_uuid in already_printed:
                out += "[execution cached]\n"
            else:
                already_printed.add(stage_uuid)
                out += self._summarize_blocks(metadata, is_substage=False)
        elif len(self.stages) > 1:
            rounded_total = round(self.time_total_s, 2)
            if rounded_total <= 0:
                # Handle -0.0 case.
                rounded_total = 0
            out += "Stage {} {}: executed in {}s\n".format(
                self.number, self.base_name, rounded_total
            )
            for n, (stage_name, metadata) in enumerate(self.stages.items()):
                stage_uuid = self.dataset_uuid + stage_name
                out += "\n"
                out += "\tSubstage {} {}: ".format(n, stage_name)
                if stage_uuid in already_printed:
                    out += "\t[execution cached]\n"
                else:
                    already_printed.add(stage_uuid)
                    out += self._summarize_blocks(metadata, is_substage=True)
        out += self._summarize_iter()
        return out

    def _summarize_iter(self) -> str:
        out = ""
        if (
            self.iter_total_s.get()
            or self.iter_wait_s.get()
            or self.iter_next_batch_s.get()
            or self.iter_format_batch_s.get()
            or self.iter_get_s.get()
        ):
            out += "\nDataset iterator time breakdown:\n"
            out += "* In ray.wait(): {}\n".format(fmt(self.iter_wait_s.get()))
            out += "* In ray.get(): {}\n".format(fmt(self.iter_get_s.get()))
            out += "* In next_batch(): {}\n".format(fmt(self.iter_next_batch_s.get()))
            out += "* In format_batch(): {}\n".format(
                fmt(self.iter_format_batch_s.get())
            )
            out += "* In user code: {}\n".format(fmt(self.iter_user_s.get()))
            out += "* Total time: {}\n".format(fmt(self.iter_total_s.get()))
        return out

    def _summarize_blocks(self, blocks: List[BlockMetadata], is_substage: bool) -> str:
        exec_stats = [m.exec_stats for m in blocks if m.exec_stats is not None]
        indent = "\t" if is_substage else ""
        if is_substage:
            out = "{}/{} blocks executed\n".format(len(exec_stats), len(blocks))
        else:
            rounded_total = round(self.time_total_s, 2)
            if rounded_total <= 0:
                # Handle -0.0 case.
                rounded_total = 0
            if exec_stats:
                out = "{}/{} blocks executed in {}s".format(
                    len(exec_stats), len(blocks), rounded_total
                )
            else:
                out = ""
            if len(exec_stats) < len(blocks):
                if exec_stats:
                    out += ", "
                num_inherited = len(blocks) - len(exec_stats)
                out += "{}/{} blocks split from parent".format(
                    num_inherited, len(blocks)
                )
                if not exec_stats:
                    out += " in {}s".format(rounded_total)
            out += "\n"

        if exec_stats:
            out += indent
            out += "* Remote wall time: {} min, {} max, {} mean, {} total\n".format(
                fmt(min([e.wall_time_s for e in exec_stats])),
                fmt(max([e.wall_time_s for e in exec_stats])),
                fmt(np.mean([e.wall_time_s for e in exec_stats])),
                fmt(sum([e.wall_time_s for e in exec_stats])),
            )

            out += indent
            out += "* Remote cpu time: {} min, {} max, {} mean, {} total\n".format(
                fmt(min([e.cpu_time_s for e in exec_stats])),
                fmt(max([e.cpu_time_s for e in exec_stats])),
                fmt(np.mean([e.cpu_time_s for e in exec_stats])),
                fmt(sum([e.cpu_time_s for e in exec_stats])),
            )

            out += indent
            memory_stats = [
                round(e.max_rss_bytes / (1024 * 1024), 2) for e in exec_stats
            ]
            out += "* Peak heap memory usage (MiB): {} min, {} max, {} mean\n".format(
                min(memory_stats),
                max(memory_stats),
                int(np.mean(memory_stats)),
            )

        output_num_rows = [m.num_rows for m in blocks if m.num_rows is not None]
        if output_num_rows:
            out += indent
            out += "* Output num rows: {} min, {} max, {} mean, {} total\n".format(
                min(output_num_rows),
                max(output_num_rows),
                int(np.mean(output_num_rows)),
                sum(output_num_rows),
            )

        output_size_bytes = [m.size_bytes for m in blocks if m.size_bytes is not None]
        if output_size_bytes:
            out += indent
            out += "* Output size bytes: {} min, {} max, {} mean, {} total\n".format(
                min(output_size_bytes),
                max(output_size_bytes),
                int(np.mean(output_size_bytes)),
                sum(output_size_bytes),
            )

        if exec_stats:
            node_counts = collections.defaultdict(int)
            for s in exec_stats:
                node_counts[s.node_id] += 1
            out += indent
            out += "* Tasks per node: {} min, {} max, {} mean; {} nodes used\n".format(
                min(node_counts.values()),
                max(node_counts.values()),
                int(np.mean(list(node_counts.values()))),
                len(node_counts),
            )

        if self.extra_metrics:
            out += indent
            out += "* Extra metrics: " + str(self.extra_metrics) + "\n"

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
        self._iter_stats = {
            "iter_ds_wait_s": Timer(),
            "iter_wait_s": Timer(),
            "iter_get_s": Timer(),
            "iter_next_batch_s": Timer(),
            "iter_format_batch_s": Timer(),
            "iter_user_s": Timer(),
            "iter_total_s": Timer(),
        }

    # Make iteration stats also accessible via attributes.
    def __getattr__(self, name):
        if name == "_iter_stats":
            raise AttributeError
        if name in self._iter_stats:
            return self._iter_stats[name]
        raise AttributeError

    def add(self, stats: DatasetStats) -> None:
        """Called to add stats for a newly computed window."""
        self.history_buffer.append((self.count, stats))
        if len(self.history_buffer) > self.max_history:
            self.history_buffer.pop(0)
        self.count += 1

    def add_pipeline_stats(self, other_stats: "DatasetPipelineStats"):
        """Add the provided pipeline stats to the current stats."""
        for _, dataset_stats in other_stats.history_buffer:
            self.add(dataset_stats)

        self.wait_time_s.extend(other_stats.wait_time_s)

        for stat_name, timer in self._iter_stats:
            timer.add(other_stats._iter_stats[stat_name].get())

    def _summarize_iter(self) -> str:
        out = ""
        if (
            self.iter_total_s.get()
            or self.iter_wait_s.get()
            or self.iter_next_batch_s.get()
            or self.iter_format_batch_s.get()
            or self.iter_get_s.get()
        ):
            out += "\nDatasetPipeline iterator time breakdown:\n"
            out += "* Waiting for next dataset: {}\n".format(
                fmt(self.iter_ds_wait_s.get())
            )
            out += "* In ray.wait(): {}\n".format(fmt(self.iter_wait_s.get()))
            out += "* In ray.get(): {}\n".format(fmt(self.iter_get_s.get()))
            out += "* In next_batch(): {}\n".format(fmt(self.iter_next_batch_s.get()))
            out += "* In format_batch(): {}\n".format(
                fmt(self.iter_format_batch_s.get())
            )
            out += "* In user code: {}\n".format(fmt(self.iter_user_s.get()))
            out += "* Total time: {}\n".format(fmt(self.iter_total_s.get()))

        return out

    def summary_string(self, exclude_first_window: bool = True) -> str:
        """Return a human-readable summary of this pipeline's stats."""
        already_printed = set()
        out = ""
        if not self.history_buffer:
            return "No stats available: This pipeline hasn't been run yet."
        for i, stats in self.history_buffer:
            out += "== Pipeline Window {} ==\n".format(i)
            out += stats.summary_string(already_printed)
            out += "\n"
        out += "##### Overall Pipeline Time Breakdown #####\n"
        # Drop the first sample since there's no pipelining there.
        wait_time_s = self.wait_time_s[1 if exclude_first_window else 0 :]
        if wait_time_s:
            out += (
                "* Time stalled waiting for next dataset: "
                "{} min, {} max, {} mean, {} total\n".format(
                    fmt(min(wait_time_s)),
                    fmt(max(wait_time_s)),
                    fmt(np.mean(wait_time_s)),
                    fmt(sum(wait_time_s)),
                )
            )
        out += self._summarize_iter()
        return out
