import collections
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import numpy as np

import ray
from ray.data._internal.block_list import BlockList
from ray.data._internal.dataset_logger import DatasetLogger
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics
from ray.data._internal.util import capfirst
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI
from ray.util.metrics import Gauge
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = DatasetLogger(__name__)

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


def leveled_indent(lvl: int = 0, spaces_per_indent: int = 3) -> str:
    """Returns a string of spaces which contains `level` indents,
    each indent containing `spaces_per_indent` spaces. For example:
    >>> leveled_indent(2, 3)
    '      '
    """
    return (" " * spaces_per_indent) * lvl


class Timer:
    """Helper class for tracking accumulated time (in seconds)."""

    def __init__(self):
        self._value: float = 0
        self._min: float = float("inf")
        self._max: float = 0
        self._total_count: float = 0

    @contextmanager
    def timer(self) -> None:
        time_start = time.perf_counter()
        try:
            yield
        finally:
            self.add(time.perf_counter() - time_start)

    def add(self, value: float) -> None:
        self._value += value
        if value < self._min:
            self._min = value
        if value > self._max:
            self._max = value
        self._total_count += 1

    def get(self) -> float:
        return self._value

    def min(self) -> float:
        return self._min

    def max(self) -> float:
        return self._max

    def avg(self) -> float:
        return self._value / self._total_count if self._total_count else float("inf")


class _DatasetStatsBuilder:
    """Helper class for building dataset stats.

    When this class is created, we record the start time. When build() is
    called with the final blocks of the new dataset, the time delta is
    saved as part of the stats."""

    def __init__(
        self,
        stage_name: str,
        parent: "DatasetStats",
        override_start_time: Optional[float],
    ):
        self.stage_name = stage_name
        self.parent = parent
        self.start_time = override_start_time or time.perf_counter()

    def build_multistage(self, stages: StatsDict) -> "DatasetStats":
        stage_infos = {}
        for i, (k, v) in enumerate(stages.items()):
            capped_k = capfirst(k)
            if len(stages) > 1:
                if i == 0:
                    stage_infos[self.stage_name + capped_k] = v
                else:
                    stage_infos[self.stage_name.split("->")[-1] + capped_k] = v
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

        # Assign dataset uuids with a global counter.
        self.next_dataset_id = 0
        # Dataset metadata to be queried directly by DashboardHead api.
        self.datasets: Dict[str, Any] = {}

        # Ray Data dashboard metrics
        # Everything is a gauge because we need to reset all of
        # a dataset's metrics to 0 after each finishes execution.
        op_tags_keys = ("dataset", "operator")
        self.bytes_spilled = Gauge(
            "data_spilled_bytes",
            description="""Bytes spilled by dataset operators.
                DataContext.enable_get_object_locations_for_metrics
                must be set to True to report this metric""",
            tag_keys=op_tags_keys,
        )
        self.bytes_allocated = Gauge(
            "data_allocated_bytes",
            description="Bytes allocated by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.bytes_freed = Gauge(
            "data_freed_bytes",
            description="Bytes freed by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.bytes_current = Gauge(
            "data_current_bytes",
            description="Bytes currently in memory store used by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.cpu_usage = Gauge(
            "data_cpu_usage_cores",
            description="CPUs allocated to dataset operators",
            tag_keys=op_tags_keys,
        )
        self.gpu_usage = Gauge(
            "data_gpu_usage_cores",
            description="GPUs allocated to dataset operators",
            tag_keys=op_tags_keys,
        )
        self.bytes_outputted = Gauge(
            "data_output_bytes",
            description="Bytes outputted by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.rows_outputted = Gauge(
            "data_output_rows",
            description="Rows outputted by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.block_generation_time = Gauge(
            "data_block_generation_seconds",
            description="Time spent generating blocks.",
            tag_keys=op_tags_keys,
        )

        iter_tag_keys = ("dataset",)
        self.iter_total_blocked_s = Gauge(
            "data_iter_total_blocked_seconds",
            description="Seconds user thread is blocked by iter_batches()",
            tag_keys=iter_tag_keys,
        )
        self.iter_user_s = Gauge(
            "data_iter_user_seconds",
            description="Seconds spent in user code",
            tag_keys=iter_tag_keys,
        )

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

    def get_dataset_id(self):
        dataset_id = str(self.next_dataset_id)
        self.next_dataset_id += 1
        return dataset_id

    def update_metrics(self, execution_metrics, iteration_metrics):
        for metrics in execution_metrics:
            self.update_execution_metrics(*metrics)
        for metrics in iteration_metrics:
            self.update_iteration_metrics(*metrics)

    def update_execution_metrics(
        self,
        dataset_tag: str,
        op_metrics: List[Dict[str, Union[int, float]]],
        operator_tags: List[str],
        state: Dict[str, Any],
    ):
        for stats, operator_tag in zip(op_metrics, operator_tags):
            tags = self._create_tags(dataset_tag, operator_tag)
            self.bytes_spilled.set(stats.get("obj_store_mem_spilled", 0), tags)
            self.bytes_allocated.set(stats.get("obj_store_mem_alloc", 0), tags)
            self.bytes_freed.set(stats.get("obj_store_mem_freed", 0), tags)
            self.bytes_current.set(stats.get("obj_store_mem_cur", 0), tags)
            self.bytes_outputted.set(stats.get("bytes_outputs_generated", 0), tags)
            self.rows_outputted.set(stats.get("rows_outputs_generated", 0), tags)
            self.cpu_usage.set(stats.get("cpu_usage", 0), tags)
            self.gpu_usage.set(stats.get("gpu_usage", 0), tags)
            self.block_generation_time.set(stats.get("block_generation_time", 0), tags)

        # This update is called from a dataset's executor,
        # so all tags should contain the same dataset
        self.update_dataset(dataset_tag, state)

    def update_iteration_metrics(
        self,
        stats: "DatasetStats",
        dataset_tag,
    ):
        tags = self._create_tags(dataset_tag)
        self.iter_total_blocked_s.set(stats.iter_total_blocked_s.get(), tags)
        self.iter_user_s.set(stats.iter_user_s.get(), tags)

    def clear_execution_metrics(self, dataset_tag: str, operator_tags: List[str]):
        for operator_tag in operator_tags:
            tags = self._create_tags(dataset_tag, operator_tag)
            self.bytes_spilled.set(0, tags)
            self.bytes_allocated.set(0, tags)
            self.bytes_freed.set(0, tags)
            self.bytes_current.set(0, tags)
            self.bytes_outputted.set(0, tags)
            self.rows_outputted.set(0, tags)
            self.cpu_usage.set(0, tags)
            self.gpu_usage.set(0, tags)
            self.block_generation_time.set(0, tags)

    def clear_iteration_metrics(self, dataset_tag: str):
        tags = self._create_tags(dataset_tag)
        self.iter_total_blocked_s.set(0, tags)
        self.iter_user_s.set(0, tags)

    def register_dataset(self, dataset_tag: str, operator_tags: List[str]):
        self.datasets[dataset_tag] = {
            "state": "RUNNING",
            "progress": 0,
            "total": 0,
            "start_time": time.time(),
            "end_time": None,
            "operators": {
                operator: {
                    "state": "RUNNING",
                    "progress": 0,
                    "total": 0,
                }
                for operator in operator_tags
            },
        }

    def update_dataset(self, dataset_tag, state):
        self.datasets[dataset_tag].update(state)

    def get_datasets(self):
        return self.datasets

    def _create_tags(self, dataset_tag: str, operator_tag: Optional[str] = None):
        tags = {"dataset": dataset_tag}
        if operator_tag is not None:
            tags["operator"] = operator_tag
        return tags


# Creating/getting an actor from multiple threads is not safe.
# https://github.com/ray-project/ray/issues/41324
_stats_actor_lock: threading.RLock = threading.RLock()


def _get_or_create_stats_actor():
    ctx = DataContext.get_current()
    scheduling_strategy = ctx.scheduling_strategy
    if not ray.util.client.ray.is_connected():
        # Pin the stats actor to the local node
        # so it fate-shares with the driver.
        scheduling_strategy = NodeAffinitySchedulingStrategy(
            ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    with _stats_actor_lock:
        return _StatsActor.options(
            name=STATS_ACTOR_NAME,
            namespace=STATS_ACTOR_NAMESPACE,
            get_if_exists=True,
            lifetime="detached",
            scheduling_strategy=scheduling_strategy,
        ).remote()


class _StatsManager:
    """A Class containing util functions that manage remote calls to _StatsActor.

    This class collects stats from execution and iteration codepaths and keeps
    track of the latest snapshot.

    An instance of this class runs a single background thread that periodically
    forwards the latest execution/iteration stats to the _StatsActor.

    This thread will terminate itself after being inactive (meaning that there are
    no active executors or iterators) for STATS_ACTOR_UPDATE_THREAD_INACTIVITY_LIMIT
    iterations. After terminating, a new thread will start if more calls are made
    to this class.
    """

    # Interval for making remote calls to the _StatsActor.
    STATS_ACTOR_UPDATE_INTERVAL_SECONDS = 5

    # After this many iterations of inactivity,
    # _StatsManager._update_thread will close itself.
    UPDATE_THREAD_INACTIVITY_LIMIT = 5

    def __init__(self):
        # Lazily get stats actor handle to avoid circular import.
        self._stats_actor_handle = None
        self._stats_actor_cluster_id = None

        # Last execution stats snapshots for all executing datasets
        self._last_execution_stats = {}
        # Last iteration stats snapshots for all running iterators
        self._last_iteration_stats: Dict[
            str, Tuple[Dict[str, str], "DatasetStats"]
        ] = {}
        # Lock for updating stats snapshots
        self._stats_lock: threading.Lock = threading.Lock()

        # Background thread to make remote calls to _StatsActor
        self._update_thread: Optional[threading.Thread] = None
        self._update_thread_lock: threading.Lock = threading.Lock()

    def _stats_actor(self, create_if_not_exists=True) -> _StatsActor:
        if ray._private.worker._global_node is None:
            raise RuntimeError("Global node is not initialized.")
        current_cluster_id = ray._private.worker._global_node.cluster_id
        if (
            self._stats_actor_handle is None
            or self._stats_actor_cluster_id != current_cluster_id
        ):
            if create_if_not_exists:
                self._stats_actor_handle = _get_or_create_stats_actor()
            else:
                self._stat_actor_handle = ray.get_actor(
                    name=STATS_ACTOR_NAME, namespace=STATS_ACTOR_NAMESPACE
                )
            self._stats_actor_cluster_id = current_cluster_id
        return self._stats_actor_handle

    def _start_thread_if_not_running(self):
        # Start background update thread if not running.
        with self._update_thread_lock:
            if self._update_thread is None or not self._update_thread.is_alive():

                def _run_update_loop():
                    iter_stats_inactivity = 0
                    while True:
                        if self._last_iteration_stats or self._last_execution_stats:
                            try:
                                # Do not create _StatsActor if it doesn't exist because
                                # this thread can be running even after the cluster is
                                # shutdown. Creating an actor will automatically start
                                # a new cluster.
                                self._stats_actor(
                                    create_if_not_exists=False
                                ).update_metrics.remote(
                                    execution_metrics=list(
                                        self._last_execution_stats.values()
                                    ),
                                    iteration_metrics=list(
                                        self._last_iteration_stats.values()
                                    ),
                                )
                                iter_stats_inactivity = 0
                            except Exception:
                                logger.get_logger(log_to_stdout=False).exception(
                                    "Error occurred during remote call to _StatsActor."
                                )
                                return
                        else:
                            iter_stats_inactivity += 1
                            if (
                                iter_stats_inactivity
                                >= _StatsManager.UPDATE_THREAD_INACTIVITY_LIMIT
                            ):
                                logger.get_logger(log_to_stdout=False).info(
                                    "Terminating StatsManager thread due to inactivity."
                                )
                                return
                        time.sleep(StatsManager.STATS_ACTOR_UPDATE_INTERVAL_SECONDS)

                self._update_thread = threading.Thread(
                    target=_run_update_loop, daemon=True
                )
                self._update_thread.start()

    # Execution methods

    def update_execution_metrics(
        self,
        dataset_tag: str,
        op_metrics: List[OpRuntimeMetrics],
        operator_tags: List[str],
        state: Dict[str, Any],
        force_update: bool = False,
    ):
        op_metrics_dicts = [metric.as_dict() for metric in op_metrics]
        args = (dataset_tag, op_metrics_dicts, operator_tags, state)
        if force_update:
            self._stats_actor().update_execution_metrics.remote(*args)
        else:
            with self._stats_lock:
                self._last_execution_stats[dataset_tag] = args
            self._start_thread_if_not_running()

    def clear_execution_metrics(self, dataset_tag: str, operator_tags: List[str]):
        with self._stats_lock:
            if dataset_tag in self._last_execution_stats:
                del self._last_execution_stats[dataset_tag]

        try:
            self._stats_actor(
                create_if_not_exists=False
            ).clear_execution_metrics.remote(dataset_tag, operator_tags)
        except Exception:
            # Cluster may be shut down.
            pass

    # Iteration methods

    def update_iteration_metrics(self, stats: "DatasetStats", dataset_tag: str):
        with self._stats_lock:
            self._last_iteration_stats[dataset_tag] = (stats, dataset_tag)
        self._start_thread_if_not_running()

    def clear_iteration_metrics(self, dataset_tag: str):
        with self._stats_lock:
            if dataset_tag in self._last_iteration_stats:
                del self._last_iteration_stats[dataset_tag]

        try:
            self._stats_actor(
                create_if_not_exists=False
            ).clear_iteration_metrics.remote(dataset_tag)
        except Exception:
            # Cluster may be shut down.
            pass

    # Other methods

    def register_dataset_to_stats_actor(self, dataset_tag, operator_tags):
        self._stats_actor().register_dataset.remote(dataset_tag, operator_tags)

    def get_dataset_id_from_stats_actor(self) -> str:
        try:
            return ray.get(self._stats_actor().get_dataset_id.remote())
        except Exception:
            # Getting dataset id from _StatsActor may fail, in this case
            # fall back to uuid4
            return uuid4().hex


StatsManager = _StatsManager()


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
                stats collection. This is currently only used for Datasets using a
                lazy datasource (i.e. a LazyBlockList).
            stats_uuid: The uuid for the stats, used to fetch the right stats
                from the stats actor.
            base_name: The name of the base operation for a multi-stage operation.
        """

        self.stages: StatsDict = stages
        if parent is not None and not isinstance(parent, list):
            parent = [parent]
        self.parents: List["DatasetStats"] = parent or []
        self.number: int = (
            0 if not self.parents else max(p.number for p in self.parents) + 1
        )
        self.base_name = base_name
        # TODO(ekl) deprecate and remove the notion of dataset UUID once we move
        # fully to streaming execution.
        self.dataset_uuid: str = "unknown_uuid"
        self.time_total_s: float = 0
        self.needs_stats_actor = needs_stats_actor
        self.stats_uuid = stats_uuid

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: Timer = Timer()
        self.iter_get_s: Timer = Timer()
        self.iter_next_batch_s: Timer = Timer()
        self.iter_format_batch_s: Timer = Timer()
        self.iter_collate_batch_s: Timer = Timer()
        self.iter_finalize_batch_s: Timer = Timer()
        self.iter_total_blocked_s: Timer = Timer()
        self.iter_user_s: Timer = Timer()
        self.iter_total_s: Timer = Timer()
        self.extra_metrics = {}

        # Block fetch stats during iteration.
        # These are stats about locations of blocks when the iterator is trying to
        # consume them. The iteration performance will be affected depending on
        # whether the block is in the local object store of the node where the
        # iterator is running.
        # This serves as an indicator of block prefetching effectiveness.
        self.iter_blocks_local: int = 0
        self.iter_blocks_remote: int = 0
        self.iter_unknown_location: int = 0

        # Memory usage stats
        self.global_bytes_spilled: int = 0
        self.global_bytes_restored: int = 0
        self.dataset_bytes_spilled: int = 0

    @property
    def stats_actor(self):
        return _get_or_create_stats_actor()

    def child_builder(
        self, name: str, override_start_time: Optional[float] = None
    ) -> _DatasetStatsBuilder:
        """Start recording stats for an op of the given name (e.g., map)."""
        return _DatasetStatsBuilder(name, self, override_start_time)

    def child_TODO(self, name: str) -> "DatasetStats":
        """Placeholder for child ops not yet instrumented."""
        return DatasetStats(stages={name + "_TODO": []}, parent=self)

    @staticmethod
    def TODO():
        """Placeholder for ops not yet instrumented."""
        return DatasetStats(stages={"TODO": []}, parent=None)

    def to_summary(self) -> "DatasetStatsSummary":
        """Generate a `DatasetStatsSummary` object from the given `DatasetStats`
        object, which can be used to generate a summary string."""
        if self.needs_stats_actor:
            ac = self.stats_actor
            # TODO(chengsu): this is a super hack, clean it up.
            stats_map, self.time_total_s = ray.get(ac.get.remote(self.stats_uuid))
            # Only populate stats when stats from all read tasks are ready at
            # stats actor.
            if len(stats_map.items()) == len(self.stages["Read"]):
                self.stages["Read"] = []
                for _, blocks_metadata in sorted(stats_map.items()):
                    self.stages["Read"] += blocks_metadata

        stages_stats = []
        is_substage = len(self.stages) > 1
        for stage_name, metadata in self.stages.items():
            stages_stats.append(
                StageStatsSummary.from_block_metadata(
                    metadata,
                    stage_name,
                    is_substage=is_substage,
                )
            )

        iter_stats = IterStatsSummary(
            self.iter_wait_s,
            self.iter_get_s,
            self.iter_next_batch_s,
            self.iter_format_batch_s,
            self.iter_collate_batch_s,
            self.iter_finalize_batch_s,
            self.iter_total_blocked_s,
            self.iter_user_s,
            self.iter_total_s,
            self.iter_blocks_local,
            self.iter_blocks_remote,
            self.iter_unknown_location,
        )
        stats_summary_parents = []
        if self.parents is not None:
            stats_summary_parents = [p.to_summary() for p in self.parents]
        return DatasetStatsSummary(
            stages_stats,
            iter_stats,
            stats_summary_parents,
            self.number,
            self.dataset_uuid,
            self.time_total_s,
            self.base_name,
            self.extra_metrics,
            self.global_bytes_spilled,
            self.global_bytes_restored,
            self.dataset_bytes_spilled,
        )


@DeveloperAPI
@dataclass
class DatasetStatsSummary:
    stages_stats: List["StageStatsSummary"]
    iter_stats: "IterStatsSummary"
    parents: List["DatasetStatsSummary"]
    number: int
    dataset_uuid: str
    time_total_s: float
    base_name: str
    extra_metrics: Dict[str, Any]
    global_bytes_spilled: int
    global_bytes_restored: int
    dataset_bytes_spilled: int

    def to_string(
        self,
        already_printed: Optional[Set[str]] = None,
        include_parent: bool = True,
        add_global_stats=True,
    ) -> str:
        """Return a human-readable summary of this Dataset's stats.

        Args:
            already_printed: Set of stage IDs that have already had its stats printed
            out.
            include_parent: If true, also include parent stats summary; otherwise, only
            log stats of the latest stage.
            add_global_stats: If true, includes global stats to this summary.
        Returns:
            String with summary statistics for executing the Dataset.
        """
        if already_printed is None:
            already_printed = set()

        out = ""
        if self.parents and include_parent:
            for p in self.parents:
                parent_sum = p.to_string(already_printed, add_global_stats=False)
                if parent_sum:
                    out += parent_sum
                    out += "\n"
        stage_stats_summary = None
        if len(self.stages_stats) == 1:
            stage_stats_summary = self.stages_stats[0]
            stage_name = stage_stats_summary.stage_name
            stage_uuid = self.dataset_uuid + stage_name
            out += "Stage {} {}: ".format(self.number, stage_name)
            if stage_uuid in already_printed:
                out += "[execution cached]\n"
            else:
                already_printed.add(stage_uuid)
                out += str(stage_stats_summary)
        elif len(self.stages_stats) > 1:
            rounded_total = round(self.time_total_s, 2)
            if rounded_total <= 0:
                # Handle -0.0 case.
                rounded_total = 0
            out += "Stage {} {}: executed in {}s\n".format(
                self.number, self.base_name, rounded_total
            )
            for n, stage_stats_summary in enumerate(self.stages_stats):
                stage_name = stage_stats_summary.stage_name
                stage_uuid = self.dataset_uuid + stage_name
                out += "\n"
                out += "\tSubstage {} {}: ".format(n, stage_name)
                if stage_uuid in already_printed:
                    out += "\t[execution cached]\n"
                else:
                    already_printed.add(stage_uuid)
                    out += str(stage_stats_summary)
        if self.extra_metrics:
            indent = (
                "\t" if stage_stats_summary and stage_stats_summary.is_substage else ""
            )
            out += indent
            out += "* Extra metrics: " + str(self.extra_metrics) + "\n"
        out += str(self.iter_stats)

        if len(self.stages_stats) > 0 and add_global_stats:
            mb_spilled = round(self.global_bytes_spilled / 1e6)
            mb_restored = round(self.global_bytes_restored / 1e6)
            if mb_spilled or mb_restored:
                out += "\nCluster memory:\n"
                out += "* Spilled to disk: {}MB\n".format(mb_spilled)
                out += "* Restored from disk: {}MB\n".format(mb_restored)

            dataset_mb_spilled = round(self.dataset_bytes_spilled / 1e6)
            if dataset_mb_spilled:
                out += "\nDataset memory:\n"
                out += "* Spilled to disk: {}MB\n".format(dataset_mb_spilled)

        return out

    def __repr__(self, level=0) -> str:
        indent = leveled_indent(level)
        stage_stats = "\n".join([ss.__repr__(level + 2) for ss in self.stages_stats])
        parent_stats = "\n".join([ps.__repr__(level + 2) for ps in self.parents])
        extra_metrics = "\n".join(
            f"{leveled_indent(level + 2)}{k}: {v},"
            for k, v in self.extra_metrics.items()
        )

        # Handle formatting case for empty outputs.
        stage_stats = f"\n{stage_stats},\n{indent}   " if stage_stats else ""
        parent_stats = f"\n{parent_stats},\n{indent}   " if parent_stats else ""
        extra_metrics = f"\n{extra_metrics}\n{indent}   " if extra_metrics else ""
        return (
            f"{indent}DatasetStatsSummary(\n"
            f"{indent}   dataset_uuid={self.dataset_uuid},\n"
            f"{indent}   base_name={self.base_name},\n"
            f"{indent}   number={self.number},\n"
            f"{indent}   extra_metrics={{{extra_metrics}}},\n"
            f"{indent}   stage_stats=[{stage_stats}],\n"
            f"{indent}   iter_stats={self.iter_stats.__repr__(level+1)},\n"
            f"{indent}   global_bytes_spilled={self.global_bytes_spilled / 1e6}MB,\n"
            f"{indent}   global_bytes_restored={self.global_bytes_restored / 1e6}MB,\n"
            f"{indent}   dataset_bytes_spilled={self.dataset_bytes_spilled / 1e6}MB,\n"
            f"{indent}   parents=[{parent_stats}],\n"
            f"{indent})"
        )

    def get_total_wall_time(self) -> float:
        parent_wall_times = [p.get_total_wall_time() for p in self.parents]
        parent_max_wall_time = max(parent_wall_times) if parent_wall_times else 0
        return parent_max_wall_time + sum(
            ss.wall_time.get("max", 0) for ss in self.stages_stats
        )

    def get_total_cpu_time(self) -> float:
        parent_sum = sum(p.get_total_cpu_time() for p in self.parents)
        return parent_sum + sum(ss.cpu_time.get("sum", 0) for ss in self.stages_stats)

    def get_max_heap_memory(self) -> float:
        parent_memory = [p.get_max_heap_memory() for p in self.parents]
        parent_max = max(parent_memory) if parent_memory else 0
        if not self.stages_stats:
            return parent_max

        return max(
            parent_max,
            *[ss.memory.get("max", 0) for ss in self.stages_stats],
        )


@dataclass
class StageStatsSummary:
    stage_name: str
    # Whether the stage associated with this StageStatsSummary object is a substage
    is_substage: bool
    # This is the total walltime of the entire stage, typically obtained from
    # `DatasetStats.time_total_s`. An important distinction is that this is the
    # overall runtime of the stage, pulled from the stats actor, whereas the
    # computed walltimes in `self.wall_time` are calculated on a substage level.
    time_total_s: float
    # String summarizing high-level statistics from executing the stage
    block_execution_summary_str: str
    # The fields below are dicts with stats aggregated across blocks
    # processed in this stage. For example:
    # {"min": ..., "max": ..., "mean": ..., "sum": ...}
    wall_time: Optional[Dict[str, float]] = None
    cpu_time: Optional[Dict[str, float]] = None
    # memory: no "sum" stat
    memory: Optional[Dict[str, float]] = None
    output_num_rows: Optional[Dict[str, float]] = None
    output_size_bytes: Optional[Dict[str, float]] = None
    # node_count: "count" stat instead of "sum"
    node_count: Optional[Dict[str, float]] = None

    @classmethod
    def from_block_metadata(
        cls,
        block_metas: List[BlockMetadata],
        stage_name: str,
        is_substage: bool,
    ) -> "StageStatsSummary":
        """Calculate the stats for a stage from a given list of blocks,
        and generates a `StageStatsSummary` object with the results.

        Args:
            block_metas: List of `BlockMetadata` to calculate stats of
            stage_name: Name of stage associated with `blocks`
            is_substage: Whether this set of blocks belongs to a substage.
        Returns:
            A `StageStatsSummary` object initialized with the calculated statistics
        """
        exec_stats = [m.exec_stats for m in block_metas if m.exec_stats is not None]
        rounded_total = 0
        time_total_s = 0

        if is_substage:
            exec_summary_str = "{}/{} blocks executed\n".format(
                len(exec_stats), len(block_metas)
            )
        else:
            if exec_stats:
                # Calculate the total execution time of stage as
                # the difference between the latest end time and
                # the earliest start time of all blocks in the stage.
                earliest_start_time = min(s.start_time_s for s in exec_stats)
                latest_end_time = max(s.end_time_s for s in exec_stats)
                time_total_s = latest_end_time - earliest_start_time

                rounded_total = round(time_total_s, 2)
                if rounded_total <= 0:
                    # Handle -0.0 case.
                    rounded_total = 0
                exec_summary_str = "{}/{} blocks executed in {}s".format(
                    len(exec_stats), len(block_metas), rounded_total
                )
            else:
                exec_summary_str = ""
            if len(exec_stats) < len(block_metas):
                if exec_stats:
                    exec_summary_str += ", "
                num_inherited = len(block_metas) - len(exec_stats)
                exec_summary_str += "{}/{} blocks split from parent".format(
                    num_inherited, len(block_metas)
                )
                if not exec_stats:
                    exec_summary_str += " in {}s".format(rounded_total)
            exec_summary_str += "\n"

        wall_time_stats = None
        if exec_stats:
            wall_time_stats = {
                "min": min([e.wall_time_s for e in exec_stats]),
                "max": max([e.wall_time_s for e in exec_stats]),
                "mean": np.mean([e.wall_time_s for e in exec_stats]),
                "sum": sum([e.wall_time_s for e in exec_stats]),
            }

        cpu_stats, memory_stats = None, None
        if exec_stats:
            cpu_stats = {
                "min": min([e.cpu_time_s for e in exec_stats]),
                "max": max([e.cpu_time_s for e in exec_stats]),
                "mean": np.mean([e.cpu_time_s for e in exec_stats]),
                "sum": sum([e.cpu_time_s for e in exec_stats]),
            }

            memory_stats_mb = [
                round(e.max_rss_bytes / (1024 * 1024), 2) for e in exec_stats
            ]
            memory_stats = {
                "min": min(memory_stats_mb),
                "max": max(memory_stats_mb),
                "mean": int(np.mean(memory_stats_mb)),
            }

        output_num_rows_stats = None
        output_num_rows = [m.num_rows for m in block_metas if m.num_rows is not None]
        if output_num_rows:
            output_num_rows_stats = {
                "min": min(output_num_rows),
                "max": max(output_num_rows),
                "mean": int(np.mean(output_num_rows)),
                "sum": sum(output_num_rows),
            }

        output_size_bytes_stats = None
        output_size_bytes = [
            m.size_bytes for m in block_metas if m.size_bytes is not None
        ]
        if output_size_bytes:
            output_size_bytes_stats = {
                "min": min(output_size_bytes),
                "max": max(output_size_bytes),
                "mean": int(np.mean(output_size_bytes)),
                "sum": sum(output_size_bytes),
            }

        node_counts_stats = None
        if exec_stats:
            node_counts = collections.defaultdict(int)
            for s in exec_stats:
                node_counts[s.node_id] += 1
            node_counts_stats = {
                "min": min(node_counts.values()),
                "max": max(node_counts.values()),
                "mean": int(np.mean(list(node_counts.values()))),
                "count": len(node_counts),
            }

        return StageStatsSummary(
            stage_name=stage_name,
            is_substage=is_substage,
            time_total_s=time_total_s,
            block_execution_summary_str=exec_summary_str,
            wall_time=wall_time_stats,
            cpu_time=cpu_stats,
            memory=memory_stats,
            output_num_rows=output_num_rows_stats,
            output_size_bytes=output_size_bytes_stats,
            node_count=node_counts_stats,
        )

    def __str__(self) -> str:
        """For a given (pre-calculated) `StageStatsSummary` object (e.g. generated from
        `StageStatsSummary.from_block_metadata()`), returns a human-friendly string
        that summarizes stage execution statistics.

        Returns:
            String with summary statistics for executing the given stage.
        """
        indent = "\t" if self.is_substage else ""
        out = self.block_execution_summary_str

        wall_time_stats = self.wall_time
        if wall_time_stats:
            out += indent
            out += "* Remote wall time: {} min, {} max, {} mean, {} total\n".format(
                fmt(wall_time_stats["min"]),
                fmt(wall_time_stats["max"]),
                fmt(wall_time_stats["mean"]),
                fmt(wall_time_stats["sum"]),
            )

        cpu_stats = self.cpu_time
        if cpu_stats:
            out += indent
            out += "* Remote cpu time: {} min, {} max, {} mean, {} total\n".format(
                fmt(cpu_stats["min"]),
                fmt(cpu_stats["max"]),
                fmt(cpu_stats["mean"]),
                fmt(cpu_stats["sum"]),
            )

        memory_stats = self.memory
        if memory_stats:
            out += indent
            out += "* Peak heap memory usage (MiB): {} min, {} max, {} mean\n".format(
                memory_stats["min"],
                memory_stats["max"],
                memory_stats["mean"],
            )

        output_num_rows_stats = self.output_num_rows
        if output_num_rows_stats:
            out += indent
            out += "* Output num rows: {} min, {} max, {} mean, {} total\n".format(
                output_num_rows_stats["min"],
                output_num_rows_stats["max"],
                output_num_rows_stats["mean"],
                output_num_rows_stats["sum"],
            )

        output_size_bytes_stats = self.output_size_bytes
        if output_size_bytes_stats:
            out += indent
            out += "* Output size bytes: {} min, {} max, {} mean, {} total\n".format(
                output_size_bytes_stats["min"],
                output_size_bytes_stats["max"],
                output_size_bytes_stats["mean"],
                output_size_bytes_stats["sum"],
            )

        node_count_stats = self.node_count
        if node_count_stats:
            out += indent
            out += "* Tasks per node: {} min, {} max, {} mean; {} nodes used\n".format(
                node_count_stats["min"],
                node_count_stats["max"],
                node_count_stats["mean"],
                node_count_stats["count"],
            )
        return out

    def __repr__(self, level=0) -> str:
        """For a given (pre-calculated) `StageStatsSummary` object (e.g. generated from
        `StageStatsSummary.from_block_metadata()`), returns a human-friendly string
        that summarizes stage execution statistics.

        Returns:
            String with summary statistics for executing the given stage.
        """
        indent = leveled_indent(level)
        indent += leveled_indent(1) if self.is_substage else ""

        wall_time_stats = {k: fmt(v) for k, v in (self.wall_time or {}).items()}
        cpu_stats = {k: fmt(v) for k, v in (self.cpu_time or {}).items()}
        memory_stats = {k: fmt(v) for k, v in (self.memory or {}).items()}
        output_num_rows_stats = {
            k: fmt(v) for k, v in (self.output_num_rows or {}).items()
        }
        output_size_bytes_stats = {
            k: fmt(v) for k, v in (self.output_size_bytes or {}).items()
        }
        node_conut_stats = {k: fmt(v) for k, v in (self.node_count or {}).items()}
        out = (
            f"{indent}StageStatsSummary(\n"
            f"{indent}   stage_name='{self.stage_name}',\n"
            f"{indent}   is_substage={self.is_substage},\n"
            f"{indent}   time_total_s={fmt(self.time_total_s)},\n"
            # block_execution_summary_str already ends with \n
            f"{indent}   block_execution_summary_str={self.block_execution_summary_str}"
            f"{indent}   wall_time={wall_time_stats or None},\n"
            f"{indent}   cpu_time={cpu_stats or None},\n"
            f"{indent}   memory={memory_stats or None},\n"
            f"{indent}   output_num_rows={output_num_rows_stats or None},\n"
            f"{indent}   output_size_bytes={output_size_bytes_stats or None},\n"
            f"{indent}   node_count={node_conut_stats or None},\n"
            f"{indent})"
        )
        return out


@dataclass
class IterStatsSummary:
    # Time spent in actor based prefetching, in seconds.
    wait_time: Timer
    # Time spent in `ray.get()`, in seconds
    get_time: Timer
    # Time spent in batch building, in seconds
    next_time: Timer
    # Time spent in `_format_batch_()`, in seconds
    format_time: Timer
    # Time spent in collate fn, in seconds
    collate_time: Timer
    # Time spent in finalize_fn, in seconds
    finalize_batch_time: Timer
    # Total time user thread is blocked by iter_batches
    block_time: Timer
    # Time spent in user code, in seconds
    user_time: Timer
    # Total time taken by Dataset iterator, in seconds
    total_time: Timer
    # Num of blocks that are in local object store
    iter_blocks_local: int
    # Num of blocks that are in remote node and have to fetch locally
    iter_blocks_remote: int
    # Num of blocks with unknown locations
    iter_unknown_location: int

    def __str__(self) -> str:
        return self.to_string()

    def to_string(self) -> str:
        out = ""
        if (
            self.block_time.get()
            or self.total_time.get()
            or self.get_time.get()
            or self.next_time.get()
            or self.format_time.get()
            or self.collate_time.get()
            or self.finalize_batch_time.get()
        ):
            out += "\nDataset iterator time breakdown:\n"
            if self.block_time.get():
                out += "* Total time user code is blocked: {}\n".format(
                    fmt(self.block_time.get())
                )
            if self.user_time.get():
                out += "* Total time in user code: {}\n".format(
                    fmt(self.user_time.get())
                )
            if self.total_time.get():
                out += "* Total time overall: {}\n".format(fmt(self.total_time.get()))
            out += "* Num blocks local: {}\n".format(self.iter_blocks_local)
            out += "* Num blocks remote: {}\n".format(self.iter_blocks_remote)
            out += "* Num blocks unknown location: {}\n".format(
                self.iter_unknown_location
            )
            out += (
                "* Batch iteration time breakdown (summed across prefetch threads):\n"
            )
            if self.get_time.get():
                out += "    * In ray.get(): {} min, {} max, {} avg, {} total\n".format(
                    fmt(self.get_time.min()),
                    fmt(self.get_time.max()),
                    fmt(self.get_time.avg()),
                    fmt(self.get_time.get()),
                )
            if self.next_time.get():
                batch_creation_str = (
                    "    * In batch creation: {} min, {} max, " "{} avg, {} total\n"
                )
                out += batch_creation_str.format(
                    fmt(self.next_time.min()),
                    fmt(self.next_time.max()),
                    fmt(self.next_time.avg()),
                    fmt(self.next_time.get()),
                )
            if self.format_time.get():
                format_str = (
                    "    * In batch formatting: {} min, {} max, " "{} avg, {} total\n"
                )
                out += format_str.format(
                    fmt(self.format_time.min()),
                    fmt(self.format_time.max()),
                    fmt(self.format_time.avg()),
                    fmt(self.format_time.get()),
                )
            if self.collate_time.get():
                out += "    * In collate_fn: {} min, {} max, {} avg, {} total\n".format(
                    fmt(self.collate_time.min()),
                    fmt(self.collate_time.max()),
                    fmt(self.collate_time.avg()),
                    fmt(self.collate_time.get()),
                )
            if self.finalize_batch_time.get():
                format_str = (
                    "   * In host->device transfer: {} min, {} max, {} avg, {} total\n"
                )
                out += format_str.format(
                    fmt(self.finalize_batch_time.min()),
                    fmt(self.finalize_batch_time.max()),
                    fmt(self.finalize_batch_time.avg()),
                    fmt(self.finalize_batch_time.get()),
                )

        return out

    def __repr__(self, level=0) -> str:
        indent = leveled_indent(level)
        return (
            f"IterStatsSummary(\n"
            f"{indent}   wait_time={fmt(self.wait_time.get()) or None},\n"
            f"{indent}   get_time={fmt(self.get_time.get()) or None},\n"
            f"{indent}   iter_blocks_local={self.iter_blocks_local or None},\n"
            f"{indent}   iter_blocks_remote={self.iter_blocks_remote or None},\n"
            f"{indent}   iter_unknown_location={self.iter_unknown_location or None},\n"
            f"{indent}   next_time={fmt(self.next_time.get()) or None},\n"
            f"{indent}   format_time={fmt(self.format_time.get()) or None},\n"
            f"{indent}   user_time={fmt(self.user_time.get()) or None},\n"
            f"{indent}   total_time={fmt(self.total_time.get()) or None},\n"
            f"{indent})"
        )
