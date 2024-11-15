import collections
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import uuid4

import numpy as np

import ray
from ray.actor import ActorHandle
from ray.data._internal.block_list import BlockList
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    MetricsGroup,
    OpRuntimeMetrics,
)
from ray.data._internal.util import capfirst
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI
from ray.util.metrics import Gauge
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

logger = logging.getLogger(__name__)

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
        operator_name: str,
        parent: "DatasetStats",
        override_start_time: Optional[float],
    ):
        self.operator_name = operator_name
        self.parent = parent
        self.start_time = override_start_time or time.perf_counter()

    def build_multioperator(self, metadata: StatsDict) -> "DatasetStats":
        op_metadata = {}
        for i, (k, v) in enumerate(metadata.items()):
            capped_k = capfirst(k)
            if len(metadata) > 1:
                if i == 0:
                    op_metadata[self.operator_name + capped_k] = v
                else:
                    op_metadata[self.operator_name.split("->")[-1] + capped_k] = v
            else:
                op_metadata[self.operator_name] = v
        stats = DatasetStats(
            metadata=op_metadata,
            parent=self.parent,
            base_name=self.operator_name,
        )
        stats.time_total_s = time.perf_counter() - self.start_time
        return stats

    def build(self, final_blocks: BlockList) -> "DatasetStats":
        stats = DatasetStats(
            metadata={self.operator_name: final_blocks.get_metadata()},
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

        # TODO(scottjlee): move these overvie metrics as fields in a
        # separate dataclass, similar to OpRuntimeMetrics.
        self.spilled_bytes = Gauge(
            "data_spilled_bytes",
            description="""Bytes spilled by dataset operators.
                DataContext.enable_get_object_locations_for_metrics
                must be set to True to report this metric""",
            tag_keys=op_tags_keys,
        )
        self.allocated_bytes = Gauge(
            "data_allocated_bytes",
            description="Bytes allocated by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.freed_bytes = Gauge(
            "data_freed_bytes",
            description="Bytes freed by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.current_bytes = Gauge(
            "data_current_bytes",
            description="Bytes currently in memory store used by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.cpu_usage_cores = Gauge(
            "data_cpu_usage_cores",
            description="CPUs allocated to dataset operators",
            tag_keys=op_tags_keys,
        )
        self.gpu_usage_cores = Gauge(
            "data_gpu_usage_cores",
            description="GPUs allocated to dataset operators",
            tag_keys=op_tags_keys,
        )
        self.output_bytes = Gauge(
            "data_output_bytes",
            description="Bytes outputted by dataset operators",
            tag_keys=op_tags_keys,
        )
        self.output_rows = Gauge(
            "data_output_rows",
            description="Rows outputted by dataset operators",
            tag_keys=op_tags_keys,
        )

        # === Metrics from OpRuntimeMetrics ===
        # Inputs-related metrics
        self.execution_metrics_inputs = (
            self._create_prometheus_metrics_for_execution_metrics(
                metrics_group=MetricsGroup.INPUTS,
                tag_keys=op_tags_keys,
            )
        )

        # Outputs-related metrics
        self.execution_metrics_outputs = (
            self._create_prometheus_metrics_for_execution_metrics(
                metrics_group=MetricsGroup.OUTPUTS,
                tag_keys=op_tags_keys,
            )
        )

        # Task-related metrics
        self.execution_metrics_tasks = (
            self._create_prometheus_metrics_for_execution_metrics(
                metrics_group=MetricsGroup.TASKS,
                tag_keys=op_tags_keys,
            )
        )

        # Object store memory-related metrics
        self.execution_metrics_obj_store_memory = (
            self._create_prometheus_metrics_for_execution_metrics(
                metrics_group=MetricsGroup.OBJECT_STORE_MEMORY,
                tag_keys=op_tags_keys,
            )
        )

        # Miscellaneous metrics
        self.execution_metrics_misc = (
            self._create_prometheus_metrics_for_execution_metrics(
                metrics_group=MetricsGroup.MISC,
                tag_keys=op_tags_keys,
            )
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
        self.iter_initialize_s = Gauge(
            "data_iter_initialize_seconds",
            description="Seconds spent in iterator initialization code",
            tag_keys=iter_tag_keys,
        )

    def _create_prometheus_metrics_for_execution_metrics(
        self, metrics_group: MetricsGroup, tag_keys: Tuple[str, ...]
    ) -> Dict[str, Gauge]:
        metrics = {}
        for metric in OpRuntimeMetrics.get_metrics():
            if not metric.metrics_group == metrics_group:
                continue
            metric_name = f"data_{metric.name}"
            metric_description = metric.description
            metrics[metric.name] = Gauge(
                metric_name,
                description=metric_description,
                tag_keys=tag_keys,
            )
        return metrics

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

            self.spilled_bytes.set(stats.get("obj_store_mem_spilled", 0), tags)
            self.freed_bytes.set(stats.get("obj_store_mem_freed", 0), tags)
            self.current_bytes.set(stats.get("obj_store_mem_used", 0), tags)
            self.output_bytes.set(stats.get("bytes_task_outputs_generated", 0), tags)
            self.output_rows.set(stats.get("rows_task_outputs_generated", 0), tags)
            self.cpu_usage_cores.set(stats.get("cpu_usage", 0), tags)
            self.gpu_usage_cores.set(stats.get("gpu_usage", 0), tags)

            for field_name, prom_metric in self.execution_metrics_inputs.items():
                prom_metric.set(stats.get(field_name, 0), tags)

            for field_name, prom_metric in self.execution_metrics_outputs.items():
                prom_metric.set(stats.get(field_name, 0), tags)

            for field_name, prom_metric in self.execution_metrics_tasks.items():
                prom_metric.set(stats.get(field_name, 0), tags)

            for (
                field_name,
                prom_metric,
            ) in self.execution_metrics_obj_store_memory.items():
                prom_metric.set(stats.get(field_name, 0), tags)

            for field_name, prom_metric in self.execution_metrics_misc.items():
                prom_metric.set(stats.get(field_name, 0), tags)

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
        self.iter_initialize_s.set(stats.iter_initialize_s.get(), tags)

    def register_dataset(self, job_id: str, dataset_tag: str, operator_tags: List[str]):
        self.datasets[dataset_tag] = {
            "job_id": job_id,
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

    def get_datasets(self, job_id: Optional[str] = None):
        if not job_id:
            return self.datasets
        return {k: v for k, v in self.datasets.items() if v["job_id"] == job_id}

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
        self._stats_actor_handle: Optional[ActorHandle] = None
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

    def _stats_actor(self, create_if_not_exists=True) -> Optional[ActorHandle]:
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
                try:
                    self._stats_actor_handle = ray.get_actor(
                        name=STATS_ACTOR_NAME, namespace=STATS_ACTOR_NAMESPACE
                    )
                except ValueError:
                    return None
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
                                stats_actor = self._stats_actor(
                                    create_if_not_exists=False
                                )
                                if stats_actor is None:
                                    continue
                                stats_actor.update_metrics.remote(
                                    execution_metrics=list(
                                        self._last_execution_stats.values()
                                    ),
                                    iteration_metrics=list(
                                        self._last_iteration_stats.values()
                                    ),
                                )
                                iter_stats_inactivity = 0
                            except Exception:
                                logger.debug(
                                    "Error occurred during remote call to _StatsActor.",
                                    exc_info=True,
                                )
                                return
                        else:
                            iter_stats_inactivity += 1
                            if (
                                iter_stats_inactivity
                                >= _StatsManager.UPDATE_THREAD_INACTIVITY_LIMIT
                            ):
                                logger.debug(
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

    def clear_last_execution_stats(self, dataset_tag: str):
        # After dataset completes execution, remove cached execution stats.
        # Marks the dataset as finished on job page's Ray Data Overview.
        with self._stats_lock:
            if dataset_tag in self._last_execution_stats:
                del self._last_execution_stats[dataset_tag]

    # Iteration methods

    def update_iteration_metrics(self, stats: "DatasetStats", dataset_tag: str):
        with self._stats_lock:
            self._last_iteration_stats[dataset_tag] = (stats, dataset_tag)
        self._start_thread_if_not_running()

    def clear_iteration_metrics(self, dataset_tag: str):
        # Delete the last iteration stats so that update thread will have
        # a chance to terminate.
        # Note we don't reset the actual metric values through the StatsActor
        # since the value is essentially a counter value. See
        # https://github.com/ray-project/ray/pull/48618 for more context.
        with self._stats_lock:
            if dataset_tag in self._last_iteration_stats:
                del self._last_iteration_stats[dataset_tag]

    # Other methods

    def register_dataset_to_stats_actor(self, dataset_tag, operator_tags):
        self._stats_actor().register_dataset.remote(
            ray.get_runtime_context().get_job_id(),
            dataset_tag,
            operator_tags,
        )

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
        metadata: StatsDict,
        parent: Union[Optional["DatasetStats"], List["DatasetStats"]],
        needs_stats_actor: bool = False,
        stats_uuid: str = None,
        base_name: str = None,
    ):
        """Create dataset stats.

        Args:
            metadata: Dict of operators used to create this Dataset from the
                previous one. Typically one entry, e.g., {"map": [...]}.
            parent: Reference to parent Dataset's stats, or a list of parents
                if there are multiple.
            needs_stats_actor: Whether this Dataset's stats needs a stats actor for
                stats collection. This is currently only used for Datasets using a
                lazy datasource (i.e. a LazyBlockList).
            stats_uuid: The uuid for the stats, used to fetch the right stats
                from the stats actor.
            base_name: The name of the base operation for a multi-operator operation.
        """

        self.metadata: StatsDict = metadata
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

        # Streaming executor stats
        self.streaming_exec_schedule_s: Timer = Timer()

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: Timer = Timer()
        self.iter_get_s: Timer = Timer()
        self.iter_next_batch_s: Timer = Timer()
        self.iter_format_batch_s: Timer = Timer()
        self.iter_collate_batch_s: Timer = Timer()
        self.iter_finalize_batch_s: Timer = Timer()
        self.iter_total_blocked_s: Timer = Timer()
        self.iter_user_s: Timer = Timer()
        self.iter_initialize_s: Timer = Timer()
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

        # Streaming split coordinator stats (dataset level)
        self.streaming_split_coordinator_s: Timer = Timer()

    @property
    def stats_actor(self):
        return _get_or_create_stats_actor()

    def child_builder(
        self, name: str, override_start_time: Optional[float] = None
    ) -> _DatasetStatsBuilder:
        """Start recording stats for an op of the given name (e.g., map)."""
        return _DatasetStatsBuilder(name, self, override_start_time)

    def to_summary(self) -> "DatasetStatsSummary":
        """Generate a `DatasetStatsSummary` object from the given `DatasetStats`
        object, which can be used to generate a summary string."""
        if self.needs_stats_actor:
            ac = self.stats_actor
            # TODO(chengsu): this is a super hack, clean it up.
            stats_map, self.time_total_s = ray.get(ac.get.remote(self.stats_uuid))
            # Only populate stats when stats from all read tasks are ready at
            # stats actor.
            if len(stats_map.items()) == len(self.metadata["Read"]):
                self.metadata["Read"] = []
                for _, blocks_metadata in sorted(stats_map.items()):
                    self.metadata["Read"] += blocks_metadata

        operators_stats = []
        is_sub_operator = len(self.metadata) > 1
        for name, meta in self.metadata.items():
            operators_stats.append(
                OperatorStatsSummary.from_block_metadata(
                    name,
                    meta,
                    is_sub_operator=is_sub_operator,
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
            self.iter_initialize_s,
            self.iter_total_s,
            self.streaming_split_coordinator_s,
            self.iter_blocks_local,
            self.iter_blocks_remote,
            self.iter_unknown_location,
        )
        stats_summary_parents = []
        if self.parents is not None:
            stats_summary_parents = [p.to_summary() for p in self.parents]
        streaming_exec_schedule_s = (
            self.streaming_exec_schedule_s.get()
            if self.streaming_exec_schedule_s
            else 0
        )
        return DatasetStatsSummary(
            operators_stats,
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
            streaming_exec_schedule_s,
        )

    def runtime_metrics(self) -> str:
        """Generate a string representing the runtime metrics of a Dataset. This is
        a high level summary of the time spent in Ray Data code broken down by operator.
        It also includes the time spent in the scheduler. Times are shown as the total
        time for each operator and percentages of time are shown as a fraction of the
        total time for the whole dataset."""
        return self.to_summary().runtime_metrics()


@DeveloperAPI
@dataclass
class DatasetStatsSummary:
    operators_stats: List["OperatorStatsSummary"]
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
    streaming_exec_schedule_s: float

    def to_string(
        self,
        already_printed: Optional[Set[str]] = None,
        include_parent: bool = True,
        add_global_stats=True,
    ) -> str:
        """Return a human-readable summary of this Dataset's stats.

        Args:
            already_printed: Set of operator IDs that have already had its stats printed
            out.
            include_parent: If true, also include parent stats summary; otherwise, only
            log stats of the latest operator.
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
        operators_stats_summary = None
        if len(self.operators_stats) == 1:
            operators_stats_summary = self.operators_stats[0]
            operator_name = operators_stats_summary.operator_name
            operator_uuid = self.dataset_uuid + operator_name
            out += "Operator {} {}: ".format(self.number, operator_name)
            if operator_uuid in already_printed:
                out += "[execution cached]\n"
            else:
                already_printed.add(operator_uuid)
                out += str(operators_stats_summary)
        elif len(self.operators_stats) > 1:
            rounded_total = round(self.time_total_s, 2)
            if rounded_total <= 0:
                # Handle -0.0 case.
                rounded_total = 0
            out += "Operator {} {}: executed in {}s\n".format(
                self.number, self.base_name, rounded_total
            )
            for n, operators_stats_summary in enumerate(self.operators_stats):
                operator_name = operators_stats_summary.operator_name
                operator_uuid = self.dataset_uuid + operator_name
                out += "\n"
                out += "\tSuboperator {} {}: ".format(n, operator_name)
                if operator_uuid in already_printed:
                    out += "\t[execution cached]\n"
                else:
                    already_printed.add(operator_uuid)
                    out += str(operators_stats_summary)
        verbose_stats_logs = DataContext.get_current().verbose_stats_logs
        if verbose_stats_logs and self.extra_metrics:
            indent = (
                "\t"
                if operators_stats_summary and operators_stats_summary.is_sub_operator
                else ""
            )
            out += indent
            out += "* Extra metrics: " + str(self.extra_metrics) + "\n"
        out += str(self.iter_stats)

        if len(self.operators_stats) > 0 and add_global_stats:
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

            # For throughput, we compute both an observed Ray Data dataset throughput
            # and an estimated single node dataset throughput.

            # The observed dataset throughput is computed by dividing the total number
            # of rows produced by the total wall time of the dataset (i.e. from start to
            # finish how long did the dataset take to be processed). With the recursive
            # nature of the DatasetStatsSummary, we use get_total_wall_time to determine
            # the total wall time (this finds the difference between the earliest start
            # and latest end for any block in any operator).

            # The estimated single node dataset throughput is computed by dividing the
            # total number of rows produced the sum of the wall times across all blocks
            # of all operators. This assumes that on a single node the work done would
            # be equivalent, with no concurrency.
            output_num_rows = self.operators_stats[-1].output_num_rows
            total_num_out_rows = output_num_rows["sum"] if output_num_rows else 0
            wall_time = self.get_total_wall_time()
            total_time_all_blocks = self.get_total_time_all_blocks()
            if total_num_out_rows and wall_time and total_time_all_blocks:
                out += "\n"
                out += "Dataset throughput:\n"
                out += (
                    "\t* Ray Data throughput:"
                    f" {total_num_out_rows / wall_time} "
                    "rows/s\n"
                )
                out += (
                    "\t* Estimated single node throughput:"
                    f" {total_num_out_rows / total_time_all_blocks} "
                    "rows/s\n"
                )
        if verbose_stats_logs and add_global_stats:
            out += "\n" + self.runtime_metrics()

        return out

    @staticmethod
    def _collect_dataset_stats_summaries(
        curr: "DatasetStatsSummary",
    ) -> List["DatasetStatsSummary"]:
        summs = []
        # TODO: Do operators ever have multiple parents? Do we need to deduplicate?
        for p in curr.parents:
            if p and p.parents:
                summs.extend(DatasetStatsSummary._collect_dataset_stats_summaries(p))
        return summs + [curr]

    @staticmethod
    def _find_start_and_end(summ: "DatasetStatsSummary") -> Tuple[float, float]:
        earliest_start = min(ops.earliest_start_time for ops in summ.operators_stats)
        latest_end = max(ops.latest_end_time for ops in summ.operators_stats)
        return earliest_start, latest_end

    def runtime_metrics(self) -> str:
        total_wall_time = self.get_total_wall_time()

        def fmt_line(name: str, time: float) -> str:
            return f"* {name}: {fmt(time)} ({time / total_wall_time * 100:.3f}%)\n"

        summaries = DatasetStatsSummary._collect_dataset_stats_summaries(self)
        out = "Runtime Metrics:\n"
        for summ in summaries:
            if len(summ.operators_stats) > 0:
                earliest_start, latest_end = DatasetStatsSummary._find_start_and_end(
                    summ
                )
                op_total_time = latest_end - earliest_start
                out += fmt_line(summ.base_name, op_total_time)
        out += fmt_line("Scheduling", self.streaming_exec_schedule_s)
        out += fmt_line("Total", total_wall_time)
        return out

    def __repr__(self, level=0) -> str:
        indent = leveled_indent(level)
        operators_stats = "\n".join(
            [ss.__repr__(level + 2) for ss in self.operators_stats]
        )
        parent_stats = "\n".join([ps.__repr__(level + 2) for ps in self.parents])
        extra_metrics = "\n".join(
            f"{leveled_indent(level + 2)}{k}: {v},"
            for k, v in self.extra_metrics.items()
        )

        # Handle formatting case for empty outputs.
        operators_stats = (
            f"\n{operators_stats},\n{indent}   " if operators_stats else ""
        )
        parent_stats = f"\n{parent_stats},\n{indent}   " if parent_stats else ""
        extra_metrics = f"\n{extra_metrics}\n{indent}   " if extra_metrics else ""
        return (
            f"{indent}DatasetStatsSummary(\n"
            f"{indent}   dataset_uuid={self.dataset_uuid},\n"
            f"{indent}   base_name={self.base_name},\n"
            f"{indent}   number={self.number},\n"
            f"{indent}   extra_metrics={{{extra_metrics}}},\n"
            f"{indent}   operators_stats=[{operators_stats}],\n"
            f"{indent}   iter_stats={self.iter_stats.__repr__(level+1)},\n"
            f"{indent}   global_bytes_spilled={self.global_bytes_spilled / 1e6}MB,\n"
            f"{indent}   global_bytes_restored={self.global_bytes_restored / 1e6}MB,\n"
            f"{indent}   dataset_bytes_spilled={self.dataset_bytes_spilled / 1e6}MB,\n"
            f"{indent}   parents=[{parent_stats}],\n"
            f"{indent})"
        )

    def get_total_wall_time(self) -> float:
        """Calculate the total wall time for the dataset, this is done by finding
        the earliest start time and latest end time for any block in any operator.
        The wall time is the difference of these two times.
        """
        start_ends = [
            DatasetStatsSummary._find_start_and_end(summ)
            for summ in DatasetStatsSummary._collect_dataset_stats_summaries(self)
            if len(summ.operators_stats) > 0
        ]
        if len(start_ends) == 0:
            return 0
        else:
            earliest_start = min(start_end[0] for start_end in start_ends)
            latest_end = max(start_end[1] for start_end in start_ends)
            return latest_end - earliest_start

    def get_total_time_all_blocks(self) -> float:
        """Calculate the sum of the wall times across all blocks of all operators."""
        summaries = DatasetStatsSummary._collect_dataset_stats_summaries(self)
        return sum(
            (
                sum(
                    ops.wall_time.get("sum", 0) if ops.wall_time else 0
                    for ops in summ.operators_stats
                )
            )
            for summ in summaries
        )

    def get_total_cpu_time(self) -> float:
        parent_sum = sum(p.get_total_cpu_time() for p in self.parents)
        return parent_sum + sum(
            ss.cpu_time.get("sum", 0) for ss in self.operators_stats
        )

    def get_max_heap_memory(self) -> float:
        parent_memory = [p.get_max_heap_memory() for p in self.parents]
        parent_max = max(parent_memory) if parent_memory else 0
        if not self.operators_stats:
            return parent_max

        return max(
            parent_max,
            *[ss.memory.get("max", 0) for ss in self.operators_stats],
        )


@dataclass
class OperatorStatsSummary:
    operator_name: str
    # Whether the operator associated with this OperatorStatsSummary object
    # is a suboperator
    is_sub_operator: bool
    # This is the total walltime of the entire operator, typically obtained from
    # `DatasetStats.time_total_s`. An important distinction is that this is the
    # overall runtime of the operator, pulled from the stats actor, whereas the
    # computed walltimes in `self.wall_time` are calculated on a operator level.
    time_total_s: float
    earliest_start_time: float
    latest_end_time: float
    # String summarizing high-level statistics from executing the operator
    block_execution_summary_str: str
    # The fields below are dicts with stats aggregated across blocks
    # processed in this operator. For example:
    # {"min": ..., "max": ..., "mean": ..., "sum": ...}
    wall_time: Optional[Dict[str, float]] = None
    cpu_time: Optional[Dict[str, float]] = None
    udf_time: Optional[Dict[str, float]] = None
    # memory: no "sum" stat
    memory: Optional[Dict[str, float]] = None
    output_num_rows: Optional[Dict[str, float]] = None
    output_size_bytes: Optional[Dict[str, float]] = None
    # node_count: "count" stat instead of "sum"
    node_count: Optional[Dict[str, float]] = None
    task_rows: Optional[Dict[str, float]] = None

    @classmethod
    def from_block_metadata(
        cls,
        operator_name: str,
        block_metas: List[BlockMetadata],
        is_sub_operator: bool,
    ) -> "OperatorStatsSummary":
        """Calculate the stats for a operator from a given list of blocks,
        and generates a `OperatorStatsSummary` object with the results.

        Args:
            block_metas: List of `BlockMetadata` to calculate stats of
            operator_name: Name of operator associated with `blocks`
            is_sub_operator: Whether this set of blocks belongs to a sub operator.
        Returns:
            A `OperatorStatsSummary` object initialized with the calculated statistics
        """
        exec_stats = [m.exec_stats for m in block_metas if m.exec_stats is not None]
        rounded_total = 0
        time_total_s = 0
        earliest_start_time, latest_end_time = 0, 0

        if exec_stats:
            # Calculate the total execution time of operator as
            # the difference between the latest end time and
            # the earliest start time of all blocks in the operator.
            earliest_start_time = min(s.start_time_s for s in exec_stats)
            latest_end_time = max(s.end_time_s for s in exec_stats)
            time_total_s = latest_end_time - earliest_start_time

        if is_sub_operator:
            exec_summary_str = "{} blocks produced\n".format(len(exec_stats))
        else:
            if exec_stats:
                rounded_total = round(time_total_s, 2)
                if rounded_total <= 0:
                    # Handle -0.0 case.
                    rounded_total = 0
                exec_summary_str = "{} blocks produced in {}s".format(
                    len(exec_stats), rounded_total
                )
            else:
                exec_summary_str = ""
            exec_summary_str += "\n"

        task_rows = collections.defaultdict(int)
        for meta in block_metas:
            if meta.num_rows is not None and meta.exec_stats is not None:
                task_rows[meta.exec_stats.task_idx] += meta.num_rows
        task_rows_stats = None
        if len(task_rows) > 0:
            task_rows_stats = {
                "min": min(task_rows.values()),
                "max": max(task_rows.values()),
                "mean": int(np.mean(list(task_rows.values()))),
                "count": len(task_rows),
            }
            exec_summary_str = "{} tasks executed, {}".format(
                len(task_rows), exec_summary_str
            )

        wall_time_stats, cpu_stats, memory_stats, udf_stats = None, None, None, None
        if exec_stats:
            wall_time_stats = {
                "min": min([e.wall_time_s for e in exec_stats]),
                "max": max([e.wall_time_s for e in exec_stats]),
                "mean": np.mean([e.wall_time_s for e in exec_stats]),
                "sum": sum([e.wall_time_s for e in exec_stats]),
            }
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

            udf_stats = {
                "min": min([e.udf_time_s for e in exec_stats]),
                "max": max([e.udf_time_s for e in exec_stats]),
                "mean": np.mean([e.udf_time_s for e in exec_stats]),
                "sum": sum([e.udf_time_s for e in exec_stats]),
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
            node_tasks = collections.defaultdict(set)
            for s in exec_stats:
                node_tasks[s.node_id].add(s.task_idx)

            node_counts = {node: len(tasks) for node, tasks in node_tasks.items()}
            node_counts_stats = {
                "min": min(node_counts.values()),
                "max": max(node_counts.values()),
                "mean": int(np.mean(list(node_counts.values()))),
                "count": len(node_counts),
            }

        return OperatorStatsSummary(
            operator_name=operator_name,
            is_sub_operator=is_sub_operator,
            time_total_s=time_total_s,
            earliest_start_time=earliest_start_time,
            latest_end_time=latest_end_time,
            block_execution_summary_str=exec_summary_str,
            wall_time=wall_time_stats,
            cpu_time=cpu_stats,
            udf_time=udf_stats,
            memory=memory_stats,
            output_num_rows=output_num_rows_stats,
            output_size_bytes=output_size_bytes_stats,
            node_count=node_counts_stats,
            task_rows=task_rows_stats,
        )

    def __str__(self) -> str:
        """For a given (pre-calculated) `OperatorStatsSummary` object (e.g. generated from
        `OperatorStatsSummary.from_block_metadata()`), returns a human-friendly string
        that summarizes operator execution statistics.

        Returns:
            String with summary statistics for executing the given operator.
        """
        indent = "\t" if self.is_sub_operator else ""
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

        udf_stats = self.udf_time
        if udf_stats:
            out += indent
            out += "* UDF time: {} min, {} max, {} mean, {} total\n".format(
                fmt(udf_stats["min"]),
                fmt(udf_stats["max"]),
                fmt(udf_stats["mean"]),
                fmt(udf_stats["sum"]),
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
            out += (
                "* Output num rows per block: {} min, {} max, {} mean, {} total\n"
            ).format(
                output_num_rows_stats["min"],
                output_num_rows_stats["max"],
                output_num_rows_stats["mean"],
                output_num_rows_stats["sum"],
            )

        output_size_bytes_stats = self.output_size_bytes
        if output_size_bytes_stats:
            out += indent
            out += (
                "* Output size bytes per block: {} min, {} max, {} mean, {} total\n"
            ).format(
                output_size_bytes_stats["min"],
                output_size_bytes_stats["max"],
                output_size_bytes_stats["mean"],
                output_size_bytes_stats["sum"],
            )

        task_rows = self.task_rows
        if task_rows:
            out += indent
            out += (
                "* Output rows per task: {} min, {} max, {} mean, {} tasks used\n"
            ).format(
                task_rows["min"],
                task_rows["max"],
                task_rows["mean"],
                task_rows["count"],
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
        if output_num_rows_stats and self.time_total_s and wall_time_stats:
            # For throughput, we compute both an observed Ray Data operator throughput
            # and an estimated single node operator throughput.

            # The observed Ray Data operator throughput is computed by dividing the
            # total number of rows produced by the wall time of the operator,
            # time_total_s.

            # The estimated single node operator throughput is computed by dividing the
            # total number of rows produced by the the sum of the wall times across all
            # blocks of the operator. This assumes that on a single node the work done
            # would be equivalent, with no concurrency.
            total_num_out_rows = output_num_rows_stats["sum"]
            out += indent
            out += "* Operator throughput:\n"
            out += (
                indent + "\t* Ray Data throughput:"
                f" {total_num_out_rows / self.time_total_s} "
                "rows/s\n"
            )
            out += (
                indent + "\t* Estimated single node throughput:"
                f" {total_num_out_rows / wall_time_stats['sum']} "
                "rows/s\n"
            )
        return out

    def __repr__(self, level=0) -> str:
        """For a given (pre-calculated) `OperatorStatsSummary` object (e.g. generated from
        `OperatorStatsSummary.from_block_metadata()`), returns a human-friendly string
        that summarizes operator execution statistics.

        Returns:
            String with summary statistics for executing the given operator.
        """
        indent = leveled_indent(level)
        indent += leveled_indent(1) if self.is_sub_operator else ""

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
            f"{indent}OperatorStatsSummary(\n"
            f"{indent}   operator_name='{self.operator_name}',\n"
            f"{indent}   is_suboperator={self.is_sub_operator},\n"
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
    initialize_time: Timer
    # Total time taken by Dataset iterator, in seconds
    total_time: Timer
    # Time spent in streaming split coordinator
    streaming_split_coord_time: Timer
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
            if self.total_time.get():
                out += "* Total time overall: {}\n".format(fmt(self.total_time.get()))
            if self.initialize_time.get():
                out += (
                    "    * Total time in Ray Data iterator initialization code: "
                    "{}\n".format(fmt(self.initialize_time.get()))
                )
            if self.block_time.get():
                out += (
                    "    * Total time user thread is blocked by Ray Data iter_batches: "
                    "{}\n".format(fmt(self.block_time.get()))
                )
            if self.user_time.get():
                out += "    * Total execution time for user thread: {}\n".format(
                    fmt(self.user_time.get())
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
                    "    * In host->device transfer: {} min, {} max, {} avg, {} total\n"
                )
                out += format_str.format(
                    fmt(self.finalize_batch_time.min()),
                    fmt(self.finalize_batch_time.max()),
                    fmt(self.finalize_batch_time.avg()),
                    fmt(self.finalize_batch_time.get()),
                )
            if DataContext.get_current().enable_get_object_locations_for_metrics:
                out += "Block locations:\n"
                out += "    * Num blocks local: {}\n".format(self.iter_blocks_local)
                out += "    * Num blocks remote: {}\n".format(self.iter_blocks_remote)
                out += "    * Num blocks unknown location: {}\n".format(
                    self.iter_unknown_location
                )
            if self.streaming_split_coord_time.get() != 0:
                out += "Streaming split coordinator overhead time: "
                out += f"{fmt(self.streaming_split_coord_time.get())}\n"

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
