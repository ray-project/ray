import collections
import copy
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, fields
from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Union,
)

if TYPE_CHECKING:
    from ray.data._internal.scheduling_overhead import BucketedSchedulingOverhead
from uuid import uuid4

import ray
from ray.actor import ActorHandle
from ray.data._internal.execution.dataset_state import DatasetState
from ray.data._internal.execution.interfaces.common import RuntimeMetricsHistogram
from ray.data._internal.execution.interfaces.distribution_tracker import (
    DistributionTracker,
)
from ray.data._internal.execution.interfaces.execution_options import safe_round
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    NODE_UNKNOWN,
    MetricsType,
    NodeMetrics,
    OpRuntimeMetrics,
)
from ray.data._internal.metadata_exporter import (
    DataContextMetadata,
    DatasetMetadata,
    Topology,
    get_dataset_metadata_exporter,
)
from ray.data._internal.metrics_registry import (
    _DATASET_METADATA_NAMESPACE,
    _ITERATION_NAMESPACE,
    _OP_RUNTIME_NAMESPACE,
    _OVERVIEW_NAMESPACE,
    _PER_NODE_NAMESPACE,
    METRICS_REGISTRY,
    MetricDefinition,
    MetricsGroup,
)
from ray.data._internal.util import capfirst
from ray.data.block import BlockStats
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI
from ray.util.metrics import Counter, Gauge, Histogram, Metric

logger = logging.getLogger(__name__)

STATS_ACTOR_NAME = "datasets_stats_actor"
STATS_ACTOR_NAMESPACE = "_dataset_stats_actor"
UNKNOWN = "unknown"
UNKNOWN_UUID = "unknown_uuid"


StatsDict = Dict[str, List[BlockStats]]


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


@dataclass(slots=True)
class StatsSummary:
    """Immutable summary of min/max/mean/sum/count statistics."""

    min: int | float
    max: int | float
    mean: float
    sum: int | float
    count: int

    def to_dict(
        self,
        *,
        mean_as_int: bool = False,
        include_sum: bool = True,
        include_count: bool = True,
    ) -> dict[str, int | float]:
        """Serialize to a plain dict, with optional formatting.

        Args:
            mean_as_int: If True, the ``mean`` value is truncated to ``int``.
            include_sum: If True, include a ``sum`` key.
            include_count: If True, include a ``count`` key.

        Returns:
            A dict with ``min``, ``max``, ``mean``, and optionally ``sum``
            and ``count``.
        """
        result: dict[str, int | float] = {
            "min": self.min,
            "max": self.max,
            "mean": int(self.mean) if mean_as_int else self.mean,
        }
        if include_sum:
            result["sum"] = self.sum
        if include_count:
            result["count"] = self.count
        return result


@dataclass(slots=True)
class _StatsAccumulator:
    """Tracks min/max/sum/count for incremental stats computation."""

    min_value: int | float = float("inf")
    max_value: int | float = float("-inf")
    acc_sum: int | float = 0
    count: int = 0

    def add(self, value: int | float) -> None:
        self.min_value = min(self.min_value, value)
        self.max_value = max(self.max_value, value)
        self.acc_sum += value
        self.count += 1

    def get(
        self,
        *,
        round_digits: Optional[int] = None,
    ) -> Optional[StatsSummary]:
        """Build a ``StatsSummary`` from accumulated values.

        Args:
            round_digits: If set, round ``min``, ``max``, ``mean``, and ``sum``
                to this many decimal places.

        Returns:
            A :class:`StatsSummary`, or ``None`` if no values were added via
            :meth:`add`.
        """
        if not self.count:
            return None
        mean = self.acc_sum / self.count
        return StatsSummary(
            min=safe_round(self.min_value, round_digits),
            max=safe_round(self.max_value, round_digits),
            mean=safe_round(mean, round_digits),
            sum=safe_round(self.acc_sum, round_digits),
            count=self.count,
        )


class Timer:
    """Helper class for tracking accumulated time (in seconds).

    Every value passed to :meth:`add` is also fed into an internal
    :class:`DistributionTracker` (a KLL sketch with bounded memory) so
    :meth:`percentile` can return an approximate p-th percentile at any
    time. The sketch uses O(k log(n/k)) memory (k=200 by default), so it
    stays a few kilobytes regardless of how many samples are added —
    safe for long-running production jobs.

    Percentile accuracy is the KLL guarantee — roughly 1.65% rank error
    at the default k=200. When the optional ``datasketches`` dependency
    is not installed, :meth:`percentile` returns 0 (the other stats are
    unaffected).
    """

    def __init__(self):
        self._total: float = 0
        self._min: float = float("inf")
        self._max: float = 0
        self._total_count: float = 0
        # Bounded-memory percentile backend. add() forwards every value
        # to ``add_sample`` and ``percentile`` reads from it.
        self._distribution: DistributionTracker = DistributionTracker()

    @contextmanager
    def timer(self) -> None:
        time_start = time.perf_counter()
        try:
            yield
        finally:
            self.add(time.perf_counter() - time_start)

    def add(self, value: float) -> None:
        self._total += value
        if value < self._min:
            self._min = value
        if value > self._max:
            self._max = value
        self._total_count += 1
        self._distribution.add_sample(value)

    def get(self) -> float:
        return self._total

    def min(self) -> float:
        return self._min

    def max(self) -> float:
        return self._max

    def avg(self) -> float:
        return self._total / self._total_count if self._total_count else float("inf")

    def percentile(self, p: float) -> float:
        """Approximate ``p``-th percentile in seconds.

        Backed by the internal :class:`DistributionTracker`'s KLL
        sketch. Returns 0 when no samples have been added or the
        optional ``datasketches`` package is unavailable.

        Args:
            p: Percentile as a fraction in ``[0.0, 1.0]`` (e.g. ``0.9``
                for p90 — not ``90``). Values outside this range raise
                ``ValueError``.

        Returns:
            The approximate p-th percentile of all samples seen, or 0
            when the sketch has no data / no backend.

        Raises:
            ValueError: If ``p`` is outside ``[0.0, 1.0]``.
        """
        if not 0.0 <= p <= 1.0:
            raise ValueError(
                f"p must be in [0.0, 1.0], got {p!r}. "
                "Pass a fraction like 0.9, not a percent like 90."
            )
        q = self._distribution._quantile(p)
        return q if q is not None else 0

    def as_dict(self) -> Dict[str, Optional[float]]:
        """Return a JSON-serializable snapshot of the accumulated stats.

        Only the scalar fields are included. ``_distribution`` (a
        :class:`DistributionTracker`) is intentionally omitted because it
        is not JSON-serializable and its sketch is not meant to be
        persisted across checkpoints. ``_min`` / ``_max`` are reported as
        ``None`` when no samples have been added (rather than the ``inf``
        sentinel, which JSON cannot represent).
        """
        return {
            "_total": self._total,
            "_min": self._min if self._total_count > 0 else None,
            "_max": self._max if self._total_count > 0 else None,
            "_total_count": self._total_count,
        }

    def from_dict(self, state: Optional[Dict[str, Optional[float]]]) -> None:
        """Restore the scalar stats from a dict produced by :meth:`as_dict`.

        ``_distribution`` is left untouched (it keeps the empty tracker
        created in ``__init__``), mirroring that the sketch is not
        persisted. A non-dict ``state`` is ignored, and a ``None`` value
        for any field falls back to its empty-Timer default (``.get``'s
        default only fires on a missing key, not a present ``None``).
        """
        if not isinstance(state, dict):
            return
        _total = state.get("_total")
        self._total = _total if _total is not None else 0.0
        _total_count = state.get("_total_count")
        self._total_count = _total_count if _total_count is not None else 0.0
        _min = state.get("_min")
        self._min = _min if _min is not None else float("inf")
        _max = state.get("_max")
        self._max = _max if _max is not None else 0.0


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


# === Dashboard metric declarations ===========================================
# Each block below is a single source of truth: the same table drives both
# registration (one Prometheus primitive per row, created in _StatsActor) and
# value extraction (the _*_values helpers), so a metric's name cannot drift
# between the two. Prometheus names/tag keys are a public contract; keep them
# byte-identical. These are gauges because a dataset's metrics reset to 0 when
# it finishes. Registration runs at import, before _StatsActor.__init__.

# Prometheus label keys shared by the namespaces below.
_OP_TAG_KEYS = ("dataset", "operator")
_ITER_TAG_KEYS = ("dataset",)
_DATASET_META_TAG_KEYS = ("dataset", "job_id", "start_time")
_PER_NODE_TAG_KEYS = ("dataset", "node_ip")


def _gauge(
    name: str,
    description: str,
    *,
    prometheus_name: Optional[str] = None,
    tag_keys: Tuple[str, ...] = (),
) -> MetricDefinition:
    """Build a Gauge ``MetricDefinition`` (the only primitive these blocks use)."""
    return MetricDefinition(
        name=name,
        description=description,
        metrics_group=MetricsGroup.MISC,
        metrics_type=MetricsType.Gauge,
        metrics_args={},
        prometheus_name=prometheus_name,
        tag_keys=tag_keys,
    )


# Overview: 1->1 renames of existing OpRuntimeMetrics values, tagged by
# operator. Default prometheus_name (data_{name}) matches the published series;
# the value is read from the op record dict under ``source_key``.
# (registry name, op-record source key, description)
_OVERVIEW_METRICS = [
    (
        "spilled_bytes",
        "obj_store_mem_spilled",
        """Bytes spilled by dataset operators.
                DataContext.enable_get_object_locations_for_metrics
                must be set to True to report this metric""",
    ),
    ("freed_bytes", "obj_store_mem_freed", "Bytes freed by dataset operators"),
    (
        "current_bytes",
        "obj_store_mem_used",
        "Bytes currently in memory store used by dataset operators",
    ),
    ("cpu_usage_cores", "cpu_usage", "CPUs allocated to dataset operators"),
    ("gpu_usage_cores", "gpu_usage", "GPUs allocated to dataset operators"),
    (
        "output_bytes",
        "bytes_task_outputs_generated",
        "Bytes outputted by dataset operators",
    ),
    ("output_rows", "row_outputs_taken", "Rows outputted by dataset operators"),
]


def _overview_values(op_record: Dict[str, Any]) -> Dict[str, Union[int, float]]:
    """Remap an operator's ``OpRuntimeMetrics.as_dict()`` record to overview names."""
    return {name: op_record.get(src, 0) for name, src, _desc in _OVERVIEW_METRICS}


# Iteration: tagged by dataset. The registry name equals the ``DatasetStats``
# attribute; prometheus_name is explicit because the published series use a
# ``_seconds`` suffix (and ``iter_`` prefix) the attribute name lacks.
# (DatasetStats attr == registry name, prometheus_name, description)
_ITERATION_METRICS = [
    (
        "iter_time_to_first_batch_s",
        "data_iter_time_to_first_batch_seconds",
        "Total time spent waiting for the first batch after starting iteration. "
        "This includes the dataset pipeline warmup time. This metric is "
        "accumulated across different epochs.",
    ),
    (
        "iter_total_blocked_s",
        "data_iter_total_blocked_seconds",
        "Seconds user thread is blocked by iter_batches()",
    ),
    ("iter_user_s", "data_iter_user_seconds", "Seconds spent in user code"),
    (
        "iter_initialize_s",
        "data_iter_initialize_seconds",
        "Seconds spent in iterator initialization code",
    ),
    (
        "iter_get_ref_bundles_s",
        "data_iter_get_ref_bundles_seconds",
        "Seconds spent getting RefBundles from the dataset iterator",
    ),
    (
        "iter_get_s",
        "data_iter_get_seconds",
        "Seconds spent in ray.get() while resolving block references",
    ),
    (
        "iter_next_batch_s",
        "data_iter_next_batch_seconds",
        "Seconds spent getting the next batch from the block buffer",
    ),
    (
        "iter_format_batch_s",
        "data_iter_format_batch_seconds",
        "Seconds spent formatting the batch",
    ),
    (
        "iter_collate_batch_s",
        "data_iter_collate_batch_seconds",
        "Seconds spent collating the batch",
    ),
    (
        "iter_finalize_batch_s",
        "data_iter_finalize_batch_seconds",
        "Seconds spent finalizing the batch",
    ),
    (
        "iter_blocks_local",
        "data_iter_blocks_local",
        "Number of blocks already on the local node",
    ),
    (
        "iter_blocks_remote",
        "data_iter_blocks_remote",
        "Number of blocks that require fetching from another node",
    ),
    (
        "iter_unknown_location",
        "data_iter_unknown_location",
        "Number of blocks that have unknown locations",
    ),
    (
        "iter_prefetched_bytes",
        "data_iter_prefetched_bytes",
        "Current bytes of prefetched blocks in the iterator",
    ),
]


def _iteration_values_from_stats(
    stats: "DatasetStats",
) -> Dict[str, Union[int, float]]:
    """Read each iteration value once from a ``DatasetStats``, keyed by name.

    Timer fields expose ``.get()``; locality/byte counts are plain scalars.
    """
    values = {}
    for attr, _prom, _desc in _ITERATION_METRICS:
        value = getattr(stats, attr)
        values[attr] = value.get() if isinstance(value, Timer) else value
    return values


def _state_value(state_dict: Dict[str, Any]) -> int:
    """Map a dataset/operator state dict to its ``DatasetState`` enum value."""
    state_string = state_dict.get("state", DatasetState.UNKNOWN.name)
    return DatasetState.from_string(state_string).value


# Dataset/operator metadata. The two groups carry different tag keys; default
# prometheus_name matches the published data_dataset_* / data_operator_* series.
# (registry name, description, value getter over the state dict)
_DATASET_METADATA_METRICS = [
    (
        "dataset_estimated_total_blocks",
        "Total work units in blocks for dataset",
        _DATASET_META_TAG_KEYS,
        lambda st: st.get("total", 0),
    ),
    (
        "dataset_estimated_total_rows",
        "Total work units in rows for dataset",
        _DATASET_META_TAG_KEYS,
        lambda st: st.get("total_rows", 0),
    ),
    ("dataset_state", None, _DATASET_META_TAG_KEYS, _state_value),
    (
        "operator_estimated_total_blocks",
        "Total work units in blocks for operator",
        _OP_TAG_KEYS,
        lambda st: st.get("total", 0),
    ),
    (
        "operator_estimated_total_rows",
        "Total work units in rows for operator",
        _OP_TAG_KEYS,
        lambda st: st.get("total_rows", 0),
    ),
    (
        "operator_queued_blocks",
        "Number of queued blocks for operator",
        _OP_TAG_KEYS,
        lambda st: st.get("queued_blocks", 0),
    ),
    ("operator_state", None, _OP_TAG_KEYS, _state_value),
]


def _register_dashboard_metrics() -> None:
    """Register the overview/iteration/metadata/per-node definitions."""
    for name, _src, description in _OVERVIEW_METRICS:
        METRICS_REGISTRY.register(
            _OVERVIEW_NAMESPACE, _gauge(name, description, tag_keys=_OP_TAG_KEYS)
        )
    for name, prometheus_name, description in _ITERATION_METRICS:
        METRICS_REGISTRY.register(
            _ITERATION_NAMESPACE,
            _gauge(
                name,
                description,
                prometheus_name=prometheus_name,
                tag_keys=_ITER_TAG_KEYS,
            ),
        )
    states = ", ".join(f"{s.value}={s.name}" for s in DatasetState)
    for name, description, tag_keys, _getter in _DATASET_METADATA_METRICS:
        if name == "dataset_state":
            description = f"State of dataset ({states})"
        elif name == "operator_state":
            description = f"State of operator ({states})"
        METRICS_REGISTRY.register(
            _DATASET_METADATA_NAMESPACE, _gauge(name, description, tag_keys=tag_keys)
        )
    # Per-node metrics: one gauge per NodeMetrics field.
    for node_field in fields(NodeMetrics):
        METRICS_REGISTRY.register(
            _PER_NODE_NAMESPACE,
            _gauge(
                node_field.name,
                "",
                prometheus_name=f"data_{node_field.name}_per_node",
                tag_keys=_PER_NODE_TAG_KEYS,
            ),
        )


_register_dashboard_metrics()


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

        # Assign dataset uuids with a global counter.
        self.next_dataset_id = 0
        # Dataset metadata to be queried directly by DashboardHead api.
        self.datasets: Dict[str, Any] = {}

        # Cache of calls to ray.nodes() to prevent unnecessary network calls
        self._ray_nodes_cache: Dict[str, str] = {}

        # Initialize the metadata exporter
        self._metadata_exporter = get_dataset_metadata_exporter()
        self.dataset_metadatas: Dict[str, DatasetMetadata] = {}

        # A FIFO queue of dataset_tags for finished datasets. This is used to
        # efficiently evict the oldest finished datasets when max_stats is reached.
        self.finished_datasets_queue = collections.deque()

        # Ray Data dashboard metrics. Every metric is declared in the registry
        # (see the _register_dashboard_metrics module function and
        # OpRuntimeMetrics); build one Prometheus primitive per definition,
        # keyed by namespace -> {metric_name: Metric}.
        self._prom_metrics: Dict[str, Dict[str, Metric]] = {}
        for namespace, default_tag_keys in (
            (_OP_RUNTIME_NAMESPACE, _OP_TAG_KEYS),
            (_OVERVIEW_NAMESPACE, _OP_TAG_KEYS),
            (_ITERATION_NAMESPACE, _ITER_TAG_KEYS),
            (_DATASET_METADATA_NAMESPACE, ()),
            (_PER_NODE_NAMESPACE, _PER_NODE_TAG_KEYS),
        ):
            self._prom_metrics[namespace] = self._create_prometheus_metrics(
                namespace, default_tag_keys=default_tag_keys
            )

    def _create_prometheus_metrics(
        self, namespace: str, default_tag_keys: Tuple[str, ...]
    ) -> Dict[str, Metric]:
        """Build the Prometheus primitives for one registry namespace.

        Args:
            namespace: Registry namespace to build primitives for (e.g.
                ``"op_runtime"``).
            default_tag_keys: Prometheus label keys applied to any definition
                that does not declare its own ``tag_keys``.

        Returns:
            A ``{metric_name: Metric}`` dict with one Gauge/Counter/Histogram per
            registered metric definition (``Unsupported`` types are skipped). The
            dict key is the metric's ``name``, the value the Prometheus primitive.
        """
        metrics = {}
        for metric in METRICS_REGISTRY.definitions(namespace):
            if metric.metrics_type == MetricsType.Unsupported:
                continue
            tag_keys = metric.tag_keys or default_tag_keys
            if metric.metrics_type == MetricsType.Gauge:
                metrics[metric.name] = Gauge(
                    metric.prometheus_name,
                    description=metric.description,
                    tag_keys=tag_keys,
                )
            elif metric.metrics_type == MetricsType.Histogram:
                metrics[metric.name] = Histogram(
                    metric.prometheus_name,
                    description=metric.description,
                    tag_keys=tag_keys,
                    **metric.metrics_args,
                )
            elif metric.metrics_type == MetricsType.Counter:
                metrics[metric.name] = Counter(
                    metric.prometheus_name,
                    description=metric.description,
                    tag_keys=tag_keys,
                )
        return metrics

    def gen_dataset_id(self) -> str:
        """Generate a unique dataset_id for tracking datasets."""
        dataset_id = str(self.next_dataset_id)
        self.next_dataset_id += 1
        return dataset_id

    def _record_metric(
        self,
        prom_metric: Metric,
        value: Union[int, float, List[int]],
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        if isinstance(prom_metric, Gauge):
            prom_metric.set(value, tags)
        elif isinstance(prom_metric, Counter):
            prom_metric.inc(value, tags)
        elif isinstance(prom_metric, Histogram):
            if isinstance(value, RuntimeMetricsHistogram):
                value.export_to(prom_metric, tags)

    def _record_values(
        self,
        namespace: str,
        values: Dict[str, Union[int, float]],
        tags: Dict[str, str],
    ) -> None:
        """Record a caller-built ``{metric_name: value}`` group for ``namespace``.

        ``tags`` is built once per group and reused. Only names present in
        ``values`` are recorded, so a single namespace can hold metrics with
        different tag sets (e.g. dataset- vs operator-tagged metadata).
        """
        prom_metrics = self._prom_metrics[namespace]
        for name, value in values.items():
            self._record_metric(prom_metrics[name], value, tags)

    def update_execution_metrics(
        self,
        dataset_tag: str,
        op_metrics: List[Dict[str, int | float]],
        operator_tags: List[str],
        state: Dict[str, Any],
        per_node_metrics: Optional[Dict[str, Dict[str, int | float]]] = None,
    ):
        for stats, operator_tag in zip(op_metrics, operator_tags):
            tags = self._create_tags(dataset_tag, operator_tag)

            self._record_values(_OVERVIEW_NAMESPACE, _overview_values(stats), tags)
            for field_name, prom_metric in self._prom_metrics[
                _OP_RUNTIME_NAMESPACE
            ].items():
                self._record_metric(prom_metric, stats.get(field_name, 0), tags)

        # Update per node metrics if they exist, the creation of these metrics is controlled
        # by the _data_context.enable_per_node_metrics flag in the streaming executor but
        # that is not exposed in the _StatsActor so here we simply check if the metrics exist
        # and if so, update them
        if per_node_metrics is not None:
            for node_id, node_metrics in per_node_metrics.items():
                # Translate node_id into node_name (the node ip), cache node info
                if node_id not in self._ray_nodes_cache:
                    # Rebuilding this cache will fetch all nodes, this
                    # only needs to be done up to once per loop
                    self._rebuild_ray_nodes_cache()

                node_ip = self._ray_nodes_cache.get(node_id, NODE_UNKNOWN)

                tags = self._create_tags(dataset_tag=dataset_tag, node_ip_tag=node_ip)
                self._record_values(_PER_NODE_NAMESPACE, node_metrics, tags)

        # This update is called from a dataset's executor,
        # so all tags should contain the same dataset
        self.update_dataset(dataset_tag, state)

    def _rebuild_ray_nodes_cache(self) -> None:
        current_nodes = ray.nodes()
        for node in current_nodes:
            node_id = node.get("NodeID", None)
            node_name = node.get("NodeName", None)
            if node_id is not None and node_name is not None:
                self._ray_nodes_cache[node_id] = node_name

    def update_iteration_metrics(
        self,
        stats: "DatasetStats",
        dataset_tag,
    ):
        tags = self._create_tags(dataset_tag)
        self._record_values(
            _ITERATION_NAMESPACE, _iteration_values_from_stats(stats), tags
        )

    def register_dataset(
        self,
        job_id: str,
        dataset_tag: str,
        operator_tags: List[str],
        topology: Topology,
        data_context: DataContextMetadata,
    ):
        start_time = time.time()
        self.datasets[dataset_tag] = {
            "job_id": job_id,
            "state": DatasetState.PENDING.name,
            "progress": 0,
            "total": 0,
            "total_rows": 0,
            "start_time": start_time,
            "end_time": None,
            "operators": {
                operator: {
                    "state": DatasetState.PENDING.name,
                    "progress": 0,
                    "total": 0,
                    "queued_blocks": 0,
                }
                for operator in operator_tags
            },
        }
        if self._metadata_exporter is not None:
            self.dataset_metadatas[dataset_tag] = DatasetMetadata(
                job_id=job_id,
                topology=topology,
                dataset_id=dataset_tag,
                start_time=start_time,
                data_context=data_context,
                execution_start_time=None,
                execution_end_time=None,
                state=DatasetState.PENDING.name,
            )
            self._metadata_exporter.export_dataset_metadata(
                self.dataset_metadatas[dataset_tag]
            )

    def update_dataset(self, dataset_tag: str, state: Dict[str, Any]):
        self.datasets[dataset_tag].update(state)
        state = self.datasets[dataset_tag]

        job_id = self.datasets[dataset_tag].get("job_id", "None")
        start_time = str(int(self.datasets[dataset_tag].get("start_time", 0)))

        # Update dataset-level metrics
        dataset_tags = {
            "dataset": dataset_tag,
            "job_id": job_id,
            "start_time": start_time,
        }
        dataset_values = {
            name: getter(state)
            for name, _desc, tag_keys, getter in _DATASET_METADATA_METRICS
            if tag_keys == _DATASET_META_TAG_KEYS
        }
        self._record_values(_DATASET_METADATA_NAMESPACE, dataset_values, dataset_tags)
        state_string = state.get("state", DatasetState.UNKNOWN.name)
        self.update_dataset_metadata_state(dataset_tag, state_string)

        # Update operator-level metrics
        operator_states: Dict[str, str] = {}
        for operator, op_state in state.get("operators", {}).items():
            operator_tags = {
                "dataset": dataset_tag,
                "operator": operator,
            }
            operator_values = {
                name: getter(op_state)
                for name, _desc, tag_keys, getter in _DATASET_METADATA_METRICS
                if tag_keys == _OP_TAG_KEYS
            }
            self._record_values(
                _DATASET_METADATA_NAMESPACE, operator_values, operator_tags
            )
            operator_states[operator] = op_state.get("state", DatasetState.UNKNOWN.name)

        self.update_dataset_metadata_operator_states(dataset_tag, operator_states)

        # Evict the oldest finished datasets to ensure the `max_stats` limit is enforced.
        if state["state"] in {DatasetState.FINISHED.name, DatasetState.FAILED.name}:
            self.finished_datasets_queue.append(dataset_tag)
            while len(self.datasets) > self.max_stats and self.finished_datasets_queue:
                tag_to_evict = self.finished_datasets_queue.popleft()
                self.datasets.pop(tag_to_evict, None)
                self.dataset_metadatas.pop(tag_to_evict, None)

    def get_datasets(self, job_id: Optional[str] = None):
        if not job_id:
            return self.datasets
        return {k: v for k, v in self.datasets.items() if v["job_id"] == job_id}

    def update_dataset_metadata_state(self, dataset_id: str, new_state: str):
        if dataset_id not in self.dataset_metadatas:
            return
        update_time = time.time()
        dataset_metadata = self.dataset_metadatas[dataset_id]
        if dataset_metadata.state == new_state:
            return
        updated_dataset_metadata = copy.deepcopy(dataset_metadata)
        updated_dataset_metadata.state = new_state
        if new_state == DatasetState.RUNNING.name:
            updated_dataset_metadata.execution_start_time = update_time
        elif new_state in (DatasetState.FINISHED.name, DatasetState.FAILED.name):
            updated_dataset_metadata.execution_end_time = update_time
            # Update metadata of running operators
            for operator in updated_dataset_metadata.topology.operators:
                if operator.state == DatasetState.RUNNING.name:
                    operator.state = new_state
                    operator.execution_end_time = update_time

        self.dataset_metadatas[dataset_id] = updated_dataset_metadata
        if self._metadata_exporter is not None:
            self._metadata_exporter.export_dataset_metadata(
                updated_dataset_metadata,
                include_data_context=False,
                include_op_args=False,
            )

    def update_dataset_metadata_operator_states(
        self, dataset_id: str, operator_states: Dict[str, str]
    ):
        if dataset_id not in self.dataset_metadatas:
            return

        dataset_metadata = self.dataset_metadatas[dataset_id]
        update_needed = False
        for operator in dataset_metadata.topology.operators:
            if (
                operator.id in operator_states
                and operator.state != operator_states[operator.id]
            ):
                update_needed = True
                break

        if not update_needed:
            return

        updated_dataset_metadata = copy.deepcopy(dataset_metadata)
        update_time = time.time()
        for operator in updated_dataset_metadata.topology.operators:
            if operator.id in operator_states:
                new_state = operator_states[operator.id]
                if operator.state == new_state:
                    continue
                operator.state = new_state
                if new_state == DatasetState.RUNNING.name:
                    operator.execution_start_time = update_time
                elif new_state in (
                    DatasetState.FINISHED.name,
                    DatasetState.FAILED.name,
                ):
                    operator.execution_end_time = update_time
                    # Handle outlier case for InputDataBuffer, which is marked as finished immediately and does not have a RUNNING state.
                    # Set the execution time the same as its end time
                    if not operator.execution_start_time:
                        operator.execution_start_time = update_time

        self.dataset_metadatas[dataset_id] = updated_dataset_metadata
        if self._metadata_exporter is not None:
            self._metadata_exporter.export_dataset_metadata(
                updated_dataset_metadata,
                include_data_context=False,
                include_op_args=False,
            )

    def _create_tags(
        self,
        dataset_tag: str,
        operator_tag: Optional[str] = None,
        node_ip_tag: Optional[str] = None,
    ):
        tags = {"dataset": dataset_tag}
        if operator_tag is not None:
            tags["operator"] = operator_tag
        if node_ip_tag is not None:
            tags["node_ip"] = node_ip_tag
        return tags


def get_or_create_stats_actor() -> ActorHandle[_StatsActor]:
    """Each cluster will contain exactly 1 _StatsActor. This function
    returns the current _StatsActor handle, or create a new one if one
    does not exist in the connected cluster. The _StatsActor is pinned on
    on driver process' node.
    """
    if not ray.is_initialized():
        raise RuntimeError(
            "Ray is not initialized. Driver might be not connected to Ray."
        )

    # `_global_node` is None under Ray Client (the driver is not a cluster
    # worker), so only log the cluster_id when it is available.
    global_node = ray._private.worker._global_node
    if global_node is not None:
        logger.debug(f"Stats Actor located on cluster_id={global_node.cluster_id}")

    # so it fate-shares with the driver.
    label_selector = {
        ray._raylet.RAY_NODE_ID_KEY: ray.get_runtime_context().get_node_id()
    }

    return _StatsActor.options(
        name=STATS_ACTOR_NAME,
        namespace=STATS_ACTOR_NAMESPACE,
        get_if_exists=True,
        lifetime="detached",
        label_selector=label_selector,
    ).remote()


class _StatsManager:
    """A Class containing util functions that manage remote calls to _StatsActor.

    Ray Data updates metrics through the _StatsManager, and direct remote calls
    to the _StatsActor is discouraged. Some functionalities provided by
    _StatsManager:
        - Format and update iteration metrics
        - Format and update execution metrics
        - Aggregate per node metrics
        - Dataset registration
    """

    @staticmethod
    def _aggregate_per_node_metrics(
        op_metrics: List[OpRuntimeMetrics],
    ) -> Optional[Mapping[str, Mapping[str, int | float]]]:
        """
        Aggregate per-node metrics from a list of OpRuntimeMetrics objects.

        If per-node metrics are disabled in the current DataContext, returns None.
        Otherwise, it sums up all NodeMetrics fields across the provided metrics and
        returns a nested dictionary mapping each node ID to a dict of field values.
        """
        if not DataContext.get_current().enable_per_node_metrics:
            return None

        aggregated_by_node = defaultdict(lambda: defaultdict(int))
        for metrics in op_metrics:
            for node_id, node_metrics in metrics._per_node_metrics.items():
                agg_node_metrics = aggregated_by_node[node_id]
                for f in fields(NodeMetrics):
                    agg_node_metrics[f.name] += getattr(node_metrics, f.name)

        return aggregated_by_node

    @staticmethod
    def update_execution_metrics(
        dataset_tag: str,
        op_metrics: List[OpRuntimeMetrics],
        operator_tags: List[str],
        state: Dict[str, Any],
    ):
        per_node_metrics = _StatsManager._aggregate_per_node_metrics(op_metrics)
        op_metrics_dicts = [metric.as_dict() for metric in op_metrics]
        args = (
            dataset_tag,
            op_metrics_dicts,
            operator_tags,
            state,
            per_node_metrics,
        )
        try:
            get_or_create_stats_actor().update_execution_metrics.remote(*args)
        except Exception as e:
            logger.warning(
                f"Error occurred during update_execution_metrics.remote call to _StatsActor: {e}",
                exc_info=True,
            )
            return

    @staticmethod
    def update_iteration_metrics(stats: "DatasetStats", dataset_tag: str):
        args = (stats, dataset_tag)
        try:
            get_or_create_stats_actor().update_iteration_metrics.remote(*args)
        except Exception as e:
            logger.warning(
                f"Error occurred during update_iteration_metrics.remote call to _StatsActor: {e}",
                exc_info=True,
            )

    @staticmethod
    def register_dataset_to_stats_actor(
        dataset_tag: str,
        operator_tags: List[str],
        topology: Topology,
        data_context: DataContext,
    ):
        """Register a dataset with the stats actor.

        Args:
            dataset_tag: Tag for the dataset
            operator_tags: List of operator tags
            topology: Optional Topology representing the DAG structure to export
            data_context: The DataContext attached to the dataset
        """
        # Convert DataContext to DataContextMetadata before serialization to avoid
        # module dependency issues during Ray's cloudpickle serialization.
        data_context = DataContextMetadata.from_data_context(data_context)

        get_or_create_stats_actor().register_dataset.remote(
            ray.get_runtime_context().get_job_id(),
            dataset_tag,
            operator_tags,
            topology,
            data_context,
        )

    @staticmethod
    def gen_dataset_id_from_stats_actor() -> str:
        try:
            stats_actor = get_or_create_stats_actor()

            return ray.get(stats_actor.gen_dataset_id.remote())
        except Exception as e:
            logger.warning(
                f"Failed to generate dataset_id, falling back to random uuid_v4: {e}"
            )
            # Getting dataset id from _StatsActor may fail, in this case
            # fall back to uuid4
            return uuid4().hex


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
        base_name: str = None,
    ):
        """Create dataset stats.

        Args:
            metadata: Dict of operators used to create this Dataset from the
                previous one. Typically one entry, e.g., {"map": [...]}.
            parent: Reference to parent Dataset's stats, or a list of parents
                if there are multiple.
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
        self.dataset_uuid: str = UNKNOWN_UUID
        self.time_total_s: float = 0

        # Streaming executor stats. Timer's KLL-sketch percentile
        # backend has bounded memory, so p50/p90 tracking is always on
        # — no opt-in needed.
        self.streaming_exec_schedule_s: Timer = Timer()

        # Iteration stats, filled out if the user iterates over the dataset.
        self.iter_wait_s: Timer = Timer()
        self.iter_get_ref_bundles_s: Timer = Timer()
        self.iter_get_s: Timer = Timer()
        self.iter_next_batch_s: Timer = Timer()
        self.iter_format_batch_s: Timer = Timer()
        self.iter_collate_batch_s: Timer = Timer()
        self.iter_finalize_batch_s: Timer = Timer()
        self.iter_time_to_first_batch_s: Timer = Timer()
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
        self.iter_prefetched_bytes: int = 0

        # Memory usage stats
        self.global_bytes_spilled: int = 0
        self.global_bytes_restored: int = 0
        self.dataset_bytes_spilled: int = 0

        # Streaming split coordinator stats (dataset level)
        self.streaming_split_coordinator_s: Timer = Timer()

    @property
    def stats_actor(self):
        return get_or_create_stats_actor()

    def child_builder(
        self, name: str, override_start_time: Optional[float] = None
    ) -> _DatasetStatsBuilder:
        """Start recording stats for an op of the given name (e.g., map)."""
        return _DatasetStatsBuilder(name, self, override_start_time)

    def to_summary(self) -> "DatasetStatsSummary":
        """Generate a `DatasetStatsSummary` object from the given `DatasetStats`
        object, which can be used to generate a summary string."""
        operators_stats = []
        is_sub_operator = len(self.metadata) > 1

        iter_stats = IterStatsSummary(
            self.iter_wait_s,
            self.iter_get_ref_bundles_s,
            self.iter_get_s,
            self.iter_next_batch_s,
            self.iter_format_batch_s,
            self.iter_collate_batch_s,
            self.iter_finalize_batch_s,
            self.iter_time_to_first_batch_s,
            self.iter_total_blocked_s,
            self.iter_user_s,
            self.iter_initialize_s,
            self.iter_total_s,
            self.streaming_split_coordinator_s,
            self.iter_blocks_local,
            self.iter_blocks_remote,
            self.iter_unknown_location,
            self.iter_prefetched_bytes,
        )

        stats_summary_parents = []
        if self.parents is not None:
            stats_summary_parents = [p.to_summary() for p in self.parents]

        # Collect the sum of the final output row counts from all parent nodes
        parent_total_output = 0
        for i, parent_summary in enumerate(stats_summary_parents):
            if parent_summary.operators_stats:
                # Get the last operator stats from the current parent summary
                last_parent_op = parent_summary.operators_stats[-1]
                # Extract output row count (handle dict type with "sum" key)
                op_output = (
                    last_parent_op.output_num_rows.sum
                    if last_parent_op.output_num_rows
                    else 0
                )
                logger.debug(
                    f"Parent {i + 1} (operator: {last_parent_op.operator_name}) contributes {op_output} rows to input"
                )
                parent_total_output += op_output

        # Create temporary operator stats objects from block metadata
        op_stats = [
            OperatorStatsSummary.from_block_metadata(
                name, stats, is_sub_operator=is_sub_operator
            )
            for name, stats in self.metadata.items()
        ]

        for i, op_stat in enumerate(op_stats):
            # For sub-operators: inherit input based on the order in the current list
            if is_sub_operator:
                if i == 0:
                    # Input of the first sub-operator is the total output from parent nodes
                    op_stat.total_input_num_rows = parent_total_output
                else:
                    # Input of subsequent sub-operators is the output of the previous sub-operator
                    prev_op = op_stats[i - 1]
                    op_stat.total_input_num_rows = (
                        prev_op.output_num_rows.sum if prev_op.output_num_rows else 0
                    )
            else:
                # Single operator scenario: input rows = total output from all parent nodes
                op_stat.total_input_num_rows = parent_total_output
            operators_stats.append(op_stat)
        # Keep ``streaming_exec_schedule_s`` as the total wall-clock time so
        # ``runtime_metrics()`` can still divide by total_wall_time and
        # produce a meaningful percentage. Per-iteration avg/max are
        # exposed separately. ``StreamingExecutor._generate_stats``
        # always assigns a ``Timer`` (never ``None``), so this call site
        # needs no guard.
        schedule_timer = self.streaming_exec_schedule_s
        streaming_exec_schedule_s = schedule_timer.get()
        streaming_exec_schedule_avg_s = schedule_timer.avg()
        streaming_exec_schedule_max_s = schedule_timer.max()
        streaming_exec_schedule_p50_s = schedule_timer.percentile(0.5)
        streaming_exec_schedule_p90_s = schedule_timer.percentile(0.9)
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
            streaming_exec_schedule_avg_s,
            streaming_exec_schedule_max_s,
            streaming_exec_schedule_p50_s,
            streaming_exec_schedule_p90_s,
        )

    def runtime_metrics(self) -> str:
        """Generate a string representing the runtime metrics of a Dataset. This is
        a high level summary of the time spent in Ray Data code broken down by operator.
        It also includes the time spent in the scheduler. Times are shown as the total
        time for each operator and percentages of time are shown as a fraction of the
        total time for the whole dataset."""
        return self.to_summary().runtime_metrics()

    def set_uuid_recursive(self, dataset_uuid: Optional[str]) -> None:
        """Recursively set the dataset uuid (if not None) throughout all stats parents."""
        if (
            self.dataset_uuid is None or self.dataset_uuid == UNKNOWN_UUID
        ) and dataset_uuid is not None:
            self.dataset_uuid = dataset_uuid
        for parent in self.parents:
            parent.set_uuid_recursive(dataset_uuid)


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
    streaming_exec_schedule_avg_s: float
    streaming_exec_schedule_max_s: float
    # KLL-sketch-approximate percentiles (k=200, ~1.65% rank error).
    # 0 when no samples have been added, or when the optional
    # ``datasketches`` dependency is unavailable.
    streaming_exec_schedule_p50_s: float
    streaming_exec_schedule_p90_s: float

    def to_string(
        self,
        already_printed: Optional[Set[str]] = None,
        include_parent: bool = True,
        add_global_stats: bool = True,
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

            if self.num_rows_per_s:
                out += "\n"
                out += "Dataset throughput:\n"
                out += f"\t* Ray Data throughput: {self.num_rows_per_s} rows/s\n"
        if verbose_stats_logs and add_global_stats:
            out += "\n" + self.runtime_metrics()

        return out

    @property
    def num_rows_per_s(self) -> float:
        """Calculates the throughput in rows per second for the entire dataset."""
        # The observed dataset throughput is computed by dividing the total number
        # of rows produced by the total wall time of the dataset (i.e. from start to
        # finish how long did the dataset take to be processed). With the recursive
        # nature of the DatasetStatsSummary, we use get_total_wall_time to determine
        # the total wall time (this finds the difference between the earliest start
        # and latest end for any block in any operator).
        output_num_rows = (
            self.operators_stats[-1].output_num_rows if self.operators_stats else None
        )
        total_num_out_rows = output_num_rows.sum if output_num_rows else 0
        wall_time = self.get_total_wall_time()
        if not total_num_out_rows or not wall_time:
            return 0.0
        return total_num_out_rows / wall_time

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
        start_times = [
            ops.earliest_start_time
            for ops in summ.operators_stats
            if ops.earliest_start_time is not None
        ]
        end_times = [
            ops.latest_end_time
            for ops in summ.operators_stats
            if ops.latest_end_time is not None
        ]
        earliest_start = min(start_times) if start_times else 0
        latest_end = max(end_times) if end_times else 0
        return earliest_start, latest_end

    def runtime_metrics(self) -> str:
        total_wall_time = self.get_total_wall_time()

        def fmt_line(name: str, time: float) -> str:
            fraction = time / total_wall_time if total_wall_time > 0 else 0
            return f"* {name}: {fmt(time)} ({fraction * 100:.3f}%)\n"

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
            f"{indent}   iter_stats={self.iter_stats.__repr__(level + 1)},\n"
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
                    ops.wall_time.sum if ops.wall_time else 0
                    for ops in summ.operators_stats
                )
            )
            for summ in summaries
        )

    def get_total_cpu_time(self) -> float:
        parent_sum = sum(p.get_total_cpu_time() for p in self.parents)
        return parent_sum + sum(
            ss.cpu_time.sum if ss.cpu_time else 0 for ss in self.operators_stats
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
    earliest_start_time: Optional[float]
    latest_end_time: Optional[float]
    # String summarizing high-level statistics from executing the operator
    block_execution_summary_str: str
    wall_time: Optional[StatsSummary] = None
    cpu_time: Optional[StatsSummary] = None
    udf_time: Optional[StatsSummary] = None
    total_input_num_rows: Optional[int] = None
    output_num_rows: Optional[StatsSummary] = None
    output_size_bytes: Optional[StatsSummary] = None
    node_count: Optional[StatsSummary] = None
    task_rows: Optional[StatsSummary] = None
    scheduling_overhead: Optional[List["BucketedSchedulingOverhead"]] = None

    @property
    def num_rows_per_s(self) -> float:
        # The observed Ray Data operator throughput is computed by dividing the
        # total number of rows produced by the wall time of the operator,
        # time_total_s.
        if not self.output_num_rows or not self.time_total_s:
            return 0.0
        return self.output_num_rows.sum / self.time_total_s

    @property
    def num_rows_per_task_s(self) -> float:
        """Calculates the estimated single-task throughput in rows per second."""
        # The estimated single task operator throughput is computed by dividing the
        # total number of rows produced by the sum of the wall times across all
        # blocks of the operator. This assumes that on a single task the work done
        # would be equivalent, with no concurrency.
        if not self.output_num_rows or not self.wall_time or not self.wall_time.sum:
            return 0.0
        return self.output_num_rows.sum / self.wall_time.sum

    @classmethod
    def from_block_metadata(
        cls,
        operator_name: str,
        block_stats: List[BlockStats],
        is_sub_operator: bool,
    ) -> "OperatorStatsSummary":
        """Calculate the stats for a operator from a given list of blocks,
        and generates a `OperatorStatsSummary` object with the results.

        Args:
            operator_name: Name of operator associated with `blocks`
            block_stats: List of `BlockStats` to calculate stats of
            is_sub_operator: Whether this set of blocks belongs to a sub operator.
        Returns:
            A `OperatorStatsSummary` object initialized with the calculated statistics
        """
        # Single pass over block_stats to collect all metrics.
        wall_time_acc: _StatsAccumulator = _StatsAccumulator()
        cpu_time_acc: _StatsAccumulator = _StatsAccumulator()
        udf_time_acc: _StatsAccumulator = _StatsAccumulator()
        output_rows_acc: _StatsAccumulator = _StatsAccumulator()
        output_sizes_acc: _StatsAccumulator = _StatsAccumulator()
        rows_per_task: DefaultDict[int, int] = collections.defaultdict(int)
        tasks_per_node: DefaultDict[str, Set[int]] = collections.defaultdict(set)
        num_exec = 0
        earliest_start_time, latest_end_time = float("inf"), float("-inf")

        for block_meta in block_stats:
            if block_meta.num_rows is not None:
                output_rows_acc.add(block_meta.num_rows)
            if block_meta.size_bytes is not None:
                output_sizes_acc.add(block_meta.size_bytes)

            es = block_meta.exec_stats
            if es is not None:
                num_exec += 1
                if es.wall_time_s is not None:
                    wall_time_acc.add(es.wall_time_s)
                if es.cpu_time_s is not None:
                    cpu_time_acc.add(es.cpu_time_s)
                if es.udf_time_s is not None:
                    udf_time_acc.add(es.udf_time_s)
                tasks_per_node[es.node_id].add(es.task_idx)
                if es.start_time_s is not None:
                    earliest_start_time = min(earliest_start_time, es.start_time_s)
                if es.end_time_s is not None:
                    latest_end_time = max(latest_end_time, es.end_time_s)
                if block_meta.num_rows is not None:
                    rows_per_task[es.task_idx] += block_meta.num_rows

        # Compute timing totals.
        if num_exec and earliest_start_time != float("inf"):
            time_total_s = latest_end_time - earliest_start_time
            # Handle -0.0 case.
            rounded_total = round(time_total_s, 2)
            if rounded_total <= 0:
                rounded_total = 0
        else:
            time_total_s = 0
            rounded_total = 0
            earliest_start_time, latest_end_time = None, None

        # Build execution summary string.
        if is_sub_operator:
            exec_summary_str = f"{num_exec} blocks produced\n"
        elif num_exec:
            exec_summary_str = f"{num_exec} blocks produced in {rounded_total}s\n"
        else:
            exec_summary_str = "\n"

        # Task-level row stats.
        task_rows_stats = None
        if rows_per_task:
            task_rows_acc = _StatsAccumulator()
            for count in rows_per_task.values():
                task_rows_acc.add(count)
            task_rows_stats = task_rows_acc.get()
            exec_summary_str = (
                f"{task_rows_acc.count} tasks executed, {exec_summary_str}"
            )

        # Execution stats.
        wall_time_stats = wall_time_acc.get()
        cpu_stats = cpu_time_acc.get()
        udf_stats = udf_time_acc.get()

        # Output stats.
        output_num_rows_stats = output_rows_acc.get()
        output_size_bytes_stats = output_sizes_acc.get()

        # Node distribution stats.
        node_counts_stats = None
        if tasks_per_node:
            node_counts_acc = _StatsAccumulator()
            for tasks in tasks_per_node.values():
                node_counts_acc.add(len(tasks))
            node_counts_stats = node_counts_acc.get()

        # Assign a value in to_summary and initialize it as None.
        total_input_num_rows = None

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
            total_input_num_rows=total_input_num_rows,
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

        if self.wall_time:
            out += indent
            out += "* Remote wall time: {} min, {} max, {} mean, {} total\n".format(
                fmt(self.wall_time.min),
                fmt(self.wall_time.max),
                fmt(self.wall_time.mean),
                fmt(self.wall_time.sum),
            )

        if self.cpu_time:
            out += indent
            out += "* Remote cpu time: {} min, {} max, {} mean, {} total\n".format(
                fmt(self.cpu_time.min),
                fmt(self.cpu_time.max),
                fmt(self.cpu_time.mean),
                fmt(self.cpu_time.sum),
            )

        if self.udf_time:
            out += indent
            out += "* UDF time: {} min, {} max, {} mean, {} total\n".format(
                fmt(self.udf_time.min),
                fmt(self.udf_time.max),
                fmt(self.udf_time.mean),
                fmt(self.udf_time.sum),
            )

        if self.output_num_rows:
            out += indent
            out += (
                "* Output num rows per block: {} min, {} max, {} mean, {} total\n"
            ).format(
                self.output_num_rows.min,
                self.output_num_rows.max,
                int(self.output_num_rows.mean),
                self.output_num_rows.sum,
            )

        if self.output_size_bytes:
            out += indent
            out += (
                "* Output size bytes per block: {} min, {} max, {} mean, {} total\n"
            ).format(
                self.output_size_bytes.min,
                self.output_size_bytes.max,
                int(self.output_size_bytes.mean),
                self.output_size_bytes.sum,
            )

        if self.task_rows:
            out += indent
            out += (
                "* Output rows per task: {} min, {} max, {} mean, {} tasks used\n"
            ).format(
                self.task_rows.min,
                self.task_rows.max,
                int(self.task_rows.mean),
                self.task_rows.count,
            )

        if self.node_count:
            out += indent
            out += "* Tasks per node: {} min, {} max, {} mean; {} nodes used\n".format(
                self.node_count.min,
                self.node_count.max,
                int(self.node_count.mean),
                self.node_count.count,
            )
        if self.num_rows_per_s and self.num_rows_per_task_s:
            total_num_in_rows = (
                self.total_input_num_rows if self.total_input_num_rows else 0
            )
            total_num_out_rows = self.output_num_rows.sum
            out += indent
            out += "* Operator throughput:\n"
            out += indent + f"\t* Total input num rows: {total_num_in_rows} rows\n"
            out += indent + f"\t* Total output num rows: {total_num_out_rows} rows\n"
            out += indent + f"\t* Ray Data throughput: {self.num_rows_per_s} rows/s\n"
            out += (
                indent + "\t* Estimated single task throughput:"
                f" {self.num_rows_per_task_s} "
                "rows/s\n"
            )
        return out

    def __repr__(self, level: int = 0) -> str:
        """For a given (pre-calculated) `OperatorStatsSummary` object (e.g. generated from
        `OperatorStatsSummary.from_block_metadata()`), returns a human-friendly string
        that summarizes operator execution statistics.

        Args:
            level: The indentation level to use when formatting nested summaries.

        Returns:
            String with summary statistics for executing the given operator.
        """
        indent = leveled_indent(level)
        indent += leveled_indent(1) if self.is_sub_operator else ""

        def _fmt_dict(
            s: Optional[StatsSummary],
            include_sum: bool = True,
            include_count: bool = False,
        ) -> Optional[dict]:
            if s is None:
                return None
            return {
                k: fmt(v)
                for k, v in s.to_dict(
                    include_sum=include_sum, include_count=include_count
                ).items()
            }

        out = (
            f"{indent}OperatorStatsSummary(\n"
            f"{indent}   operator_name='{self.operator_name}',\n"
            f"{indent}   is_suboperator={self.is_sub_operator},\n"
            f"{indent}   time_total_s={fmt(self.time_total_s)},\n"
            # block_execution_summary_str already ends with \n
            f"{indent}   block_execution_summary_str={self.block_execution_summary_str}"
            f"{indent}   wall_time={_fmt_dict(self.wall_time)},\n"
            f"{indent}   cpu_time={_fmt_dict(self.cpu_time)},\n"
            f"{indent}   output_num_rows={_fmt_dict(self.output_num_rows)},\n"
            f"{indent}   output_size_bytes={_fmt_dict(self.output_size_bytes)},\n"
            f"{indent}   node_count={_fmt_dict(self.node_count, include_sum=False, include_count=True)},\n"
            f"{indent})"
        )
        return out


@dataclass
class IterStatsSummary:
    # Time spent in actor based prefetching, in seconds.
    wait_time: Timer
    # Time spent getting RefBundles from the dataset iterator, in seconds
    get_ref_bundles_time: Timer
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
    # Time user thread is blocked waiting for first batch
    time_to_first_batch: Timer
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
    # Current bytes of prefetched blocks in the iterator
    iter_prefetched_bytes: int

    def __str__(self) -> str:
        return self.to_string()

    def to_string(self) -> str:
        out = ""
        if (
            self.block_time.get()
            or self.time_to_first_batch.get()
            or self.total_time.get()
            or self.get_ref_bundles_time.get()
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
            if self.time_to_first_batch.get():
                out += (
                    "    * Total time spent waiting for the first batch after starting iteration: "
                    "{}\n".format(fmt(self.time_to_first_batch.get()))
                )
            if self.user_time.get():
                out += "    * Total execution time for user thread: {}\n".format(
                    fmt(self.user_time.get())
                )
            out += (
                "* Batch iteration time breakdown (summed across prefetch threads):\n"
            )
            if self.get_ref_bundles_time.get():
                out += "    * In get RefBundles: {} min, {} max, {} avg, {} total\n".format(
                    fmt(self.get_ref_bundles_time.min()),
                    fmt(self.get_ref_bundles_time.max()),
                    fmt(self.get_ref_bundles_time.avg()),
                    fmt(self.get_ref_bundles_time.get()),
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
                    "    * In batch creation: {} min, {} max, {} avg, {} total\n"
                )
                out += batch_creation_str.format(
                    fmt(self.next_time.min()),
                    fmt(self.next_time.max()),
                    fmt(self.next_time.avg()),
                    fmt(self.next_time.get()),
                )
            if self.format_time.get():
                format_str = (
                    "    * In batch formatting: {} min, {} max, {} avg, {} total\n"
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
            if self.iter_prefetched_bytes:
                out += "    * Prefetched bytes: {}\n".format(self.iter_prefetched_bytes)
            if self.streaming_split_coord_time.get() != 0:
                out += "Streaming split coordinator overhead time: "
                out += f"{fmt(self.streaming_split_coord_time.get())}\n"

        return out

    def __repr__(self, level=0) -> str:
        indent = leveled_indent(level)
        return (
            f"IterStatsSummary(\n"
            f"{indent}   wait_time={fmt(self.wait_time.get()) or None},\n"
            f"{indent}   get_ref_bundles_time={fmt(self.get_ref_bundles_time.get()) or None},\n"
            f"{indent}   get_time={fmt(self.get_time.get()) or None},\n"
            f"{indent}   iter_blocks_local={self.iter_blocks_local or None},\n"
            f"{indent}   iter_blocks_remote={self.iter_blocks_remote or None},\n"
            f"{indent}   iter_unknown_location={self.iter_unknown_location or None},\n"
            f"{indent}   iter_prefetched_bytes={self.iter_prefetched_bytes or None},\n"
            f"{indent}   next_time={fmt(self.next_time.get()) or None},\n"
            f"{indent}   format_time={fmt(self.format_time.get()) or None},\n"
            f"{indent}   user_time={fmt(self.user_time.get()) or None},\n"
            f"{indent}   total_time={fmt(self.total_time.get()) or None},\n"
            f"{indent})"
        )
