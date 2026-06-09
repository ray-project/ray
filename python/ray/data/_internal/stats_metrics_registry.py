"""Shared registry for Ray Data dashboard (Prometheus) metrics.

This module owns the boilerplate that lets a metric be declared here
and published generically by ``_StatsActor``.

A metric is described by a :class:`MetricDefinition`. Definitions are grouped by
namespace (e.g. ``"op_runtime"``, ``"iteration"``, ``"dataset_metadata"``) in
the process-global :data:`GLOBAL_METRICS_REGISTRY`. ``_StatsActor`` reads the
registry to create one Prometheus primitive per definition.

Authors usually declare metrics with the :func:`metric_field` /
:func:`metric_property` decorators (sugar that registers via the
:class:`OpRuntimesMetricsMeta` metaclass), but may also call
:meth:`MetricsRegistry.register` directly for metrics that are not stored
dataclass fields (e.g. overview aliases that rename an existing value).
"""

from dataclasses import Field, dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

# Default namespace for metrics declared on ``OpRuntimeMetrics``.
_OP_RUNTIME_NAMESPACE = "op_runtime"

# A metadata key used to mark a dataclass field as a metric.
_IS_FIELD_METRIC_KEY = "__is_metric"
# Metadata keys used to store information about a metric.
_METRIC_FIELD_DESCRIPTION_KEY = "__metric_description"
_METRIC_FIELD_METRICS_GROUP_KEY = "__metric_metrics_group"
_METRIC_FIELD_METRICS_TYPE_KEY = "__metric_metrics_type"
_METRIC_FIELD_METRICS_ARGS_KEY = "__metric_metrics_args"
_METRIC_FIELD_INTERNAL_ONLY_KEY = "__metric_internal_only"
_METRIC_FIELD_SOURCE_KEY_KEY = "__metric_source_key"
_METRIC_FIELD_PROMETHEUS_NAME_KEY = "__metric_prometheus_name"
_METRIC_FIELD_TAG_KEYS_KEY = "__metric_tag_keys"


class MetricsGroup(Enum):
    INPUTS = "inputs"
    OUTPUTS = "outputs"
    TASKS = "tasks"
    OBJECT_STORE_MEMORY = "object_store_memory"
    MISC = "misc"
    ACTORS = "actors"


class MetricsType(Enum):
    Counter = 0
    Gauge = 1
    Histogram = 2
    Unsupported = 3


@dataclass(frozen=True)
class MetricDefinition:
    """Describes a single metric: how it is identified, where its value comes
    from, and how it is published to Prometheus.

    A metric's value flows through three stages, and this class names the key
    used at each one. They are usually the same string, but keeping them
    separate lets a published series rename a value it borrows from elsewhere::

        producer record dict        registry lookup            Prometheus
        {source_key: value}   --->  match by `name`     --->   `prometheus_name`

    1. A producer (e.g. ``OpRuntimeMetrics.as_dict()``) emits a flat dict of
       values. ``_StatsActor`` reads this metric's value from that dict at
       ``source_key``.
    2. The definition is found in the registry by ``name`` (its identity within
       a namespace).
    3. The value is written to the Prometheus primitive named
       ``prometheus_name``.

    Args:
        name: Identity within a namespace, used to look the definition up in the
            registry. Also the default for ``source_key`` and the base of the
            default ``prometheus_name``. For a metric declared as a dataclass
            field, this is the attribute name.
        description: Human-readable description, i.e. for a chart
            description
        metrics_group: Logical group the metric belongs to, used to organize
            metrics in ``_StatsActor``.
        metrics_type: The Prometheus primitive type (Gauge/Counter/Histogram).
        metrics_args: Extra args forwarded to the Prometheus primitive
            constructor (e.g. histogram buckets).
        internal_only: If True, the metric is hidden from the user (excluded
            from ``as_dict`` when internal metrics are skipped).
        source_key: Key to read the value from under in the producer's record
            dict. Defaults to ``name``; set it only for aliases whose produced
            key differs from the published name.
        prometheus_name: Exact internal Prometheus metric name. Defaults to
            ``f"data_{name}"``. The externally-scraped series additionally
            prepends ``ray_`` (e.g. ``data_spilled_bytes`` -> ``ray_data_spilled_bytes``).
        tag_keys: Prometheus label keys this metric requires (e.g.
            ``("dataset", "operator")``). Every recorded value must supply a
            value for each key.
    """

    name: str
    description: str
    metrics_group: str
    metrics_type: MetricsType
    metrics_args: Dict[str, Any]
    internal_only: bool = False  # do not expose this metric to the user
    source_key: Optional[str] = None
    prometheus_name: Optional[str] = None
    tag_keys: Tuple[str, ...] = ()

    def __post_init__(self):
        # Resolve optional overrides to concrete values once, so consumers can
        # read ``source_key`` / ``prometheus_name`` directly. ``object.__setattr__``
        # is required because the dataclass is frozen.
        if self.source_key is None:
            object.__setattr__(self, "source_key", self.name)
        if self.prometheus_name is None:
            object.__setattr__(self, "prometheus_name", f"data_{self.name}")


class MetricsRegistry:
    """Process-global registry of :class:`MetricDefinition`, keyed by namespace.

    Metrics register once, at import time. The registry just
    collects them. ``_StatsActor`` keys its primitives by metric name.
    """

    def __init__(self):
        # namespace -> list of definitions, in registration order.
        self._by_namespace: Dict[str, List[MetricDefinition]] = {}

    def register(self, namespace: str, definition: MetricDefinition) -> None:
        """Register ``definition`` under ``namespace``."""
        self._by_namespace.setdefault(namespace, []).append(definition)

    def definitions(self, namespace: Optional[str] = None) -> List[MetricDefinition]:
        """Return registered definitions.

        Args:
            namespace: If given, return only that namespace's definitions (in
                registration order). Otherwise return all definitions across
                namespaces.
        """
        if namespace is not None:
            return list(self._by_namespace.get(namespace, []))
        return [d for defs in self._by_namespace.values() for d in defs]

    def namespaces(self) -> List[str]:
        return list(self._by_namespace.keys())


# Process-global singleton. Populated at import time by metric declarations.
GLOBAL_METRICS_REGISTRY = MetricsRegistry()


def metric_field(
    *,
    description: str,
    metrics_group: str,
    metrics_type: MetricsType = MetricsType.Gauge,
    metrics_args: Dict[str, Any] = None,
    internal_only: bool = False,  # do not expose this metric to the user
    source_key: Optional[str] = None,
    prometheus_name: Optional[str] = None,
    tag_keys: Tuple[str, ...] = (),
    **field_kwargs,
):
    """A dataclass field that represents a metric.

    Registration happens later, in :class:`OpRuntimesMetricsMeta`, because
    ``Field.name`` is not set until the dataclass is created.
    """
    metadata = field_kwargs.get("metadata", {})

    metadata[_IS_FIELD_METRIC_KEY] = True

    metadata[_METRIC_FIELD_DESCRIPTION_KEY] = description
    metadata[_METRIC_FIELD_METRICS_GROUP_KEY] = metrics_group
    metadata[_METRIC_FIELD_METRICS_TYPE_KEY] = metrics_type
    metadata[_METRIC_FIELD_METRICS_ARGS_KEY] = metrics_args or {}
    metadata[_METRIC_FIELD_INTERNAL_ONLY_KEY] = internal_only
    metadata[_METRIC_FIELD_SOURCE_KEY_KEY] = source_key
    metadata[_METRIC_FIELD_PROMETHEUS_NAME_KEY] = prometheus_name
    metadata[_METRIC_FIELD_TAG_KEYS_KEY] = tag_keys

    return field(metadata=metadata, **field_kwargs)


def metric_property(
    *,
    description: str,
    metrics_group: str,
    metrics_type: MetricsType = MetricsType.Gauge,
    metrics_args: Dict[str, Any] = None,
    internal_only: bool = False,  # do not expose this metric to the user
    source_key: Optional[str] = None,
    prometheus_name: Optional[str] = None,
    tag_keys: Tuple[str, ...] = (),
    namespace: str = _OP_RUNTIME_NAMESPACE,
):
    """A property that represents a metric."""

    def wrap(func):
        metric = MetricDefinition(
            name=func.__name__,
            description=description,
            metrics_group=metrics_group,
            metrics_type=metrics_type,
            metrics_args=(metrics_args or {}),
            internal_only=internal_only,
            source_key=source_key,
            prometheus_name=prometheus_name,
            tag_keys=tag_keys,
        )

        GLOBAL_METRICS_REGISTRY.register(namespace, metric)

        return property(func)

    return wrap


class OpRuntimesMetricsMeta(type):
    """Metaclass that registers ``metric_field``-declared fields.

    The namespace is read from the class attribute ``_REGISTRY_NAMESPACE``
    (defaulting to ``"op_runtime"``), so other declarative metric groups can
    reuse this metaclass under their own namespace.
    """

    def __init__(cls, name, bases, dict):
        # NOTE: `Field.name` isn't set until the dataclass is created, so we
        # can't create the metrics in `metric_field` directly.
        super().__init__(name, bases, dict)

        namespace = dict.get("_REGISTRY_NAMESPACE", _OP_RUNTIME_NAMESPACE)

        # Iterate over the attributes and methods of the class. If an attribute
        # is a dataclass field with _IS_FIELD_METRIC_KEY in its metadata, create
        # a metric from the field metadata and register it. See `metric_field`.
        for name, value in dict.items():
            if isinstance(value, Field) and value.metadata.get(_IS_FIELD_METRIC_KEY):
                metric = MetricDefinition(
                    name=name,
                    description=value.metadata[_METRIC_FIELD_DESCRIPTION_KEY],
                    metrics_group=value.metadata[_METRIC_FIELD_METRICS_GROUP_KEY],
                    metrics_type=value.metadata[_METRIC_FIELD_METRICS_TYPE_KEY],
                    metrics_args=value.metadata[_METRIC_FIELD_METRICS_ARGS_KEY],
                    internal_only=value.metadata[_METRIC_FIELD_INTERNAL_ONLY_KEY],
                    source_key=value.metadata[_METRIC_FIELD_SOURCE_KEY_KEY],
                    prometheus_name=value.metadata[_METRIC_FIELD_PROMETHEUS_NAME_KEY],
                    tag_keys=value.metadata[_METRIC_FIELD_TAG_KEYS_KEY],
                )
                GLOBAL_METRICS_REGISTRY.register(namespace, metric)
