import asyncio
from typing import Dict, Optional, Tuple

from ray.serve.constants import METRIC_PUSH_INTERVAL_S
from ray.serve.metric.types import (
    MetricType,
    convert_event_type_to_class,
    MetricMetadata,
    MetricRecord,
)
from ray.serve.utils import retry_actor_failures, retry_actor_failures_async


class MetricClient:
    def __init__(self,
                 metric_sink_actor,
                 default_labels: Optional[Dict[str, str]] = None,
                 push_interval: float = METRIC_PUSH_INTERVAL_S):
        """Initialize a client to push metric to sink.

        Args:
            metric_sink_actor: The actor to push metric batch to.
            default_labels(dict): The set of labels to apply for all metrics
                created by this actor. For example, {"source": "worker"}
        """
        self.sink = metric_sink_actor
        self.default_labels = default_labels or dict()

        self.registered_metrics: Dict[str, MetricMetadata] = dict()
        self.metric_records = []

        self.push_task = asyncio.get_event_loop().create_task(
            self.push_forever(push_interval))

    @staticmethod
    def connect_from_serve(default_labels: Optional[Dict[str, str]] = None):
        """Create the metric client automatically when running inside serve"""
        from ray.serve.api import _get_master_actor

        master_actor = _get_master_actor()
        [metric_sink,
         push_interval] = retry_actor_failures(master_actor.get_metric_sink)
        return MetricClient(
            metric_sink_actor=metric_sink,
            default_labels=default_labels,
            push_interval=push_interval)

    def new_counter(self,
                    name: str,
                    *,
                    description: Optional[str] = "",
                    label_names: Optional[Tuple[str]] = ()):
        """Create a new counter.

        Counters are used to capture changes in running sums. An essential
        property of Counter instruments is that two events add(m) and add(n)
        are semantically equivalent to one event add(m+n). This property means
        that Counter events can be combined.

        Args:
            name(str): The unique name for the counter.
            description(Optional[str]): The description for the counter.
            label_names(Optional[Tuple[str]]): The set of label names to be
                added when recording the metrics.

        Usage:
        >>> client = MetricClient(...)
        >>> counter = client.new_counter(
                "http_counter",
                description="This is a simple counter for HTTP status",
                label_names=("route", "status_code"))
        >>> counter.labels(route="/hi", status_code=200).add()
        """
        return self._new_metric(name, MetricType.COUNTER, description,
                                label_names)

    def new_measure(self,
                    name,
                    *,
                    description: Optional[str] = "",
                    label_names: Optional[Tuple[str]] = ()):
        """Create a new measure.

        Measure instruments are independent. They cannot be combined as with
        counters. Measures can be aggregated after recording to compute
        statistics about the distribution along selected dimension.

        Args:
            name(str): The unique name for the measure.
            description(Optional[str]): The description for the measure.
            label_names(Optional[Tuple[str]]): The set of label names to be
                added when recording the metrics.

        Usage:
        >>> client = MetricClient(...)
        >>> measure = client.new_measure(
                "latency_measure",
                description="This is a simple measure for latency in ms",
                label_names=("route"))
        >>> measure.labels(route="/hi").record(42)
        """
        return self._new_metric(name, MetricType.MEASURE, description,
                                label_names)

    def record_event(self, record: MetricRecord):
        """Called by Counter or Measure to record one observation"""
        self.metric_records.append(record)

    def _new_metric(
            self,
            name,
            metric_type: MetricType,
            description: str,
            label_names: Tuple[str] = (),
    ):
        if name in self.registered_metrics:
            raise ValueError(
                "Metric with name {} is already registered.".format(name))

        if not isinstance(label_names, tuple):
            raise ValueError("label_names need to be a tuple, it is {}".format(
                type(label_names)))

        metric_metadata = MetricMetadata(
            name=name,
            type=metric_type,
            description=description,
            label_names=label_names,
            default_labels=self.default_labels.copy(),
        )
        metric_class = convert_event_type_to_class(metric_type)
        metric_object = metric_class(
            client=self, name=name, label_names=label_names)

        self.registered_metrics[name] = metric_metadata
        return metric_object

    async def _push_once(self):
        old_batch, self.metric_records = self.metric_records, []
        await retry_actor_failures_async(self.sink.push_batch,
                                         self.registered_metrics, old_batch)

    async def push_forever(self, interval_s):
        while True:
            await self._push_once()
            await asyncio.sleep(interval_s)
