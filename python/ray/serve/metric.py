import enum
import asyncio
from typing import Dict, Optional, List, Tuple, Callable

from prometheus_client import (CollectorRegistry, Counter as PromCounter, Gauge
                               as PromGauge, generate_latest)

import ray
from ray.serve.constants import METRIC_PUSH_INTERVAL_S


class PrometheusSink:
    def __init__(self):
        self.metrics_cache = dict()
        self.registry = CollectorRegistry()

    def get_metric_text(self):
        return generate_latest(self.registry)

    def get_metric_dict(self):
        collected = dict()
        for metric in self.registry.collect():
            for sample in metric.samples:
                collected[sample.name] = {
                    "labels": sample.labels,
                    "value": sample.value
                }
        return collected

    async def push_batch(self, metadata, batch):
        self._process_metadata(metadata)
        self._process_batch(batch)

    def _process_metadata(self, metadata):
        for name, metadata in metadata.items():
            if name not in self.metrics_cache:
                constructor = metadata["type"].to_prometheus_class()
                description = metadata["description"]
                labels = metadata["labels"]

                metric_object = constructor(
                    name,
                    description,
                    labelnames=tuple(labels.keys()),
                    registry=self.registry)
                if labels:
                    metric_object = metric_object.labels(**labels)
                self.metrics_cache[name] = metric_object

    def _process_batch(self, batch):
        for event_type, name, value in batch:
            assert name in self.metrics_cache, (
                "Metrics {} was not registered.".format(name))
            metric = self.metrics_cache[name]
            if event_type == EventType.COUNTER:
                metric.inc(value)
            elif event_type == EventType.MEASURE:
                metric.set(value)
            else:
                raise ValueError(
                    "Unrecognized event type {}".format(event_type))


@ray.remote(num_cpus=0)
class PrometheusSinkActor(PrometheusSink):
    pass


# The metric types are inspired by OpenTelemetry spec:
# https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/metrics/api.md#three-kinds-of-instrument
class Counter:
    def __init__(self, collector, name):
        self.collector = collector
        self.name = name

    def add(self, increment=1):
        self.collector.push_event(EventType.COUNTER, self.name, increment)


class Measure:
    def __init__(self, collector, name):
        self.collector = collector
        self.name = name

    def record(self, value):
        self.collector.push_event(EventType.MEASURE, self.name, value)


class EventType(enum.IntEnum):
    COUNTER = 1
    MEASURE = 2

    def to_user_facing_class(self):
        return {self.COUNTER: Counter, self.MEASURE: Measure}[self.value]

    def to_prometheus_class(self):
        return {self.COUNTER: PromCounter, self.MEASURE: PromGauge}[self.value]


MetricMetadata = Dict[str, Dict]
MetricEventBatch = List[Tuple[EventType, str, float]]


class PushCollector:
    def __init__(self,
                 push_batch_callback: Callable[
                     [MetricMetadata, MetricEventBatch], None],
                 default_labels: Optional[Dict[str, str]] = None):
        self.default_labels = default_labels or dict()

        self.metric_metadata: MetricMetadata = dict()
        self.metric_events: MetricEventBatch = []

        self.push_batch_callback = push_batch_callback
        self.push_task = asyncio.get_event_loop().create_task(
            self.push_forever(METRIC_PUSH_INTERVAL_S))
        self.new_metric_added = asyncio.Event()

    @staticmethod
    def connect_from_serve(default_labels=None):
        from ray.serve.api import _get_master_actor

        master_actor = _get_master_actor()
        [metric_sink] = ray.get(master_actor.get_metric_sink.remote())
        return PushCollector(
            lambda *args: metric_sink.push_batch.remote(*args),
            default_labels=default_labels)

    def new_counter(self,
                    name,
                    *,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None) -> Counter:
        return self._new_metric(EventType.COUNTER, name, description, labels)

    def new_measure(self,
                    name,
                    *,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None) -> Measure:
        return self._new_metric(EventType.MEASURE, name, description, labels)

    def _new_metric(self,
                    metric_type: EventType,
                    name,
                    description: Optional[str] = "",
                    labels: Optional[Dict[str, str]] = None):
        # Normalize Labels
        labels = labels or dict()
        merged_labels = self.default_labels.copy()
        merged_labels.update(labels)

        self.metric_metadata[name] = {
            "type": metric_type,
            "description": description,
            "labels": merged_labels
        }

        metric_class = metric_type.to_user_facing_class()
        return metric_class(collector=self, name=name)

    def push_event(self, metric_type: EventType, name, value):
        self.metric_events.append((metric_type, name, value))

    async def push_once(self):
        old_batch, self.metric_events = self.metric_events, []
        await self.push_batch_callback(self.metric_metadata, old_batch)

    async def push_forever(self, interval_s):
        while True:
            await self.push_once()
            await asyncio.sleep(interval_s)
