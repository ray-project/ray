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

