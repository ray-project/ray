from typing import Dict
from collections import Counter, namedtuple

import ray
from ray.serve.metric.types import MetricType, MetricMetadata, MetricBatch
from ray.serve.utils import _get_logger

logger = _get_logger()


def make_metric_namedtuple(metric_metadata: MetricMetadata,
                           record: MetricBatch):
    fields = ["name", "type"]
    fields += list(metric_metadata.default_labels.keys())
    fields += list(record.labels.keys())

    merged_labels = metric_metadata.default_labels.copy()
    merged_labels.update(record.labels)

    tuple_type = namedtuple(metric_metadata.name, fields)
    return tuple_type(
        name=metric_metadata.name, type=metric_metadata.type, **merged_labels)


class ExporterInterface:
    def export(self, metric_metadata: Dict[str, MetricMetadata],
               metric_batch: MetricBatch):
        raise NotImplementedError(
            "This method should be implemented by subclass.")

    def inspect_metrics(self):
        raise NotImplementedError(
            "This method should be implemented by subclass.")


@ray.remote(num_cpus=0)
class MetricExporterActor:
    """Aggregate metrics from all RayServe actors and export them.

    Metrics are aggregated pushed from other actors in the system to
    this actor. They can then be stored or pushed to an external monitoring
    system.
    """

    def __init__(self, exporter_class: ExporterInterface):
        # TODO(simon): Add support for initializer args and kwargs.
        self.exporter = exporter_class()

        # Stores the mapping metric_name -> MetricMetadata
        # This field is tolerant to failures since each client will always push
        # an updated copy of the metadata for each ingest call.
        self.metric_metadata = dict()

        logger.debug("Initialized with metric exporter of type {}".format(
            type(self.exporter)))

    def ingest(self, metric_metadata: Dict[str, MetricMetadata],
               batch: MetricBatch):
        self.metric_metadata.update(metric_metadata)
        self.exporter.export(self.metric_metadata, batch)

    def inspect_metrics(self):
        return self.exporter.inspect_metrics()


class InMemoryExporter(ExporterInterface):
    def __init__(self):
        # Keep track of counters
        self.counters: Counter[namedtuple, float] = Counter()
        # Keep track of latest observation of measures
        self.latest_measures: Dict[namedtuple, float] = dict()

    def export(self, metric_metadata, metric_batch):
        for record in metric_batch:
            metadata = metric_metadata[record.name]
            metric_key = make_metric_namedtuple(metadata, record)
            if metadata.type == MetricType.COUNTER:
                self.counters[metric_key] += record.value
            elif metadata.type == MetricType.MEASURE:
                self.latest_measures[metric_key] = record.value
            else:
                raise RuntimeError("Unrecognized metric type {}".format(
                    metadata.type))

    def inspect_metrics(self):
        items = []
        metrics_to_collect = {**self.counters, **self.latest_measures}
        for info_tuple, value in metrics_to_collect.items():
            # Represent the metric type as a human readable name
            info_tuple = info_tuple._replace(type=str(info_tuple.type))
            items.append({"info": info_tuple._asdict(), "value": value})
        return items


class PrometheusExporter(ExporterInterface):
    def __init__(self):
        super().__init__()
        from prometheus_client import (CollectorRegistry, Counter, Gauge,
                                       generate_latest)
        self.metric_type_to_prom_type = {
            MetricType.COUNTER: Counter,
            MetricType.MEASURE: Gauge
        }
        self.prom_generate_latest = generate_latest

        self.metrics_cache = dict()
        self.default_labels = dict()
        self.registry = CollectorRegistry()

    def inspect_metrics(self):
        return self.prom_generate_latest(self.registry)

    def export(self, metric_metadata, metric_batch):
        self._process_metric_metadata(metric_metadata)
        self._process_batch(metric_batch)

    def _process_metric_metadata(self, metric_metadata):
        for name, metric_metadata in metric_metadata.items():
            if name not in self.metrics_cache:
                constructor = self.metric_type_to_prom_type[
                    metric_metadata.type]

                default_labels = metric_metadata.default_labels
                label_names = tuple(
                    default_labels.keys()) + metric_metadata.label_names
                metric_object = constructor(
                    metric_metadata.name,
                    metric_metadata.description,
                    labelnames=label_names,
                    registry=self.registry,
                )

                self.metrics_cache[name] = (metric_object,
                                            metric_metadata.type)
                self.default_labels[name] = default_labels

    def _process_batch(self, batch):
        for name, labels, value in batch:
            assert name in self.metrics_cache, (
                "Metrics {} was not registered.".format(name))
            metric, metric_type = self.metrics_cache[name]
            default_labels = self.default_labels[name]
            merged_labels = {**default_labels, **labels}
            if metric_type == MetricType.COUNTER:
                metric.labels(**merged_labels).inc(value)
            elif metric_type == MetricType.MEASURE:
                metric.labels(**merged_labels).set(value)
            else:
                raise RuntimeError(
                    "Unrecognized metric type {}".format(metric_type))
