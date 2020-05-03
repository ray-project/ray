from typing import Dict
from collections import Counter, namedtuple

import ray
from ray.serve.metric.types import MetricType, MetricMetadata, MetricBatch
from ray.serve.utils import _get_logger

logger = _get_logger()


def make_metric_namedtuple(metadata: MetricMetadata, record: MetricBatch):
    fields = ["name", "type"]
    fields += list(metadata.default_labels.keys())
    fields += list(record.labels.keys())

    merged_labels = metadata.default_labels.copy()
    merged_labels.update(record.labels)

    tuple_type = namedtuple(metadata.name, fields)
    return tuple_type(name=metadata.name, type=metadata.type, **merged_labels)


class BaseSink:
    """A sink is the place to store or forward metrics to external services"""

    def __init__(self):
        self.metadata = dict()
        self.recording = []
        logger.debug("Sink initialized {}".format(type(self)))

    def push_batch(self, metadata: Dict[str, MetricMetadata],
                   batch: MetricBatch):
        self.metadata.update(metadata)
        self.recording.extend(batch)
        self.compact()

    def compact(self):
        raise NotImplementedError(
            "This method should be implemented by subclass.")

    def get_metric(self):
        raise NotImplementedError(
            "This method should be implemented by subclass.")


@ray.remote(num_cpus=0)
class InMemorySink(BaseSink):
    def __init__(self):
        super().__init__()

        # Keep track of counters
        self.counters: Counter[namedtuple, float] = Counter()
        # Keep track of latest observation of measures
        self.latest_measures: Dict[namedtuple, float] = dict()

    def compact(self):
        for record in self.recording:
            metadata = self.metadata[record.name]
            metric_key = make_metric_namedtuple(metadata, record)
            if metadata.type == MetricType.COUNTER:
                self.counters[metric_key] += record.value
            elif metadata.type == MetricType.MEASURE:
                self.latest_measures[metric_key] = record.value
            else:
                raise RuntimeError("Unrecognized metric type {}".format(
                    metadata.type))
        self.recording = []

    def get_metric(self):
        items = []
        metrics_to_collect = {**self.counters, **self.latest_measures}
        for info_tuple, value in metrics_to_collect.items():
            # Represent the metric type in human readable name
            info_tuple = info_tuple._replace(type=str(info_tuple.type))
            items.append({"info": info_tuple._asdict(), "value": value})
        return items


@ray.remote(num_cpus=0)
class PrometheusSink(BaseSink):
    def __init__(self):
        super().__init__()
        from prometheus_client import CollectorRegistry

        self.metrics_cache = dict()
        self.default_labels = dict()
        self.registry = CollectorRegistry()

    def get_metric(self):
        from prometheus_client import generate_latest
        return generate_latest(self.registry)

    def compact(self):
        self._process_metadata(self.metadata)
        self._process_batch(self.recording)
        self.recording = []

    def _metric_type_to_prometheus_class(self, metric_type: MetricType):
        from prometheus_client import Counter, Gauge
        if metric_type == MetricType.COUNTER:
            return Counter
        elif metric_type == MetricType.MEASURE:
            return Gauge
        else:
            raise RuntimeError("Unknown metric type {}".format(metric_type))

    def _process_metadata(self, metadata):
        for name, metadata in metadata.items():
            if name not in self.metrics_cache:
                constructor = self._metric_type_to_prometheus_class(
                    metadata.type)

                default_labels = metadata.default_labels
                label_names = tuple(
                    default_labels.keys()) + metadata.label_names
                metric_object = constructor(
                    metadata.name,
                    metadata.description,
                    labelnames=label_names,
                    registry=self.registry,
                )

                self.metrics_cache[name] = (metric_object, metadata.type)
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
