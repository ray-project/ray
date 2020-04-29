import enum
from typing import Dict, Optional, Tuple, List
from collections import namedtuple

# We split the information about a metric into two parts: the MetricMetadata
# and MetricRecord. Metadata is declared at creation time and include names
# and the label names. The label values will be supplied at observation time.
MetricMetadata = namedtuple(
    "MetricMetadata",
    ["name", "type", "description", "label_names", "default_labels"])
MetricRecord = namedtuple("MetricRecord", ["name", "labels", "value"])
MetricBatch = List[MetricRecord]


class BaseMetric:
    def __init__(self, client: "MetricClient", name: str,
                 label_names: Tuple[str]):
        self.client = client
        self.name = name
        self.dynamic_labels = dict()
        self.labelnames = label_names

    def check_all_labels_fulfilled_or_error(self):
        unfulfilled = set(self.labelnames) - set(self.dynamic_labels.keys())
        if len(unfulfilled) != 0:
            raise ValueError("The following labels doesn't have associated "
                             "values: {}".format(unfulfilled))

    def labels(self, **kwargs):
        """Apply dynamic label to the metric
        
        Usage:
        >>> metric = BaseMetric(..., label_names=("a", "b"))
        >>> metric.labels(a=1, b=2)
        >>> metric.labels(a=1).labels(b=2) # Equivalent
        """
        for k, v in kwargs.items():
            if k not in self.labelnames:
                raise ValueError(
                    "Label {} was not part of registered "
                    "label names. Allowed label names are {}.".format(
                        k, self.labelnames))
            self.dynamic_labels[k] = str(v)
        return self


# The metric types are inspired by OpenTelemetry spec:
# https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/metrics/api.md#three-kinds-of-instrument
class Counter(BaseMetric):
    def add(self, increment=1):
        """Increment the counter by some amount. Default is 1"""
        self.check_all_labels_fulfilled_or_error()
        self.client.record_event(
            MetricRecord(self.name, self.dynamic_labels, increment))


class Measure(BaseMetric):
    def record(self, value):
        """Record the given value for the measure"""
        self.check_all_labels_fulfilled_or_error()
        self.client.record_event(
            MetricRecord(self.name, self.dynamic_labels, value))


class MetricType(enum.IntEnum):
    COUNTER = 1
    MEASURE = 2


def convert_event_type_to_class(event_type: MetricType) -> BaseMetric:
    if event_type == MetricType.COUNTER:
        return Counter
    if event_type == MetricType.MEASURE:
        return Measure
    raise RuntimeError("Unknown event type {}".format(event_type))
