import enum
from typing import Tuple, List, Dict, Optional
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
    def __init__(self,
                 client,
                 name: str,
                 label_names: Tuple[str],
                 dynamic_labels: Optional[Dict[str, str]] = None):
        """Represent a single metric stream

        Args:
            client(MetricClient): The client object to push update to.
            name(str): The name of the metric.
            label_names(Tuple[str]): The names of the labels that must be set
                before an observation.
            dynamic_labels(Optional[Dict[str,str]]): A partially preset labels.
                This fields make it possible to chain label calls together:
                ``metric.labels(a=b).labels(c=d)``.
        """
        self.client = client
        self.name = name
        self.dynamic_labels = dynamic_labels or dict()
        self.label_names = label_names

    def check_all_labels_fulfilled_or_error(self):
        unfulfilled = set(self.label_names) - set(self.dynamic_labels.keys())
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
        new_dynamic_labels = self.dynamic_labels.copy()
        for k, v in kwargs.items():
            if k not in self.label_names:
                raise ValueError(
                    "Label {} was not part of registered "
                    "label names. Allowed label names are {}.".format(
                        k, self.label_names))
            new_dynamic_labels[k] = str(v)
        return type(self)(self.client, self.name, self.label_names,
                          new_dynamic_labels)


# The metric types are inspired by OpenTelemetry spec:
# https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/metrics/api.md#three-kinds-of-instrument
class Counter(BaseMetric):
    def add(self, increment=1):
        """Increment the counter by some amount. Default is 1"""
        self.check_all_labels_fulfilled_or_error()
        self.client.metric_records.append(
            MetricRecord(self.name, self.dynamic_labels, increment))


class Measure(BaseMetric):
    def record(self, value):
        """Record the given value for the measure"""
        self.check_all_labels_fulfilled_or_error()
        self.client.metric_records.append(
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
