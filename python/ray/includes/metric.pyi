# source: metric.pxi
from typing import Iterable

class TagKey:
    """Cython wrapper class of C++ `opencensus::stats::TagKey`."""
    def __init__(self,name:str)->None: ...
    def name(self)->bytes: ...

class Metric:
    """Cython wrapper class of C++ `ray::stats::Metric`.

        It's an abstract class of all metric types.
    """
    def __init__(self, tag_keys:Iterable[str])->None: ...

    def record(self, value:float, tags:dict[str,str]|None=None):
        """Record a measurement of metric.

           Flush a metric raw point to stats module with a key-value dict tags.
        Args:
            value (double): metric name.
            tags (dict): default none.
        """
        ...

    def get_name(self)->bytes: ...

class Gauge(Metric):
    """Cython wrapper class of C++ `ray::stats::Gauge`.

        Gauge: Keeps the last recorded value, drops everything before.

        Example:

        >>> gauge = Gauge(
                "ray.worker.metric",
                "description",
                ["tagk1", "tagk2"]).
            value = 5
            key1= "key1"
            key2 = "key2"
            gauge.record(value, {"tagk1": key1, "tagk2": key2})
    """
    def __init__(self, name:str, description:str, tag_keys:Iterable[str]):
        """Create a gauge metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """
        ...

class Count(Metric):
    """Cython wrapper class of C++ `ray::stats::Count`.

        Example:

        >>> count = Count(
                "ray.worker.metric",
                "description",
                ["tagk1", "tagk2"]).
            value = 5
            key1= "key1"
            key2 = "key2"

            count.record(value, {"tagk1": key1, "tagk2": key2})

       Count: The count of the number of metric points.
    """
    def __init__(self, name:str, description:str, tag_keys:Iterable[str]):
        """Create a count metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """
        ...


class Sum(Metric):
    """Cython wrapper class of C++ `ray::stats::Sum`.

        Example:

        >>> metric_sum = Sum(
                "ray.worker.metric",
                "description",
                ["tagk1", "tagk2"]).
            value = 5
            key1= "key1"
            key2 = "key2"

            metric_sum.record(value, {"tagk1": key1, "tagk2": key2})

       Sum: A sum up of the metric points.
    """
    def __init__(self, name:str, description:str, tag_keys:Iterable[str]):
        """Create a sum metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """
        ...


class Histogram(Metric):
    """Cython wrapper class of C++ `ray::stats::Histogram`.

        Example:

        >>> histogram = Histogram(
                "ray.worker.histogram1",
                "description",
                [1.0, 2.0], # boundaries.
                ["tagk1"])
            value = 5
            key1= "key1"

            histogram.record(value, {"tagk1": key1})

       Histogram: Histogram distribution of metric points.
    """
    def __init__(self, name:str, description:str, boundaries:Iterable[float], tag_keys:Iterable[str]):
        """Create a sum metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            boundaries (list): a double type list boundaries of histogram.
            tag_keys (list): a list of tay key in string format.
        """
        ...
