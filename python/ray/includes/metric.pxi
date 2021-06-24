from ray.includes.metric cimport (
    CCount,
    CGauge,
    CHistogram,
    CTagKey,
    CSum,
    CMetric,
)
from libcpp.memory cimport unique_ptr
from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

cdef class TagKey:
    """Cython wrapper class of C++ `opencensus::stats::TagKey`."""
    cdef c_string name

    def __init__(self, name):
        self.name = name.encode("ascii")
        CTagKey.Register(self.name)

    def name(self):
        return self.name


cdef class Metric:
    """Cython wrapper class of C++ `ray::stats::Metric`.

        It's an abstract class of all metric types.
    """
    cdef:
        unique_ptr[CMetric] metric
        c_vector[CTagKey] c_tag_keys

    def __init__(self, tag_keys):
        for tag_key in tag_keys:
            self.c_tag_keys.push_back(
                CTagKey.Register(tag_key.encode("ascii")))

    def record(self, value, tags=None):
        """Record a measurement of metric.

           Flush a metric raw point to stats module with a key-value dict tags.
        Args:
            value (double): metric name.
            tags (dict): default none.
        """
        cdef unordered_map[c_string, c_string] c_tags
        cdef double c_value
        # Default tags will be exported if it's empty map.
        if tags:
            for tag_k, tag_v in tags.items():
                if tag_v is not None:
                    c_tags[tag_k.encode("ascii")] = tag_v.encode("ascii")
        c_value = value
        with nogil:
            self.metric.get().Record(c_value, c_tags)

    def get_name(self):
        return self.metric.get().GetName()


cdef class Gauge(Metric):
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
    def __init__(self, name, description, tag_keys):
        """Create a gauge metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """
        super().__init__(tag_keys)

        self.metric.reset(
            new CGauge(
                name.encode("ascii"),
                description.encode("ascii"),
                b"",  # Unit, unused.
                self.c_tag_keys
            )
        )


cdef class Count(Metric):
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
    def __init__(self, name, description, tag_keys):
        """Create a count metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """
        super().__init__(tag_keys)

        self.metric.reset(
            new CCount(
                name.encode("ascii"),
                description.encode("ascii"),
                b"",  # Unit, unused.
                self.c_tag_keys
            )
        )


cdef class Sum(Metric):
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
    def __init__(self, name, description, tag_keys):
        """Create a sum metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            tag_keys (list): a list of tay keys in string format.
        """

        super().__init__(tag_keys)

        self.metric.reset(
            new CSum(
                name.encode("ascii"),
                description.encode("ascii"),
                b"",  # Unit, unused.
                self.c_tag_keys
            )
        )


cdef class Histogram(Metric):
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
    def __init__(self, name, description, boundaries, tag_keys):
        """Create a sum metric

        Args:
            name (string): metric name.
            description (string): description of this metric.
            boundaries (list): a double type list boundaries of histogram.
            tag_keys (list): a list of tay key in string format.
        """

        super().__init__(tag_keys)

        cdef c_vector[double] c_boundaries
        for value in boundaries:
            c_boundaries.push_back(value)

        self.metric.reset(
            new CHistogram(
                name.encode("ascii"),
                description.encode("ascii"),
                b"",  # Unit, unused.
                c_boundaries,
                self.c_tag_keys
            )
        )
