from libcpp.string cimport string as c_string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

cdef extern from "opencensus/tags/tag_key.h" nogil:
    cdef cppclass CTagKey "opencensus::tags::TagKey":
        @staticmethod
        CTagKey Register(c_string &name)
        const c_string &name() const

cdef extern from "ray/stats/metric.h" nogil:
    cdef cppclass CMetric "ray::stats::Metric":
        CMetric(const c_string &name,
                const c_string &description,
                const c_string &unit,
                const c_vector[c_string] &tag_keys)
        c_string GetName() const
        void Record(double value)
        void Record(double value,
                    unordered_map[c_string, c_string] &tags)

    cdef cppclass CGauge "ray::stats::Gauge":
        CGauge(const c_string &name,
               const c_string &description,
               const c_string &unit,
               const c_vector[c_string] &tag_keys)

    cdef cppclass CCount "ray::stats::Count":
        CCount(const c_string &name,
               const c_string &description,
               const c_string &unit,
               const c_vector[c_string] &tag_keys)

    cdef cppclass CSum "ray::stats::Sum":
        CSum(const c_string &name,
             const c_string &description,
             const c_string &unit,
             const c_vector[c_string] &tag_keys)

    cdef cppclass CHistogram "ray::stats::Histogram":
        CHistogram(const c_string &name,
                   const c_string &description,
                   const c_string &unit,
                   const c_vector[double] &boundaries,
                   const c_vector[c_string] &tag_keys)
