from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

cdef extern from "ray/stats/metric.h" nogil:
    cdef cppclass CMetric "ray::stats::Metric":
        CMetric(const c_string &name,
                const c_string &description,
                const c_string &unit,
                const c_vector[c_string] &tag_keys)
        c_string GetName() const
        void Record(double value)
        void RecordForCython(double value,
                    c_vector[c_pair[c_string, c_string]] tags)

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
