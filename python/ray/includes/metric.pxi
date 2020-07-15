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
    cdef c_string __name

    def __init__(self, name):
        self.__name = name.encode("ascii")
        CTagKey.Register(self.__name)
    
    def name(self):
        return self.__name

cdef class Metric:
    cdef:
        unique_ptr[CMetric] metric
        c_vector[CTagKey] c_tag_keys

    def __init__(self, tag_keys):
        for tag_key in tag_keys:
            self.c_tag_keys.push_back(CTagKey.Register(tag_key.encode("ascii")))
    
    def record(self, value, tags = None):
        cdef unordered_map[c_string, c_string] c_tags
        cdef double c_value
        # Default tags will be exported if it's empty map.
        if tags:
            for tag_k, tag_v in tags.items():
                c_tags[tag_v.encode("ascii")] = tag_v.encode("ascii")
        c_value = value
        with nogil:
            self.metric.get().Record(c_value, c_tags)

    def get_name(self):
        return self.metric.get().GetName()


cdef class Gauge(Metric):
    def __init__(self, name, description, uint, tag_keys):
        super(Gauge, self).__init__(tag_keys)

        self.metric.reset(
            new CGauge(
                name.encode("ascii"),
                description.encode("ascii"),
                uint.encode("ascii"),
                self.c_tag_keys
            )
        )

cdef class Count(Metric):
    def __init__(self, name, description, uint, tag_keys):
      super(Count, self).__init__(tag_keys)

      self.metric.reset(
          new CCount(
              name.encode("ascii"),
              description.encode("ascii"),
              uint.encode("ascii"),
              self.c_tag_keys
          )
      )

cdef class Sum(Metric):
    def __init__(self, name, description, uint, tag_keys):
        super(Sum, self).__init__(tag_keys)

        self.metric.reset(
            new CSum(
                name.encode("ascii"),
                description.encode("ascii"),
                uint.encode("ascii"),
                self.c_tag_keys
            )
        )

cdef class Histogram(Metric):
    def __init__(self, name, description, uint, boundaries, tag_keys):
        super(Histogram, self).__init__(tag_keys)

        cdef c_vector[double] c_boundaries
        for value in boundaries:
            c_boundaries.push_back(value)

        self.metric.reset(
            new CHistogram(
                name.encode("ascii"),
                description.encode("ascii"),
                uint.encode("ascii"),
                c_boundaries,
                self.c_tag_keys
            )
        )