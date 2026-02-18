from libcpp.string cimport string as c_string
from libcpp.memory cimport unique_ptr
from libcpp.vector cimport vector as c_vector

cdef extern from "ray/observability/ray_event_interface.h" namespace "ray::observability" nogil:
    cdef cppclass CRayEventInterface "ray::observability::RayEventInterface":
        pass

cdef extern from "ray/observability/python_event_interface.h" namespace "ray::observability" nogil:
    unique_ptr[CRayEventInterface] CreatePythonRayEvent(
        int source_type,
        int event_type,
        int severity,
        const c_string &entity_id,
        const c_string &message,
        const c_string &session_name,
        const c_string &serialized_event_data,
        int nested_event_field_number)

    cdef cppclass CPythonEventRecorder "ray::observability::PythonEventRecorder":
        CPythonEventRecorder(const c_string &aggregator_address,
                             int aggregator_port,
                             const c_string &node_ip,
                             const c_string &node_id_hex,
                             size_t max_buffer_size,
                             const c_string &metric_source)
        void AddEvents(c_vector[unique_ptr[CRayEventInterface]] &&data_list)
        void Shutdown()
