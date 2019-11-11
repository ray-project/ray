# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.memory cimport (
    shared_ptr,
    unique_ptr
)

from ray.includes.common cimport CBuffer
from ray.includes.libcoreworker cimport CCoreWorker
from ray.includes.unique_ids cimport (
    CObjectID
)

cdef class BaseID:
    # To avoid the error of "Python int too large to convert to C ssize_t",
    # here `cdef size_t` is required.
    cdef size_t hash(self)

cdef class ObjectID(BaseID):
    cdef:
        CObjectID data
        object buffer_ref
        # Flag indicating whether or not this object ID was added to the set
        # of active IDs in the core worker so we know whether we should clean
        # it up.
        c_bool in_core_worker

    cdef CObjectID native(self)

cdef class CoreWorker:
    cdef:
        unique_ptr[CCoreWorker] core_worker

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectID object_id,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data)

cdef c_vector[c_string] string_vector_from_list(list string_list)