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

from ray.includes.common cimport (
    CBuffer,
    CRayObject
)
from ray.includes.libcoreworker cimport CCoreWorker
from ray.includes.unique_ids cimport (
    CObjectID,
    CActorID
)

cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buffer
        Py_ssize_t shape
        Py_ssize_t strides

    @staticmethod
    cdef make(const shared_ptr[CBuffer]& buffer)

cdef class BaseID:
    # To avoid the error of "Python int too large to convert to C ssize_t",
    # here `cdef size_t` is required.
    cdef size_t hash(self)

cdef class ObjectID(BaseID):
    cdef:
        CObjectID data
        # Flag indicating whether or not this object ID was added to the set
        # of active IDs in the core worker so we know whether we should clean
        # it up.
        c_bool in_core_worker

    cdef CObjectID native(self)

cdef class ActorID(BaseID):
    cdef CActorID data

    cdef CActorID native(self)

    cdef size_t hash(self)

cdef class CoreWorker:
    cdef:
        unique_ptr[CCoreWorker] core_worker
        object async_thread
        object async_event_loop

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectID object_id,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data)
    # TODO: handle noreturn better
    cdef store_task_outputs(
            self, worker, outputs, const c_vector[CObjectID] return_ids,
            c_vector[shared_ptr[CRayObject]] *returns)

cdef c_vector[c_string] string_vector_from_list(list string_list)
