# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from cpython.pystate cimport PyThreadState_Get

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
from ray.includes.libcoreworker cimport (
    ActorHandleSharedPtr,
    CActorHandle,
    CFiberEvent,
)

from ray.includes.unique_ids cimport (
    CObjectID,
    CActorID
)
from ray.includes.function_descriptor cimport (
    CFunctionDescriptor,
)

cdef extern from *:
    """
    #if __OPTIMIZE__ && __OPTIMIZE__ == 1
    #undef __OPTIMIZE__
    int __OPTIMIZE__ = 1;
    #define __OPTIMIZE__ 1
    #elif defined(BAZEL_OPT)
    // For compilers that don't define __OPTIMIZE__
    int __OPTIMIZE__ = 1;
    #else
    int __OPTIMIZE__ = 0;
    #endif
    """
    int __OPTIMIZE__

cdef extern from "Python.h":
    # Note(simon): This is used to configure asyncio actor stack size.
    # Cython made PyThreadState an opaque types. Saying that if the user wants
    # specific attributes, they can be declared manually.

    # You can find the cpython definition in Include/cpython/pystate.h#L59
    ctypedef struct CPyThreadState "PyThreadState":
        int recursion_depth

    # From Include/ceveal.h#67
    int Py_GetRecursionLimit()
    void Py_SetRecursionLimit(int)

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

cdef class ObjectRef(BaseID):
    cdef:
        CObjectID data
        # Flag indicating whether or not this object ref was added to the set
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
        c_bool is_driver
        object async_thread
        object async_event_loop
        object plasma_event_handler
        c_bool is_local_mode

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectRef object_ref,
                            c_vector[CObjectID] contained_ids,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data,
                            owner_address=*)
    cdef store_task_outputs(
            self, worker, outputs, const c_vector[CObjectID] return_ids,
            c_vector[shared_ptr[CRayObject]] *returns)
    cdef yield_current_fiber(self, CFiberEvent &fiber_event)
    cdef make_actor_handle(self, ActorHandleSharedPtr c_actor_handle)

cdef class FunctionDescriptor:
    cdef:
        CFunctionDescriptor descriptor
