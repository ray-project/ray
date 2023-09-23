# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from cpython.pystate cimport PyThreadState_Get

from libc.stdint cimport (
    int64_t,
)
from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport (
    shared_ptr,
    unique_ptr
)
from libcpp.pair cimport pair as c_pair
from libcpp.utility cimport pair
from ray.includes.optional cimport (
    optional,
    nullopt,
    make_optional,
)

from ray.includes.common cimport (
    CBuffer,
    CRayObject,
    CAddress,
    CConcurrencyGroup,
    CSchedulingStrategy,
    CLabelMatchExpressions,
)
from ray.includes.libcoreworker cimport (
    ActorHandleSharedPtr,
    CActorHandle,
    CFiberEvent,
)

from ray.includes.unique_ids cimport (
    CObjectID,
    CActorID,
    CTaskID,
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
        int recursion_limit
        int recursion_remaining

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
        c_string owner_addr
        # Flag indicating whether or not this object ref was added to the set
        # of active IDs in the core worker so we know whether we should clean
        # it up.
        c_bool in_core_worker
        c_string call_site_data

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
        object job_config
        object current_runtime_env
        c_bool is_local_mode

        object cgname_to_eventloop_dict
        object eventloop_for_default_cg
        object thread_for_default_cg
        object fd_to_cgname_dict
        object _task_id_to_future_lock
        dict _task_id_to_future
        object thread_pool_for_async_event_loop

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectRef object_ref,
                            c_vector[CObjectID] contained_ids,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data,
                            c_bool created_by_worker,
                            owner_address=*,
                            c_bool inline_small_object=*)
    cdef unique_ptr[CAddress] _convert_python_address(self, address=*)
    cdef store_task_output(
            self, serialized_object,
            const CObjectID &return_id,
            const CObjectID &generator_id,
            size_t data_size, shared_ptr[CBuffer] &metadata, const c_vector[CObjectID]
            &contained_id, const CAddress &caller_address,
            int64_t *task_output_inlined_bytes,
            shared_ptr[CRayObject] *return_ptr)
    cdef store_task_outputs(
            self,
            worker, outputs,
            const CAddress &caller_address,
            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
            CObjectID ref_generator_id=*)
    cdef yield_current_fiber(self, CFiberEvent &fiber_event)
    cdef make_actor_handle(self, ActorHandleSharedPtr c_actor_handle)
    cdef c_function_descriptors_to_python(
        self, const c_vector[CFunctionDescriptor] &c_function_descriptors)
    cdef initialize_eventloops_for_actor_concurrency_group(
        self, const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups)
    cdef python_scheduling_strategy_to_c(
        self, python_scheduling_strategy,
        CSchedulingStrategy *c_scheduling_strategy)
    cdef python_label_match_expressions_to_c(
        self, python_expressions,
        CLabelMatchExpressions *c_expressions)
    cdef CObjectID allocate_dynamic_return_id_for_generator(
            self,
            const CAddress &owner_address,
            const CTaskID &task_id,
            return_size,
            generator_index,
            is_async_actor)

cdef class FunctionDescriptor:
    cdef:
        CFunctionDescriptor descriptor
