# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from cpython.exc cimport PyErr_CheckSignals

import asyncio
import copy
import gc
import inspect
import threading
import traceback
import time
import logging
import os
import pickle
import sys
import _thread
import setproctitle

from libc.stdint cimport (
    int32_t,
    int64_t,
    INT64_MAX,
    uint64_t,
    uint8_t,
)
from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport (
    dynamic_pointer_cast,
    make_shared,
    shared_ptr,
    make_unique,
    unique_ptr,
)
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

from cython.operator import dereference, postincrement

from ray.includes.common cimport (
    CBuffer,
    CAddress,
    CObjectReference,
    CLanguage,
    CObjectReference,
    CRayObject,
    CRayStatus,
    CGcsClientOptions,
    CTaskArg,
    CTaskArgByReference,
    CTaskArgByValue,
    CTaskType,
    CPlacementStrategy,
    CRayFunction,
    CWorkerType,
    CJobConfig,
    move,
    LANGUAGE_CPP,
    LANGUAGE_JAVA,
    LANGUAGE_PYTHON,
    LocalMemoryBuffer,
    TASK_TYPE_NORMAL_TASK,
    TASK_TYPE_ACTOR_CREATION_TASK,
    TASK_TYPE_ACTOR_TASK,
    WORKER_TYPE_WORKER,
    WORKER_TYPE_DRIVER,
    WORKER_TYPE_SPILL_WORKER,
    WORKER_TYPE_RESTORE_WORKER,
    WORKER_TYPE_UTIL_WORKER,
    PLACEMENT_STRATEGY_PACK,
    PLACEMENT_STRATEGY_SPREAD,
    PLACEMENT_STRATEGY_STRICT_PACK,
    PLACEMENT_STRATEGY_STRICT_SPREAD,
)
from ray.includes.unique_ids cimport (
    CActorID,
    CObjectID,
    CNodeID,
    CPlacementGroupID,
)
from ray.includes.libcoreworker cimport (
    ActorHandleSharedPtr,
    CActorCreationOptions,
    CPlacementGroupCreationOptions,
    CCoreWorkerOptions,
    CCoreWorkerProcess,
    CTaskOptions,
    ResourceMappingType,
    CFiberEvent,
    CActorHandle,
)

from ray.includes.gcs_client cimport CGcsClient
from ray.includes.ray_config cimport RayConfig
from ray.includes.global_state_accessor cimport CGlobalStateAccessor

import ray
import ray._private.gcs_utils as gcs_utils
import ray._private.util_worker_handlers as util_worker_handlers
from ray import external_storage
from ray._private.async_compat import (
    sync_to_async, get_new_event_loop)
import ray._private.memory_monitor as memory_monitor
import ray.ray_constants as ray_constants
import ray._private.profiling as profiling
from ray.exceptions import (
    RayActorError,
    RayError,
    RaySystemError,
    RayTaskError,
    ObjectStoreFullError,
    GetTimeoutError,
    TaskCancelledError,
    AsyncioActorExit,
)
from ray._private.utils import decode
from ray._private.client_mode_hook import (
    disable_client_hook,
)
import msgpack

cimport cpython

include "includes/object_ref.pxi"
include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/function_descriptor.pxi"
include "includes/buffer.pxi"
include "includes/common.pxi"
include "includes/serialization.pxi"
include "includes/libcoreworker.pxi"
include "includes/global_state_accessor.pxi"
include "includes/metric.pxi"
include "includes/gcs_client.pxi"

# Expose GCC & Clang macro to report
# whether C++ optimizations were enabled during compilation.
OPTIMIZED = __OPTIMIZE__

logger = logging.getLogger(__name__)

cdef int check_status(const CRayStatus& status) nogil except -1:
    if status.ok():
        return 0

    with gil:
        message = status.message().decode()

    if status.IsObjectStoreFull():
        raise ObjectStoreFullError(message)
    elif status.IsInterrupted():
        raise KeyboardInterrupt()
    elif status.IsTimedOut():
        raise GetTimeoutError(message)
    elif status.IsNotFound():
        raise ValueError(message)
    else:
        raise RaySystemError(message)

cdef RayObjectsToDataMetadataPairs(
        const c_vector[shared_ptr[CRayObject]] objects):
    data_metadata_pairs = []
    for i in range(objects.size()):
        # core_worker will return a nullptr for objects that couldn't be
        # retrieved from the store or if an object was an exception.
        if not objects[i].get():
            data_metadata_pairs.append((None, None))
        else:
            data = None
            metadata = None
            if objects[i].get().HasData():
                data = Buffer.make(objects[i].get().GetData())
            if objects[i].get().HasMetadata():
                metadata = Buffer.make(
                    objects[i].get().GetMetadata()).to_pybytes()
            data_metadata_pairs.append((data, metadata))
    return data_metadata_pairs


cdef VectorToObjectRefs(const c_vector[CObjectReference] &object_refs):
    result = []
    for i in range(object_refs.size()):
        result.append(ObjectRef(object_refs[i].object_id(),
                                object_refs[i].call_site()))
    return result


cdef c_vector[CObjectID] ObjectRefsToVector(object_refs):
    """A helper function that converts a Python list of object refs to a vector.

    Args:
        object_refs (list): The Python list of object refs.

    Returns:
        The output vector.
    """
    cdef:
        c_vector[CObjectID] result
    for object_ref in object_refs:
        result.push_back((<ObjectRef>object_ref).native())
    return result


def compute_task_id(ObjectRef object_ref):
    return TaskID(object_ref.native().TaskId().Binary())


cdef increase_recursion_limit():
    """Double the recusion limit if current depth is close to the limit"""
    cdef:
        CPyThreadState * s = <CPyThreadState *> PyThreadState_Get()
        int current_depth = s.recursion_depth
        int current_limit = Py_GetRecursionLimit()
        int new_limit = current_limit * 2

    if current_limit - current_depth < 500:
        Py_SetRecursionLimit(new_limit)
        logger.debug("Increasing Python recursion limit to {} "
                     "current recursion depth is {}.".format(
                         new_limit, current_depth))


cdef CObjectLocationPtrToDict(CObjectLocation* c_object_location):
    """A helper function that converts a CObjectLocation to a Python dict.

    Returns:
        A Dict with following attributes:
        - node_ids:
            The hex IDs of the nodes that have a copy of this object.
        - object_size:
            The size of data + metadata in bytes.
    """
    object_size = c_object_location.GetObjectSize()

    node_ids = set()
    c_node_ids = c_object_location.GetNodeIDs()
    for i in range(c_node_ids.size()):
        node_id = c_node_ids[i].Hex().decode("ascii")
        node_ids.add(node_id)

    # add primary_node_id into node_ids
    if not c_object_location.GetPrimaryNodeID().IsNil():
        node_ids.add(
            c_object_location.GetPrimaryNodeID().Hex().decode("ascii"))

    # add spilled_node_id into node_ids
    if not c_object_location.GetSpilledNodeID().IsNil():
        node_ids.add(
            c_object_location.GetSpilledNodeID().Hex().decode("ascii"))

    return {
        "node_ids": list(node_ids),
        "object_size": object_size,
    }


@cython.auto_pickle(False)
cdef class Language:
    cdef CLanguage lang

    def __cinit__(self, int32_t lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<int32_t>lang)

    def value(self):
        return <int32_t>self.lang

    def __eq__(self, other):
        return (isinstance(other, Language) and
                (<int32_t>self.lang) == (<int32_t>(<Language>other).lang))

    def __repr__(self):
        if <int32_t>self.lang == <int32_t>LANGUAGE_PYTHON:
            return "PYTHON"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_CPP:
            return "CPP"
        elif <int32_t>self.lang == <int32_t>LANGUAGE_JAVA:
            return "JAVA"
        else:
            raise Exception("Unexpected error")

    def __reduce__(self):
        return Language, (<int32_t>self.lang,)

    PYTHON = Language.from_native(LANGUAGE_PYTHON)
    CPP = Language.from_native(LANGUAGE_CPP)
    JAVA = Language.from_native(LANGUAGE_JAVA)


cdef int prepare_resources(
        dict resource_dict,
        unordered_map[c_string, double] *resource_map) except -1:
    cdef:
        unordered_map[c_string, double] out
        c_string resource_name

    if resource_dict is None:
        raise ValueError("Must provide resource map.")

    for key, value in resource_dict.items():
        if not (isinstance(value, int) or isinstance(value, float)):
            raise ValueError("Resource quantities may only be ints or floats.")
        if value < 0:
            raise ValueError("Resource quantities may not be negative.")
        if value > 0:
            if (value >= 1 and isinstance(value, float)
                    and not value.is_integer()):
                raise ValueError(
                    "Resource quantities >1 must be whole numbers.")
            resource_map[0][key.encode("ascii")] = float(value)
    return 0


cdef prepare_args(
        CoreWorker core_worker,
        Language language, args, c_vector[unique_ptr[CTaskArg]] *args_vector):
    cdef:
        size_t size
        int64_t put_threshold
        int64_t rpc_inline_threshold
        int64_t total_inlined
        shared_ptr[CBuffer] arg_data
        c_vector[CObjectID] inlined_ids
        c_string put_arg_call_site
        c_vector[CObjectReference] inlined_refs

    worker = ray.worker.global_worker
    put_threshold = RayConfig.instance().max_direct_call_object_size()
    total_inlined = 0
    rpc_inline_threshold = RayConfig.instance().task_rpc_inlined_bytes_limit()
    for arg in args:
        if isinstance(arg, ObjectRef):
            c_arg = (<ObjectRef>arg).native()
            args_vector.push_back(
                unique_ptr[CTaskArg](new CTaskArgByReference(
                    c_arg,
                    CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                        c_arg),
                    arg.call_site())))

        else:
            serialized_arg = worker.get_serialization_context().serialize(arg)
            metadata = serialized_arg.metadata
            if language != Language.PYTHON:
                metadata_fields = metadata.split(b",")
                if metadata_fields[0] not in [
                        ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
                        ray_constants.OBJECT_METADATA_TYPE_RAW,
                        ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE]:
                    raise Exception("Can't transfer {} data to {}".format(
                        metadata_fields[0], language))
            size = serialized_arg.total_bytes

            if RayConfig.instance().record_ref_creation_sites():
                get_py_stack(&put_arg_call_site)
            # TODO(edoakes): any objects containing ObjectRefs are spilled to
            # plasma here. This is inefficient for small objects, but inlined
            # arguments aren't associated ObjectRefs right now so this is a
            # simple fix for reference counting purposes.
            if <int64_t>size <= put_threshold and \
                    (<int64_t>size + total_inlined <= rpc_inline_threshold):
                arg_data = dynamic_pointer_cast[CBuffer, LocalMemoryBuffer](
                        make_shared[LocalMemoryBuffer](size))
                if size > 0:
                    (<SerializedObject>serialized_arg).write_to(
                        Buffer.make(arg_data))
                for object_ref in serialized_arg.contained_object_refs:
                    inlined_ids.push_back((<ObjectRef>object_ref).native())
                inlined_refs = (CCoreWorkerProcess.GetCoreWorker()
                                .GetObjectRefs(inlined_ids))
                args_vector.push_back(
                    unique_ptr[CTaskArg](new CTaskArgByValue(
                        make_shared[CRayObject](
                            arg_data, string_to_buffer(metadata),
                            inlined_refs))))
                inlined_ids.clear()
                total_inlined += <int64_t>size
            else:
                args_vector.push_back(unique_ptr[CTaskArg](
                    new CTaskArgByReference(CObjectID.FromBinary(
                        core_worker.put_serialized_object(
                            serialized_arg, inline_small_object=False)),
                        CCoreWorkerProcess.GetCoreWorker().GetRpcAddress(),
                        put_arg_call_site)))


cdef raise_if_dependency_failed(arg):
    """This method is used to improve the readability of backtrace.

    With this method, the backtrace will always contain
    raise_if_dependency_failed when the task is failed with dependency
    failures.
    """
    if isinstance(arg, RayError):
        raise arg


cdef execute_task(
        CTaskType task_type,
        const c_string name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        const c_vector[CObjectID] &c_return_ids,
        const c_string debugger_breakpoint,
        c_vector[shared_ptr[CRayObject]] *returns,
        c_bool *is_application_level_error):

    is_application_level_error[0] = False

    worker = ray.worker.global_worker
    manager = worker.function_actor_manager
    actor = None

    cdef:
        dict execution_infos = manager.execution_infos
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        TaskID task_id = core_worker.get_current_task_id()
        CFiberEvent task_done_event

    # Automatically restrict the GPUs available to this task.
    ray._private.utils.set_cuda_visible_devices(ray.get_gpu_ids())

    # Helper method used to exit current asyncio actor.
    # This is called when a KeyboardInterrupt is received by the main thread.
    # Upon receiving a KeyboardInterrupt signal, Ray will exit the current
    # worker. If the worker is processing normal tasks, Ray treat it as task
    # cancellation from ray.cancel(object_ref). If the worker is an asyncio
    # actor, Ray will exit the actor.
    def exit_current_actor_if_asyncio():
        if core_worker.current_actor_is_asyncio():
            error = SystemExit(0)
            error.is_ray_terminate = True
            raise error

    function_descriptor = CFunctionDescriptorToPython(
        ray_function.GetFunctionDescriptor())

    if <int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK:
        actor_class = manager.load_actor_class(job_id, function_descriptor)
        actor_id = core_worker.get_actor_id()
        actor = actor_class.__new__(actor_class)
        worker.actors[actor_id] = actor
        # Record the actor name via :actor_name: magic token in the log file.
        # This is used for the prefix in driver logs `(actor_repr pid=123)...`
        if (hasattr(actor_class, "__ray_actor_class__") and
                "__repr__" in actor_class.__ray_actor_class__.__dict__):
            print("{}{}".format(
                ray_constants.LOG_PREFIX_ACTOR_NAME, repr(actor)))
        else:
            print("{}{}".format(
                ray_constants.LOG_PREFIX_ACTOR_NAME, actor_class.__name__))

    execution_info = execution_infos.get(function_descriptor)
    if not execution_info:
        execution_info = manager.get_execution_info(
            job_id, function_descriptor)
        execution_infos[function_descriptor] = execution_info

    function_name = execution_info.function_name
    extra_data = (b'{"name": ' + function_name.encode("ascii") +
                  b' "task_id": ' + task_id.hex().encode("ascii") + b'}')

    task_name = name.decode("utf-8")
    title = f"ray::{task_name}"

    if <int>task_type == <int>TASK_TYPE_NORMAL_TASK:
        next_title = "ray::IDLE"
        function_executor = execution_info.function
        # Record the task name via :task_name: magic token in the log file.
        # This is used for the prefix in driver logs `(task_name pid=123) ...`
        print("{}{}".format(
            ray_constants.LOG_PREFIX_TASK_NAME, task_name.replace("()", "")))
    else:
        actor = worker.actors[core_worker.get_actor_id()]
        class_name = actor.__class__.__name__
        next_title = f"ray::{class_name}"
        pid = os.getpid()
        worker_name = f"ray_{class_name}_{pid}"
        if c_resources.find(b"object_store_memory") != c_resources.end():
            worker.core_worker.set_object_store_client_options(
                worker_name,
                int(ray_constants.from_memory_units(
                        dereference(
                            c_resources.find(b"object_store_memory")).second)))

        def function_executor(*arguments, **kwarguments):
            function = execution_info.function

            if core_worker.current_actor_is_asyncio():
                # Increase recursion limit if necessary. In asyncio mode,
                # we have many parallel callstacks (represented in fibers)
                # that's suspended for execution. Python interpreter will
                # mistakenly count each callstack towards recusion limit.
                # We don't need to worry about stackoverflow here because
                # the max number of callstacks is limited in direct actor
                # transport with max_concurrency flag.
                increase_recursion_limit()

                if inspect.iscoroutinefunction(function.method):
                    async_function = function
                else:
                    # Just execute the method if it's ray internal method.
                    if function.name.startswith("__ray"):
                        return function(actor, *arguments, **kwarguments)
                    async_function = sync_to_async(function)

                return core_worker.run_async_func_in_event_loop(
                    async_function, actor, *arguments, **kwarguments)

            return function(actor, *arguments, **kwarguments)

    with core_worker.profile_event(b"task", extra_data=extra_data):
        try:
            task_exception = False
            if not (<int>task_type == <int>TASK_TYPE_ACTOR_TASK
                    and function_name == "__ray_terminate__"):
                worker.memory_monitor.raise_if_low_memory()

            with core_worker.profile_event(b"task:deserialize_arguments"):
                if c_args.empty():
                    args, kwargs = [], {}
                else:
                    metadata_pairs = RayObjectsToDataMetadataPairs(c_args)
                    object_refs = VectorToObjectRefs(c_arg_refs)

                    if core_worker.current_actor_is_asyncio():
                        # We deserialize objects in event loop thread to
                        # prevent segfaults. See #7799
                        async def deserialize_args():
                            return (ray.worker.global_worker
                                    .deserialize_objects(
                                        metadata_pairs, object_refs))
                        args = core_worker.run_async_func_in_event_loop(
                            deserialize_args)
                    else:
                        args = ray.worker.global_worker.deserialize_objects(
                            metadata_pairs, object_refs)

                    for arg in args:
                        raise_if_dependency_failed(arg)
                    args, kwargs = ray._private.signature.recover_args(args)

            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                actor = worker.actors[core_worker.get_actor_id()]
                class_name = actor.__class__.__name__
                actor_title = f"{class_name}({args!r}, {kwargs!r})"
                core_worker.set_actor_title(actor_title.encode("utf-8"))
            # Execute the task.
            with core_worker.profile_event(b"task:execute"):
                task_exception = True
                try:
                    is_existing = core_worker.is_exiting()
                    if is_existing:
                        title = f"{title}::Exiting"
                        next_title = f"{next_title}::Exiting"
                    with ray.worker._changeproctitle(title, next_title):
                        if debugger_breakpoint != b"":
                            ray.util.pdb.set_trace(
                                breakpoint_uuid=debugger_breakpoint)
                        outputs = function_executor(*args, **kwargs)
                        next_breakpoint = (
                            ray.worker.global_worker.debugger_breakpoint)
                        if next_breakpoint != b"":
                            # If this happens, the user typed "remote" and
                            # there were no more remote calls left in this
                            # task. In that case we just exit the debugger.
                            ray.experimental.internal_kv._internal_kv_put(
                                "RAY_PDB_{}".format(next_breakpoint),
                                "{\"exit_debugger\": true}")
                            ray.experimental.internal_kv._internal_kv_del(
                                "RAY_PDB_CONTINUE_{}".format(next_breakpoint)
                            )
                            ray.worker.global_worker.debugger_breakpoint = b""
                    task_exception = False
                except AsyncioActorExit as e:
                    exit_current_actor_if_asyncio()
                except KeyboardInterrupt as e:
                    raise TaskCancelledError(
                            core_worker.get_current_task_id())
                except Exception as e:
                    is_application_level_error[0] = True
                    if core_worker.get_current_task_retry_exceptions():
                        logger.info("Task failed with retryable exception:"
                                    " {}.".format(
                                        core_worker.get_current_task_id()),
                                    exc_info=True)
                    raise e
                if c_return_ids.size() == 1:
                    outputs = (outputs,)
            # Check for a cancellation that was called when the function
            # was exiting and was raised after the except block.
            if not check_signals().ok():
                task_exception = True
                raise TaskCancelledError(
                            core_worker.get_current_task_id())
            if (c_return_ids.size() > 0 and
                    len(outputs) != int(c_return_ids.size())):
                raise ValueError(
                    "Task returned {} objects, but num_returns={}.".format(
                        len(outputs), c_return_ids.size()))
            # Store the outputs in the object store.
            with core_worker.profile_event(b"task:store_outputs"):
                core_worker.store_task_outputs(
                    worker, outputs, c_return_ids, returns)
        except Exception as error:
            # If the debugger is enabled, drop into the remote pdb here.
            if "RAY_PDB" in os.environ:
                ray.util.pdb.post_mortem()

            backtrace = ray._private.utils.format_error_message(
                traceback.format_exc(), task_exception=task_exception)

            # Generate the actor repr from the actor class.
            actor_repr = repr(actor) if actor else None

            if isinstance(error, RayTaskError):
                # Avoid recursive nesting of RayTaskError.
                failure_object = RayTaskError(function_name, backtrace,
                                              error.cause, proctitle=title,
                                              actor_repr=actor_repr)
            else:
                failure_object = RayTaskError(function_name, backtrace,
                                              error, proctitle=title,
                                              actor_repr=actor_repr)
            errors = []
            for _ in range(c_return_ids.size()):
                errors.append(failure_object)
            core_worker.store_task_outputs(
                worker, errors, c_return_ids, returns)
            ray._private.utils.push_error_to_driver(
                worker,
                ray_constants.TASK_PUSH_ERROR,
                str(failure_object),
                job_id=worker.current_job_id)
            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                raise RayActorError.from_task_error(failure_object)

    if execution_info.max_calls != 0:
        # Reset the state of the worker for the next task to execute.
        # Increase the task execution counter.
        manager.increase_task_counter(function_descriptor)
        # If we've reached the max number of executions for this worker, exit.
        task_counter = manager.get_task_counter(function_descriptor)
        if task_counter == execution_info.max_calls:
            exit = SystemExit(0)
            exit.is_ray_terminate = True
            raise exit

# return a protobuf-serialized ray_exception
cdef shared_ptr[LocalMemoryBuffer] ray_error_to_memory_buf(ray_error):
    cdef bytes py_bytes = ray_error.to_bytes()
    return make_shared[LocalMemoryBuffer](
        <uint8_t*>py_bytes, len(py_bytes), True)

cdef CRayStatus task_execution_handler(
        CTaskType task_type,
        const c_string task_name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        const c_vector[CObjectID] &c_return_ids,
        const c_string debugger_breakpoint,
        c_vector[shared_ptr[CRayObject]] *returns,
        shared_ptr[LocalMemoryBuffer] &creation_task_exception_pb_bytes,
        c_bool *is_application_level_error) nogil:
    with gil, disable_client_hook():
        try:
            try:
                # The call to execute_task should never raise an exception. If
                # it does, that indicates that there was an internal error.
                execute_task(task_type, task_name, ray_function, c_resources,
                             c_args, c_arg_refs, c_return_ids,
                             debugger_breakpoint, returns,
                             is_application_level_error)
            except Exception as e:
                sys_exit = SystemExit()
                if isinstance(e, RayActorError) and \
                   e.has_creation_task_error():
                    traceback_str = str(e)
                    logger.error("Exception raised "
                                 f"in creation task: {traceback_str}")
                    # Cython's bug that doesn't allow reference assignment,
                    # this is a workaroud.
                    # See https://github.com/cython/cython/issues/1863
                    (&creation_task_exception_pb_bytes)[0] = (
                        ray_error_to_memory_buf(e))
                    sys_exit.is_creation_task_error = True
                else:
                    traceback_str = traceback.format_exc() + (
                        "An unexpected internal error "
                        "occurred while the worker "
                        "was executing a task.")
                    ray._private.utils.push_error_to_driver(
                        ray.worker.global_worker,
                        "worker_crash",
                        traceback_str,
                        job_id=None)
                raise sys_exit
        except SystemExit as e:
            # Tell the core worker to exit as soon as the result objects
            # are processed.
            if hasattr(e, "is_ray_terminate"):
                return CRayStatus.IntentionalSystemExit()
            elif hasattr(e, "is_creation_task_error"):
                return CRayStatus.CreationTaskError()
            else:
                logger.exception("SystemExit was raised from the worker")
                return CRayStatus.UnexpectedSystemExit()

    return CRayStatus.OK()

cdef c_bool kill_main_task() nogil:
    with gil:
        if setproctitle.getproctitle() != "ray::IDLE":
            _thread.interrupt_main()
            return True
        return False


cdef CRayStatus check_signals() nogil:
    with gil:
        try:
            PyErr_CheckSignals()
        except KeyboardInterrupt:
            return CRayStatus.Interrupted(b"")
    return CRayStatus.OK()


cdef void gc_collect() nogil:
    with gil:
        start = time.perf_counter()
        num_freed = gc.collect()
        end = time.perf_counter()
        if num_freed > 0:
            logger.debug(
                "gc.collect() freed {} refs in {} seconds".format(
                    num_freed, end - start))


cdef void run_on_util_worker_handler(
        c_string req,
        c_vector[c_string] args) nogil:
    with gil:
        util_worker_handlers.dispatch(
            req.decode(), [arg.decode() for arg in args])


cdef c_vector[c_string] spill_objects_handler(
        const c_vector[CObjectReference]& object_refs_to_spill) nogil:
    cdef:
        c_vector[c_string] return_urls
        c_vector[c_string] owner_addresses

    with gil:
        object_refs = VectorToObjectRefs(object_refs_to_spill)
        for i in range(object_refs_to_spill.size()):
            owner_addresses.push_back(
                    object_refs_to_spill[i].owner_address()
                    .SerializeAsString())
        try:
            with ray.worker._changeproctitle(
                    ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER,
                    ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE):
                urls = external_storage.spill_objects(
                    object_refs, owner_addresses)
            for url in urls:
                return_urls.push_back(url)
        except Exception as err:
            exception_str = (
                "An unexpected internal error occurred while the IO worker "
                "was spilling objects: {}".format(err))
            logger.exception(exception_str)
            ray._private.utils.push_error_to_driver(
                ray.worker.global_worker,
                "spill_objects_error",
                traceback.format_exc() + exception_str,
                job_id=None)
        return return_urls


cdef int64_t restore_spilled_objects_handler(
        const c_vector[CObjectReference]& object_refs_to_restore,
        const c_vector[c_string]& object_urls) nogil:
    cdef:
        int64_t bytes_restored = 0
    with gil:
        urls = []
        size = object_urls.size()
        for i in range(size):
            urls.append(object_urls[i])
        object_refs = VectorToObjectRefs(object_refs_to_restore)
        try:
            with ray.worker._changeproctitle(
                    ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER,
                    ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE):
                bytes_restored = external_storage.restore_spilled_objects(
                    object_refs, urls)
        except Exception:
            exception_str = (
                "An unexpected internal error occurred while the IO worker "
                "was restoring spilled objects.")
            logger.exception(exception_str)
            if os.getenv("RAY_BACKEND_LOG_LEVEL") == "debug":
                ray._private.utils.push_error_to_driver(
                    ray.worker.global_worker,
                    "restore_objects_error",
                    traceback.format_exc() + exception_str,
                    job_id=None)
    return bytes_restored


cdef void delete_spilled_objects_handler(
        const c_vector[c_string]& object_urls,
        CWorkerType worker_type) nogil:
    with gil:
        urls = []
        size = object_urls.size()
        for i in range(size):
            urls.append(object_urls[i])
        try:
            # Get proctitle.
            if <int> worker_type == <int> WORKER_TYPE_SPILL_WORKER:
                original_proctitle = (
                    ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_IDLE)
                proctitle = (
                    ray_constants.WORKER_PROCESS_TYPE_SPILL_WORKER_DELETE)
            elif <int> worker_type == <int> WORKER_TYPE_RESTORE_WORKER:
                original_proctitle = (
                    ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_IDLE)
                proctitle = (
                    ray_constants.WORKER_PROCESS_TYPE_RESTORE_WORKER_DELETE)
            else:
                assert False, ("This line shouldn't be reachable.")

            # Delete objects.
            with ray.worker._changeproctitle(
                    proctitle,
                    original_proctitle):
                external_storage.delete_spilled_objects(urls)
        except Exception:
            exception_str = (
                "An unexpected internal error occurred while the IO worker "
                "was deleting spilled objects.")
            logger.exception(exception_str)
            ray._private.utils.push_error_to_driver(
                ray.worker.global_worker,
                "delete_spilled_objects_error",
                traceback.format_exc() + exception_str,
                job_id=None)


cdef void unhandled_exception_handler(const CRayObject& error) nogil:
    with gil:
        worker = ray.worker.global_worker
        data = None
        metadata = None
        if error.HasData():
            data = Buffer.make(error.GetData())
        if error.HasMetadata():
            metadata = Buffer.make(error.GetMetadata()).to_pybytes()
        # TODO(ekl) why does passing a ObjectRef.nil() lead to shutdown errors?
        object_ids = [None]
        worker.raise_errors([(data, metadata)], object_ids)


# This function introduces ~2-7us of overhead per call (i.e., it can be called
# up to hundreds of thousands of times per second).
cdef void get_py_stack(c_string* stack_out) nogil:
    """Get the Python call site.

    This can be called from within C++ code to retrieve the file name and line
    number of the Python code that is calling into the core worker.
    """
    with gil:
        try:
            frame = inspect.currentframe()
        except ValueError:  # overhead of exception handling is about 20us
            stack_out[0] = "".encode("ascii")
            return
        msg_frames = []
        while frame and len(msg_frames) < 4:
            filename = frame.f_code.co_filename
            # Decode Ray internal frames to add annotations.
            if filename.endswith("ray/worker.py"):
                if frame.f_code.co_name == "put":
                    msg_frames = ["(put object) "]
            elif filename.endswith("ray/workers/default_worker.py"):
                pass
            elif filename.endswith("ray/remote_function.py"):
                # TODO(ekl) distinguish between task return objects and
                # arguments. This can only be done in the core worker.
                msg_frames = ["(task call) "]
            elif filename.endswith("ray/actor.py"):
                # TODO(ekl) distinguish between actor return objects and
                # arguments. This can only be done in the core worker.
                msg_frames = ["(actor call) "]
            elif filename.endswith("ray/serialization.py"):
                if frame.f_code.co_name == "id_deserializer":
                    msg_frames = ["(deserialize task arg) "]
            else:
                msg_frames.append("{}:{}:{}".format(
                    frame.f_code.co_filename, frame.f_code.co_name,
                    frame.f_lineno))
            frame = frame.f_back
        stack_out[0] = (ray_constants.CALL_STACK_LINE_DELIMITER
                        .join(msg_frames).encode("ascii"))

cdef shared_ptr[CBuffer] string_to_buffer(c_string& c_str):
    cdef shared_ptr[CBuffer] empty_metadata
    if c_str.size() == 0:
        return empty_metadata
    return dynamic_pointer_cast[
        CBuffer, LocalMemoryBuffer](
            make_shared[LocalMemoryBuffer](
                <uint8_t*>(c_str.data()), c_str.size(), True))


cdef void terminate_asyncio_thread() nogil:
    with gil:
        core_worker = ray.worker.global_worker.core_worker
        core_worker.destroy_event_loop_if_exists()


def connect_to_gcs(ip, port, password):
    return GcsClient.make_from_address(ip, port, password)


def disconnect_from_gcs(gcs_client):
    gcs_client.disconnect()


# An empty profile event context to be used when the timeline is disabled.
cdef class EmptyProfileEvent:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass

cdef class CoreWorker:

    def __cinit__(self, worker_type, store_socket, raylet_socket,
                  JobID job_id, GcsClientOptions gcs_options, log_dir,
                  node_ip_address, node_manager_port, raylet_ip_address,
                  local_mode, driver_name, stdout_file, stderr_file,
                  serialized_job_config, metrics_agent_port, runtime_env_hash,
                  worker_shim_pid):
        self.is_local_mode = local_mode

        cdef CCoreWorkerOptions options = CCoreWorkerOptions()
        if worker_type in (ray.LOCAL_MODE, ray.SCRIPT_MODE):
            self.is_driver = True
            options.worker_type = WORKER_TYPE_DRIVER
        elif worker_type == ray.WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_WORKER
        elif worker_type == ray.SPILL_WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_SPILL_WORKER
        elif worker_type == ray.RESTORE_WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_RESTORE_WORKER
        elif worker_type == ray.UTIL_WORKER_MODE:
            self.is_driver = False
            options.worker_type = WORKER_TYPE_UTIL_WORKER
        else:
            raise ValueError(f"Unknown worker type: {worker_type}")
        options.language = LANGUAGE_PYTHON
        options.store_socket = store_socket.encode("ascii")
        options.raylet_socket = raylet_socket.encode("ascii")
        options.job_id = job_id.native()
        options.gcs_options = gcs_options.native()[0]
        options.enable_logging = True
        options.log_dir = log_dir.encode("utf-8")
        options.install_failure_signal_handler = True
        # https://stackoverflow.com/questions/2356399/tell-if-python-is-in-interactive-mode
        options.interactive = hasattr(sys, "ps1")
        options.node_ip_address = node_ip_address.encode("utf-8")
        options.node_manager_port = node_manager_port
        options.raylet_ip_address = raylet_ip_address.encode("utf-8")
        options.driver_name = driver_name
        options.stdout_file = stdout_file
        options.stderr_file = stderr_file
        options.task_execution_callback = task_execution_handler
        options.check_signals = check_signals
        options.gc_collect = gc_collect
        options.spill_objects = spill_objects_handler
        options.restore_spilled_objects = restore_spilled_objects_handler
        options.delete_spilled_objects = delete_spilled_objects_handler
        options.run_on_util_worker_handler = run_on_util_worker_handler
        options.unhandled_exception_handler = unhandled_exception_handler
        options.get_lang_stack = get_py_stack
        options.is_local_mode = local_mode
        options.num_workers = 1
        options.kill_main = kill_main_task
        options.terminate_asyncio_thread = terminate_asyncio_thread
        options.serialized_job_config = serialized_job_config
        options.metrics_agent_port = metrics_agent_port
        options.connect_on_start = False
        options.runtime_env_hash = runtime_env_hash
        options.worker_shim_pid = worker_shim_pid
        CCoreWorkerProcess.Initialize(options)

    def shutdown(self):
        with nogil:
            # If it's a worker, the core worker process should have been
            # shutdown. So we can't call
            # `CCoreWorkerProcess.GetCoreWorker().GetWorkerType()` here.
            # Instead, we use the cached `is_driver` flag to test if it's a
            # driver.
            if self.is_driver:
                CCoreWorkerProcess.Shutdown()

    def get_gcs_client(self):
        return GcsClient.make_from_existing(
            CCoreWorkerProcess.GetCoreWorker().GetGcsClient())

    def notify_raylet(self):
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().ConnectToRaylet()

    def run_task_loop(self):
        with nogil:
            CCoreWorkerProcess.RunTaskExecutionLoop()

    def get_current_task_retry_exceptions(self):
        return CCoreWorkerProcess.GetCoreWorker(
            ).GetCurrentTaskRetryExceptions()

    def get_current_task_id(self):
        return TaskID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskId().Binary())

    def get_current_job_id(self):
        return JobID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentJobId().Binary())

    def get_current_node_id(self):
        return NodeID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentNodeId().Binary())

    def get_actor_id(self):
        return ActorID(
            CCoreWorkerProcess.GetCoreWorker().GetActorId().Binary())

    def get_placement_group_id(self):
        return PlacementGroupID(
            CCoreWorkerProcess.GetCoreWorker()
            .GetCurrentPlacementGroupId().Binary())

    def get_worker_id(self):
        return WorkerID(
            CCoreWorkerProcess.GetCoreWorker().GetWorkerID().Binary())

    def should_capture_child_tasks_in_placement_group(self):
        return CCoreWorkerProcess.GetCoreWorker(
            ).ShouldCaptureChildTasksInPlacementGroup()

    def set_webui_display(self, key, message):
        CCoreWorkerProcess.GetCoreWorker().SetWebuiDisplay(key, message)

    def set_actor_title(self, title):
        CCoreWorkerProcess.GetCoreWorker().SetActorTitle(title)

    def get_plasma_event_handler(self):
        return self.plasma_event_handler

    def get_objects(self, object_refs, TaskID current_task_id,
                    int64_t timeout_ms=-1):
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            CTaskID c_task_id = current_task_id.native()
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Get(
                c_object_ids, timeout_ms, &results))

        return RayObjectsToDataMetadataPairs(results)

    def get_if_local(self, object_refs):
        """Get objects from local plasma store directly
        without a fetch request to raylet."""
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetIfLocal(
                    c_object_ids, &results))
        return RayObjectsToDataMetadataPairs(results)

    def object_exists(self, ObjectRef object_ref, memory_store_only=False):
        cdef:
            c_bool has_object
            c_bool is_in_plasma
            CObjectID c_object_id = object_ref.native()

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Contains(
                c_object_id, &has_object, &is_in_plasma))

        return has_object and (not memory_store_only or not is_in_plasma)

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectRef object_ref,
                            c_vector[CObjectID] contained_ids,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data,
                            c_bool created_by_worker,
                            owner_address=None,
                            c_bool inline_small_object=True):
        cdef:
            unique_ptr[CAddress] c_owner_address

        c_owner_address = move(self._convert_python_address(owner_address))

        if object_ref is None:
            with nogil:
                check_status(CCoreWorkerProcess.GetCoreWorker().CreateOwned(
                             metadata, data_size, contained_ids,
                             c_object_id, data, created_by_worker,
                             move(c_owner_address),
                             inline_small_object))
        else:
            c_object_id[0] = object_ref.native()
            if owner_address is None:
                c_owner_address = make_unique[CAddress]()
                dereference(
                    c_owner_address
                ).CopyFrom(CCoreWorkerProcess.GetCoreWorker().GetRpcAddress())
            with nogil:
                check_status(CCoreWorkerProcess.GetCoreWorker().CreateExisting(
                            metadata, data_size, c_object_id[0],
                            dereference(c_owner_address), data,
                            created_by_worker))

        # If data is nullptr, that means the ObjectRef already existed,
        # which we ignore.
        # TODO(edoakes): this is hacky, we should return the error instead
        # and deal with it here.
        return data.get() == NULL

    cdef unique_ptr[CAddress] _convert_python_address(self, address=None):
        """ convert python address to `CAddress`, If not provided,
        return nullptr.

        Args:
            address: worker address.
        """
        cdef:
            unique_ptr[CAddress] c_address

        if address is not None:
            c_address = make_unique[CAddress]()
            dereference(c_address).ParseFromString(address)
        return move(c_address)

    def put_file_like_object(
            self, metadata, data_size, file_like, ObjectRef object_ref,
            owner_address):
        """Directly create a new Plasma Store object from a file like
        object. This avoids extra memory copy.

        Args:
            metadata (bytes): The metadata of the object.
            data_size (int): The size of the data buffer.
            file_like: A python file object that provides the `readinto`
                interface.
            object_ref: The new ObjectRef.
            owner_address: Owner address for this object ref.
        """
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data_buf
            shared_ptr[CBuffer] metadata_buf
            int64_t put_threshold
            c_bool put_small_object_in_memory_store
            c_vector[CObjectID] c_object_id_vector
            unique_ptr[CAddress] c_owner_address
        # TODO(suquark): This method does not support put objects to
        # in memory store currently.
        metadata_buf = string_to_buffer(metadata)
        object_already_exists = self._create_put_buffer(
            metadata_buf, data_size, object_ref,
            ObjectRefsToVector([]),
            &c_object_id, &data_buf, False, owner_address)
        if object_already_exists:
            logger.debug("Object already exists in 'put_file_like_object'.")
            return
        data = Buffer.make(data_buf)
        view = memoryview(data)
        index = 0
        while index < data_size:
            bytes_read = file_like.readinto(view[index:])
            index += bytes_read
        c_owner_address = move(self._convert_python_address(owner_address))
        with nogil:
            # Using custom object refs is not supported because we
            # can't track their lifecycle, so we don't pin the object
            # in this case.
            check_status(
                CCoreWorkerProcess.GetCoreWorker().SealExisting(
                            c_object_id, pin_object=False,
                            owner_address=move(c_owner_address)))

    def put_serialized_object(self, serialized_object,
                              ObjectRef object_ref=None,
                              c_bool pin_object=True,
                              owner_address=None,
                              c_bool inline_small_object=True):
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data
            shared_ptr[CBuffer] metadata
            int64_t put_threshold
            c_bool put_small_object_in_memory_store
            unique_ptr[CAddress] c_owner_address
            c_vector[CObjectID] contained_object_ids
            c_vector[CObjectReference] contained_object_refs

        metadata = string_to_buffer(serialized_object.metadata)
        put_threshold = RayConfig.instance().max_direct_call_object_size()
        put_small_object_in_memory_store = (
            RayConfig.instance().put_small_object_in_memory_store() and
            inline_small_object)
        total_bytes = serialized_object.total_bytes
        contained_object_ids = ObjectRefsToVector(
                serialized_object.contained_object_refs)
        object_already_exists = self._create_put_buffer(
            metadata, total_bytes, object_ref,
            contained_object_ids,
            &c_object_id, &data, True, owner_address, inline_small_object)

        if not object_already_exists:
            if total_bytes > 0:
                (<SerializedObject>serialized_object).write_to(
                    Buffer.make(data))
            if self.is_local_mode or (put_small_object_in_memory_store
               and <int64_t>total_bytes < put_threshold):
                contained_object_refs = (
                        CCoreWorkerProcess.GetCoreWorker().
                        GetObjectRefs(contained_object_ids))
                if owner_address is not None:
                    raise Exception(
                        "cannot put data into memory store directly"
                        " and assign owner at the same time")
                check_status(CCoreWorkerProcess.GetCoreWorker().Put(
                        CRayObject(data, metadata, contained_object_refs),
                        contained_object_ids, c_object_id))
            else:
                c_owner_address = move(self._convert_python_address(
                    owner_address))
                with nogil:
                    if object_ref is None:
                        check_status(
                            CCoreWorkerProcess.GetCoreWorker().SealOwned(
                                        c_object_id,
                                        pin_object,
                                        move(c_owner_address)))
                    else:
                        # Using custom object refs is not supported because we
                        # can't track their lifecycle, so we don't pin the
                        # object in this case.
                        check_status(
                            CCoreWorkerProcess.GetCoreWorker().SealExisting(
                                        c_object_id, pin_object=False,
                                        owner_address=move(c_owner_address)))

        return c_object_id.Binary()

    def wait(self, object_refs, int num_returns, int64_t timeout_ms,
             TaskID current_task_id, c_bool fetch_local):
        cdef:
            c_vector[CObjectID] wait_ids
            c_vector[c_bool] results
            CTaskID c_task_id = current_task_id.native()

        wait_ids = ObjectRefsToVector(object_refs)
        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Wait(
                wait_ids, num_returns, timeout_ms, &results, fetch_local))

        assert len(results) == len(object_refs)

        ready, not_ready = [], []
        for i, object_ref in enumerate(object_refs):
            if results[i]:
                ready.append(object_ref)
            else:
                not_ready.append(object_ref)

        return ready, not_ready

    def free_objects(self, object_refs, c_bool local_only):
        cdef:
            c_vector[CObjectID] free_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Delete(
                free_ids, local_only))

    def get_object_locations(self, object_refs, int64_t timeout_ms):
        cdef:
            c_vector[shared_ptr[CObjectLocation]] results
            c_vector[CObjectID] lookup_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetLocationFromOwner(
                    lookup_ids, timeout_ms, &results))

        object_locations = {}
        for i in range(results.size()):
            # core_worker will return a nullptr for objects that couldn't be
            # located
            if not results[i].get():
                continue
            else:
                object_locations[object_refs[i]] = \
                    CObjectLocationPtrToDict(results[i].get())
        return object_locations

    def global_gc(self):
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().TriggerGlobalGC()

    def dump_object_store_memory_usage(self):
        message = CCoreWorkerProcess.GetCoreWorker().MemoryUsageString()
        logger.warning("Local object store memory usage:\n{}\n".format(
            message.decode("utf-8")))

    def submit_task(self,
                    Language language,
                    FunctionDescriptor function_descriptor,
                    args,
                    c_string name,
                    int num_returns,
                    resources,
                    int max_retries,
                    c_bool retry_exceptions,
                    PlacementGroupID placement_group_id,
                    int64_t placement_group_bundle_index,
                    c_bool placement_group_capture_child_tasks,
                    c_string debugger_breakpoint,
                    runtime_env_dict,
                    override_environment_variables
                    ):
        cdef:
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            CPlacementGroupID c_placement_group_id = \
                placement_group_id.native()
            c_string c_serialized_runtime_env
            unordered_map[c_string, c_string] \
                c_override_environment_variables = \
                override_environment_variables
            c_vector[CObjectReference] return_refs

        with self.profile_event(b"submit_task"):
            c_serialized_runtime_env = \
                self.prepare_runtime_env(runtime_env_dict)
            prepare_resources(resources, &c_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, language, args, &args_vector)

            # NOTE(edoakes): releasing the GIL while calling this method causes
            # segfaults. See relevant issue for details:
            # https://github.com/ray-project/ray/pull/12803
            return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
                ray_function, args_vector, CTaskOptions(
                    name, num_returns, c_resources,
                    b"",
                    c_serialized_runtime_env,
                    c_override_environment_variables),
                max_retries, retry_exceptions,
                c_pair[CPlacementGroupID, int64_t](
                    c_placement_group_id, placement_group_bundle_index),
                placement_group_capture_child_tasks,
                debugger_breakpoint)

            return VectorToObjectRefs(return_refs)

    def create_actor(self,
                     Language language,
                     FunctionDescriptor function_descriptor,
                     args,
                     int64_t max_restarts,
                     int64_t max_task_retries,
                     resources,
                     placement_resources,
                     int32_t max_concurrency,
                     c_bool is_detached,
                     c_string name,
                     c_string ray_namespace,
                     c_bool is_asyncio,
                     PlacementGroupID placement_group_id,
                     int64_t placement_group_bundle_index,
                     c_bool placement_group_capture_child_tasks,
                     c_string extension_data,
                     runtime_env_dict,
                     override_environment_variables
                     ):
        cdef:
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[c_string] dynamic_worker_options
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, double] c_placement_resources
            CActorID c_actor_id
            CPlacementGroupID c_placement_group_id = \
                placement_group_id.native()
            c_string c_serialized_runtime_env
            unordered_map[c_string, c_string] \
                c_override_environment_variables = \
                override_environment_variables

        with self.profile_event(b"submit_task"):
            c_serialized_runtime_env = \
                self.prepare_runtime_env(runtime_env_dict)
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, language, args, &args_vector)

            with nogil:
                check_status(CCoreWorkerProcess.GetCoreWorker().CreateActor(
                    ray_function, args_vector,
                    CActorCreationOptions(
                        max_restarts, max_task_retries, max_concurrency,
                        c_resources, c_placement_resources,
                        dynamic_worker_options, is_detached, name,
                        ray_namespace,
                        is_asyncio,
                        c_pair[CPlacementGroupID, int64_t](
                            c_placement_group_id,
                            placement_group_bundle_index),
                        placement_group_capture_child_tasks,
                        c_serialized_runtime_env,
                        c_override_environment_variables),
                    extension_data,
                    &c_actor_id))

            return ActorID(c_actor_id.Binary())

    def create_placement_group(
                            self,
                            c_string name,
                            c_vector[unordered_map[c_string, double]] bundles,
                            c_string strategy,
                            c_bool is_detached):
        cdef:
            CPlacementGroupID c_placement_group_id
            CPlacementStrategy c_strategy

        if strategy == b"PACK":
            c_strategy = PLACEMENT_STRATEGY_PACK
        elif strategy == b"SPREAD":
            c_strategy = PLACEMENT_STRATEGY_SPREAD
        elif strategy == b"STRICT_PACK":
            c_strategy = PLACEMENT_STRATEGY_STRICT_PACK
        else:
            if strategy == b"STRICT_SPREAD":
                c_strategy = PLACEMENT_STRATEGY_STRICT_SPREAD
            else:
                raise TypeError(strategy)

        with nogil:
            check_status(
                        CCoreWorkerProcess.GetCoreWorker().
                        CreatePlacementGroup(
                            CPlacementGroupCreationOptions(
                                name,
                                c_strategy,
                                bundles,
                                is_detached
                            ),
                            &c_placement_group_id))

        return PlacementGroupID(c_placement_group_id.Binary())

    def remove_placement_group(self, PlacementGroupID placement_group_id):
        cdef:
            CPlacementGroupID c_placement_group_id = \
                placement_group_id.native()

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().
                RemovePlacementGroup(c_placement_group_id))

    def wait_placement_group_ready(self,
                                   PlacementGroupID placement_group_id,
                                   int32_t timeout_seconds):
        cdef CRayStatus status
        cdef CPlacementGroupID cplacement_group_id = (
            CPlacementGroupID.FromBinary(placement_group_id.binary()))
        cdef int ctimeout_seconds = timeout_seconds
        with nogil:
            status = CCoreWorkerProcess.GetCoreWorker() \
                .WaitPlacementGroupReady(cplacement_group_id, ctimeout_seconds)
            if status.IsNotFound():
                raise Exception("Placement group {} does not exist.".format(
                    placement_group_id))
        return status.ok()

    def submit_actor_task(self,
                          Language language,
                          ActorID actor_id,
                          FunctionDescriptor function_descriptor,
                          args,
                          c_string name,
                          int num_returns,
                          double num_method_cpus):

        cdef:
            CActorID c_actor_id = actor_id.native()
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs

        with self.profile_event(b"submit_task"):
            if num_method_cpus > 0:
                c_resources[b"CPU"] = num_method_cpus
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, language, args, &args_vector)

            # NOTE(edoakes): releasing the GIL while calling this method causes
            # segfaults. See relevant issue for details:
            # https://github.com/ray-project/ray/pull/12803
            return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitActorTask(
                c_actor_id,
                ray_function,
                args_vector, CTaskOptions(name, num_returns, c_resources))

            return VectorToObjectRefs(return_refs)

    def kill_actor(self, ActorID actor_id, c_bool no_restart):
        cdef:
            CActorID c_actor_id = actor_id.native()

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().KillActor(
                  c_actor_id, True, no_restart))

    def cancel_task(self, ObjectRef object_ref, c_bool force_kill,
                    c_bool recursive):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CRayStatus status = CRayStatus.OK()

        status = CCoreWorkerProcess.GetCoreWorker().CancelTask(
                                            c_object_id, force_kill, recursive)

        if not status.ok():
            raise TypeError(status.message().decode())

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = (
                CCoreWorkerProcess.GetCoreWorker().GetResourceIDs())
            unordered_map[
                c_string, c_vector[pair[int64_t, double]]
            ].iterator iterator = resource_mapping.begin()
            c_vector[pair[int64_t, double]] c_value

        resources_dict = {}
        while iterator != resource_mapping.end():
            key = decode(dereference(iterator).first)
            c_value = dereference(iterator).second
            ids_and_fractions = []
            for i in range(c_value.size()):
                ids_and_fractions.append(
                    (c_value[i].first, c_value[i].second))
            resources_dict[key] = ids_and_fractions
            postincrement(iterator)

        return resources_dict

    def profile_event(self, c_string event_type, object extra_data=None):
        if RayConfig.instance().enable_timeline():
            return ProfileEvent.make(
                CCoreWorkerProcess.GetCoreWorker().CreateProfileEvent(
                    event_type), extra_data)
        else:
            return EmptyProfileEvent()

    def remove_actor_handle_reference(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
        CCoreWorkerProcess.GetCoreWorker().RemoveActorHandleReference(
            c_actor_id)

    cdef make_actor_handle(self, ActorHandleSharedPtr c_actor_handle):
        worker = ray.worker.global_worker
        worker.check_connected()
        manager = worker.function_actor_manager

        actor_id = ActorID(dereference(c_actor_handle).GetActorID().Binary())
        job_id = JobID(dereference(c_actor_handle).CreationJobID().Binary())
        language = Language.from_native(
            dereference(c_actor_handle).ActorLanguage())
        actor_creation_function_descriptor = CFunctionDescriptorToPython(
            dereference(c_actor_handle).ActorCreationTaskFunctionDescriptor())
        if language == Language.PYTHON:
            assert isinstance(actor_creation_function_descriptor,
                              PythonFunctionDescriptor)
            # Load actor_method_cpu from actor handle's extension data.
            extension_data = <str>dereference(c_actor_handle).ExtensionData()
            if extension_data:
                actor_method_cpu = int(extension_data)
            else:
                actor_method_cpu = 0  # Actor is created by non Python worker.
            actor_class = manager.load_actor_class(
                job_id, actor_creation_function_descriptor)
            method_meta = ray.actor.ActorClassMethodMetadata.create(
                actor_class, actor_creation_function_descriptor)
            return ray.actor.ActorHandle(language, actor_id,
                                         method_meta.decorators,
                                         method_meta.signatures,
                                         method_meta.num_returns,
                                         actor_method_cpu,
                                         actor_creation_function_descriptor,
                                         worker.current_session_and_job)
        else:
            return ray.actor.ActorHandle(language, actor_id,
                                         {},  # method decorators
                                         {},  # method signatures
                                         {},  # method num_returns
                                         0,  # actor method cpu
                                         actor_creation_function_descriptor,
                                         worker.current_session_and_job)

    def deserialize_and_register_actor_handle(self, const c_string &bytes,
                                              ObjectRef
                                              outer_object_ref):
        cdef:
            CObjectID c_outer_object_id = (outer_object_ref.native() if
                                           outer_object_ref else
                                           CObjectID.Nil())
        c_actor_id = (CCoreWorkerProcess
                      .GetCoreWorker()
                      .DeserializeAndRegisterActorHandle(
                          bytes, c_outer_object_id))
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id))

    def get_named_actor_handle(self, const c_string &name,
                               const c_string &ray_namespace):
        cdef:
            pair[ActorHandleSharedPtr, CRayStatus] named_actor_handle_pair

        # We need it because GetNamedActorHandle needs
        # to call a method that holds the gil.
        with nogil:
            named_actor_handle_pair = (
                CCoreWorkerProcess.GetCoreWorker().GetNamedActorHandle(
                    name, ray_namespace))
        check_status(named_actor_handle_pair.second)

        return self.make_actor_handle(named_actor_handle_pair.first)

    def get_actor_handle(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id))

    def list_named_actors(self, c_bool all_namespaces):
        """Returns (namespace, name) for named actors in the system.

        If all_namespaces is True, returns all actors in all namespaces,
        else returns only the actors in the current namespace.
        """
        cdef:
            pair[c_vector[pair[c_string, c_string]], CRayStatus] result_pair

        result_pair = CCoreWorkerProcess.GetCoreWorker().ListNamedActors(
            all_namespaces)
        check_status(result_pair.second)
        return [
            (namespace.decode("utf-8"),
             name.decode("utf-8")) for namespace, name in result_pair.first]

    def serialize_actor_handle(self, ActorID actor_id):
        cdef:
            c_string output
            CObjectID c_actor_handle_id
        check_status(CCoreWorkerProcess.GetCoreWorker().SerializeActorHandle(
            actor_id.native(), &output, &c_actor_handle_id))
        return output, ObjectRef(c_actor_handle_id.Binary())

    def add_object_ref_reference(self, ObjectRef object_ref):
        # Note: faster to not release GIL for short-running op.
        CCoreWorkerProcess.GetCoreWorker().AddLocalReference(
            object_ref.native())

    def remove_object_ref_reference(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
        # We need to release the gil since object destruction may call the
        # unhandled exception handler.
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                c_object_id)

    def serialize_and_promote_object_ref(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CAddress c_owner_address = CAddress()
            c_string serialized_object_status
        CCoreWorkerProcess.GetCoreWorker().PromoteObjectToPlasma(c_object_id)
        CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(
                c_object_id, &c_owner_address, &serialized_object_status)
        return (object_ref,
                c_owner_address.SerializeAsString(),
                serialized_object_status)

    def deserialize_and_register_object_ref(
            self, const c_string &object_ref_binary,
            ObjectRef outer_object_ref,
            const c_string &serialized_owner_address,
            const c_string &serialized_object_status,
    ):
        cdef:
            CObjectID c_object_id = CObjectID.FromBinary(object_ref_binary)
            CObjectID c_outer_object_id = (outer_object_ref.native() if
                                           outer_object_ref else
                                           CObjectID.Nil())
            CAddress c_owner_address = CAddress()

        c_owner_address.ParseFromString(serialized_owner_address)
        (CCoreWorkerProcess.GetCoreWorker()
            .RegisterOwnershipInfoAndResolveFuture(
                c_object_id,
                c_outer_object_id,
                c_owner_address,
                serialized_object_status))

    cdef store_task_outputs(
            self, worker, outputs, const c_vector[CObjectID] return_ids,
            c_vector[shared_ptr[CRayObject]] *returns):
        cdef:
            CObjectID return_id
            size_t data_size
            shared_ptr[CBuffer] metadata
            c_vector[CObjectID] contained_id
            c_vector[CObjectID] return_ids_vector
            int64_t task_output_inlined_bytes

        if return_ids.size() == 0:
            return

        n_returns = len(outputs)
        returns.resize(n_returns)
        task_output_inlined_bytes = 0
        for i in range(n_returns):
            return_id, output = return_ids[i], outputs[i]
            context = worker.get_serialization_context()
            serialized_object = context.serialize(output)
            data_size = serialized_object.total_bytes
            metadata_str = serialized_object.metadata
            if ray.worker.global_worker.debugger_get_breakpoint:
                breakpoint = (
                    ray.worker.global_worker.debugger_get_breakpoint)
                metadata_str += (
                    b"," + ray_constants.OBJECT_METADATA_DEBUG_PREFIX +
                    breakpoint.encode())
                # Reset debugging context of this worker.
                ray.worker.global_worker.debugger_get_breakpoint = b""
            metadata = string_to_buffer(metadata_str)
            contained_id = ObjectRefsToVector(
                serialized_object.contained_object_refs)

            with nogil:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
                        return_id, data_size, metadata, contained_id,
                        task_output_inlined_bytes, &returns[0][i]))

            if returns[0][i].get() != NULL:
                if returns[0][i].get().HasData():
                    (<SerializedObject>serialized_object).write_to(
                        Buffer.make(returns[0][i].get().GetData()))
                if self.is_local_mode:
                    return_ids_vector.push_back(return_ids[i])
                    check_status(
                        CCoreWorkerProcess.GetCoreWorker().Put(
                            CRayObject(returns[0][i].get().GetData(),
                                       returns[0][i].get().GetMetadata(),
                                       c_vector[CObjectReference]()),
                            c_vector[CObjectID](), return_ids[i]))
                    return_ids_vector.clear()

            with nogil:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
                        return_id, returns[0][i]))

    def create_or_get_event_loop(self):
        if self.async_event_loop is None:
            self.async_event_loop = get_new_event_loop()
            asyncio.set_event_loop(self.async_event_loop)

        if self.async_thread is None:
            self.async_thread = threading.Thread(
                target=lambda: self.async_event_loop.run_forever(),
                name="AsyncIO Thread"
            )
            # Making the thread a daemon causes it to exit
            # when the main thread exits.
            self.async_thread.daemon = True
            self.async_thread.start()

        return self.async_event_loop

    def run_async_func_in_event_loop(self, func, *args, **kwargs):
        cdef:
            CFiberEvent event
        loop = self.create_or_get_event_loop()
        coroutine = func(*args, **kwargs)
        if threading.get_ident() == self.async_thread.ident:
            future = asyncio.ensure_future(coroutine, loop)
        else:
            future = asyncio.run_coroutine_threadsafe(coroutine, loop)
        future.add_done_callback(lambda _: event.Notify())
        with nogil:
            (CCoreWorkerProcess.GetCoreWorker()
                .YieldCurrentFiber(event))
        return future.result()

    def destroy_event_loop_if_exists(self):
        if self.async_event_loop is not None:
            self.async_event_loop.stop()
        if self.async_thread is not None:
            self.async_thread.join()

    def current_actor_is_asyncio(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorIsAsync())

    def get_current_runtime_env_dict(self):
        # This should never change, so we can safely cache it to avoid ser/de
        if self.current_runtime_env_dict is None:
            if self.is_driver:
                self.current_runtime_env_dict = \
                    json.loads(self.get_job_config().serialized_runtime_env)
            else:
                self.current_runtime_env_dict = json.loads(
                    CCoreWorkerProcess.GetCoreWorker()
                    .GetWorkerContext()
                    .GetCurrentSerializedRuntimeEnv()
                )
        return self.current_runtime_env_dict

    def is_exiting(self):
        return CCoreWorkerProcess.GetCoreWorker().IsExiting()

    cdef yield_current_fiber(self, CFiberEvent &fiber_event):
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().YieldCurrentFiber(fiber_event)

    def get_all_reference_counts(self):
        cdef:
            unordered_map[CObjectID, pair[size_t, size_t]] c_ref_counts
            unordered_map[CObjectID, pair[size_t, size_t]].iterator it

        c_ref_counts = (
            CCoreWorkerProcess.GetCoreWorker().GetAllReferenceCounts())
        it = c_ref_counts.begin()

        ref_counts = {}
        while it != c_ref_counts.end():
            object_ref = dereference(it).first.Hex()
            ref_counts[object_ref] = {
                "local": dereference(it).second.first,
                "submitted": dereference(it).second.second}
            postincrement(it)

        return ref_counts

    def set_get_async_callback(self, ObjectRef object_ref, callback):
        cpython.Py_INCREF(callback)
        CCoreWorkerProcess.GetCoreWorker().GetAsync(
            object_ref.native(),
            async_callback,
            <void*>callback
        )

    def push_error(self, JobID job_id, error_type, error_message,
                   double timestamp):
        check_status(CCoreWorkerProcess.GetCoreWorker().PushError(
            job_id.native(), error_type.encode("ascii"),
            error_message.encode("ascii"), timestamp))

    def set_resource(self, basestring resource_name,
                     double capacity, NodeID client_id):
        CCoreWorkerProcess.GetCoreWorker().SetResource(
            resource_name.encode("ascii"), capacity,
            CNodeID.FromBinary(client_id.binary()))

    def get_job_config(self):
        cdef CJobConfig c_job_config
        # We can cache the deserialized job config object here because
        # the job config will not change after a job is submitted.
        if self.job_config is None:
            c_job_config = CCoreWorkerProcess.GetCoreWorker().GetJobConfig()
            self.job_config = gcs_utils.JobConfig()
            self.job_config.ParseFromString(c_job_config.SerializeAsString())
        return self.job_config

    def prepare_runtime_env(self, runtime_env_dict: dict) -> str:
        """Merge the given new runtime env with the current runtime env.

        If running in a driver, the current runtime env comes from the
        JobConfig.  Otherwise, we are running in a worker for an actor or
        task, and the current runtime env comes from the current TaskSpec.

        The child's runtime env dict is merged with the parents via a simple
        dict update, except for runtime_env["env_vars"], which is merged
        with runtime_env["env_vars"] of the parent rather than overwriting it.
        This is so that env vars set in the parent propagate to child actors
        and tasks even if a new env var is set in the child.

        Args:
            runtime_env_dict (dict): A runtime env for a child actor or task.
        Returns:
            The resulting merged JSON-serialized runtime env.
        """

        result_dict = copy.deepcopy(self.get_current_runtime_env_dict())

        result_env_vars = copy.deepcopy(result_dict.get("env_vars") or {})
        child_env_vars = runtime_env_dict.get("env_vars") or {}
        result_env_vars.update(child_env_vars)

        result_dict.update(runtime_env_dict)
        result_dict["env_vars"] = result_env_vars

        # NOTE(architkulkarni): This allows worker caching code in C++ to
        # check if a runtime env is empty without deserializing it.
        if result_dict["env_vars"] == {}:
            result_dict["env_vars"] = None
        if all(val is None for val in result_dict.values()):
            result_dict = {}

        # TODO(architkulkarni): We should just use RuntimeEnvDict here
        # so all the serialization and validation is done in one place
        return json.dumps(result_dict, sort_keys=True)

    def get_task_submission_stats(self):
        cdef:
            int64_t num_tasks_submitted
            int64_t num_leases_requested

        with nogil:
            num_tasks_submitted = (
                    CCoreWorkerProcess.GetCoreWorker().GetNumTasksSubmitted())
            num_leases_requested = (
                    CCoreWorkerProcess.GetCoreWorker().GetNumLeasesRequested())

        return (num_tasks_submitted, num_leases_requested)

cdef void async_callback(shared_ptr[CRayObject] obj,
                         CObjectID object_ref,
                         void *user_callback) with gil:
    cdef:
        c_vector[shared_ptr[CRayObject]] objects_to_deserialize

    # Object is retrieved from in memory store.
    # Here we go through the code path used to deserialize objects.
    objects_to_deserialize.push_back(obj)
    data_metadata_pairs = RayObjectsToDataMetadataPairs(
        objects_to_deserialize)
    ids_to_deserialize = [ObjectRef(object_ref.Binary())]
    result = ray.worker.global_worker.deserialize_objects(
        data_metadata_pairs, ids_to_deserialize)[0]

    py_callback = <object>user_callback
    py_callback(result)
    cpython.Py_DECREF(py_callback)
