# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from cpython.exc cimport PyErr_CheckSignals

import asyncio
import gc
import inspect
import logging
import msgpack
import os
import pickle
import setproctitle
import sys
import threading
import time
import traceback
import _thread

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
from ray.includes.optional cimport (
    optional,
    nullopt,
    make_optional,
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
    CSchedulingStrategy,
    CPlacementGroupSchedulingStrategy,
    CRayFunction,
    CWorkerType,
    CJobConfig,
    CConcurrencyGroup,
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

from ray.includes.ray_config cimport RayConfig
from ray.includes.global_state_accessor cimport CGlobalStateAccessor

from ray.includes.optional cimport (
    optional
)

import ray
from ray.exceptions import (
    RayActorError,
    RayError,
    RaySystemError,
    RayTaskError,
    ObjectStoreFullError,
    GetTimeoutError,
    TaskCancelledError,
    AsyncioActorExit,
    PendingCallsLimitExceeded,
)
from ray import external_storage
from ray.util.scheduling_strategies import (
    DEFAULT_SCHEDULING_STRATEGY,
    SPREAD_SCHEDULING_STRATEGY,
    PlacementGroupSchedulingStrategy,
)
import ray.ray_constants as ray_constants
from ray._private.async_compat import sync_to_async, get_new_event_loop
from ray._private.client_mode_hook import disable_client_hook
import ray._private.gcs_utils as gcs_utils
import ray._private.memory_monitor as memory_monitor
import ray._private.profiling as profiling
from ray._private.utils import decode

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
        result.append(ObjectRef(
            object_refs[i].object_id(),
            object_refs[i].owner_address().SerializeAsString(),
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

cdef c_vector[CFunctionDescriptor] prepare_function_descriptors(pyfd_list):
    cdef:
        c_vector[CFunctionDescriptor] fd_list
        CRayFunction ray_function

    for pyfd in pyfd_list:
        fd_list.push_back(CFunctionDescriptorBuilder.BuildPython(
            pyfd.module_name, pyfd.class_name, pyfd.function_name, b""))
    return fd_list


cdef int prepare_actor_concurrency_groups(
        dict concurrency_groups_dict,
        c_vector[CConcurrencyGroup] *concurrency_groups):

    cdef:
        CConcurrencyGroup cg
        c_vector[CFunctionDescriptor] c_fd_list

    if concurrency_groups_dict is None:
        raise ValueError("Must provide it...")

    for key, value in concurrency_groups_dict.items():
        c_fd_list = prepare_function_descriptors(value["function_descriptors"])
        cg = CConcurrencyGroup(
            key.encode("ascii"), value["max_concurrency"], c_fd_list)
        concurrency_groups.push_back(cg)
    return 1

cdef prepare_args(
        CoreWorker core_worker,
        Language language, args,
        c_vector[unique_ptr[CTaskArg]] *args_vector, function_descriptor):
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
            try:
                serialized_arg = worker.get_serialization_context(
                ).serialize(arg)
            except TypeError as e:
                msg = (
                    "Could not serialize the argument "
                    f"{repr(arg)} for a task or actor "
                    f"{function_descriptor.repr}. Check "
                    "https://docs.ray.io/en/master/serialization.html#troubleshooting " # noqa
                    "for more information.")
                raise TypeError(msg) from e
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
        c_bool *is_application_level_error,
        # This parameter is only used for actor creation task to define
        # the concurrency groups of this actor.
        const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups,
        const c_string c_name_of_concurrency_group_to_execute):

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
        if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
            # Record the actor class via :actor_name: magic token in the log.
            #
            # (Phase 1): this covers code run before __init__ finishes.
            # We need to handle this separately because `__repr__` may not be
            # runnable until after `__init__` (e.g., if it accesses fields
            # defined in the constructor).
            actor_magic_token = "{}{}\n".format(
                ray_constants.LOG_PREFIX_ACTOR_NAME, actor_class.__name__)
            # Flush to both .out and .err
            print(actor_magic_token, end="")
            print(actor_magic_token, file=sys.stderr, end="")

        # Initial eventloops for asyncio for this actor.
        if core_worker.current_actor_is_asyncio():
            core_worker.initialize_eventloops_for_actor_concurrency_group(
                c_defined_concurrency_groups)

    execution_info = execution_infos.get(function_descriptor)
    if not execution_info:
        execution_info = manager.get_execution_info(
            job_id, function_descriptor)
        execution_infos[function_descriptor] = execution_info

    function_name = execution_info.function_name
    extra_data = (b'{"name": ' + function_name.encode("ascii") +
                  b' "task_id": ' + task_id.hex().encode("ascii") + b'}')

    task_name = name.decode("utf-8")
    name_of_concurrency_group_to_execute = \
        c_name_of_concurrency_group_to_execute.decode("ascii")
    title = f"ray::{task_name}"

    if <int>task_type == <int>TASK_TYPE_NORMAL_TASK:
        next_title = "ray::IDLE"
        function_executor = execution_info.function
        # Record the task name via :task_name: magic token in the log file.
        # This is used for the prefix in driver logs `(task_name pid=123) ...`
        task_name_magic_token = "{}{}\n".format(
            ray_constants.LOG_PREFIX_TASK_NAME, task_name.replace("()", ""))
        # Print on both .out and .err
        print(task_name_magic_token, end="")
        print(task_name_magic_token, file=sys.stderr, end="")
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
                    async_function, function_descriptor,
                    name_of_concurrency_group_to_execute, actor,
                    *arguments, **kwarguments)

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
                            deserialize_args, function_descriptor,
                            name_of_concurrency_group_to_execute)
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
                                "{\"exit_debugger\": true}",
                                namespace=ray_constants.KV_NAMESPACE_PDB
                            )
                            ray.experimental.internal_kv._internal_kv_del(
                                "RAY_PDB_CONTINUE_{}".format(next_breakpoint),
                                namespace=ray_constants.KV_NAMESPACE_PDB
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
            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                # Record actor repr via :actor_name: magic token in the log.
                #
                # (Phase 2): after `__init__` finishes, we override the
                # log prefix with the full repr of the actor. The log monitor
                # will pick up the updated token.
                if (hasattr(actor_class, "__ray_actor_class__") and
                        "__repr__" in
                        actor_class.__ray_actor_class__.__dict__):
                    actor_magic_token = "{}{}\n".format(
                        ray_constants.LOG_PREFIX_ACTOR_NAME, repr(actor))
                    # Flush on both stdout and stderr.
                    print(actor_magic_token, end="")
                    print(actor_magic_token, file=sys.stderr, end="")
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
        c_bool *is_application_level_error,
        const c_vector[CConcurrencyGroup] &defined_concurrency_groups,
        const c_string name_of_concurrency_group_to_execute) nogil:
    with gil, disable_client_hook():
        try:
            try:
                # The call to execute_task should never raise an exception. If
                # it does, that indicates that there was an internal error.
                execute_task(task_type, task_name, ray_function, c_resources,
                             c_args, c_arg_refs, c_return_ids,
                             debugger_breakpoint, returns,
                             is_application_level_error,
                             defined_concurrency_groups,
                             name_of_concurrency_group_to_execute)
            except Exception as e:
                sys_exit = SystemExit()
                if isinstance(e, RayActorError) and \
                   e.actor_init_failed:
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
            elif e.code and e.code == 0:
                # This means the system exit was
                # normal based on the python convention.
                # https://docs.python.org/3/library/sys.html#sys.exit
                return CRayStatus.IntentionalSystemExit()
            else:
                msg = "SystemExit was raised from the worker."
                # In K8s, SIGTERM likely means we hit memory limits, so print
                # a more informative message there.
                if "KUBERNETES_SERVICE_HOST" in os.environ:
                    msg += (
                        " The worker may have exceeded K8s pod memory limits.")
                logger.exception(msg)
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
                  worker_shim_pid, startup_token):
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
        options.startup_token = startup_token
        CCoreWorkerProcess.Initialize(options)

        self.cgname_to_eventloop_dict = None
        self.fd_to_cgname_dict = None
        self.eventloop_for_default_cg = None

    def shutdown(self):
        with nogil:
            # If it's a worker, the core worker process should have been
            # shutdown. So we can't call
            # `CCoreWorkerProcess.GetCoreWorker().GetWorkerType()` here.
            # Instead, we use the cached `is_driver` flag to test if it's a
            # driver.
            if self.is_driver:
                CCoreWorkerProcess.Shutdown()

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
            unique_ptr[CAddress] c_owner_address
            c_vector[CObjectID] contained_object_ids
            c_vector[CObjectReference] contained_object_refs

        metadata = string_to_buffer(serialized_object.metadata)
        put_threshold = RayConfig.instance().max_direct_call_object_size()
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
            if self.is_local_mode:
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

    cdef python_scheduling_strategy_to_c(
            self, python_scheduling_strategy,
            CSchedulingStrategy *c_scheduling_strategy):
        cdef:
            CPlacementGroupSchedulingStrategy \
                *c_placement_group_scheduling_strategy
        assert python_scheduling_strategy is not None
        if python_scheduling_strategy == DEFAULT_SCHEDULING_STRATEGY:
            c_scheduling_strategy[0].mutable_default_scheduling_strategy()
        elif python_scheduling_strategy == SPREAD_SCHEDULING_STRATEGY:
            c_scheduling_strategy[0].mutable_spread_scheduling_strategy()
        elif isinstance(python_scheduling_strategy,
                        PlacementGroupSchedulingStrategy):
            c_placement_group_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_placement_group_scheduling_strategy()
            c_placement_group_scheduling_strategy[0].set_placement_group_id(
                python_scheduling_strategy
                .placement_group.id.binary())
            c_placement_group_scheduling_strategy[0] \
                .set_placement_group_bundle_index(
                    python_scheduling_strategy.placement_group_bundle_index)
            c_placement_group_scheduling_strategy[0]\
                .set_placement_group_capture_child_tasks(
                    python_scheduling_strategy
                    .placement_group_capture_child_tasks)
        else:
            raise ValueError(
                f"Invalid scheduling_strategy value "
                f"{python_scheduling_strategy}. "
                f"Valid values are [\"DEFAULT\""
                f" | \"SPREAD\""
                f" | PlacementGroupSchedulingStrategy]")

    def submit_task(self,
                    Language language,
                    FunctionDescriptor function_descriptor,
                    args,
                    c_string name,
                    int num_returns,
                    resources,
                    int max_retries,
                    c_bool retry_exceptions,
                    scheduling_strategy,
                    c_string debugger_breakpoint,
                    c_string serialized_runtime_env,
                    ):
        cdef:
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            CSchedulingStrategy c_scheduling_strategy

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(
                self, language, args, &args_vector, function_descriptor)

            # NOTE(edoakes): releasing the GIL while calling this method causes
            # segfaults. See relevant issue for details:
            # https://github.com/ray-project/ray/pull/12803
            return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
                ray_function, args_vector, CTaskOptions(
                    name, num_returns, c_resources,
                    b"",
                    serialized_runtime_env),
                max_retries, retry_exceptions,
                c_scheduling_strategy,
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
                     is_detached,
                     c_string name,
                     c_string ray_namespace,
                     c_bool is_asyncio,
                     c_string extension_data,
                     c_string serialized_runtime_env,
                     concurrency_groups_dict,
                     int32_t max_pending_calls,
                     scheduling_strategy,
                     ):
        cdef:
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[c_string] dynamic_worker_options
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, double] c_placement_resources
            CActorID c_actor_id
            c_vector[CConcurrencyGroup] c_concurrency_groups
            CSchedulingStrategy c_scheduling_strategy
            optional[c_bool] is_detached_optional = nullopt

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(
                self, language, args, &args_vector, function_descriptor)
            prepare_actor_concurrency_groups(
                concurrency_groups_dict, &c_concurrency_groups)

            if is_detached is not None:
                is_detached_optional = make_optional[c_bool](
                    True if is_detached else False)

            with nogil:
                check_status(CCoreWorkerProcess.GetCoreWorker().CreateActor(
                    ray_function, args_vector,
                    CActorCreationOptions(
                        max_restarts, max_task_retries, max_concurrency,
                        c_resources, c_placement_resources,
                        dynamic_worker_options, is_detached_optional, name,
                        ray_namespace,
                        is_asyncio,
                        c_scheduling_strategy,
                        serialized_runtime_env,
                        c_concurrency_groups,
                        # execute out of order for
                        # async or threaded actors.
                        is_asyncio or max_concurrency > 1,
                        max_pending_calls),
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
                          double num_method_cpus,
                          c_string concurrency_group_name):

        cdef:
            CActorID c_actor_id = actor_id.native()
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            optional[c_vector[CObjectReference]] return_refs

        with self.profile_event(b"submit_task"):
            if num_method_cpus > 0:
                c_resources[b"CPU"] = num_method_cpus
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(
                self, language, args, &args_vector, function_descriptor)

            # NOTE(edoakes): releasing the GIL while calling this method causes
            # segfaults. See relevant issue for details:
            # https://github.com/ray-project/ray/pull/12803
            return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitActorTask(
                c_actor_id,
                ray_function,
                args_vector,
                CTaskOptions(
                    name, num_returns, c_resources, concurrency_group_name))
            if return_refs.has_value():
                return VectorToObjectRefs(return_refs.value())
            else:
                actor = self.get_actor_handle(actor_id)
                actor_handle = (CCoreWorkerProcess.GetCoreWorker()
                                .GetActorHandle(c_actor_id))
                raise PendingCallsLimitExceeded("The task {} could not be "
                                                "submitted to {} because more "
                                                "than {} tasks are queued on "
                                                "the actor. This limit "
                                                "can be adjusted with the "
                                                "`max_pending_calls` actor "
                                                "option.".format(
                                                    function_descriptor
                                                    .function_name,
                                                    repr(actor),
                                                    (dereference(actor_handle)
                                                        .MaxPendingCalls())
                                                ))

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

    def get_owner_address(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
        return CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                c_object_id).SerializeAsString()

    def serialize_object_ref(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CAddress c_owner_address = CAddress()
            c_string serialized_object_status
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

    cdef store_task_output(self, serialized_object, const CObjectID &return_id,
                           size_t data_size, shared_ptr[CBuffer] &metadata,
                           const c_vector[CObjectID] &contained_id,
                           int64_t *task_output_inlined_bytes,
                           shared_ptr[CRayObject] *return_ptr):
        """Store a task return value in plasma or as an inlined object."""
        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
                    return_id, data_size, metadata, contained_id,
                    task_output_inlined_bytes, return_ptr))

        if return_ptr.get() != NULL:
            if return_ptr.get().HasData():
                (<SerializedObject>serialized_object).write_to(
                    Buffer.make(return_ptr.get().GetData()))
            if self.is_local_mode:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().Put(
                        CRayObject(return_ptr.get().GetData(),
                                   return_ptr.get().GetMetadata(),
                                   c_vector[CObjectReference]()),
                        c_vector[CObjectID](), return_id))
            else:
                with nogil:
                    check_status(
                        CCoreWorkerProcess.GetCoreWorker().SealReturnObject(
                            return_id, return_ptr[0]))
            return True
        else:
            with nogil:
                success = (CCoreWorkerProcess.GetCoreWorker()
                           .PinExistingReturnObject(return_id, return_ptr))
            return success

    cdef store_task_outputs(
            self, worker, outputs, const c_vector[CObjectID] return_ids,
            c_vector[shared_ptr[CRayObject]] *returns):
        cdef:
            CObjectID return_id
            size_t data_size
            shared_ptr[CBuffer] metadata
            c_vector[CObjectID] contained_id
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

            if not self.store_task_output(
                    serialized_object, return_id,
                    data_size, metadata, contained_id,
                    &task_output_inlined_bytes, &returns[0][i]):
                # If the object already exists, but we fail to pin the copy, it
                # means the existing copy might've gotten evicted. Try to
                # create another copy.
                self.store_task_output(
                        serialized_object, return_id, data_size, metadata,
                        contained_id, &task_output_inlined_bytes,
                        &returns[0][i])

    cdef c_function_descriptors_to_python(
            self,
            const c_vector[CFunctionDescriptor] &c_function_descriptors):

        ret = []
        for i in range(c_function_descriptors.size()):
            ret.append(CFunctionDescriptorToPython(c_function_descriptors[i]))
        return ret

    cdef initialize_eventloops_for_actor_concurrency_group(
            self,
            const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups):

        cdef:
            CConcurrencyGroup c_concurrency_group
            c_vector[CFunctionDescriptor] c_function_descriptors

        self.cgname_to_eventloop_dict = {}
        self.fd_to_cgname_dict = {}

        self.eventloop_for_default_cg = get_new_event_loop()
        self.thread_for_default_cg = threading.Thread(
            target=lambda: self.eventloop_for_default_cg.run_forever(),
            name="AsyncIO Thread: default"
            )
        # Making the thread as daemon to let it exit
        # when the main thread exits.
        self.thread_for_default_cg.daemon = True
        self.thread_for_default_cg.start()

        for i in range(c_defined_concurrency_groups.size()):
            c_concurrency_group = c_defined_concurrency_groups[i]
            cg_name = c_concurrency_group.GetName().decode("ascii")
            function_descriptors = self.c_function_descriptors_to_python(
                c_concurrency_group.GetFunctionDescriptors())

            async_eventloop = get_new_event_loop()
            async_thread = threading.Thread(
                target=lambda: async_eventloop.run_forever(),
                name="AsyncIO Thread: {}".format(cg_name)
            )
            # Making the thread a daemon causes it to exit
            # when the main thread exits.
            async_thread.daemon = True
            async_thread.start()

            self.cgname_to_eventloop_dict[cg_name] = {
                "eventloop": async_eventloop,
                "thread": async_thread,
            }

            for fd in function_descriptors:
                self.fd_to_cgname_dict[fd] = cg_name

    def get_event_loop(self, function_descriptor, specified_cgname):
        # __init__ will be invoked in default eventloop
        if function_descriptor.function_name == "__init__":
            return self.eventloop_for_default_cg, self.thread_for_default_cg

        if specified_cgname is not None:
            if specified_cgname in self.cgname_to_eventloop_dict:
                this_group = self.cgname_to_eventloop_dict[specified_cgname]
                return (this_group["eventloop"], this_group["thread"])

        if function_descriptor in self.fd_to_cgname_dict:
            curr_cgname = self.fd_to_cgname_dict[function_descriptor]
            if curr_cgname in self.cgname_to_eventloop_dict:
                return (
                    self.cgname_to_eventloop_dict[curr_cgname]["eventloop"],
                    self.cgname_to_eventloop_dict[curr_cgname]["thread"])
            else:
                raise ValueError(
                    "The function {} is defined to be executed "
                    "in the concurrency group {} . But there is no this group."
                    .format(function_descriptor, curr_cgname))

        return self.eventloop_for_default_cg, self.thread_for_default_cg

    def run_async_func_in_event_loop(
          self, func, function_descriptor, specified_cgname, *args, **kwargs):

        cdef:
            CFiberEvent event
        eventloop, async_thread = self.get_event_loop(
            function_descriptor, specified_cgname)
        coroutine = func(*args, **kwargs)
        if threading.get_ident() == async_thread.ident:
            future = asyncio.ensure_future(coroutine, eventloop)
        else:
            future = asyncio.run_coroutine_threadsafe(coroutine, eventloop)
        future.add_done_callback(lambda _: event.Notify())
        with nogil:
            (CCoreWorkerProcess.GetCoreWorker()
                .YieldCurrentFiber(event))
        return future.result()

    def destroy_event_loop_if_exists(self):
        if self.async_event_loop is not None:
            self.async_event_loop.call_soon_threadsafe(
                self.async_event_loop.stop)
        if self.async_thread is not None:
            self.async_thread.join()

    def current_actor_is_asyncio(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorIsAsync())

    def get_current_runtime_env(self) -> str:
        # This should never change, so we can safely cache it to avoid ser/de
        if self.current_runtime_env is None:
            if self.is_driver:
                job_config = self.get_job_config()
                serialized_env = job_config.runtime_env_info \
                                           .serialized_runtime_env
            else:
                serialized_env = CCoreWorkerProcess.GetCoreWorker() \
                        .GetWorkerContext().GetCurrentSerializedRuntimeEnv() \
                        .decode("utf-8")

            self.current_runtime_env = serialized_env

        return self.current_runtime_env

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

    def get_actor_call_stats(self):
        cdef:
            unordered_map[c_string, c_vector[uint64_t]] c_tasks_count

        c_tasks_count = (
            CCoreWorkerProcess.GetCoreWorker().GetActorCallStats())
        it = c_tasks_count.begin()

        tasks_count = dict()
        while it != c_tasks_count.end():
            func_name = <unicode>dereference(it).first
            counters = dereference(it).second
            tasks_count[func_name] = {
                "pending": counters[0],
                "running": counters[1],
                "finished": counters[2],
            }
            postincrement(it)
        return tasks_count

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

    def get_job_config(self):
        cdef CJobConfig c_job_config
        # We can cache the deserialized job config object here because
        # the job config will not change after a job is submitted.
        if self.job_config is None:
            c_job_config = CCoreWorkerProcess.GetCoreWorker().GetJobConfig()
            self.job_config = gcs_utils.JobConfig()
            self.job_config.ParseFromString(c_job_config.SerializeAsString())
        return self.job_config

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
