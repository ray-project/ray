# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from cpython.exc cimport PyErr_CheckSignals

import asyncio
import numpy
import gc
import inspect
import threading
import time
import logging
import os
import pickle
import sys

from libc.stdint cimport (
    int32_t,
    int64_t,
    INT64_MAX,
    uint64_t,
    uint8_t,
)
from libcpp cimport bool as c_bool
from libcpp.memory cimport (
    dynamic_pointer_cast,
    make_shared,
    shared_ptr,
    unique_ptr,
)
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector

from cython.operator import dereference, postincrement

from ray.includes.common cimport (
    CBuffer,
    CAddress,
    CLanguage,
    CRayObject,
    CRayStatus,
    CGcsClientOptions,
    CTaskArg,
    CTaskType,
    CRayFunction,
    LocalMemoryBuffer,
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
)
from ray.includes.unique_ids cimport (
    CActorID,
    CActorCheckpointID,
    CObjectID,
    CClientID,
)
from ray.includes.libcoreworker cimport (
    CActorCreationOptions,
    CCoreWorker,
    CTaskOptions,
    ResourceMappingType,
    CFiberEvent,
    CActorHandle,
)
from ray.includes.task cimport CTaskSpec
from ray.includes.ray_config cimport RayConfig

import ray
from ray.async_compat import (sync_to_async,
                              AsyncGetResponse, AsyncMonitorState)
import ray.experimental.signal as ray_signal
import ray.memory_monitor as memory_monitor
import ray.ray_constants as ray_constants
from ray import profiling
from ray.exceptions import (
    RayError,
    RayletError,
    RayTaskError,
    ObjectStoreFullError,
    RayTimeoutError,
)
from ray.experimental.no_return import NoReturn
from ray.utils import decode

cimport cpython

include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/function_descriptor.pxi"
include "includes/task.pxi"
include "includes/buffer.pxi"
include "includes/common.pxi"
include "includes/serialization.pxi"
include "includes/libcoreworker.pxi"


logger = logging.getLogger(__name__)

MEMCOPY_THREADS = 6


def set_internal_config(dict options):
    cdef:
        unordered_map[c_string, c_string] c_options

    if options is None:
        return

    for key, value in options.items():
        c_options[str(key).encode("ascii")] = str(value).encode("ascii")

    RayConfig.instance().initialize(c_options)


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
        raise RayTimeoutError(message)
    else:
        raise RayletError(message)

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


cdef VectorToObjectIDs(const c_vector[CObjectID] &object_ids):
    result = []
    for i in range(object_ids.size()):
        result.append(ObjectID(object_ids[i].Binary()))
    return result


cdef c_vector[CObjectID] ObjectIDsToVector(object_ids):
    """A helper function that converts a Python list of object IDs to a vector.

    Args:
        object_ids (list): The Python list of object IDs.

    Returns:
        The output vector.
    """
    cdef:
        ObjectID object_id
        c_vector[CObjectID] result
    for object_id in object_ids:
        result.push_back(object_id.native())
    return result


def compute_task_id(ObjectID object_id):
    return TaskID(object_id.native().TaskId().Binary())


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


@cython.auto_pickle(False)
cdef class Language:
    cdef CLanguage lang

    def __cinit__(self, int32_t lang):
        self.lang = <CLanguage>lang

    @staticmethod
    cdef from_native(const CLanguage& lang):
        return Language(<int32_t>lang)

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


cdef void prepare_args(
        CoreWorker core_worker, args, c_vector[CTaskArg] *args_vector):
    cdef:
        size_t size
        int64_t put_threshold
        shared_ptr[CBuffer] arg_data
        c_vector[CObjectID] inlined_ids
        ObjectID obj_id

    worker = ray.worker.global_worker
    put_threshold = RayConfig.instance().max_direct_call_object_size()
    for arg in args:
        if isinstance(arg, ObjectID):
            args_vector.push_back(
                CTaskArg.PassByReference((<ObjectID>arg).native()))

        else:
            serialized_arg = worker.get_serialization_context().serialize(arg)
            size = serialized_arg.total_bytes

            # TODO(edoakes): any objects containing ObjectIDs are spilled to
            # plasma here. This is inefficient for small objects, but inlined
            # arguments aren't associated ObjectIDs right now so this is a
            # simple fix for reference counting purposes.
            if <int64_t>size <= put_threshold:
                arg_data = dynamic_pointer_cast[CBuffer, LocalMemoryBuffer](
                        make_shared[LocalMemoryBuffer](size))
                write_serialized_object(serialized_arg, arg_data)
                for obj_id in serialized_arg.contained_object_ids:
                    inlined_ids.push_back(obj_id.native())
                args_vector.push_back(
                    CTaskArg.PassByValue(make_shared[CRayObject](
                        arg_data, string_to_buffer(serialized_arg.metadata),
                        inlined_ids)))
                inlined_ids.clear()
            else:
                args_vector.push_back(
                    CTaskArg.PassByReference((CObjectID.FromBinary(
                        core_worker.put_serialized_cobject(serialized_arg)))))

cdef deserialize_args(
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectID] &arg_reference_ids):
    if c_args.empty():
        return [], {}

    args = ray.worker.global_worker.deserialize_objects(
             RayObjectsToDataMetadataPairs(c_args),
             VectorToObjectIDs(arg_reference_ids))

    for arg in args:
        if isinstance(arg, RayError):
            raise arg

    return ray.signature.recover_args(args)


cdef execute_task(
        CTaskType task_type,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectID] &c_arg_reference_ids,
        const c_vector[CObjectID] &c_return_ids,
        c_vector[shared_ptr[CRayObject]] *returns):

    worker = ray.worker.global_worker
    manager = worker.function_actor_manager

    cdef:
        dict execution_infos = manager.execution_infos
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        CTaskID task_id = core_worker.core_worker.get().GetCurrentTaskId()
        CFiberEvent fiber_event

    # Automatically restrict the GPUs available to this task.
    ray.utils.set_cuda_visible_devices(ray.get_gpu_ids())

    function_descriptor = CFunctionDescriptorToPython(
        ray_function.GetFunctionDescriptor())

    if <int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK:
        actor_class = manager.load_actor_class(job_id, function_descriptor)
        actor_id = core_worker.get_actor_id()
        worker.actors[actor_id] = actor_class.__new__(actor_class)
        worker.actor_checkpoint_info[actor_id] = (
            ray.worker.ActorCheckpointInfo(
                num_tasks_since_last_checkpoint=0,
                last_checkpoint_timestamp=int(1000 * time.time()),
                checkpoint_ids=[]))

    execution_info = execution_infos.get(function_descriptor)
    if not execution_info:
        execution_info = manager.get_execution_info(
            job_id, function_descriptor)
        execution_infos[function_descriptor] = execution_info

    function_name = execution_info.function_name
    extra_data = (b'{"name": ' + function_name.encode("ascii") +
                  b' "task_id": ' + task_id.Hex() + b'}')

    if <int>task_type == <int>TASK_TYPE_NORMAL_TASK:
        title = "ray::{}()".format(function_name)
        next_title = "ray::IDLE"
        function_executor = execution_info.function
    else:
        actor = worker.actors[core_worker.get_actor_id()]
        class_name = actor.__class__.__name__
        title = "ray::{}.{}()".format(class_name, function_name)
        next_title = "ray::{}".format(class_name)
        worker_name = "ray_{}_{}".format(class_name, os.getpid())
        if c_resources.find(b"memory") != c_resources.end():
            worker.memory_monitor.set_heap_limit(
                worker_name,
                ray_constants.from_memory_units(
                    dereference(c_resources.find(b"memory")).second))
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

                coroutine = async_function(actor, *arguments, **kwarguments)
                loop = core_worker.create_or_get_event_loop()
                monitor_state = loop.monitor_state
                monitor_state.register_coroutine(coroutine,
                                                 str(function.method))
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)

                def callback(future):
                    fiber_event.Notify()
                    monitor_state.unregister_coroutine(coroutine)

                future.add_done_callback(callback)
                with nogil:
                    (core_worker.core_worker.get()
                        .YieldCurrentFiber(fiber_event))

                return future.result()

            return function(actor, *arguments, **kwarguments)

    with core_worker.profile_event(b"task", extra_data=extra_data):
        try:
            task_exception = False
            if not (<int>task_type == <int>TASK_TYPE_ACTOR_TASK
                    and function_name == "__ray_terminate__"):
                worker.reraise_actor_init_error()
                worker.memory_monitor.raise_if_low_memory()

            with core_worker.profile_event(b"task:deserialize_arguments"):
                args, kwargs = deserialize_args(c_args, c_arg_reference_ids)
            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                actor = worker.actors[core_worker.get_actor_id()]
                class_name = actor.__class__.__name__
                actor_title = "{}({}, {})".format(
                    class_name, repr(args), repr(kwargs))
                core_worker.set_actor_title(actor_title.encode("utf-8"))
            # Execute the task.
            with ray.worker._changeproctitle(title, next_title):
                with core_worker.profile_event(b"task:execute"):
                    task_exception = True
                    outputs = function_executor(*args, **kwargs)
                    task_exception = False
                    if c_return_ids.size() == 1:
                        outputs = (outputs,)

            # Store the outputs in the object store.
            with core_worker.profile_event(b"task:store_outputs"):
                core_worker.store_task_outputs(
                    worker, outputs, c_return_ids, returns)
        except Exception as error:
            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                worker.mark_actor_init_failed(error)

            backtrace = ray.utils.format_error_message(
                traceback.format_exc(), task_exception=task_exception)
            if isinstance(error, RayTaskError):
                # Avoid recursive nesting of RayTaskError.
                failure_object = RayTaskError(function_name, backtrace,
                                              error.cause_cls)
            else:
                failure_object = RayTaskError(function_name, backtrace,
                                              error.__class__)
            errors = []
            for _ in range(c_return_ids.size()):
                errors.append(failure_object)
            core_worker.store_task_outputs(
                worker, errors, c_return_ids, returns)
            ray.utils.push_error_to_driver(
                worker,
                ray_constants.TASK_PUSH_ERROR,
                str(failure_object),
                job_id=worker.current_job_id)

            # Send signal with the error.
            ray_signal.send(ray_signal.ErrorSignal(str(failure_object)))

    # Don't need to reset `current_job_id` if the worker is an
    # actor. Because the following tasks should all have the
    # same driver id.
    if <int>task_type == <int>TASK_TYPE_NORMAL_TASK:
        # Reset signal counters so that the next task can get
        # all past signals.
        ray_signal.reset()

    if execution_info.max_calls != 0:
        # Reset the state of the worker for the next task to execute.
        # Increase the task execution counter.
        manager.increase_task_counter(job_id, function_descriptor)

        # If we've reached the max number of executions for this worker, exit.
        task_counter = manager.get_task_counter(job_id, function_descriptor)
        if task_counter == execution_info.max_calls:
            exit = SystemExit(0)
            exit.is_ray_terminate = True
            raise exit


cdef CRayStatus task_execution_handler(
        CTaskType task_type,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectID] &c_arg_reference_ids,
        const c_vector[CObjectID] &c_return_ids,
        c_vector[shared_ptr[CRayObject]] *returns,
        const CWorkerID &c_worker_id) nogil:

    with gil:
        try:
            try:
                # The call to execute_task should never raise an exception. If
                # it does, that indicates that there was an internal error.
                execute_task(task_type, ray_function, c_resources, c_args,
                             c_arg_reference_ids, c_return_ids, returns)
            except Exception:
                traceback_str = traceback.format_exc() + (
                    "An unexpected internal error occurred while the worker "
                    "was executing a task.")
                ray.utils.push_error_to_driver(
                    ray.worker.global_worker,
                    "worker_crash",
                    traceback_str,
                    job_id=None)
                sys.exit(1)
        except SystemExit as e:
            # Tell the core worker to exit as soon as the result objects
            # are processed.
            if hasattr(e, "is_ray_terminate"):
                return CRayStatus.IntentionalSystemExit()
            else:
                logger.exception("SystemExit was raised from the worker")
                return CRayStatus.UnexpectedSystemExit()

    return CRayStatus.OK()

cdef void async_plasma_callback(CObjectID object_id,
                                int64_t data_size,
                                int64_t metadata_size) with gil:
    message = [tuple([ObjectID(object_id.Binary()), data_size, metadata_size])]
    core_worker = ray.worker.global_worker.core_worker
    event_handler = core_worker.get_plasma_event_handler()
    if event_handler is not None:
        event_handler.process_notifications(message)

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
            logger.info(
                "gc.collect() freed {} refs in {} seconds".format(
                    num_freed, end - start))


cdef shared_ptr[CBuffer] string_to_buffer(c_string& c_str):
    cdef shared_ptr[CBuffer] empty_metadata
    if c_str.size() == 0:
        return empty_metadata
    return dynamic_pointer_cast[
        CBuffer, LocalMemoryBuffer](
            make_shared[LocalMemoryBuffer](
                <uint8_t*>(c_str.data()), c_str.size(), True))


cdef write_serialized_object(
        serialized_object, const shared_ptr[CBuffer]& buf):
    from ray.serialization import Pickle5SerializedObject, RawSerializedObject

    if isinstance(serialized_object, RawSerializedObject):
        if buf.get() != NULL and buf.get().Size() > 0:
            size = serialized_object.total_bytes
            if MEMCOPY_THREADS > 1 and size > kMemcopyDefaultThreshold:
                parallel_memcopy(buf.get().Data(),
                                 <const uint8_t*> serialized_object.value,
                                 size, kMemcopyDefaultBlocksize,
                                 MEMCOPY_THREADS)
            else:
                memcpy(buf.get().Data(),
                       <const uint8_t*>serialized_object.value, size)

    elif isinstance(serialized_object, Pickle5SerializedObject):
        (<Pickle5Writer>serialized_object.writer).write_to(
            serialized_object.inband, buf, MEMCOPY_THREADS)
    else:
        raise TypeError("Unsupported serialization type.")


cdef class CoreWorker:

    def __cinit__(self, is_driver, store_socket, raylet_socket,
                  JobID job_id, GcsClientOptions gcs_options, log_dir,
                  node_ip_address, node_manager_port):

        self.core_worker.reset(new CCoreWorker(
            WORKER_TYPE_DRIVER if is_driver else WORKER_TYPE_WORKER,
            LANGUAGE_PYTHON, store_socket.encode("ascii"),
            raylet_socket.encode("ascii"), job_id.native(),
            gcs_options.native()[0], log_dir.encode("utf-8"),
            node_ip_address.encode("utf-8"), node_manager_port,
            task_execution_handler, check_signals, gc_collect, True))

    def run_task_loop(self):
        with nogil:
            self.core_worker.get().StartExecutingTasks()

    def get_current_task_id(self):
        return TaskID(self.core_worker.get().GetCurrentTaskId().Binary())

    def get_current_job_id(self):
        return JobID(self.core_worker.get().GetCurrentJobId().Binary())

    def get_actor_id(self):
        return ActorID(self.core_worker.get().GetActorId().Binary())

    def set_webui_display(self, key, message):
        self.core_worker.get().SetWebuiDisplay(key, message)

    def set_actor_title(self, title):
        self.core_worker.get().SetActorTitle(title)

    def subscribe_to_plasma(self, plasma_event_handler):
        self.plasma_event_handler = plasma_event_handler
        self.core_worker.get().SubscribeToAsyncPlasma(async_plasma_callback)

    def get_plasma_event_handler(self):
        return self.plasma_event_handler

    def get_objects(self, object_ids, TaskID current_task_id,
                    int64_t timeout_ms=-1):
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            CTaskID c_task_id = current_task_id.native()
            c_vector[CObjectID] c_object_ids = ObjectIDsToVector(object_ids)

        with nogil:
            check_status(self.core_worker.get().Get(
                c_object_ids, timeout_ms, &results))

        return RayObjectsToDataMetadataPairs(results)

    def object_exists(self, ObjectID object_id):
        cdef:
            c_bool has_object
            CObjectID c_object_id = object_id.native()

        with nogil:
            check_status(self.core_worker.get().Contains(
                c_object_id, &has_object))

        return has_object

    cdef _create_put_buffer(self, shared_ptr[CBuffer] &metadata,
                            size_t data_size, ObjectID object_id,
                            c_vector[CObjectID] contained_ids,
                            CObjectID *c_object_id, shared_ptr[CBuffer] *data):
        if object_id is None:
            with nogil:
                check_status(self.core_worker.get().Create(
                             metadata, data_size, contained_ids,
                             c_object_id, data))
        else:
            c_object_id[0] = object_id.native()
            with nogil:
                check_status(self.core_worker.get().Create(
                            metadata, data_size,
                            c_object_id[0], data))

        # If data is nullptr, that means the ObjectID already existed,
        # which we ignore.
        # TODO(edoakes): this is hacky, we should return the error instead
        # and deal with it here.
        return data.get() == NULL

    def put_serialized_object(self, serialized_object,
                              ObjectID object_id=None,
                              c_bool pin_object=True):
        return ObjectID(self.put_serialized_cobject(
            serialized_object, object_id, pin_object))

    def put_serialized_cobject(self, serialized_object,
                               ObjectID object_id=None,
                               c_bool pin_object=True):
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data
            shared_ptr[CBuffer] metadata

        metadata = string_to_buffer(serialized_object.metadata)
        total_bytes = serialized_object.total_bytes
        object_already_exists = self._create_put_buffer(
            metadata, total_bytes, object_id,
            ObjectIDsToVector(serialized_object.contained_object_ids),
            &c_object_id, &data)

        if not object_already_exists:
            write_serialized_object(serialized_object, data)
            with nogil:
                # Using custom object IDs is not supported because we can't
                # track their lifecycle, so don't pin the object in that case.
                check_status(
                    self.core_worker.get().Seal(
                        c_object_id, pin_object and object_id is None))

        return c_object_id.Binary()

    def wait(self, object_ids, int num_returns, int64_t timeout_ms,
             TaskID current_task_id):
        cdef:
            c_vector[CObjectID] wait_ids
            c_vector[c_bool] results
            CTaskID c_task_id = current_task_id.native()

        wait_ids = ObjectIDsToVector(object_ids)
        with nogil:
            check_status(self.core_worker.get().Wait(
                wait_ids, num_returns, timeout_ms, &results))

        assert len(results) == len(object_ids)

        ready, not_ready = [], []
        for i, object_id in enumerate(object_ids):
            if results[i]:
                ready.append(object_id)
            else:
                not_ready.append(object_id)

        return ready, not_ready

    def free_objects(self, object_ids, c_bool local_only,
                     c_bool delete_creating_tasks):
        cdef:
            c_vector[CObjectID] free_ids = ObjectIDsToVector(object_ids)

        with nogil:
            check_status(self.core_worker.get().Delete(
                free_ids, local_only, delete_creating_tasks))

    def global_gc(self):
        with nogil:
            self.core_worker.get().TriggerGlobalGC()

    def set_object_store_client_options(self, client_name,
                                        int64_t limit_bytes):
        try:
            logger.debug("Setting plasma memory limit to {} for {}".format(
                limit_bytes, client_name))
            check_status(self.core_worker.get().SetClientOptions(
                client_name.encode("ascii"), limit_bytes))
        except RayError as e:
            self.dump_object_store_memory_usage()
            raise memory_monitor.RayOutOfMemoryError(
                "Failed to set object_store_memory={} for {}. The "
                "plasma store may have insufficient memory remaining "
                "to satisfy this limit (30% of object store memory is "
                "permanently reserved for shared usage). The current "
                "object store memory status is:\n\n{}".format(
                    limit_bytes, client_name, e))

    def dump_object_store_memory_usage(self):
        message = self.core_worker.get().MemoryUsageString()
        logger.warning("Local object store memory usage:\n{}\n".format(
            message.decode("utf-8")))

    def submit_task(self,
                    Language language,
                    FunctionDescriptor function_descriptor,
                    args,
                    int num_return_vals,
                    resources,
                    int max_retries):
        cdef:
            unordered_map[c_string, double] c_resources
            CTaskOptions task_options
            CRayFunction ray_function
            c_vector[CTaskArg] args_vector
            c_vector[CObjectID] return_ids

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            task_options = CTaskOptions(
                num_return_vals, True, c_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, args, &args_vector)

            with nogil:
                check_status(self.core_worker.get().SubmitTask(
                    ray_function, args_vector, task_options, &return_ids,
                    max_retries))

            return VectorToObjectIDs(return_ids)

    def create_actor(self,
                     Language language,
                     FunctionDescriptor function_descriptor,
                     args,
                     uint64_t max_reconstructions,
                     resources,
                     placement_resources,
                     int32_t max_concurrency,
                     c_bool is_detached,
                     c_bool is_asyncio,
                     c_string extension_data):
        cdef:
            CRayFunction ray_function
            c_vector[CTaskArg] args_vector
            c_vector[c_string] dynamic_worker_options
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, double] c_placement_resources
            CActorID c_actor_id

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, args, &args_vector)

            with nogil:
                check_status(self.core_worker.get().CreateActor(
                    ray_function, args_vector,
                    CActorCreationOptions(
                        max_reconstructions, True, max_concurrency,
                        c_resources, c_placement_resources,
                        dynamic_worker_options, is_detached, is_asyncio),
                    extension_data,
                    &c_actor_id))

            return ActorID(c_actor_id.Binary())

    def submit_actor_task(self,
                          Language language,
                          ActorID actor_id,
                          FunctionDescriptor function_descriptor,
                          args,
                          int num_return_vals,
                          double num_method_cpus):

        cdef:
            CActorID c_actor_id = actor_id.native()
            unordered_map[c_string, double] c_resources
            CTaskOptions task_options
            CRayFunction ray_function
            c_vector[CTaskArg] args_vector
            c_vector[CObjectID] return_ids

        with self.profile_event(b"submit_task"):
            if num_method_cpus > 0:
                c_resources[b"CPU"] = num_method_cpus
            task_options = CTaskOptions(num_return_vals, False, c_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args(self, args, &args_vector)

            with nogil:
                check_status(self.core_worker.get().SubmitActorTask(
                      c_actor_id,
                      ray_function,
                      args_vector, task_options, &return_ids))

            return VectorToObjectIDs(return_ids)

    def kill_actor(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()

        with nogil:
            check_status(self.core_worker.get().KillActor(
                  c_actor_id))

    def resource_ids(self):
        cdef:
            ResourceMappingType resource_mapping = (
                self.core_worker.get().GetResourceIDs())
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
        return ProfileEvent.make(
            self.core_worker.get().CreateProfileEvent(event_type),
            extra_data)

    def deserialize_and_register_actor_handle(self, const c_string &bytes):
        cdef CActorHandle* c_actor_handle
        worker = ray.worker.get_global_worker()
        worker.check_connected()
        manager = worker.function_actor_manager
        c_actor_id = self.core_worker.get().DeserializeAndRegisterActorHandle(
            bytes)
        check_status(self.core_worker.get().GetActorHandle(
            c_actor_id, &c_actor_handle))
        actor_id = ActorID(c_actor_id.Binary())
        job_id = JobID(c_actor_handle.CreationJobID().Binary())
        language = Language.from_native(c_actor_handle.ActorLanguage())
        actor_creation_function_descriptor = \
            CFunctionDescriptorToPython(
                c_actor_handle.ActorCreationTaskFunctionDescriptor())
        if language == Language.PYTHON:
            assert isinstance(actor_creation_function_descriptor,
                              PythonFunctionDescriptor)
            # Load actor_method_cpu from actor handle's extension data.
            extension_data = <str>c_actor_handle.ExtensionData()
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
                                         method_meta.num_return_vals,
                                         actor_method_cpu,
                                         actor_creation_function_descriptor,
                                         worker.current_session_and_job)
        else:
            return ray.actor.ActorHandle(language, actor_id,
                                         {},  # method decorators
                                         {},  # method signatures
                                         {},  # method num_return_vals
                                         0,  # actor method cpu
                                         actor_creation_function_descriptor,
                                         worker.current_session_and_job)

    def serialize_actor_handle(self, actor_handle):
        assert isinstance(actor_handle, ray.actor.ActorHandle)
        cdef:
            ActorID actor_id = actor_handle._ray_actor_id
            c_string output
        check_status(self.core_worker.get().SerializeActorHandle(
            actor_id.native(), &output))
        return output

    def add_object_id_reference(self, ObjectID object_id):
        # Note: faster to not release GIL for short-running op.
        self.core_worker.get().AddLocalReference(object_id.native())

    def remove_object_id_reference(self, ObjectID object_id):
        # Note: faster to not release GIL for short-running op.
        self.core_worker.get().RemoveLocalReference(object_id.native())

    def serialize_and_promote_object_id(self, ObjectID object_id):
        cdef:
            CObjectID c_object_id = object_id.native()
            CTaskID c_owner_id = CTaskID.Nil()
            CAddress c_owner_address = CAddress()
        self.core_worker.get().PromoteToPlasmaAndGetOwnershipInfo(
                c_object_id, &c_owner_id, &c_owner_address)
        return (object_id,
                TaskID(c_owner_id.Binary()),
                c_owner_address.SerializeAsString())

    def deserialize_and_register_object_id(
            self, const c_string &object_id_binary, ObjectID outer_object_id,
            const c_string &owner_id_binary,
            const c_string &serialized_owner_address):
        cdef:
            CObjectID c_object_id = CObjectID.FromBinary(object_id_binary)
            CObjectID c_outer_object_id = outer_object_id.native()
            CTaskID c_owner_id = CTaskID.FromBinary(owner_id_binary)
            CAddress c_owner_address = CAddress()
        c_owner_address.ParseFromString(serialized_owner_address)
        self.core_worker.get().RegisterOwnershipInfoAndResolveFuture(
                c_object_id,
                c_outer_object_id,
                c_owner_id,
                c_owner_address)

    # TODO: handle noreturn better
    cdef store_task_outputs(
            self, worker, outputs, const c_vector[CObjectID] return_ids,
            c_vector[shared_ptr[CRayObject]] *returns):
        cdef:
            c_vector[size_t] data_sizes
            c_vector[shared_ptr[CBuffer]] metadatas
            c_vector[c_vector[CObjectID]] contained_ids

        if return_ids.size() == 0:
            return

        serialized_objects = []
        for i in range(len(outputs)):
            return_id, output = return_ids[i], outputs[i]
            if isinstance(output, ray.actor.ActorHandle):
                raise Exception("Returning an actor handle from a remote "
                                "function is not allowed).")
            elif output is NoReturn:
                serialized_objects.append(output)
                data_sizes.push_back(0)
                metadatas.push_back(string_to_buffer(b''))
            else:
                context = worker.get_serialization_context()
                serialized_object = context.serialize(output)
                data_sizes.push_back(serialized_object.total_bytes)
                metadatas.push_back(
                    string_to_buffer(serialized_object.metadata))
                serialized_objects.append(serialized_object)
                contained_ids.push_back(
                    ObjectIDsToVector(serialized_object.contained_object_ids))

        with nogil:
            check_status(self.core_worker.get().AllocateReturnObjects(
                return_ids, data_sizes, metadatas, contained_ids, returns))

        for i, serialized_object in enumerate(serialized_objects):
            # A nullptr is returned if the object already exists.
            if returns[0][i].get() == NULL:
                continue
            if serialized_object is NoReturn:
                returns[0][i].reset()
            else:
                write_serialized_object(
                    serialized_object, returns[0][i].get().GetData())

    def create_or_get_event_loop(self):
        if self.async_event_loop is None:
            self.async_event_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.async_event_loop)
            # Initialize the async plasma connection.
            # Delayed import due to async_api depends on _raylet.
            from ray.experimental.async_api import _async_init
            self.async_event_loop.run_until_complete(_async_init())

            # Create and attach the monitor object
            monitor_state = AsyncMonitorState(self.async_event_loop)
            self.async_event_loop.monitor_state = monitor_state

        if self.async_thread is None:
            self.async_thread = threading.Thread(
                target=lambda: self.async_event_loop.run_forever()
            )
            # Making the thread a daemon causes it to exit
            # when the main thread exits.
            self.async_thread.daemon = True
            self.async_thread.start()

        return self.async_event_loop

    def destory_event_loop_if_exists(self):
        if self.async_event_loop is not None:
            self.async_event_loop.stop()
        if self.async_thread is not None:
            self.async_thread.join()

    def current_actor_is_asyncio(self):
        return self.core_worker.get().GetWorkerContext().CurrentActorIsAsync()

    def get_all_reference_counts(self):
        cdef:
            unordered_map[CObjectID, pair[size_t, size_t]] c_ref_counts
            unordered_map[CObjectID, pair[size_t, size_t]].iterator it

        c_ref_counts = self.core_worker.get().GetAllReferenceCounts()
        it = c_ref_counts.begin()

        ref_counts = {}
        while it != c_ref_counts.end():
            object_id = dereference(it).first.Hex()
            ref_counts[object_id] = {
                "local": dereference(it).second.first,
                "submitted": dereference(it).second.second}
            postincrement(it)

        return ref_counts

    def in_memory_store_get_async(self, ObjectID object_id, future):
        self.core_worker.get().GetAsync(
            object_id.native(),
            async_set_result_callback,
            async_retry_with_plasma_callback,
            <void*>future)

    def push_error(self, JobID job_id, error_type, error_message,
                   double timestamp):
        check_status(self.core_worker.get().PushError(
            job_id.native(), error_type.encode("ascii"),
            error_message.encode("ascii"), timestamp))

    def prepare_actor_checkpoint(self, ActorID actor_id):
        cdef:
            CActorCheckpointID checkpoint_id
            CActorID c_actor_id = actor_id.native()

        # PrepareActorCheckpoint will wait for raylet's reply, release
        # the GIL so other Python threads can run.
        with nogil:
            check_status(self.core_worker.get().PrepareActorCheckpoint(
                c_actor_id, &checkpoint_id))
        return ActorCheckpointID(checkpoint_id.Binary())

    def notify_actor_resumed_from_checkpoint(self, ActorID actor_id,
                                             ActorCheckpointID checkpoint_id):
        check_status(self.core_worker.get().NotifyActorResumedFromCheckpoint(
            actor_id.native(), checkpoint_id.native()))

    def set_resource(self, basestring resource_name,
                     double capacity, ClientID client_id):
        self.core_worker.get().SetResource(
            resource_name.encode("ascii"), capacity,
            CClientID.FromBinary(client_id.binary()))

cdef void async_set_result_callback(shared_ptr[CRayObject] obj,
                                    CObjectID object_id,
                                    void *future) with gil:
    cdef:
        c_vector[shared_ptr[CRayObject]] objects_to_deserialize

    py_future = <object>(future)
    loop = py_future._loop

    # Object is retrieved from in memory store.
    # Here we go through the code path used to deserialize objects.
    objects_to_deserialize.push_back(obj)
    data_metadata_pairs = RayObjectsToDataMetadataPairs(
        objects_to_deserialize)
    ids_to_deserialize = [ObjectID(object_id.Binary())]
    objects = ray.worker.global_worker.deserialize_objects(
        data_metadata_pairs, ids_to_deserialize)
    loop.call_soon_threadsafe(lambda: py_future.set_result(
        AsyncGetResponse(
            plasma_fallback_id=None, result=objects[0])))

cdef void async_retry_with_plasma_callback(shared_ptr[CRayObject] obj,
                                           CObjectID object_id,
                                           void *future) with gil:
    py_future = <object>(future)
    loop = py_future._loop
    loop.call_soon_threadsafe(lambda: py_future.set_result(
                AsyncGetResponse(
                    plasma_fallback_id=ObjectID(object_id.Binary()),
                    result=None)))
