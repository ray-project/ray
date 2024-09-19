# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: c_string_encoding = default

from cpython.exc cimport PyErr_CheckSignals

import asyncio
from functools import wraps
import gc
import inspect
import logging
import msgpack
import io
import os
import pickle
import random
import signal
import sys
import threading
import time
import traceback
import _thread
import typing
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Optional,
    Tuple,
    Union,
)

import contextvars
import concurrent
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import Future as ConcurrentFuture

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
    CWorkerExitType,
    CRayObject,
    CRayStatus,
    CActorTableData,
    CErrorTableData,
    CGcsClientOptions,
    CGcsNodeInfo,
    CJobTableData,
    CLogBatch,
    CTaskArg,
    CTaskArgByReference,
    CTaskArgByValue,
    CTaskType,
    CPlacementStrategy,
    CPythonFunction,
    CSchedulingStrategy,
    CPlacementGroupSchedulingStrategy,
    CNodeAffinitySchedulingStrategy,
    CNodeLabelSchedulingStrategy,
    CLabelMatchExpressions,
    CLabelMatchExpression,
    CLabelIn,
    CLabelNotIn,
    CLabelExists,
    CLabelDoesNotExist,
    CLabelOperator,
    CRayFunction,
    CWorkerType,
    CJobConfig,
    CConcurrencyGroup,
    CGrpcStatusCode,
    CLineageReconstructionTask,
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
    CChannelType,
    RAY_ERROR_INFO_CHANNEL,
    RAY_LOG_CHANNEL,
    GCS_ACTOR_CHANNEL,
    PythonGetLogBatchLines,
    WORKER_EXIT_TYPE_USER_ERROR,
    WORKER_EXIT_TYPE_SYSTEM_ERROR,
    WORKER_EXIT_TYPE_INTENTIONAL_SYSTEM_ERROR,
    kResourceUnitScaling,
    kImplicitResourcePrefix,
    kWorkerSetupHookKeyName,
    PythonGetNodeLabels,
    PythonGetResourcesTotal,
)
from ray.includes.unique_ids cimport (
    CActorID,
    CClusterID,
    CNodeID,
    CObjectID,
    CPlacementGroupID,
    ObjectIDIndexType,
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
    CGeneratorBackpressureWaiter,
    CReaderRefInfo,
)

from ray.includes.ray_config cimport RayConfig
from ray.includes.global_state_accessor cimport CGlobalStateAccessor
from ray.includes.global_state_accessor cimport (
    RedisDelKeyPrefixSync,
    RedisGetKeySync
)
from ray.includes.optional cimport (
    optional, nullopt
)

import ray
from ray.exceptions import (
    RayActorError,
    ActorDiedError,
    RayError,
    RaySystemError,
    RayTaskError,
    ObjectStoreFullError,
    OutOfDiskError,
    GetTimeoutError,
    TaskCancelledError,
    AsyncioActorExit,
    PendingCallsLimitExceeded,
    RpcError,
    ObjectRefStreamEndOfStreamError,
    RayChannelError,
    RayChannelTimeoutError,
)
from ray._private import external_storage
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    NodeAffinitySchedulingStrategy,
    NodeLabelSchedulingStrategy,
    In,
    NotIn,
    Exists,
    DoesNotExist,
)
import ray._private.ray_constants as ray_constants
import ray.cloudpickle as ray_pickle
from ray.core.generated.common_pb2 import ActorDiedErrorContext
from ray.core.generated.gcs_pb2 import JobTableData, GcsNodeInfo, ActorTableData
from ray.core.generated.gcs_service_pb2 import GetAllResourceUsageReply
from ray._private.async_compat import (
    sync_to_async,
    get_new_event_loop,
    is_async_func,
    has_async_methods,
)
from ray._private.client_mode_hook import disable_client_hook
import ray.core.generated.common_pb2 as common_pb2
import ray._private.memory_monitor as memory_monitor
import ray._private.profiling as profiling
from ray._private.utils import decode, DeferSigint
from ray.util.annotations import PublicAPI

cimport cpython

include "includes/object_ref.pxi"
include "includes/unique_ids.pxi"
include "includes/ray_config.pxi"
include "includes/function_descriptor.pxi"
include "includes/buffer.pxi"
include "includes/common.pxi"
include "includes/gcs_client.pxi"
include "includes/serialization.pxi"
include "includes/libcoreworker.pxi"
include "includes/global_state_accessor.pxi"
include "includes/metric.pxi"

# Expose GCC & Clang macro to report
# whether C++ optimizations were enabled during compilation.
OPTIMIZED = __OPTIMIZE__

GRPC_STATUS_CODE_UNAVAILABLE = CGrpcStatusCode.UNAVAILABLE
GRPC_STATUS_CODE_UNKNOWN = CGrpcStatusCode.UNKNOWN
GRPC_STATUS_CODE_DEADLINE_EXCEEDED = CGrpcStatusCode.DEADLINE_EXCEEDED
GRPC_STATUS_CODE_RESOURCE_EXHAUSTED = CGrpcStatusCode.RESOURCE_EXHAUSTED
GRPC_STATUS_CODE_UNIMPLEMENTED = CGrpcStatusCode.UNIMPLEMENTED

logger = logging.getLogger(__name__)

# The currently executing task, if any. These are used to synchronize task
# interruption for ray.cancel.
current_task_id = None
current_task_id_lock = threading.Lock()

job_config_initialized = False
job_config_initialization_lock = threading.Lock()

# It is used to indicate optional::nullopt for
# AllocateDynamicReturnId.
cdef optional[ObjectIDIndexType] NULL_PUT_INDEX = nullopt
# This argument is used to obtain the correct task id inside
# an asyncio task. It is because task_id can be obtained
# by the worker_context_ API, which is per thread, not per
# asyncio task. TODO(sang): We should properly fix it.
# Note that the context var is recommended to be defined
# in the top module.
# https://docs.python.org/3/library/contextvars.html#contextvars.ContextVar
# It is thread-safe.
async_task_id = contextvars.ContextVar('async_task_id', default=None)


class DynamicObjectRefGenerator:
    def __init__(self, refs):
        # TODO(swang): As an optimization, can also store the generator
        # ObjectID so that we don't need to keep individual ref counts for the
        # inner ObjectRefs.
        self._refs = refs

    def __iter__(self):
        while self._refs:
            yield self._refs.pop(0)

    def __len__(self):
        return len(self._refs)


class ObjectRefGenerator:
    """A generator to obtain object references
    from a task in a streaming manner.

    The class is compatible with generator and
    async generator interface.

    The class is not thread-safe.

    Do not initialize the class and create an instance directly.
    The instance should be created by `.remote`.

    >>> gen = generator_task.remote()
    >>> next(gen)
    >>> await gen.__anext__()
    """
    def __init__(self, generator_ref: ObjectRef, worker: "Worker"):
        # The reference to a generator task.
        self._generator_ref = generator_ref
        # The exception raised from a generator task.
        self._generator_task_exception = None
        # Ray's worker class. ray._private.worker.global_worker
        self.worker = worker
        self.worker.check_connected()
        assert hasattr(worker, "core_worker")

    """
    Public APIs
    """

    def __iter__(self) -> "ObjectRefGenerator":
        return self

    def __next__(self) -> ObjectRef:
        """Waits until a next ref is available and returns the object ref.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).
        """
        return self._next_sync()

    def send(self, value):
        raise NotImplementedError("`gen.send` is not supported.")

    def throw(self, value):
        raise NotImplementedError("`gen.throw` is not supported.")

    def close(self):
        raise NotImplementedError("`gen.close` is not supported.")

    def __aiter__(self) -> "ObjectRefGenerator":
        return self

    async def __anext__(self):
        return await self._next_async()

    async def asend(self, value):
        raise NotImplementedError("`gen.asend` is not supported.")

    async def athrow(self, value):
        raise NotImplementedError("`gen.athrow` is not supported.")

    async def aclose(self):
        raise NotImplementedError("`gen.aclose` is not supported.")

    def completed(self) -> ObjectRef:
        """Returns an object ref that is ready when
        a generator task completes.

        If the task is failed unexpectedly (e.g., worker failure),
        the `ray.get(gen.completed())` raises an exception.

        The function returns immediately.

        >>> ray.get(gen.completed())
        """
        return self._generator_ref

    def next_ready(self) -> bool:
        """If True, it means the output of next(gen) is ready and
        ray.get(next(gen)) returns immediately. False otherwise.

        It returns False when next(gen) raises a StopIteration
        (this condition should be checked using is_finished).

        The function returns immediately.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker

        if self.is_finished():
            return False

        expected_ref, is_ready = core_worker.peek_object_ref_stream(
            self._generator_ref)

        if is_ready:
            return True

        ready, _ = ray.wait(
            [expected_ref], timeout=0, fetch_local=False)
        return len(ready) > 0

    def is_finished(self) -> bool:
        """If True, it means the generator is finished
        and all output is taken. False otherwise.

        When True, if next(gen) is called, it will raise StopIteration
        or StopAsyncIteration

        The function returns immediately.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker

        finished = core_worker.is_object_ref_stream_finished(
            self._generator_ref)

        if finished:
            if self._generator_task_exception:
                return True
            else:
                # We should try ray.get on a generator ref.
                # If it raises an exception and
                # _generator_task_exception is not set,
                # this means the last ref is not taken yet.
                try:
                    ray.get(self._generator_ref)
                except Exception:
                    # The exception from _generator_ref
                    # hasn't been taken yet.
                    return False
                else:
                    return True

    """
    Private APIs
    """

    def _get_next_ref(self) -> ObjectRef:
        """Return the next reference from a generator.

        Note that the ObjectID generated from a generator
        is always deterministic.
        """
        self.worker.check_connected()
        core_worker = self.worker.core_worker
        return core_worker.peek_object_ref_stream(
            self._generator_ref)[0]

    def _next_sync(
        self,
        timeout_s: Optional[float] = None
    ) -> ObjectRef:
        """Waits for timeout_s and returns the object ref if available.

        If an object is not available within the given timeout, it
        returns a nil object reference.

        If -1 timeout is provided, it means it waits infinitely.

        Waiting is implemented as busy waiting.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).

        Args:
            timeout_s: If the next object is not ready within
                this timeout, it returns the nil object ref.
        """
        core_worker = self.worker.core_worker

        # Wait for the next ObjectRef to become ready.
        expected_ref, is_ready = core_worker.peek_object_ref_stream(
            self._generator_ref)

        if not is_ready:
            _, unready = ray.wait(
                [expected_ref], timeout=timeout_s, fetch_local=False)
            if len(unready) > 0:
                return ObjectRef.nil()

        try:
            ref = core_worker.try_read_next_object_ref_stream(
                self._generator_ref)
            assert not ref.is_nil()
        except ObjectRefStreamEndOfStreamError:
            if self._generator_task_exception:
                # Exception has been returned.
                raise StopIteration

            try:
                # The generator ref contains an exception
                # if there's any failure. It contains nothing otherwise.
                # In that case, it should raise StopIteration.
                ray.get(self._generator_ref)
            except Exception as e:
                self._generator_task_exception = e
                return self._generator_ref
            else:
                # The task finished without an exception.
                raise StopIteration
        return ref

    async def _suppress_exceptions(self, ref: ObjectRef) -> None:
        # Wrap a streamed ref to avoid asyncio warnings about not retrieving
        # the exception when we are just waiting for the ref to become ready.
        # The exception will get returned (or warned) to the user once they
        # actually await the ref.
        try:
            await ref
        except Exception:
            pass

    async def _next_async(
            self,
            timeout_s: Optional[float] = None
    ):
        """Same API as _next_sync, but it is for async context."""
        core_worker = self.worker.core_worker
        ref, is_ready = core_worker.peek_object_ref_stream(
            self._generator_ref)

        if not is_ready:
            # TODO(swang): Avoid fetching the value.
            ready, unready = await asyncio.wait(
                [asyncio.create_task(self._suppress_exceptions(ref))],
                timeout=timeout_s
            )
            if len(unready) > 0:
                return ObjectRef.nil()

        try:
            ref = core_worker.try_read_next_object_ref_stream(
                self._generator_ref)
            assert not ref.is_nil()
        except ObjectRefStreamEndOfStreamError:
            if self._generator_task_exception:
                # Exception has been returned. raise StopIteration.
                raise StopAsyncIteration

            try:
                # The generator ref contains an exception
                # if there's any failure. It contains nothing otherwise.
                # In that case, it should raise StopIteration.
                await self._generator_ref
            except Exception as e:
                self._generator_task_exception = e
                return self._generator_ref
            else:
                # meaning the task succeed without failure raise StopIteration.
                raise StopAsyncIteration

        return ref

    def __del__(self):
        if hasattr(self.worker, "core_worker"):
            # The stream is created when a task is first submitted.
            # NOTE: This can be called multiple times
            # because python doesn't guarantee __del__ is called
            # only once.
            self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)

    def __getstate__(self):
        raise TypeError(
            "You cannot return or pass a generator to other task. "
            "Serializing a ObjectRefGenerator is not allowed.")


# For backward compatibility.
StreamingObjectRefGenerator = ObjectRefGenerator

cdef c_bool is_plasma_object(shared_ptr[CRayObject] obj):
    """Return True if the given object is a plasma object."""
    assert obj.get() != NULL
    if (obj.get().GetData().get() != NULL
            and obj.get().GetData().get().IsPlasmaBuffer()):
        return True
    return False


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


cdef VectorToObjectRefs(const c_vector[CObjectReference] &object_refs,
                        skip_adding_local_ref):
    result = []
    for i in range(object_refs.size()):
        result.append(ObjectRef(
            object_refs[i].object_id(),
            object_refs[i].owner_address().SerializeAsString(),
            object_refs[i].call_site(),
            skip_adding_local_ref=skip_adding_local_ref))
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


def _get_actor_serialized_owner_address_or_none(actor_table_data: bytes):
    cdef:
        CActorTableData data

    data.ParseFromString(actor_table_data)

    if data.address().worker_id() == b"":
        return None
    else:
        return data.address().SerializeAsString()


def compute_task_id(ObjectRef object_ref):
    return TaskID(object_ref.native().TaskId().Binary())


cdef increase_recursion_limit():
    """Double the recusion limit if current depth is close to the limit"""
    cdef:
        CPyThreadState * s = <CPyThreadState *> PyThreadState_Get()
        int current_limit = Py_GetRecursionLimit()
        int new_limit = current_limit * 2
        cdef extern from *:
            """
#if PY_VERSION_HEX >= 0x30C0000
    #define CURRENT_DEPTH(x) ((x)->py_recursion_limit - (x)->py_recursion_remaining)
#elif PY_VERSION_HEX >= 0x30B00A4
    #define CURRENT_DEPTH(x)  ((x)->recursion_limit - (x)->recursion_remaining)
#else
    #define CURRENT_DEPTH(x)  ((x)->recursion_depth)
#endif
            """
            int CURRENT_DEPTH(CPyThreadState *x)

        int current_depth = CURRENT_DEPTH(s)
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
            The size of data + metadata in bytes. Can be None if it's -1 in the source.
        - did_spill:
            Whether or not this object was spilled.
    """
    object_size = c_object_location.GetObjectSize()
    if object_size <= 0:
        object_size = None
    did_spill = c_object_location.GetDidSpill()

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
        "did_spill": did_spill,
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
        list unit_resources

    if resource_dict is None:
        raise ValueError("Must provide resource map.")

    for key, value in resource_dict.items():
        if not (isinstance(value, int) or isinstance(value, float)):
            raise ValueError("Resource quantities may only be ints or floats.")
        if value < 0:
            raise ValueError("Resource quantities may not be negative.")
        if value > 0:
            unit_resources = (
                f"{RayConfig.instance().predefined_unit_instance_resources()\
                .decode('utf-8')},"
                f"{RayConfig.instance().custom_unit_instance_resources()\
                .decode('utf-8')}"
            ).split(",")

            if (value >= 1 and isinstance(value, float)
                    and not value.is_integer() and str(key) in unit_resources):
                raise ValueError(
                    f"{key} resource quantities >1 must",
                    f" be whole numbers. The specified quantity {value} is invalid.")
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


def raise_sys_exit_with_custom_error_message(
        ray_terminate_msg: str,
        exit_code: int = 0) -> None:
    """It is equivalent to sys.exit, but it can contain
    a custom message. Custom message is reported to
    raylet and is accessible via GCS (from `ray get workers`).

    Note that sys.exit == raise SystemExit. I.e., this API
    simply raises SystemExit with a custom error message accessible
    via `e.ray_terminate_msg`.

    Args:
        ray_terminate_msg: The error message to propagate to GCS.
        exit_code: The exit code. If it is not 0, it is considered
            as a system error.
    """
    # Raising SystemExit(0) is equivalent to
    # sys.exit(0).
    # https://docs.python.org/3/library/exceptions.html#SystemExit
    e = SystemExit(exit_code)
    e.ray_terminate_msg = ray_terminate_msg
    raise e


cdef prepare_args_and_increment_put_refs(
        CoreWorker core_worker,
        Language language, args,
        c_vector[unique_ptr[CTaskArg]] *args_vector, function_descriptor,
        c_vector[CObjectID] *incremented_put_arg_ids):
    try:
        prepare_args_internal(core_worker, language, args, args_vector,
                              function_descriptor, incremented_put_arg_ids)
    except Exception as e:
        # An error occurred during arg serialization. We must remove the
        # initial local ref for all args that were successfully put into the
        # local plasma store. These objects will then get released.
        for put_arg_id in dereference(incremented_put_arg_ids):
            CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                put_arg_id)
        raise e

cdef prepare_args_internal(
        CoreWorker core_worker,
        Language language, args,
        c_vector[unique_ptr[CTaskArg]] *args_vector, function_descriptor,
        c_vector[CObjectID] *incremented_put_arg_ids):
    cdef:
        size_t size
        int64_t put_threshold
        int64_t rpc_inline_threshold
        int64_t total_inlined
        shared_ptr[CBuffer] arg_data
        c_vector[CObjectID] inlined_ids
        c_string put_arg_call_site
        c_vector[CObjectReference] inlined_refs
        CAddress c_owner_address
        CRayStatus op_status

    worker = ray._private.worker.global_worker
    put_threshold = RayConfig.instance().max_direct_call_object_size()
    total_inlined = 0
    rpc_inline_threshold = RayConfig.instance().task_rpc_inlined_bytes_limit()
    for arg in args:
        from ray.experimental.compiled_dag_ref import CompiledDAGRef
        if isinstance(arg, CompiledDAGRef):
            raise TypeError("CompiledDAGRef cannot be used as Ray task/actor argument.")
        if isinstance(arg, ObjectRef):
            c_arg = (<ObjectRef>arg).native()
            op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                    c_arg, &c_owner_address)
            check_status(op_status)
            args_vector.push_back(
                unique_ptr[CTaskArg](new CTaskArgByReference(
                    c_arg,
                    c_owner_address,
                    arg.call_site())))

        else:
            try:
                serialized_arg = worker.get_serialization_context(
                ).serialize(arg)
            except TypeError as e:
                sio = io.StringIO()
                ray.util.inspect_serializability(arg, print_file=sio)
                msg = (
                    "Could not serialize the argument "
                    f"{repr(arg)} for a task or actor "
                    f"{function_descriptor.repr}:\n"
                    f"{sio.getvalue()}")
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
                put_id = CObjectID.FromBinary(
                        core_worker.put_serialized_object_and_increment_local_ref(
                            serialized_arg, inline_small_object=False))
                args_vector.push_back(unique_ptr[CTaskArg](
                    new CTaskArgByReference(
                            put_id,
                            CCoreWorkerProcess.GetCoreWorker().GetRpcAddress(),
                            put_arg_call_site
                        )))
                incremented_put_arg_ids.push_back(put_id)


cdef raise_if_dependency_failed(arg):
    """This method is used to improve the readability of backtrace.

    With this method, the backtrace will always contain
    raise_if_dependency_failed when the task is failed with dependency
    failures.
    """
    if isinstance(arg, RayError):
        raise arg


def serialize_retry_exception_allowlist(retry_exception_allowlist, function_descriptor):
    try:
        return ray_pickle.dumps(retry_exception_allowlist)
    except TypeError as e:
        msg = (
            "Could not serialize the retry exception allowlist"
            f"{retry_exception_allowlist} for task {function_descriptor.repr}. "
            "See "
            "https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting " # noqa
            "for more information.")
        raise TypeError(msg) from e


cdef c_bool determine_if_retryable(
    c_bool should_retry_exceptions,
    Exception e,
    const c_string serialized_retry_exception_allowlist,
    FunctionDescriptor function_descriptor,
):
    """Determine if the provided exception is retryable, according to the
    (possibly null) serialized exception allowlist.

    If the serialized exception allowlist is an empty string or is None once
    deserialized, the exception is considered retryable and we return True.

    This method can raise an exception if:
        - Deserialization of exception allowlist fails (TypeError)
        - Exception allowlist is not None and not a tuple (AssertionError)
    """
    if not should_retry_exceptions:
        return False
    if len(serialized_retry_exception_allowlist) == 0:
        # No exception allowlist specified, default to all retryable.
        return True

    # Deserialize exception allowlist and check that the exception is in the allowlist.
    try:
        exception_allowlist = ray_pickle.loads(
            serialized_retry_exception_allowlist,
        )
    except TypeError as inner_e:
        # Exception allowlist deserialization failed.
        msg = (
            "Could not deserialize the retry exception allowlist "
            f"for task {function_descriptor.repr}. "
            "Check "
            "https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting " # noqa
            "for more information.")
        raise TypeError(msg) from inner_e

    if exception_allowlist is None:
        # No exception allowlist specified, default to all retryable.
        return True

    # Python API should have converted the list of exceptions to a tuple.
    assert isinstance(exception_allowlist, tuple)

    # For exceptions raised when running UDFs in Ray Data, we need to unwrap the special
    # exception type thrown by Ray Data in order to get the underlying exception.
    if isinstance(e, ray.exceptions.UserCodeException):
        e = e.__cause__
    # Check that e is in allowlist.
    return isinstance(e, exception_allowlist)

cdef store_task_errors(
        worker,
        exc,
        task_exception,
        actor,
        actor_id,
        function_name,
        CTaskType task_type,
        proctitle,
        const CAddress &caller_address,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
        c_string* application_error):
    cdef:
        CoreWorker core_worker = worker.core_worker

    # If the debugger is enabled, drop into the remote pdb here.
    if ray.util.pdb._is_ray_debugger_enabled():
        ray.util.pdb._post_mortem()

    backtrace = ray._private.utils.format_error_message(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        task_exception=task_exception)

    # Generate the actor repr from the actor class.
    actor_repr = repr(actor) if actor else None

    if actor_id is None or actor_id.is_nil():
        actor_id = None
    else:
        actor_id = actor_id.hex()

    if isinstance(exc, RayTaskError):
        # Avoid recursive nesting of RayTaskError.
        failure_object = RayTaskError(function_name, backtrace,
                                      exc.cause, proctitle=proctitle,
                                      actor_repr=actor_repr,
                                      actor_id=actor_id)
    else:
        failure_object = RayTaskError(function_name, backtrace,
                                      exc, proctitle=proctitle,
                                      actor_repr=actor_repr,
                                      actor_id=actor_id)

    # Pass the failure object back to the CoreWorker.
    # We also cap the size of the error message to the last
    # MAX_APPLICATION_ERROR_LEN characters of the error message.
    if application_error != NULL:
        application_error[0] = str(failure_object)[
            -ray_constants.MAX_APPLICATION_ERROR_LEN:]

    errors = []
    for _ in range(returns[0].size()):
        errors.append(failure_object)

    num_errors_stored = core_worker.store_task_outputs(
        worker, errors,
        caller_address,
        returns)

    if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
        raise ActorDiedError.from_task_error(failure_object)
    return num_errors_stored


cdef class StreamingGeneratorExecutionContext:
    """The context to execute streaming generator function.

    Make sure you always call `initialize` API before
    accessing any fields.

    Args:
        generator: The generator to run.
        generator_id: The object ref id of the generator task.
        task_type: The type of the task. E.g., actor task, normal task.
        caller_address: The address of the caller. By our protocol,
            the caller of the streaming generator task is always
            the owner, so we can also call it "owner address".
        task_id: The task ID of the generator task.
        serialized_retry_exception_allowlist: A list of
            exceptions that are allowed to retry this generator task.
        function_name: The name of the generator function. Used for
            writing an error message.
        function_descriptor: The function descriptor of
            the generator function. Used for writing an error message.
        title: The process title of the generator task. Used for
            writing an error message.
        actor: The instance of the actor created in this worker.
            It is used to write an error message.
        actor_id: The ID of the actor. It is used to write an error message.
        return_size: The number of static returns.
        attempt_number: The number of times the current task is retried.
            0 means it is the first execution of the task.
        should_retry_exceptions: True if the task should be
            retried upon exceptions.
        streaming_generator_returns(out): A list of a pair of (ObjectID,
        is_plasma_object) that are generated by a streaming generator
        task.
        is_retryable_error(out): It is set to True if the generator
            raises an exception, and the error is retryable.
        application_error(out): It is set if the generator raises an
            application error.
        generator_backpressure_num_objects: The backpressure threshold
            for streaming generator. The stremaing generator pauses if
            total number of unconsumed objects exceed this threshold.
    """

    cdef:
        # -- Arguments that are not passed--
        # Whether or not a generator is async
        object is_async
        # True if `initialize` API has been called. False otherwise.
        object _is_initialized
        # -- Arguments that are passed. See the docstring for details --
        object generator
        CObjectID generator_id
        CTaskType task_type
        CAddress caller_address
        TaskID task_id
        c_string serialized_retry_exception_allowlist
        object function_name
        object function_descriptor
        object title
        object actor
        object actor_id
        object name_of_concurrency_group_to_execute
        object return_size
        uint64_t attempt_number
        c_bool should_retry_exceptions
        c_vector[c_pair[CObjectID, c_bool]] *streaming_generator_returns
        c_bool *is_retryable_error
        c_string *application_error
        shared_ptr[CGeneratorBackpressureWaiter] waiter

    def initialize(self, generator: Union[Generator, AsyncGenerator]):
        # We couldn't make this a part of `make` method because
        # It looks like we cannot pass generator to cdef
        # function (`make`) in Cython.
        self.generator = generator
        self.is_async = inspect.isasyncgen(generator)
        self._is_initialized = True

    def is_initialized(self):
        return self._is_initialized

    @staticmethod
    cdef make(
        const CObjectID &generator_id,
        CTaskType task_type,
        const CAddress &caller_address,
        TaskID task_id,
        const c_string &serialized_retry_exception_allowlist,
        function_name: str,
        function_descriptor: FunctionDescriptor,
        title: str,
        actor: object,
        actor_id: ActorID,
        name_of_concurrency_group_to_execute: str,
        return_size: int,
        uint64_t attempt_number,
        c_bool should_retry_exceptions,
        c_vector[c_pair[CObjectID, c_bool]] *streaming_generator_returns,
        c_bool *is_retryable_error,
        c_string *application_error,
        int64_t generator_backpressure_num_objects,
    ):
        cdef StreamingGeneratorExecutionContext self = (
            StreamingGeneratorExecutionContext())
        self.function_name = function_name
        self.function_descriptor = function_descriptor
        self.title = title
        self.actor = actor
        self.actor_id = actor_id
        self.name_of_concurrency_group_to_execute = name_of_concurrency_group_to_execute
        self.return_size = return_size
        self._is_initialized = False
        self.generator_id = generator_id
        self.task_type = task_type
        self.caller_address = caller_address
        self.task_id = task_id
        self.serialized_retry_exception_allowlist = serialized_retry_exception_allowlist
        self.attempt_number = attempt_number
        self.streaming_generator_returns = streaming_generator_returns
        self.is_retryable_error = is_retryable_error
        self.application_error = application_error
        self.should_retry_exceptions = should_retry_exceptions

        self.waiter = make_shared[CGeneratorBackpressureWaiter](
            generator_backpressure_num_objects,
            check_signals
        )

        return self

cdef report_streaming_generator_output(
    StreamingGeneratorExecutionContext context,
    output: object,
    generator_index: int64_t,
    interrupt_signal_event: Optional[threading.Event],
):
    """Report a given generator output to a caller.

    Args:
        context: Streaming generator's execution context.
        output: The output yielded from a
            generator or raised as an exception.
        generator_index: index of the output element in the
            generated sequence
    """
    worker = ray._private.worker.global_worker

    cdef:
        # Ray Object created from an output.
        c_pair[CObjectID, shared_ptr[CRayObject]] return_obj

    # Report the intermediate result if there was no error.
    create_generator_return_obj(
        output,
        context.generator_id,
        worker,
        context.caller_address,
        context.task_id,
        context.return_size,
        generator_index,
        context.is_async,
        &return_obj)

    # Del output here so that we can GC the memory
    # usage asap.
    del output

    # NOTE: Once interrupting event is set by the caller, we can NOT access
    #       externally provided data-structures, and have to interrupt the execution
    if interrupt_signal_event is not None and interrupt_signal_event.is_set():
        return

    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](
            return_obj.first,
            is_plasma_object(return_obj.second)))

    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj,
            context.generator_id,
            context.caller_address,
            generator_index,
            context.attempt_number,
            context.waiter))


cdef report_streaming_generator_exception(
    StreamingGeneratorExecutionContext context,
    e: Exception,
    generator_index: int64_t,
    interrupt_signal_event: Optional[threading.Event],
):
    """Report a given generator exception to a caller.

    Args:
        context: Streaming generator's execution context.
        output_or_exception: The output yielded from a
            generator or raised as an exception.
        generator_index: index of the output element in the
            generated sequence
    """
    worker = ray._private.worker.global_worker

    cdef:
        # Ray Object created from an output.
        c_pair[CObjectID, shared_ptr[CRayObject]] return_obj

    create_generator_error_object(
        e,
        worker,
        context.task_type,
        context.caller_address,
        context.task_id,
        context.serialized_retry_exception_allowlist,
        context.function_name,
        context.function_descriptor,
        context.title,
        context.actor,
        context.actor_id,
        context.return_size,
        generator_index,
        context.is_async,
        context.should_retry_exceptions,
        &return_obj,
        context.is_retryable_error,
        context.application_error
    )

    # Del exception here so that we can GC the memory
    # usage asap.
    del e

    # NOTE: Once interrupting event is set by the caller, we can NOT access
    #       externally provided data-structures, and have to interrupt the execution
    if interrupt_signal_event is not None and interrupt_signal_event.is_set():
        return

    context.streaming_generator_returns[0].push_back(
        c_pair[CObjectID, c_bool](
            return_obj.first,
            is_plasma_object(return_obj.second)))

    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj,
            context.generator_id,
            context.caller_address,
            generator_index,
            context.attempt_number,
            context.waiter))

cdef execute_streaming_generator_sync(StreamingGeneratorExecutionContext context):
    """Execute a given generator and streaming-report the
        result to the given caller_address.

    The output from the generator will be stored to the in-memory
    or plasma object store. The generated return objects will be
    reported to the owner of the task as soon as they are generated.

    It means when this method is used, the result of each generator
    will be reported and available from the given "caller address"
    before the task is finished.

    Args:
        context: The context to execute streaming generator.
    """
    cdef:
        int64_t gen_index = 0
        CRayStatus return_status

    assert context.is_initialized()
    # Generator task should only have 1 return object ref,
    # which contains None or exceptions (if system error occurs).
    assert context.return_size == 1

    gen = context.generator

    try:
        for output in gen:
            report_streaming_generator_output(context, output, gen_index, None)
            gen_index += 1
    except Exception as e:
        report_streaming_generator_exception(context, e, gen_index, None)

    # The caller gets object values through the reports. If we finish the task
    # before sending the report is complete, then we may fail before the report
    # is sent to the caller. Then, the caller would never be able to ray.get
    # the yield'ed ObjectRef. Therefore, we must wait for all in-flight object
    # reports to complete before finishing the task.
    with nogil:
        return_status = context.waiter.get().WaitAllObjectsReported()
    check_status(return_status)


async def execute_streaming_generator_async(
        context: StreamingGeneratorExecutionContext):
    """Execute a given generator and report the
        result to the given caller_address in a streaming (ie as
        soon as become available) fashion.

    This method is same as `execute_streaming_generator_sync`,
    but it should be used inside an async event loop.

    NOTE: since this function runs inside an event loop thread,
    some of core worker APIs will be executed inside
    the event loop thread as well.

    E.g., core_worker.SealOwned can be called.

    At this time, if we access worker_context_ API from core worker,
    it can cause problems because worker_context_ is configured
    per thread (it is a bug & tech debt).

    Args:
        context: The context to execute streaming generator.
    """
    cdef:
        int64_t cur_generator_index = 0
        CRayStatus return_status

    assert context.is_initialized()
    # Generator task should only have 1 return object ref,
    # which contains None or exceptions (if system error occurs).
    assert context.return_size == 1

    gen = context.generator

    loop = asyncio.get_running_loop()
    worker = ray._private.worker.global_worker

    executor = worker.core_worker.get_event_loop_executor()
    interrupt_signal_event = threading.Event()

    try:
        try:
            async for output in gen:
                # NOTE: Report of streaming generator output is done in a
                # standalone thread-pool to avoid blocking the event loop,
                # since serializing and actual RPC I/O is done with "nogil". We
                # still wait for the report to finish to ensure that the task
                # does not modify the output before we serialize it.
                #
                # Note that the RPC is sent asynchronously, and we do not wait
                # for the reply here. The exception is if the user specified a
                # backpressure threshold for the streaming generator, and we
                # are currently under backpressure. Then we need to wait for an
                # ack from the caller (the reply for a possibly previous report
                # RPC) that they have consumed more ObjectRefs.
                await loop.run_in_executor(
                    executor,
                    report_streaming_generator_output,
                    context,
                    output,
                    cur_generator_index,
                    interrupt_signal_event,
                )
                cur_generator_index += 1
        except Exception as e:
            # Report the exception to the owner of the task.
            report_streaming_generator_exception(context, e, cur_generator_index, None)

    except BaseException as be:
        # NOTE: PLEASE READ CAREFULLY BEFORE CHANGING
        #
        # Upon encountering any failures in reporting generator's output we have to
        # make sure that any already scheduled (onto thread-pool executor), but not
        # finished tasks are canceled before re-throwing the exception to avoid
        # use-after-free failures where tasks could potentially access data-structures
        # that are already cleaned by the caller.
        #
        # For that we set an event to interrupt already scheduled tasks (that have
        # not finished executing), therefore interrupting their execution and
        # making sure that externally provided data-structures are not
        # accessed after this point
        #
        # For more details, please check out
        # https://github.com/ray-project/ray/issues/43771
        interrupt_signal_event.set()

        raise

    # The caller gets object values through the reports. If we finish the task
    # before sending the report is complete, then we may fail before the report
    # is sent to the caller. Then, the caller would never be able to ray.get
    # the yield'ed ObjectRef. Therefore, we must wait for all in-flight object
    # reports to complete before finishing the task.
    with nogil:
        return_status = context.waiter.get().WaitAllObjectsReported()
    check_status(return_status)


cdef create_generator_return_obj(
        output,
        const CObjectID &generator_id,
        worker: "Worker",
        const CAddress &caller_address,
        TaskID task_id,
        return_size,
        generator_index,
        is_async,
        c_pair[CObjectID, shared_ptr[CRayObject]] *return_object):
    """Create a generator return object based on a given output.

    Args:
        output: The output from a next(generator).
        generator_id: The object ref id of the generator task.
        worker: The Python worker class inside worker.py
        caller_address: The address of the caller. By our protocol,
            the caller of the streaming generator task is always
            the owner, so we can also call it "owner address".
        task_id: The task ID of the generator task.
        return_size: The number of static returns.
        generator_index: The index of a current error object.
        is_async: Whether or not the given object is created within
            an async actor.
        return_object(out): A Ray Object that contains the given output.
    """
    cdef:
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] intermediate_result
        CoreWorker core_worker = worker.core_worker

    return_id = core_worker.allocate_dynamic_return_id_for_generator(
        caller_address,
        task_id.native(),
        return_size,
        generator_index,
        is_async,
    )
    intermediate_result.push_back(
            c_pair[CObjectID, shared_ptr[CRayObject]](
                return_id, shared_ptr[CRayObject]()))
    core_worker.store_task_outputs(
        worker, [output],
        caller_address,
        &intermediate_result,
        generator_id)

    return_object[0] = intermediate_result.back()


cdef create_generator_error_object(
        e: Exception,
        worker: "Worker",
        CTaskType task_type,
        const CAddress &caller_address,
        TaskID task_id,
        const c_string &serialized_retry_exception_allowlist,
        function_name,
        function_descriptor,
        title,
        actor,
        actor_id,
        return_size,
        generator_index,
        is_async,
        c_bool should_retry_exceptions,
        c_pair[CObjectID, shared_ptr[CRayObject]] *error_object,
        c_bool *is_retryable_error,
        c_string *application_error):
    """Create a generator error object.

    This API sets is_retryable_error and application_error,
    It also creates and returns a new RayObject that
    contains the exception `e`.

    Args:
        e: The exception raised from a generator.
        worker: The Python worker class inside worker.py
        task_type: The type of the task. E.g., actor task, normal task.
        caller_address: The address of the caller. By our protocol,
            the caller of the streaming generator task is always
            the owner, so we can also call it "owner address".
        task_id: The task ID of the generator task.
        serialized_retry_exception_allowlist: A list of
            exceptions that are allowed to retry this generator task.
        function_name: The name of the generator function. Used for
            writing an error message.
        function_descriptor: The function descriptor of
            the generator function. Used for writing an error message.
        title: The process title of the generator task. Used for
            writing an error message.
        actor: The instance of the actor created in this worker.
            It is used to write an error message.
        actor_id: The ID of the actor. It is used to write an error message.
        return_size: The number of static returns.
        generator_index: The index of a current error object.
        is_async: Whether or not the given object is created within
            an async actor.
        error_object(out): A Ray Object that contains the given error exception.
        is_retryable_error(out): It is set to True if the generator
            raises an exception, and the error is retryable.
        application_error(out): It is set if the generator raises an
            application error.
    """
    cdef:
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] intermediate_result
        CoreWorker core_worker = worker.core_worker

    is_retryable_error[0] = determine_if_retryable(
        should_retry_exceptions,
        e,
        serialized_retry_exception_allowlist,
        function_descriptor,
    )

    if is_retryable_error[0]:
        logger.debug(
            "Task failed with retryable exception:"
            " {}.".format(task_id), exc_info=True)
        # Raise an exception directly and halt the execution
        # because there's no need to set the exception
        # for the return value when the task is retryable.
        raise e

    logger.debug(
        "Task failed with unretryable exception:"
        " {}.".format(task_id), exc_info=True)

    error_id = core_worker.allocate_dynamic_return_id_for_generator(
        caller_address,
        task_id.native(),
        return_size,
        generator_index,
        is_async,
    )
    intermediate_result.push_back(
            c_pair[CObjectID, shared_ptr[CRayObject]](
                error_id, shared_ptr[CRayObject]()))
    store_task_errors(
                worker, e,
                True,  # task_exception
                actor,  # actor
                actor_id,  # actor id
                function_name, task_type, title,
                caller_address,
                &intermediate_result, application_error)

    error_object[0] = intermediate_result.back()


cdef execute_dynamic_generator_and_store_task_outputs(
        generator,
        const CObjectID &generator_id,
        CTaskType task_type,
        const c_string &serialized_retry_exception_allowlist,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *dynamic_returns,
        c_bool *is_retryable_error,
        c_string *application_error,
        c_bool is_reattempt,
        function_name,
        function_descriptor,
        title,
        const CAddress &caller_address,
        c_bool should_retry_exceptions):
    worker = ray._private.worker.global_worker
    cdef:
        CoreWorker core_worker = worker.core_worker

    try:
        core_worker.store_task_outputs(
            worker, generator,
            caller_address,
            dynamic_returns,
            generator_id)
    except Exception as error:
        is_retryable_error[0] = determine_if_retryable(
            should_retry_exceptions,
            error,
            serialized_retry_exception_allowlist,
            function_descriptor,
        )
        if is_retryable_error[0]:
            logger.info("Task failed with retryable exception:"
                        " {}.".format(
                            core_worker.get_current_task_id()),
                        exc_info=True)
            raise error
        else:
            logger.debug("Task failed with unretryable exception:"
                         " {}.".format(
                             core_worker.get_current_task_id()),
                         exc_info=True)

            if not is_reattempt:
                # If this is the first execution, we should
                # generate one additional ObjectRef. This last
                # ObjectRef will contain the error.
                error_id = (CCoreWorkerProcess.GetCoreWorker()
                            .AllocateDynamicReturnId(
                                caller_address, CTaskID.Nil(), NULL_PUT_INDEX))
                dynamic_returns[0].push_back(
                        c_pair[CObjectID, shared_ptr[CRayObject]](
                            error_id, shared_ptr[CRayObject]()))

            # If a generator task fails mid-execution, we fail the
            # dynamically generated nested ObjectRefs instead of
            # the top-level DynamicObjectRefGenerator.
            num_errors_stored = store_task_errors(
                        worker, error,
                        False,  # task_exception
                        None,  # actor
                        None,  # actor id
                        function_name, task_type, title, caller_address,
                        dynamic_returns, application_error)
            if num_errors_stored == 0:
                assert is_reattempt
                # TODO(swang): The generator task failed and we
                # also failed to store the error in any of its
                # return values. This should only occur if the
                # generator task was re-executed and returned more
                # values than the initial execution.
                logger.error(
                    "Unhandled error: Re-executed generator task "
                    "returned more than the "
                    f"{dynamic_returns[0].size()} values returned "
                    "by the first execution.\n"
                    "See https://github.com/ray-project/ray/issues/28688.")


cdef void execute_task(
        const CAddress &caller_address,
        CTaskType task_type,
        const c_string name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        const c_string debugger_breakpoint,
        const c_string serialized_retry_exception_allowlist,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *dynamic_returns,
        c_vector[c_pair[CObjectID, c_bool]] *streaming_generator_returns,
        c_bool *is_retryable_error,
        c_string *application_error,
        # This parameter is only used for actor creation task to define
        # the concurrency groups of this actor.
        const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups,
        const c_string c_name_of_concurrency_group_to_execute,
        c_bool is_reattempt,
        execution_info,
        title,
        task_name,
        c_bool is_streaming_generator,
        c_bool should_retry_exceptions,
        int64_t generator_backpressure_num_objects) except *:
    worker = ray._private.worker.global_worker
    manager = worker.function_actor_manager
    actor = None
    actor_id = None
    cdef:
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        TaskID task_id = core_worker.get_current_task_id()
        uint64_t attempt_number = core_worker.get_current_task_attempt_number()
        c_vector[shared_ptr[CRayObject]] dynamic_return_ptrs

    # Helper method used to exit current asyncio actor.
    # This is called when a KeyboardInterrupt is received by the main thread.
    # Upon receiving a KeyboardInterrupt signal, Ray will exit the current
    # worker. If the worker is processing normal tasks, Ray treat it as task
    # cancellation from ray.cancel(object_ref). If the worker is an asyncio
    # actor, Ray will exit the actor.
    def exit_current_actor_if_asyncio():
        if core_worker.current_actor_is_asyncio():
            raise_sys_exit_with_custom_error_message("exit_actor() is called.")

    function_descriptor = CFunctionDescriptorToPython(
        ray_function.GetFunctionDescriptor())
    function_name = execution_info.function_name
    extra_data = (b'{"name": "' + function_name.encode("ascii") +
                  b'", "task_id": "' + task_id.hex().encode("ascii") + b'"}')

    name_of_concurrency_group_to_execute = \
        c_name_of_concurrency_group_to_execute.decode("ascii")

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
        actor_id = core_worker.get_actor_id()
        actor = worker.actors[actor_id]
        class_name = actor.__class__.__name__
        next_title = f"ray::{class_name}"

        def function_executor(*arguments, **kwarguments):
            function = execution_info.function

            if core_worker.current_actor_is_asyncio():
                if not has_async_methods(actor.__class__):
                    error_message = (
                        "Failed to create actor. You set the async flag, "
                        "but the actor does not "
                        "have any coroutine functions.")
                    raise ActorDiedError(
                        ActorDiedErrorContext(
                            error_message=error_message,
                            actor_id=core_worker.get_actor_id().binary(),
                            class_name=class_name
                            )
                        )

                if is_async_func(function.method):
                    async_function = function
                else:
                    # Just execute the method if it's ray internal method.
                    if function.name.startswith("__ray"):
                        return function(actor, *arguments, **kwarguments)
                    async_function = sync_to_async(function)

                if inspect.isasyncgenfunction(function.method):
                    # The coroutine will be handled separately by
                    # execute_dynamic_generator_and_store_task_outputs
                    return async_function(actor, *arguments, **kwarguments)
                else:
                    return core_worker.run_async_func_or_coro_in_event_loop(
                        async_function, function_descriptor,
                        name_of_concurrency_group_to_execute, task_id=task_id,
                        func_args=(actor, *arguments), func_kwargs=kwarguments)

            return function(actor, *arguments, **kwarguments)

    with core_worker.profile_event(b"task::" + name, extra_data=extra_data), \
         ray._private.worker._changeproctitle(title, next_title):
        task_exception = False
        try:
            with core_worker.profile_event(b"task:deserialize_arguments"):
                if c_args.empty():
                    args, kwargs = [], {}
                else:
                    metadata_pairs = RayObjectsToDataMetadataPairs(c_args)
                    object_refs = VectorToObjectRefs(
                            c_arg_refs,
                            skip_adding_local_ref=False)
                    if core_worker.current_actor_is_asyncio():
                        # We deserialize objects in event loop thread to
                        # prevent segfaults. See #7799
                        async def deserialize_args():
                            return (ray._private.worker.global_worker
                                    .deserialize_objects(
                                        metadata_pairs, object_refs))
                        args = core_worker.run_async_func_or_coro_in_event_loop(
                            deserialize_args, function_descriptor,
                            name_of_concurrency_group_to_execute)
                    else:
                        # Defer task cancellation (SIGINT) until after the task argument
                        # deserialization context has been left.
                        # NOTE (Clark): We defer SIGINT until after task argument
                        # deserialization completes to keep from interrupting
                        # non-reentrant imports that may be re-entered during error
                        # serialization or storage.
                        # See https://github.com/ray-project/ray/issues/30453.
                        # NOTE (Clark): Signal handlers can only be registered on the
                        # main thread.
                        with DeferSigint.create_if_main_thread():
                            args = (ray._private.worker.global_worker
                                    .deserialize_objects(
                                        metadata_pairs, object_refs))

                    for arg in args:
                        raise_if_dependency_failed(arg)
                    args, kwargs = ray._private.signature.recover_args(args)

            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                actor_id = core_worker.get_actor_id()
                actor = worker.actors[actor_id]
                class_name = actor.__class__.__name__
                actor_title = f"{class_name}({args!r}, {kwargs!r})"
                core_worker.set_actor_title(actor_title.encode("utf-8"))

            worker.record_task_log_start(task_id, attempt_number)

            # Execute the task.
            with core_worker.profile_event(b"task:execute"):
                task_exception = True
                try:
                    if debugger_breakpoint != b"":
                        ray.util.pdb.set_trace(
                            breakpoint_uuid=debugger_breakpoint)
                    outputs = function_executor(*args, **kwargs)

                    if is_streaming_generator:
                        # Streaming generator always has a single return value
                        # which is the generator task return.
                        assert returns[0].size() == 1

                        is_async_gen = inspect.isasyncgen(outputs)
                        is_sync_gen = inspect.isgenerator(outputs)

                        if (not is_sync_gen
                                and not is_async_gen):
                            raise ValueError(
                                    "Functions with "
                                    "@ray.remote(num_returns=\"streaming\" "
                                    "must return a generator")
                        context = StreamingGeneratorExecutionContext.make(
                                returns[0][0].first,  # generator object ID.
                                task_type,
                                caller_address,
                                task_id,
                                serialized_retry_exception_allowlist,
                                function_name,
                                function_descriptor,
                                title,
                                actor,
                                actor_id,
                                name_of_concurrency_group_to_execute,
                                returns[0].size(),
                                attempt_number,
                                should_retry_exceptions,
                                streaming_generator_returns,
                                is_retryable_error,
                                application_error,
                                generator_backpressure_num_objects)
                        # We cannot pass generator to cdef in Cython for some reasons.
                        # It is a workaround.
                        context.initialize(outputs)

                        if is_async_gen:
                            if generator_backpressure_num_objects != -1:
                                raise ValueError(
                                    "_generator_backpressure_num_objects is "
                                    "not supported for an async actor."
                                )
                            # Note that the report RPCs are called inside an
                            # event loop thread.
                            core_worker.run_async_func_or_coro_in_event_loop(
                                execute_streaming_generator_async(context),
                                function_descriptor,
                                name_of_concurrency_group_to_execute,
                                task_id=task_id)
                        else:
                            execute_streaming_generator_sync(context)

                        # Streaming generator output is not used, so set it to None.
                        outputs = None

                    next_breakpoint = (
                        ray._private.worker.global_worker.debugger_breakpoint)
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
                        (ray._private.worker.global_worker
                         .debugger_breakpoint) = b""
                    task_exception = False
                except AsyncioActorExit as e:
                    exit_current_actor_if_asyncio()
                except Exception as e:
                    is_retryable_error[0] = determine_if_retryable(
                                    should_retry_exceptions,
                                    e,
                                    serialized_retry_exception_allowlist,
                                    function_descriptor,
                                )
                    if is_retryable_error[0]:
                        logger.debug("Task failed with retryable exception:"
                                     " {}.".format(
                                        core_worker.get_current_task_id()),
                                     exc_info=True)
                    else:
                        logger.debug("Task failed with unretryable exception:"
                                     " {}.".format(
                                        core_worker.get_current_task_id()),
                                     exc_info=True)
                    raise e
                finally:
                    # Record the end of the task log.
                    worker.record_task_log_end(task_id, attempt_number)

                if (returns[0].size() == 1
                        and not inspect.isgenerator(outputs)
                        and not inspect.isasyncgen(outputs)):
                    # If there is only one return specified, we should return
                    # all return values as a single object.
                    outputs = (outputs,)
            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                # Record actor repr via :actor_name: magic token in the log.
                #
                # (Phase 2): after `__init__` finishes, we override the
                # log prefix with the full repr of the actor. The log monitor
                # will pick up the updated token.
                actor_class = manager.load_actor_class(job_id, function_descriptor)
                if (hasattr(actor_class, "__ray_actor_class__") and
                        (actor_class.__ray_actor_class__.__repr__
                         != object.__repr__)):
                    actor_repr = repr(actor)
                    actor_magic_token = "{}{}\n".format(
                        ray_constants.LOG_PREFIX_ACTOR_NAME, actor_repr)
                    # Flush on both stdout and stderr.
                    print(actor_magic_token, end="")
                    print(actor_magic_token, file=sys.stderr, end="")

                    # Sets the actor repr name for the actor so other components
                    # like GCS has such info.
                    core_worker.set_actor_repr_name(actor_repr)

            if (returns[0].size() > 0
                    and not inspect.isgenerator(outputs)
                    and not inspect.isasyncgen(outputs)
                    and len(outputs) != int(returns[0].size())):
                raise ValueError(
                    "Task returned {} objects, but num_returns={}.".format(
                        len(outputs), returns[0].size()))

            # Store the outputs in the object store.
            with core_worker.profile_event(b"task:store_outputs"):
                # TODO(sang): Remove it once we use streaming generator
                # by default.
                if dynamic_returns != NULL and not is_streaming_generator:
                    if not inspect.isgenerator(outputs):
                        raise ValueError(
                                "Functions with "
                                "@ray.remote(num_returns=\"dynamic\" must return a "
                                "generator")
                    task_exception = True

                    execute_dynamic_generator_and_store_task_outputs(
                            outputs,
                            returns[0][0].first,
                            task_type,
                            serialized_retry_exception_allowlist,
                            dynamic_returns,
                            is_retryable_error,
                            application_error,
                            is_reattempt,
                            function_name,
                            function_descriptor,
                            title,
                            caller_address,
                            should_retry_exceptions)

                    task_exception = False
                    dynamic_refs = []
                    for idx in range(dynamic_returns.size()):
                        dynamic_refs.append(ObjectRef(
                            dynamic_returns[0][idx].first.Binary(),
                            caller_address.SerializeAsString(),
                        ))
                    # Swap out the generator for an ObjectRef generator.
                    outputs = (DynamicObjectRefGenerator(dynamic_refs), )

                # TODO(swang): For generator tasks, iterating over outputs will
                # actually run the task. We should run the usual handlers for
                # task cancellation, retrying on application exception, etc. for
                # all generator tasks, both static and dynamic.
                core_worker.store_task_outputs(
                    worker, outputs,
                    caller_address,
                    returns)

        except Exception as e:
            num_errors_stored = store_task_errors(
                    worker, e, task_exception, actor, actor_id, function_name,
                    task_type, title, caller_address, returns, application_error)
            if returns[0].size() > 0 and num_errors_stored == 0:
                logger.exception(
                        "Unhandled error: Task threw exception, but all "
                        f"{returns[0].size()} return values already created. "
                        "This should only occur when using generator tasks.\n"
                        "See https://github.com/ray-project/ray/issues/28689.")


cdef execute_task_with_cancellation_handler(
        const CAddress &caller_address,
        CTaskType task_type,
        const c_string name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        const c_string debugger_breakpoint,
        const c_string serialized_retry_exception_allowlist,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *dynamic_returns,
        c_vector[c_pair[CObjectID, c_bool]] *streaming_generator_returns,
        c_bool *is_retryable_error,
        c_string *application_error,
        # This parameter is only used for actor creation task to define
        # the concurrency groups of this actor.
        const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups,
        const c_string c_name_of_concurrency_group_to_execute,
        c_bool is_reattempt,
        c_bool is_streaming_generator,
        c_bool should_retry_exceptions,
        int64_t generator_backpressure_num_objects):

    is_retryable_error[0] = False

    worker = ray._private.worker.global_worker
    manager = worker.function_actor_manager
    cdef:
        dict execution_infos = manager.execution_infos
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        TaskID task_id = core_worker.get_current_task_id()
        c_vector[shared_ptr[CRayObject]] dynamic_return_ptrs

    task_name = name.decode("utf-8")
    title = f"ray::{task_name}"

    # Automatically restrict the GPUs (CUDA), neuron_core, TPU accelerator
    # runtime_ids to restrict availability to this task.
    # Once actor is created, users can change the visible accelerator ids within
    # an actor task and we don't want to reset it.
    if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
        ray._private.utils.set_visible_accelerator_ids()

    # Automatically configure OMP_NUM_THREADS to the assigned CPU number.
    # It will be unset after the task execution if it was overwridden here.
    # No-op if already set.
    omp_num_threads_overriden = ray._private.utils.set_omp_num_threads_if_unset()

    # Initialize the actor if this is an actor creation task. We do this here
    # before setting the current task ID so that we can get the execution info,
    # in case executing the main task throws an exception.
    function_descriptor = CFunctionDescriptorToPython(
        ray_function.GetFunctionDescriptor())
    if <int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK:
        actor_class = manager.load_actor_class(job_id, function_descriptor)
        actor_id = core_worker.get_actor_id()
        actor = actor_class.__new__(actor_class)
        worker.actors[actor_id] = actor
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

    global current_task_id

    try:
        task_id = (ray._private.worker.
                   global_worker.core_worker.get_current_task_id())
        # Set the current task ID, which is checked by a separate thread during
        # task cancellation. We must do this inside the try block so that, if
        # the task is interrupted because of cancellation, we will catch the
        # interrupt error here.
        with current_task_id_lock:
            current_task_id = task_id

        execute_task(caller_address,
                     task_type,
                     name,
                     ray_function,
                     c_resources,
                     c_args,
                     c_arg_refs,
                     debugger_breakpoint,
                     serialized_retry_exception_allowlist,
                     returns,
                     dynamic_returns,
                     streaming_generator_returns,
                     is_retryable_error,
                     application_error,
                     c_defined_concurrency_groups,
                     c_name_of_concurrency_group_to_execute,
                     is_reattempt, execution_info, title, task_name,
                     is_streaming_generator,
                     should_retry_exceptions,
                     generator_backpressure_num_objects)

        # Check for cancellation.
        PyErr_CheckSignals()

    except KeyboardInterrupt as e:
        # Catch and handle task cancellation, which will result in an interrupt being
        # raised.
        e = TaskCancelledError(
            core_worker.get_current_task_id()).with_traceback(e.__traceback__)

        actor = None
        actor_id = core_worker.get_actor_id()
        if not actor_id.is_nil():
            actor = worker.actors[actor_id]

        store_task_errors(
                worker, e,
                # Task cancellation can happen anytime so we don't really need
                # to differentiate between mid-task or not.
                False,  # task_exception
                actor,
                actor_id,
                execution_info.function_name,
                task_type, title, caller_address,
                returns,
                # application_error: we are passing NULL since we don't want the
                # cancel tasks to fail.
                NULL)
    finally:
        with current_task_id_lock:
            current_task_id = None

        if omp_num_threads_overriden:
            # Reset the OMP_NUM_THREADS environ if it was set.
            os.environ.pop("OMP_NUM_THREADS", None)

    if execution_info.max_calls != 0:
        # Reset the state of the worker for the next task to execute.
        # Increase the task execution counter.
        manager.increase_task_counter(function_descriptor)
        # If we've reached the max number of executions for this worker, exit.
        task_counter = manager.get_task_counter(function_descriptor)
        if task_counter == execution_info.max_calls:
            raise_sys_exit_with_custom_error_message(
                f"Exited because worker reached max_calls={execution_info.max_calls}"
                " for this method.")

cdef shared_ptr[LocalMemoryBuffer] ray_error_to_memory_buf(ray_error):
    cdef bytes py_bytes = ray_error.to_bytes()
    return make_shared[LocalMemoryBuffer](
        <uint8_t*>py_bytes, len(py_bytes), True)

cdef CRayStatus task_execution_handler(
        const CAddress &caller_address,
        CTaskType task_type,
        const c_string task_name,
        const CRayFunction &ray_function,
        const unordered_map[c_string, double] &c_resources,
        const c_vector[shared_ptr[CRayObject]] &c_args,
        const c_vector[CObjectReference] &c_arg_refs,
        const c_string debugger_breakpoint,
        const c_string serialized_retry_exception_allowlist,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *returns,
        c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]] *dynamic_returns,
        c_vector[c_pair[CObjectID, c_bool]] *streaming_generator_returns,
        shared_ptr[LocalMemoryBuffer] &creation_task_exception_pb_bytes,
        c_bool *is_retryable_error,
        c_string *application_error,
        const c_vector[CConcurrencyGroup] &defined_concurrency_groups,
        const c_string name_of_concurrency_group_to_execute,
        c_bool is_reattempt,
        c_bool is_streaming_generator,
        c_bool should_retry_exceptions,
        int64_t generator_backpressure_num_objects) nogil:
    with gil, disable_client_hook():
        # Initialize job_config if it hasn't already.
        # Setup system paths configured in job_config.
        maybe_initialize_job_config()

        try:
            try:
                # Exceptions, including task cancellation, should be handled
                # internal to this call. If it does raise an exception, that
                # indicates that there was an internal error.
                execute_task_with_cancellation_handler(
                        caller_address,
                        task_type, task_name,
                        ray_function, c_resources,
                        c_args, c_arg_refs,
                        debugger_breakpoint,
                        serialized_retry_exception_allowlist,
                        returns,
                        dynamic_returns,
                        streaming_generator_returns,
                        is_retryable_error,
                        application_error,
                        defined_concurrency_groups,
                        name_of_concurrency_group_to_execute,
                        is_reattempt,
                        is_streaming_generator,
                        should_retry_exceptions,
                        generator_backpressure_num_objects)
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
                    sys_exit.init_error_message = (
                        "Exception raised from an actor init method. "
                        f"Traceback: {str(e)}")
                else:
                    traceback_str = traceback.format_exc() + (
                        "An unexpected internal error "
                        "occurred while the worker "
                        "was executing a task.")
                    ray._private.utils.push_error_to_driver(
                        ray._private.worker.global_worker,
                        "worker_crash",
                        traceback_str,
                        job_id=None)
                    sys_exit.unexpected_error_traceback = traceback_str
                raise sys_exit
        except SystemExit as e:
            # Tell the core worker to exit as soon as the result objects
            # are processed.
            if hasattr(e, "is_creation_task_error"):
                return CRayStatus.CreationTaskError(e.init_error_message)
            elif e.code is not None and e.code == 0:
                # This means the system exit was
                # normal based on the python convention.
                # https://docs.python.org/3/library/sys.html#sys.exit
                msg = f"Worker exits with an exit code {e.code}."
                if hasattr(e, "ray_terminate_msg"):
                    msg += (f" {e.ray_terminate_msg}")
                return CRayStatus.IntentionalSystemExit(msg)
            else:
                msg = f"Worker exits with an exit code {e.code}."
                # In K8s, SIGTERM likely means we hit memory limits, so print
                # a more informative message there.
                if "KUBERNETES_SERVICE_HOST" in os.environ:
                    msg += (
                        " The worker may have exceeded K8s pod memory limits.")
                if hasattr(e, "ray_terminate_msg"):
                    msg += (f" {e.ray_terminate_msg}")
                if hasattr(e, "unexpected_error_traceback"):
                    msg += (f" {e.unexpected_error_traceback}")
                return CRayStatus.UnexpectedSystemExit(msg)

    return CRayStatus.OK()

cdef c_bool kill_main_task(const CTaskID &task_id) nogil:
    with gil:
        task_id_to_kill = TaskID(task_id.Binary())
        with current_task_id_lock:
            if current_task_id != task_id_to_kill:
                return False
            _thread.interrupt_main()
            return True


cdef CRayStatus check_signals() nogil:
    with gil:
        # The Python exceptions are not handled if it is raised from cdef,
        # so we have to handle it here.
        try:
            PyErr_CheckSignals()
        except KeyboardInterrupt:
            return CRayStatus.Interrupted(b"")
        except SystemExit as e:
            error_msg = (
                "SystemExit is raised (sys.exit is called).")
            if e.code is not None:
                error_msg += f" Exit code: {e.code}."
            else:
                error_msg += " Exit code was not specified."

            if hasattr(e, "ray_terminate_msg"):
                error_msg += f" {e.ray_terminate_msg}"

            if e.code and e.code == 0:
                return CRayStatus.IntentionalSystemExit(error_msg.encode("utf-8"))
            else:
                return CRayStatus.UnexpectedSystemExit(error_msg.encode("utf-8"))
        # By default, if signals raise an exception, Python just prints them.
        # To keep the same behavior, we don't handle any other exceptions.

    return CRayStatus.OK()


cdef void gc_collect(c_bool triggered_by_global_gc) nogil:
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
        object_refs = VectorToObjectRefs(
                object_refs_to_spill,
                skip_adding_local_ref=False)
        for i in range(object_refs_to_spill.size()):
            owner_addresses.push_back(
                    object_refs_to_spill[i].owner_address()
                    .SerializeAsString())
        try:
            with ray._private.worker._changeproctitle(
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
                ray._private.worker.global_worker,
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
        object_refs = VectorToObjectRefs(
                object_refs_to_restore,
                skip_adding_local_ref=False)
        try:
            with ray._private.worker._changeproctitle(
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
                    ray._private.worker.global_worker,
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
            with ray._private.worker._changeproctitle(
                    proctitle,
                    original_proctitle):
                external_storage.delete_spilled_objects(urls)
        except Exception:
            exception_str = (
                "An unexpected internal error occurred while the IO worker "
                "was deleting spilled objects.")
            logger.exception(exception_str)
            ray._private.utils.push_error_to_driver(
                ray._private.worker.global_worker,
                "delete_spilled_objects_error",
                traceback.format_exc() + exception_str,
                job_id=None)


cdef void cancel_async_task(
        const CTaskID &c_task_id,
        const CRayFunction &ray_function,
        const c_string c_name_of_concurrency_group_to_execute) nogil:
    with gil:
        function_descriptor = CFunctionDescriptorToPython(
            ray_function.GetFunctionDescriptor())
        name_of_concurrency_group_to_execute = \
            c_name_of_concurrency_group_to_execute.decode("ascii")
        task_id = TaskID(c_task_id.Binary())

        worker = ray._private.worker.global_worker
        eventloop, _ = worker.core_worker.get_event_loop(
            function_descriptor, name_of_concurrency_group_to_execute)
        future = worker.core_worker.get_queued_future(task_id)
        if future is not None:
            future.cancel()
        # else, the task is already finished. If the task
        # wasn't finished (task is queued on a client or server side),
        # this method shouldn't have been called.


cdef void unhandled_exception_handler(const CRayObject& error) nogil:
    with gil:
        worker = ray._private.worker.global_worker
        data = None
        metadata = None
        if error.HasData():
            data = Buffer.make(error.GetData())
        if error.HasMetadata():
            metadata = Buffer.make(error.GetMetadata()).to_pybytes()
        # TODO(ekl) why does passing a ObjectRef.nil() lead to shutdown errors?
        object_ids = [None]
        worker.raise_errors([(data, metadata)], object_ids)


def maybe_initialize_job_config():
    with job_config_initialization_lock:
        global job_config_initialized
        if job_config_initialized:
            return
        # Add code search path to sys.path, set load_code_from_local.
        core_worker = ray._private.worker.global_worker.core_worker
        code_search_path = core_worker.get_job_config().code_search_path
        load_code_from_local = False
        if code_search_path:
            load_code_from_local = True
            for p in code_search_path:
                if os.path.isfile(p):
                    p = os.path.dirname(p)
                sys.path.insert(0, p)
        ray._private.worker.global_worker.set_load_code_from_local(load_code_from_local)

        # Add driver's system path to sys.path
        py_driver_sys_path = core_worker.get_job_config().py_driver_sys_path
        if py_driver_sys_path:
            for p in py_driver_sys_path:
                sys.path.insert(0, p)

        # Cache and set the current job id.
        job_id = core_worker.get_current_job_id()
        ray._private.worker.global_worker.set_cached_job_id(job_id)

        # Record the task name via :task_name: magic token in the log file.
        # This is used for the prefix in driver logs `(task_name pid=123) ...`
        job_id_magic_token = "{}{}\n".format(
            ray_constants.LOG_PREFIX_JOB_ID, job_id.hex())
        # Print on both .out and .err
        print(job_id_magic_token, end="")
        print(job_id_magic_token, file=sys.stderr, end="")

        # Configure worker process's Python logging.
        serialized_py_logging_config = \
            core_worker.get_job_config().serialized_py_logging_config
        if serialized_py_logging_config:
            logging_config = pickle.loads(serialized_py_logging_config)
            try:
                logging_config._apply()
            except Exception as e:
                backtrace = \
                    "".join(traceback.format_exception(type(e), e, e.__traceback__))
                core_worker.drain_and_exit_worker("user", backtrace)
        job_config_initialized = True


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
            if filename.endswith("_private/worker.py"):
                if frame.f_code.co_name == "put":
                    msg_frames = ["(put object) "]
            elif filename.endswith("_private/workers/default_worker.py"):
                pass
            elif filename.endswith("ray/remote_function.py"):
                # TODO(ekl) distinguish between task return objects and
                # arguments. This can only be done in the core worker.
                msg_frames = ["(task call) "]
            elif filename.endswith("ray/actor.py"):
                # TODO(ekl) distinguish between actor return objects and
                # arguments. This can only be done in the core worker.
                msg_frames = ["(actor call) "]
            elif filename.endswith("_private/serialization.py"):
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
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker.stop_and_join_asyncio_threads_if_exist()


# An empty profile event context to be used when the timeline is disabled.
cdef class EmptyProfileEvent:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


def _auto_reconnect(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if "TEST_RAY_COLLECT_KV_FREQUENCY" in os.environ:
            with ray._private.utils._CALLED_FREQ_LOCK:
                ray._private.utils._CALLED_FREQ[f.__name__] += 1
        remaining_retry = self._nums_reconnect_retry
        while True:
            try:
                return f(self, *args, **kwargs)
            except RpcError as e:
                if e.rpc_code in [
                    GRPC_STATUS_CODE_UNAVAILABLE,
                    GRPC_STATUS_CODE_UNKNOWN,
                ]:
                    if remaining_retry <= 0:
                        logger.error(
                            "Failed to connect to GCS. Please check"
                            " `gcs_server.out` for more details."
                        )
                        raise
                    logger.debug(
                        f"Failed to send request to gcs, reconnecting. Error {e}"
                    )
                    try:
                        self._connect()
                    except Exception:
                        logger.error(f"Connecting to gcs failed. Error {e}")
                    time.sleep(1)
                    remaining_retry -= 1
                    continue
                raise

    return wrapper


cdef class GcsClient:
    """
    Client to the GCS server. Only contains synchronous methods.

    This class is in transition to use the new C++ GcsClient binding. The old
    PythonGcsClient binding is not deleted until we are confident that the new
    binding is stable.

    Defaults to the new binding. If you want to use the old binding, please
    set the environment variable `RAY_USE_OLD_GCS_CLIENT=1`.
    """

    cdef object inner  # OldGcsClient or NewGcsClient
    cdef c_bool use_old_client

    def __cinit__(self, address,
                  nums_reconnect_retry=RayConfig.instance().nums_py_gcs_reconnect_retry(
                  ),
                  cluster_id: str = None):
        self.use_old_client = os.getenv("RAY_USE_OLD_GCS_CLIENT") == "1"
        if self.use_old_client:
            self.inner = OldGcsClient(address, nums_reconnect_retry, cluster_id)
        else:
            # For timeout (DEADLINE_EXCEEDED): Both OldGcsClient and NewGcsClient
            # tries once with timeout_ms.
            #
            # For other RpcError (UNAVAILABLE, UNKNOWN): OldGcsClient tries this for
            # (nums_reconnect_retry + 1) times, each time for timeous_ms (+1s sleep
            # between each retry). NewGcsClient tries indefinitely until it thinks GCS
            # is down and kills itself.
            timeout_ms = RayConfig.instance().py_gcs_connect_timeout_s() * 1000
            self.inner = NewGcsClient.standalone(address, cluster_id, timeout_ms)
        logger.debug(f"Created GcsClient. inner {self.inner}")

    def __getattr__(self, name):
        if self.use_old_client:
            return getattr(self.inner, name)
        # For new client, we collect the frequency of each method call.
        # For old client, that is done in @_auto_reconnect.
        if "TEST_RAY_COLLECT_KV_FREQUENCY" in os.environ:
            with ray._private.utils._CALLED_FREQ_LOCK:
                ray._private.utils._CALLED_FREQ[name] += 1
        return getattr(self.inner, name)

cdef class OldGcsClient:
    """Old Cython wrapper class of C++ `ray::gcs::PythonGcsClient`."""
    cdef:
        shared_ptr[CPythonGcsClient] inner
        object address
        object _nums_reconnect_retry
        ClusterID cluster_id

    def __cinit__(self, address,
                  nums_reconnect_retry,
                  cluster_id: str = None):
        cdef GcsClientOptions gcs_options
        if cluster_id:
            gcs_options = GcsClientOptions.create(
                address, cluster_id, allow_cluster_id_nil=False,
                fetch_cluster_id_if_nil=False)
        else:
            gcs_options = GcsClientOptions.create(
                address, None, allow_cluster_id_nil=True, fetch_cluster_id_if_nil=True)
        self.inner.reset(new CPythonGcsClient(dereference(gcs_options.native())))
        self.address = address
        self._nums_reconnect_retry = nums_reconnect_retry
        if cluster_id is None:
            self.cluster_id = ClusterID.nil()
        else:
            self.cluster_id = ClusterID.from_hex(cluster_id)
        self._connect(RayConfig.instance().py_gcs_connect_timeout_s())

    def _connect(self, timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            size_t num_retries = self._nums_reconnect_retry
        with nogil:
            status = self.inner.get().Connect(timeout_ms, num_retries)

        check_status(status)

        result_c_cluster_id = self.inner.get().GetClusterId()
        result_cluster_id = ClusterID(result_c_cluster_id.Binary())
        if self.cluster_id.is_nil():
            self.cluster_id = result_cluster_id
        else:
            assert self.cluster_id == result_cluster_id

    @property
    def cluster_id(self):
        return self.cluster_id

    @property
    def address(self):
        return self.address

    @property
    def _nums_reconnect_retry(self):
        return self._nums_reconnect_retry

    @_auto_reconnect
    def check_alive(
        self, node_ips: c_vector[c_string], timeout: Optional[float] = None
    ):
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_bool] c_result
        with nogil:
            check_status(self.inner.get().CheckAlive(node_ips, timeout_ms, c_result))
        result = []
        for r in c_result:
            result.append(r)
        return result

    @_auto_reconnect
    def internal_kv_get(self, c_string key, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string value
            CRayStatus status
        with nogil:
            status = self.inner.get().InternalKVGet(ns, key, timeout_ms, value)
        if status.IsKeyError():
            return None
        else:
            check_status(status)
            return value

    @_auto_reconnect
    def internal_kv_multi_get(self, keys, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            c_vector[c_string] c_keys
            c_string c_key
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            unordered_map[c_string, c_string] c_result
            unordered_map[c_string, c_string].iterator it

        for c_key in keys:
            c_keys.push_back(c_key)
        with nogil:
            check_status(self.inner.get().InternalKVMultiGet(
                ns, c_keys, timeout_ms, c_result))

        result = {}
        it = c_result.begin()
        while it != c_result.end():
            key = dereference(it).first
            value = dereference(it).second
            result[key] = value
            postincrement(it)
        return result

    @_auto_reconnect
    def internal_kv_put(self, c_string key, c_string value, c_bool overwrite=False,
                        namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            int num_added = 0
        with nogil:
            check_status(self.inner.get().InternalKVPut(
                ns, key, value, overwrite, timeout_ms, num_added))

        return num_added

    @_auto_reconnect
    def internal_kv_del(self, c_string key, c_bool del_by_prefix,
                        namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            int num_deleted = 0
        with nogil:
            check_status(self.inner.get().InternalKVDel(
                ns, key, del_by_prefix, timeout_ms, num_deleted))

        return num_deleted

    @_auto_reconnect
    def internal_kv_keys(self, c_string prefix, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] keys
            c_string key

        with nogil:
            check_status(self.inner.get().InternalKVKeys(
                ns, prefix, timeout_ms, keys))

        result = []

        for key in keys:
            result.append(key)

        return result

    @_auto_reconnect
    def internal_kv_exists(self, c_string key, namespace=None, timeout=None):
        cdef:
            c_string ns = namespace or b""
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool exists = False
        with nogil:
            check_status(self.inner.get().InternalKVExists(
                ns, key, timeout_ms, exists))
        return exists

    @_auto_reconnect
    def pin_runtime_env_uri(self, str uri, int expiration_s, timeout=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string c_uri = uri.encode()
        with nogil:
            check_status(self.inner.get().PinRuntimeEnvUri(
                c_uri, expiration_s, timeout_ms))

    @_auto_reconnect
    def get_all_node_info(self, timeout=None) -> Dict[NodeID, GcsNodeInfo]:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            CGcsNodeInfo c_node_info
            c_vector[CGcsNodeInfo] c_node_infos
            c_vector[c_string] serialized_node_infos
        with nogil:
            check_status(self.inner.get().GetAllNodeInfo(timeout_ms, c_node_infos))
            for c_node_info in c_node_infos:
                serialized_node_infos.push_back(c_node_info.SerializeAsString())

        result = {}
        for serialized in serialized_node_infos:
            node_info = GcsNodeInfo()
            node_info.ParseFromString(serialized)
            result[NodeID.from_binary(node_info.node_id)] = node_info
        return result

    @_auto_reconnect
    def get_all_job_info(
        self, *, job_or_submission_id: str = None, skip_submission_job_info_field=False,
        skip_is_running_tasks_field=False, timeout=None
    ) -> Dict[JobID, JobTableData]:
        # Ideally we should use json_format.MessageToDict(job_info),
        # but `job_info` is a cpp pb message not a python one.
        # Manually converting each and every protobuf field is out of question,
        # so we serialize the pb to string to cross the FFI interface.
        cdef:
            c_string c_job_or_submission_id
            optional[c_string] c_optional_job_or_submission_id = nullopt
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_bool c_skip_submission_job_info_field = skip_submission_job_info_field
            c_bool c_skip_is_running_tasks_field = skip_is_running_tasks_field
            CJobTableData c_job_info
            c_vector[CJobTableData] c_job_infos
            c_vector[c_string] serialized_job_infos
        if job_or_submission_id:
            c_job_or_submission_id = job_or_submission_id
            c_optional_job_or_submission_id = \
                make_optional[c_string](c_job_or_submission_id)
        with nogil:
            check_status(self.inner.get().GetAllJobInfo(
                c_optional_job_or_submission_id, c_skip_submission_job_info_field,
                c_skip_is_running_tasks_field, timeout_ms, c_job_infos))
            for c_job_info in c_job_infos:
                serialized_job_infos.push_back(c_job_info.SerializeAsString())
        result = {}
        for serialized in serialized_job_infos:
            job_info = JobTableData()
            job_info.ParseFromString(serialized)
            result[JobID.from_binary(job_info.job_id)] = job_info
        return result

    @_auto_reconnect
    def get_all_resource_usage(self, timeout=None) -> GetAllResourceUsageReply:
        cdef:
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_string serialized_reply

        with nogil:
            check_status(self.inner.get().GetAllResourceUsage(
                timeout_ms, serialized_reply))

        reply = GetAllResourceUsageReply()
        reply.ParseFromString(serialized_reply)
        return reply
    ########################################################
    # Interface for rpc::autoscaler::AutoscalerStateService
    ########################################################

    @_auto_reconnect
    def request_cluster_resource_constraint(
            self,
            bundles: c_vector[unordered_map[c_string, double]],
            count_array: c_vector[int64_t],
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
        with nogil:
            check_status(self.inner.get().RequestClusterResourceConstraint(
                timeout_ms, bundles, count_array))

    @_auto_reconnect
    def get_cluster_resource_state(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status(self.inner.get().GetClusterResourceState(timeout_ms,
                         serialized_reply))

        return serialized_reply

    @_auto_reconnect
    def get_cluster_status(
            self,
            timeout_s=None):
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
            c_string serialized_reply
        with nogil:
            check_status(self.inner.get().GetClusterStatus(timeout_ms,
                         serialized_reply))

        return serialized_reply

    @_auto_reconnect
    def report_autoscaling_state(
        self,
        serialzied_state: c_string,
        timeout_s=None
    ):
        """Report autoscaling state to GCS"""
        cdef:
            int64_t timeout_ms = round(1000 * timeout_s) if timeout_s else -1
        with nogil:
            check_status(self.inner.get().ReportAutoscalingState(
                timeout_ms, serialzied_state))

    @_auto_reconnect
    def drain_node(
            self,
            node_id: c_string,
            reason: int32_t,
            reason_message: c_string,
            deadline_timestamp_ms: int64_t):
        """Send the DrainNode request to GCS.

        This is only for testing.
        """
        cdef:
            int64_t timeout_ms = -1
            c_bool is_accepted = False
            c_string rejection_reason_message
        with nogil:
            check_status(self.inner.get().DrainNode(
                node_id, reason, reason_message,
                deadline_timestamp_ms, timeout_ms, is_accepted,
                rejection_reason_message))

        return (is_accepted, rejection_reason_message.decode())

    @_auto_reconnect
    def drain_nodes(self, node_ids, timeout=None):
        cdef:
            c_vector[c_string] c_node_ids
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_drained_node_ids
        for node_id in node_ids:
            c_node_ids.push_back(node_id)
        with nogil:
            check_status(self.inner.get().DrainNodes(
                c_node_ids, timeout_ms, c_drained_node_ids))
        result = []
        for drain_node_id in c_drained_node_ids:
            result.append(drain_node_id)
        return result

    #############################################################
    # Interface for rpc::autoscaler::AutoscalerStateService ends
    #############################################################
cdef class GcsPublisher:
    """Cython wrapper class of C++ `ray::gcs::PythonGcsPublisher`."""
    cdef:
        shared_ptr[CPythonGcsPublisher] inner

    def __cinit__(self, address):
        self.inner.reset(new CPythonGcsPublisher(address))
        check_status(self.inner.get().Connect())

    def publish_error(self, key_id: bytes, error_type: str, message: str,
                      job_id=None, num_retries=None):
        cdef:
            CErrorTableData error_info
            int64_t c_num_retries = num_retries if num_retries else -1
            c_string c_key_id = key_id

        if job_id is None:
            job_id = ray.JobID.nil()
        assert isinstance(job_id, ray.JobID)
        error_info.set_job_id(job_id.binary())
        error_info.set_type(error_type)
        error_info.set_error_message(message)
        error_info.set_timestamp(time.time())

        with nogil:
            check_status(
                self.inner.get().PublishError(c_key_id, error_info, c_num_retries))

    def publish_logs(self, log_json: dict):
        cdef:
            CLogBatch log_batch
            c_string c_job_id

        job_id = log_json.get("job")
        log_batch.set_ip(log_json.get("ip") if log_json.get("ip") else b"")
        log_batch.set_pid(
            str(log_json.get("pid")).encode() if log_json.get("pid") else b"")
        log_batch.set_job_id(job_id.encode() if job_id else b"")
        log_batch.set_is_error(bool(log_json.get("is_err")))
        for line in log_json.get("lines", []):
            log_batch.add_lines(line)
        actor_name = log_json.get("actor_name")
        log_batch.set_actor_name(actor_name.encode() if actor_name else b"")
        task_name = log_json.get("task_name")
        log_batch.set_task_name(task_name.encode() if task_name else b"")

        c_job_id = job_id.encode() if job_id else b""
        with nogil:
            check_status(self.inner.get().PublishLogs(c_job_id, log_batch))


cdef class _GcsSubscriber:
    """Cython wrapper class of C++ `ray::gcs::PythonGcsSubscriber`."""
    cdef:
        shared_ptr[CPythonGcsSubscriber] inner

    def _construct(self, address, channel, worker_id):
        cdef:
            c_worker_id = worker_id or b""
        # subscriber_id needs to match the binary format of a random
        # SubscriberID / UniqueID, which is 28 (kUniqueIDSize) random bytes.
        subscriber_id = bytes(bytearray(random.getrandbits(8) for _ in range(28)))
        gcs_address, gcs_port = address.split(":")
        self.inner.reset(new CPythonGcsSubscriber(
            gcs_address, int(gcs_port), channel, subscriber_id, c_worker_id))

    def subscribe(self):
        """Registers a subscription for the subscriber's channel type.

        Before the registration, published messages in the channel will not be
        saved for the subscriber.
        """
        with nogil:
            check_status(self.inner.get().Subscribe())

    @property
    def last_batch_size(self):
        """Batch size of the result from last poll.

        Used to indicate whether the subscriber can keep up.
        """
        return self.inner.get().last_batch_size()

    def close(self):
        """Closes the subscriber and its active subscription."""
        with nogil:
            check_status(self.inner.get().Close())


cdef class GcsErrorSubscriber(_GcsSubscriber):
    """Subscriber to error info. Thread safe.

    Usage example:
        subscriber = GcsErrorSubscriber()
        # Subscribe to the error channel.
        subscriber.subscribe()
        ...
        while running:
            error_id, error_data = subscriber.poll()
            ......
        # Unsubscribe from the error channels.
        subscriber.close()
    """

    def __init__(self, address, worker_id=None):
        self._construct(address, RAY_ERROR_INFO_CHANNEL, worker_id)

    def poll(self, timeout=None):
        """Polls for new error messages.

        Returns:
            A tuple of error message ID and dict describing the error,
            or None, None if polling times out or subscriber closed.
        """
        cdef:
            CErrorTableData error_data
            c_string key_id
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1

        with nogil:
            check_status(self.inner.get().PollError(&key_id, timeout_ms, &error_data))

        if key_id == b"":
            return None, None

        return (bytes(key_id), {
            "job_id": error_data.job_id(),
            "type": error_data.type().decode(),
            "error_message": error_data.error_message().decode(),
            "timestamp": error_data.timestamp(),
        })


cdef class GcsLogSubscriber(_GcsSubscriber):
    """Subscriber to logs. Thread safe.

    Usage example:
        subscriber = GcsLogSubscriber()
        # Subscribe to the log channel.
        subscriber.subscribe()
        ...
        while running:
            log = subscriber.poll()
            ......
        # Unsubscribe from the log channel.
        subscriber.close()
    """

    def __init__(self, address, worker_id=None):
        self._construct(address, RAY_LOG_CHANNEL, worker_id)

    def poll(self, timeout=None):
        """Polls for new log messages.

        Returns:
            A dict containing a batch of log lines and their metadata.
        """
        cdef:
            CLogBatch log_batch
            c_string key_id
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1
            c_vector[c_string] c_log_lines
            c_string c_log_line

        with nogil:
            check_status(self.inner.get().PollLogs(&key_id, timeout_ms, &log_batch))

        c_log_lines = PythonGetLogBatchLines(log_batch)

        log_lines = []
        for c_log_line in c_log_lines:
            log_lines.append(c_log_line.decode())

        return {
            "ip": log_batch.ip().decode(),
            "pid": log_batch.pid().decode(),
            "job": log_batch.job_id().decode(),
            "is_err": log_batch.is_error(),
            "lines": log_lines,
            "actor_name": log_batch.actor_name().decode(),
            "task_name": log_batch.task_name().decode(),
        }


# This class should only be used for tests
cdef class _TestOnly_GcsActorSubscriber(_GcsSubscriber):
    """Subscriber to actor updates. Thread safe.

    Usage example:
        subscriber = GcsActorSubscriber()
        # Subscribe to the actor channel.
        subscriber.subscribe()
        ...
        while running:
            actor_data = subscriber.poll()
            ......
        # Unsubscribe from the channel.
        subscriber.close()
    """

    def __init__(self, address, worker_id=None):
        self._construct(address, GCS_ACTOR_CHANNEL, worker_id)

    def poll(self, timeout=None):
        """Polls for new actor messages.

        Returns:
            A byte string of function key.
            None if polling times out or subscriber closed.
        """
        cdef:
            CActorTableData actor_data
            c_string key_id
            int64_t timeout_ms = round(1000 * timeout) if timeout else -1

        with nogil:
            check_status(self.inner.get().PollActor(
                &key_id, timeout_ms, &actor_data))

        info = ActorTableData.FromString(
            actor_data.SerializeAsString())

        return [(key_id, info)]


cdef class CoreWorker:

    def __cinit__(self, worker_type, store_socket, raylet_socket,
                  JobID job_id, GcsClientOptions gcs_options, log_dir,
                  node_ip_address, node_manager_port, raylet_ip_address,
                  local_mode, driver_name, stdout_file, stderr_file,
                  serialized_job_config, metrics_agent_port, runtime_env_hash,
                  startup_token, session_name, cluster_id, entrypoint,
                  worker_launch_time_ms, worker_launched_time_ms):
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
        options.cancel_async_task = cancel_async_task
        options.get_lang_stack = get_py_stack
        options.is_local_mode = local_mode
        options.kill_main = kill_main_task
        options.terminate_asyncio_thread = terminate_asyncio_thread
        options.serialized_job_config = serialized_job_config
        options.metrics_agent_port = metrics_agent_port
        options.connect_on_start = False
        options.runtime_env_hash = runtime_env_hash
        options.startup_token = startup_token
        options.session_name = session_name
        options.cluster_id = CClusterID.FromHex(cluster_id)
        options.entrypoint = entrypoint
        options.worker_launch_time_ms = worker_launch_time_ms
        options.worker_launched_time_ms = worker_launched_time_ms
        CCoreWorkerProcess.Initialize(options)

        self.cgname_to_eventloop_dict = None
        self.fd_to_cgname_dict = None
        self.eventloop_for_default_cg = None
        self.current_runtime_env = None
        self._task_id_to_future_lock = threading.Lock()
        self._task_id_to_future = {}
        self.event_loop_executor = None

    def shutdown_driver(self):
        # If it's a worker, the core worker process should have been
        # shutdown. So we can't call
        # `CCoreWorkerProcess.GetCoreWorker().GetWorkerType()` here.
        # Instead, we use the cached `is_driver` flag to test if it's a
        # driver.
        assert self.is_driver
        with nogil:
            CCoreWorkerProcess.Shutdown()

    def notify_raylet(self):
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().ConnectToRaylet()

    def run_task_loop(self):
        with nogil:
            CCoreWorkerProcess.RunTaskExecutionLoop()

    def drain_and_exit_worker(self, exit_type: str, c_string detail):
        """
        Exit the current worker process. This API should only be used by
        a worker. If this API is called, the worker will wait to finish
        currently executing task, initiate the shutdown, and stop
        itself gracefully. The given exit_type and detail will be
        reported to GCS, and any worker failure error will contain them.

        The behavior of this API while a task is running is undefined.
        Avoid using the API when a task is still running.
        """
        cdef:
            CWorkerExitType c_exit_type
            cdef const shared_ptr[LocalMemoryBuffer] null_ptr

        if exit_type == "user":
            c_exit_type = WORKER_EXIT_TYPE_USER_ERROR
        elif exit_type == "system":
            c_exit_type = WORKER_EXIT_TYPE_SYSTEM_ERROR
        elif exit_type == "intentional_system_exit":
            c_exit_type = WORKER_EXIT_TYPE_INTENTIONAL_SYSTEM_ERROR
        else:
            raise ValueError(f"Invalid exit type: {exit_type}")
        assert not self.is_driver
        with nogil:
            CCoreWorkerProcess.GetCoreWorker().Exit(c_exit_type, detail, null_ptr)

    def get_current_task_id(self) -> TaskID:
        """Return the current task ID.

        If it is a normal task, it returns the TaskID from the main thread.
        If it is a threaded actor, it returns the TaskID for the current thread.
        If it is async actor, it returns the TaskID stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task ID within asyncio task
        # via async_task_id contextvar. We try this first.
        # It is needed because the core Worker's GetCurrentTaskId API
        # doesn't have asyncio context, thus it cannot return the
        # correct TaskID.
        task_id = async_task_id.get()
        if task_id is None:
            # if it is not within asyncio context, fallback to TaskID
            # obtainable from core worker.
            task_id = TaskID(
                CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskId().Binary())
        return task_id

    def get_current_task_attempt_number(self):
        return CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskAttemptNumber()

    def get_task_depth(self):
        return CCoreWorkerProcess.GetCoreWorker().GetTaskDepth()

    def get_current_job_id(self):
        return JobID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentJobId().Binary())

    def get_current_node_id(self):
        return NodeID(
            CCoreWorkerProcess.GetCoreWorker().GetCurrentNodeId().Binary())

    def get_actor_id(self):
        return ActorID(
            CCoreWorkerProcess.GetCoreWorker().GetActorId().Binary())

    def get_actor_name(self):
        return CCoreWorkerProcess.GetCoreWorker().GetActorName()

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

    def update_task_is_debugger_paused(self, TaskID task_id, is_debugger_paused):
        cdef:
            CTaskID c_task_id = task_id.native()

        return CCoreWorkerProcess.GetCoreWorker(
            ).UpdateTaskIsDebuggerPaused(c_task_id, is_debugger_paused)

    def set_webui_display(self, key, message):
        CCoreWorkerProcess.GetCoreWorker().SetWebuiDisplay(key, message)

    def set_actor_title(self, title):
        CCoreWorkerProcess.GetCoreWorker().SetActorTitle(title)

    def set_actor_repr_name(self, repr_name):
        CCoreWorkerProcess.GetCoreWorker().SetActorReprName(repr_name)

    def get_plasma_event_handler(self):
        return self.plasma_event_handler

    def get_objects(self, object_refs, TaskID current_task_id,
                    int64_t timeout_ms=-1):
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            CTaskID c_task_id = current_task_id.native()
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = CCoreWorkerProcess.GetCoreWorker().Get(
                c_object_ids, timeout_ms, results)
        check_status(op_status)

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
                            c_bool inline_small_object=True,
                            c_bool is_experimental_channel=False,
                            ):
        cdef:
            unique_ptr[CAddress] c_owner_address

        c_owner_address = move(self._convert_python_address(owner_address))

        if object_ref is None:
            with nogil:
                check_status(CCoreWorkerProcess.GetCoreWorker()
                             .CreateOwnedAndIncrementLocalRef(
                             is_experimental_channel, metadata,
                             data_size, contained_ids,
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
            CObjectID c_object_id = object_ref.native()
            shared_ptr[CBuffer] data_buf
            shared_ptr[CBuffer] metadata_buf
            unique_ptr[CAddress] c_owner_address = move(self._convert_python_address(
                    object_ref.owner_address()))

        # TODO(suquark): This method does not support put objects to
        # in memory store currently.
        metadata_buf = string_to_buffer(metadata)

        status = CCoreWorkerProcess.GetCoreWorker().CreateExisting(
                    metadata_buf, data_size, object_ref.native(),
                    dereference(c_owner_address), &data_buf,
                    False)
        if not status.ok():
            logger.debug("Error putting restored object into plasma.")
            return
        if data_buf == NULL:
            logger.debug("Object already exists in 'put_file_like_object'.")
            return
        data = Buffer.make(data_buf)
        view = memoryview(data)
        index = 0
        while index < data_size:
            bytes_read = file_like.readinto(view[index:])
            index += bytes_read
        with nogil:
            # Using custom object refs is not supported because we
            # can't track their lifecycle, so we don't pin the object
            # in this case.
            check_status(
                CCoreWorkerProcess.GetCoreWorker().SealExisting(
                            c_object_id, pin_object=False,
                            generator_id=CObjectID.Nil(),
                            owner_address=c_owner_address))

    def experimental_channel_put_serialized(self, serialized_object,
                                            ObjectRef object_ref,
                                            num_readers,
                                            timeout_ms):
        cdef:
            CObjectID c_object_id = object_ref.native()
            shared_ptr[CBuffer] data
            unique_ptr[CAddress] null_owner_address
            uint64_t data_size = serialized_object.total_bytes
            int64_t c_num_readers = num_readers
            int64_t c_timeout_ms = timeout_ms

        metadata = string_to_buffer(serialized_object.metadata)
        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalChannelWriteAcquire(
                             c_object_id,
                             metadata,
                             data_size,
                             c_num_readers,
                             c_timeout_ms,
                             &data,
                             ))
        if data_size > 0:
            (<SerializedObject>serialized_object).write_to(
                Buffer.make(data))

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalChannelWriteRelease(
                             c_object_id,
                             ))

    def experimental_channel_set_error(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CRayStatus status

        with nogil:
            status = (CCoreWorkerProcess.GetCoreWorker()
                      .ExperimentalChannelSetError(c_object_id))
        return status.ok()

    def experimental_channel_register_writer(self,
                                             ObjectRef writer_ref,
                                             remote_reader_ref_info):
        cdef:
            CObjectID c_writer_ref = writer_ref.native()
            c_vector[CNodeID] c_remote_reader_nodes
            c_vector[CReaderRefInfo] c_remote_reader_ref_info
            CReaderRefInfo c_reader_ref_info

        for node_id, reader_ref_info in remote_reader_ref_info.items():
            c_reader_ref_info = CReaderRefInfo()
            c_reader_ref_info.reader_ref_id = (
                <ObjectRef>reader_ref_info.reader_ref).native()
            c_reader_ref_info.owner_reader_actor_id = (
                <ActorID>reader_ref_info.ref_owner_actor_id).native()
            num_reader_actors = reader_ref_info.num_reader_actors
            assert num_reader_actors != 0
            c_reader_ref_info.num_reader_actors = num_reader_actors
            c_remote_reader_ref_info.push_back(c_reader_ref_info)
            c_remote_reader_nodes.push_back(CNodeID.FromHex(node_id))

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalRegisterMutableObjectWriter(
                            c_writer_ref,
                            c_remote_reader_nodes,
                        ))
            check_status(
                    CCoreWorkerProcess.GetCoreWorker()
                    .ExperimentalRegisterMutableObjectReaderRemote(
                        c_writer_ref,
                        c_remote_reader_ref_info,
                    ))

    def experimental_channel_register_reader(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker()
                .ExperimentalRegisterMutableObjectReader(c_object_id))

    def experimental_channel_read_release(self, object_refs):
        """
        For experimental.channel.Channel.

        Signal to the writer that the channel is ready to write again. The read
        began when the caller calls ray.get and a written value is available. If
        ray.get is not called first, then this call will block until a value is
        written, then drop the value.
        """
        cdef:
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = (CCoreWorkerProcess.GetCoreWorker()
                         .ExperimentalChannelReadRelease(c_object_ids))
        check_status(op_status)

    def put_serialized_object_and_increment_local_ref(
            self, serialized_object,
            ObjectRef object_ref=None,
            c_bool pin_object=True,
            owner_address=None,
            c_bool inline_small_object=True,
            c_bool _is_experimental_channel=False,
            ):
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data
            shared_ptr[CBuffer] metadata
            unique_ptr[CAddress] c_owner_address
            c_vector[CObjectID] contained_object_ids
            c_vector[CObjectReference] contained_object_refs

        metadata = string_to_buffer(serialized_object.metadata)
        total_bytes = serialized_object.total_bytes
        contained_object_ids = ObjectRefsToVector(
                serialized_object.contained_object_refs)
        object_already_exists = self._create_put_buffer(
            metadata, total_bytes, object_ref,
            contained_object_ids,
            &c_object_id, &data, True, owner_address, inline_small_object,
            _is_experimental_channel)

        logger.debug(
            f"Serialized object size of {c_object_id.Hex()} is {total_bytes} bytes")

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
                                        generator_id=CObjectID.Nil(),
                                        owner_address=move(c_owner_address)))

        return c_object_id.Binary()

    def wait(self, object_refs_or_generators, int num_returns, int64_t timeout_ms,
             TaskID current_task_id, c_bool fetch_local):
        cdef:
            c_vector[CObjectID] wait_ids
            c_vector[c_bool] results
            CTaskID c_task_id = current_task_id.native()

        object_refs = []
        for ref_or_generator in object_refs_or_generators:
            if (not isinstance(ref_or_generator, ObjectRef)
                    and not isinstance(ref_or_generator, ObjectRefGenerator)):
                raise TypeError(
                    "wait() expected a list of ray.ObjectRef "
                    "or ObjectRefGenerator, "
                    f"got list containing {type(ref_or_generator)}"
                )

            if isinstance(ref_or_generator, ObjectRefGenerator):
                # Before calling wait,
                # get the next reference from a generator.
                object_refs.append(ref_or_generator._get_next_ref())
            else:
                object_refs.append(ref_or_generator)

        wait_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = CCoreWorkerProcess.GetCoreWorker().Wait(
                wait_ids, num_returns, timeout_ms, &results, fetch_local)
        check_status(op_status)

        assert len(results) == len(object_refs)

        ready, not_ready = [], []
        for i, object_ref_or_generator in enumerate(object_refs_or_generators):
            if results[i]:
                ready.append(object_ref_or_generator)
            else:
                not_ready.append(object_ref_or_generator)

        return ready, not_ready

    def free_objects(self, object_refs, c_bool local_only):
        cdef:
            c_vector[CObjectID] free_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Delete(
                free_ids, local_only))

    def get_local_ongoing_lineage_reconstruction_tasks(self):
        cdef:
            unordered_map[CLineageReconstructionTask, uint64_t] tasks
            unordered_map[CLineageReconstructionTask, uint64_t].iterator it

        with nogil:
            tasks = (CCoreWorkerProcess.GetCoreWorker().
                     GetLocalOngoingLineageReconstructionTasks())

        result = []
        it = tasks.begin()
        while it != tasks.end():
            task = common_pb2.LineageReconstructionTask()
            task.ParseFromString(dereference(it).first.SerializeAsString())
            result.append((task, dereference(it).second))
            postincrement(it)

        return result

    def get_local_object_locations(self, object_refs):
        cdef:
            c_vector[optional[CObjectLocation]] results
            c_vector[CObjectID] lookup_ids = ObjectRefsToVector(object_refs)

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().GetLocalObjectLocations(
                    lookup_ids, &results))

        object_locations = {}
        for i in range(results.size()):
            # core_worker will return a nullptr for objects that couldn't be
            # located
            if not results[i].has_value():
                continue
            else:
                object_locations[object_refs[i]] = \
                    CObjectLocationPtrToDict(&results[i].value())
        return object_locations

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

    def get_memory_store_size(self):
        return CCoreWorkerProcess.GetCoreWorker().GetMemoryStoreSize()

    cdef python_label_match_expressions_to_c(
            self, python_expressions,
            CLabelMatchExpressions *c_expressions):
        cdef:
            CLabelMatchExpression* c_expression
            CLabelIn * c_label_in
            CLabelNotIn * c_label_not_in

        for expression in python_expressions:
            c_expression = c_expressions[0].add_expressions()
            c_expression.set_key(expression.key)
            if isinstance(expression.operator, In):
                c_label_in = c_expression.mutable_operator_()[0].mutable_label_in()
                for value in expression.operator.values:
                    c_label_in[0].add_values(value)
            elif isinstance(expression.operator, NotIn):
                c_label_not_in = \
                    c_expression.mutable_operator_()[0].mutable_label_not_in()
                for value in expression.operator.values:
                    c_label_not_in[0].add_values(value)
            elif isinstance(expression.operator, Exists):
                c_expression.mutable_operator_()[0].mutable_label_exists()
            elif isinstance(expression.operator, DoesNotExist):
                c_expression.mutable_operator_()[0].mutable_label_does_not_exist()

    cdef python_scheduling_strategy_to_c(
            self, python_scheduling_strategy,
            CSchedulingStrategy *c_scheduling_strategy):
        cdef:
            CPlacementGroupSchedulingStrategy \
                *c_placement_group_scheduling_strategy
            CNodeAffinitySchedulingStrategy *c_node_affinity_scheduling_strategy
            CNodeLabelSchedulingStrategy *c_node_label_scheduling_strategy
        assert python_scheduling_strategy is not None
        if python_scheduling_strategy == "DEFAULT":
            c_scheduling_strategy[0].mutable_default_scheduling_strategy()
        elif python_scheduling_strategy == "SPREAD":
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
        elif isinstance(python_scheduling_strategy,
                        NodeAffinitySchedulingStrategy):
            c_node_affinity_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_node_affinity_scheduling_strategy()
            c_node_affinity_scheduling_strategy[0].set_node_id(
                NodeID.from_hex(python_scheduling_strategy.node_id).binary())
            c_node_affinity_scheduling_strategy[0].set_soft(
                python_scheduling_strategy.soft)
            c_node_affinity_scheduling_strategy[0].set_spill_on_unavailable(
                python_scheduling_strategy._spill_on_unavailable)
            c_node_affinity_scheduling_strategy[0].set_fail_on_unavailable(
                python_scheduling_strategy._fail_on_unavailable)
        elif isinstance(python_scheduling_strategy,
                        NodeLabelSchedulingStrategy):
            c_node_label_scheduling_strategy = \
                c_scheduling_strategy[0] \
                .mutable_node_label_scheduling_strategy()
            self.python_label_match_expressions_to_c(
                python_scheduling_strategy.hard,
                c_node_label_scheduling_strategy[0].mutable_hard())
            self.python_label_match_expressions_to_c(
                python_scheduling_strategy.soft,
                c_node_label_scheduling_strategy[0].mutable_soft())
        else:
            raise ValueError(
                f"Invalid scheduling_strategy value "
                f"{python_scheduling_strategy}. "
                f"Valid values are [\"DEFAULT\""
                f" | \"SPREAD\""
                f" | PlacementGroupSchedulingStrategy"
                f" | NodeAffinitySchedulingStrategy]")

    def submit_task(self,
                    Language language,
                    FunctionDescriptor function_descriptor,
                    args,
                    c_string name,
                    int num_returns,
                    resources,
                    int max_retries,
                    c_bool retry_exceptions,
                    retry_exception_allowlist,
                    scheduling_strategy,
                    c_string debugger_breakpoint,
                    c_string serialized_runtime_env_info,
                    int64_t generator_backpressure_num_objects,
                    c_bool enable_task_events
                    ):
        cdef:
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            CTaskOptions task_options
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            CSchedulingStrategy c_scheduling_strategy
            c_vector[CObjectID] incremented_put_arg_ids
            c_string serialized_retry_exception_allowlist
            CTaskID current_c_task_id
            TaskID current_task = self.get_current_task_id()

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)

            task_options = CTaskOptions(
                name, num_returns, c_resources,
                b"",
                generator_backpressure_num_objects,
                serialized_runtime_env_info,
                enable_task_events)

            current_c_task_id = current_task.native()

            with nogil:
                return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
                    ray_function, args_vector, task_options,
                    max_retries, retry_exceptions,
                    c_scheduling_strategy,
                    debugger_breakpoint,
                    serialized_retry_exception_allowlist,
                    current_c_task_id,
                )

            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            # The initial local reference is already acquired internally when
            # adding the pending task.
            return VectorToObjectRefs(return_refs, skip_adding_local_ref=True)

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
                     c_string serialized_runtime_env_info,
                     concurrency_groups_dict,
                     int32_t max_pending_calls,
                     scheduling_strategy,
                     c_bool enable_task_events,
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
            c_vector[CObjectID] incremented_put_arg_ids
            optional[c_bool] is_detached_optional = nullopt

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)
            prepare_actor_concurrency_groups(
                concurrency_groups_dict, &c_concurrency_groups)

            if is_detached is not None:
                is_detached_optional = make_optional[c_bool](
                    True if is_detached else False)

            with nogil:
                status = CCoreWorkerProcess.GetCoreWorker().CreateActor(
                    ray_function, args_vector,
                    CActorCreationOptions(
                        max_restarts, max_task_retries, max_concurrency,
                        c_resources, c_placement_resources,
                        dynamic_worker_options, is_detached_optional, name,
                        ray_namespace,
                        is_asyncio,
                        c_scheduling_strategy,
                        serialized_runtime_env_info,
                        c_concurrency_groups,
                        # execute out of order for
                        # async or threaded actors.
                        is_asyncio or max_concurrency > 1,
                        max_pending_calls,
                        enable_task_events),
                    extension_data,
                    &c_actor_id)

            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            check_status(status)

            return ActorID(c_actor_id.Binary())

    def create_placement_group(
                            self,
                            c_string name,
                            c_vector[unordered_map[c_string, double]] bundles,
                            c_string strategy,
                            c_bool is_detached,
                            double max_cpu_fraction_per_node,
                            soft_target_node_id):
        cdef:
            CPlacementGroupID c_placement_group_id
            CPlacementStrategy c_strategy
            CNodeID c_soft_target_node_id = CNodeID.Nil()

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

        if soft_target_node_id is not None:
            c_soft_target_node_id = CNodeID.FromHex(soft_target_node_id)

        with nogil:
            check_status(
                        CCoreWorkerProcess.GetCoreWorker().
                        CreatePlacementGroup(
                            CPlacementGroupCreationOptions(
                                name,
                                c_strategy,
                                bundles,
                                is_detached,
                                max_cpu_fraction_per_node,
                                c_soft_target_node_id),
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
                                   int64_t timeout_seconds):
        cdef CRayStatus status
        cdef CPlacementGroupID cplacement_group_id = (
            CPlacementGroupID.FromBinary(placement_group_id.binary()))
        cdef int64_t ctimeout_seconds = timeout_seconds
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
                          int max_retries,
                          c_bool retry_exceptions,
                          retry_exception_allowlist,
                          double num_method_cpus,
                          c_string concurrency_group_name,
                          int64_t generator_backpressure_num_objects,
                          c_bool enable_task_events):

        cdef:
            CActorID c_actor_id = actor_id.native()
            unordered_map[c_string, double] c_resources
            CRayFunction ray_function
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            c_vector[CObjectID] incremented_put_arg_ids
            CTaskID current_c_task_id = CTaskID.Nil()
            TaskID current_task = self.get_current_task_id()
            c_string serialized_retry_exception_allowlist
            c_string serialized_runtime_env = b"{}"

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        with self.profile_event(b"submit_task"):
            if num_method_cpus > 0:
                c_resources[b"CPU"] = num_method_cpus
            ray_function = CRayFunction(
                language.lang, function_descriptor.descriptor)
            prepare_args_and_increment_put_refs(
                self, language, args, &args_vector, function_descriptor,
                &incremented_put_arg_ids)

            current_c_task_id = current_task.native()

            with nogil:
                status = CCoreWorkerProcess.GetCoreWorker().SubmitActorTask(
                    c_actor_id,
                    ray_function,
                    args_vector,
                    CTaskOptions(
                        name,
                        num_returns,
                        c_resources,
                        concurrency_group_name,
                        generator_backpressure_num_objects,
                        serialized_runtime_env,
                        enable_task_events),
                    max_retries,
                    retry_exceptions,
                    serialized_retry_exception_allowlist,
                    return_refs,
                    current_c_task_id)
            # These arguments were serialized and put into the local object
            # store during task submission. The backend increments their local
            # ref count initially to ensure that they remain in scope until we
            # add to their submitted task ref count. Now that the task has
            # been submitted, it's safe to remove the initial local ref.
            for put_arg_id in incremented_put_arg_ids:
                CCoreWorkerProcess.GetCoreWorker().RemoveLocalReference(
                    put_arg_id)

            if status.ok():
                # The initial local reference is already acquired internally
                # when adding the pending task.
                return VectorToObjectRefs(return_refs,
                                          skip_adding_local_ref=True)
            else:
                if status.IsOutOfResource():
                    actor = self.get_actor_handle(actor_id)
                    actor_handle = (CCoreWorkerProcess.GetCoreWorker()
                                    .GetActorHandle(c_actor_id))
                    raise PendingCallsLimitExceeded(
                        f"The task {function_descriptor.function_name} could not be "
                        f"submitted to {repr(actor)} because more than"
                        f" {(dereference(actor_handle).MaxPendingCalls())}"
                        " tasks are queued on the actor. This limit can be adjusted"
                        " with the `max_pending_calls` actor option.")
                else:
                    raise Exception(f"Failed to submit task to actor {actor_id} "
                                    f"due to {status.message()}")

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

        with nogil:
            status = CCoreWorkerProcess.GetCoreWorker().CancelTask(
                                            c_object_id, force_kill, recursive)

        if status.IsInvalidArgument():
            raise ValueError(status.message().decode())

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

    def get_local_actor_state(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
            optional[int] state = nullopt
        state = CCoreWorkerProcess.GetCoreWorker().GetLocalActorState(c_actor_id)
        if state.has_value():
            return state.value()
        else:
            return None

    cdef make_actor_handle(self, ActorHandleSharedPtr c_actor_handle,
                           c_bool weak_ref):
        worker = ray._private.worker.global_worker
        worker.check_connected()
        manager = worker.function_actor_manager

        actor_id = ActorID(dereference(c_actor_handle).GetActorID().Binary())
        job_id = JobID(dereference(c_actor_handle).CreationJobID().Binary())
        language = Language.from_native(
            dereference(c_actor_handle).ActorLanguage())
        actor_creation_function_descriptor = CFunctionDescriptorToPython(
            dereference(c_actor_handle).ActorCreationTaskFunctionDescriptor())
        max_task_retries = dereference(c_actor_handle).MaxTaskRetries()
        enable_task_events = dereference(c_actor_handle).EnableTaskEvents()
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
            method_meta = ray.actor._ActorClassMethodMetadata.create(
                actor_class, actor_creation_function_descriptor)
            return ray.actor.ActorHandle(language, actor_id, max_task_retries,
                                         enable_task_events,
                                         method_meta.method_is_generator,
                                         method_meta.decorators,
                                         method_meta.signatures,
                                         method_meta.num_returns,
                                         method_meta.max_task_retries,
                                         method_meta.retry_exceptions,
                                         method_meta.generator_backpressure_num_objects, # noqa
                                         method_meta.enable_task_events,
                                         actor_method_cpu,
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref)
        else:
            return ray.actor.ActorHandle(language, actor_id,
                                         0,   # max_task_retries,
                                         True,  # enable_task_events
                                         {},  # method is_generator
                                         {},  # method decorators
                                         {},  # method signatures
                                         {},  # method num_returns
                                         {},  # method max_task_retries
                                         {},  # method retry_exceptions
                                         {},  # generator_backpressure_num_objects
                                         {},  # enable_task_events
                                         0,  # actor method cpu
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref,
                                         )

    def deserialize_and_register_actor_handle(self, const c_string &bytes,
                                              ObjectRef
                                              outer_object_ref,
                                              c_bool weak_ref):
        cdef:
            CObjectID c_outer_object_id = (outer_object_ref.native() if
                                           outer_object_ref else
                                           CObjectID.Nil())
        c_actor_id = (CCoreWorkerProcess
                      .GetCoreWorker()
                      .DeserializeAndRegisterActorHandle(
                          bytes, c_outer_object_id,
                          add_local_ref=not weak_ref))
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id),
            weak_ref)

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

        return self.make_actor_handle(named_actor_handle_pair.first,
                                      weak_ref=True)

    def get_actor_handle(self, ActorID actor_id):
        cdef:
            CActorID c_actor_id = actor_id.native()
        return self.make_actor_handle(
            CCoreWorkerProcess.GetCoreWorker().GetActorHandle(c_actor_id),
            weak_ref=True)

    def list_named_actors(self, c_bool all_namespaces):
        """Returns (namespace, name) for named actors in the system.

        If all_namespaces is True, returns all actors in all namespaces,
        else returns only the actors in the current namespace.
        """
        cdef:
            pair[c_vector[pair[c_string, c_string]], CRayStatus] result_pair

        with nogil:
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
            CAddress c_owner_address
        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                c_object_id, &c_owner_address)
        check_status(op_status)
        return c_owner_address.SerializeAsString()

    def serialize_object_ref(self, ObjectRef object_ref):
        cdef:
            CObjectID c_object_id = object_ref.native()
            CAddress c_owner_address = CAddress()
            c_string serialized_object_status
        op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnershipInfo(
                c_object_id, &c_owner_address, &serialized_object_status)
        check_status(op_status)
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
                           const CObjectID &generator_id,
                           size_t data_size, shared_ptr[CBuffer] &metadata,
                           const c_vector[CObjectID] &contained_id,
                           const CAddress &caller_address,
                           int64_t *task_output_inlined_bytes,
                           shared_ptr[CRayObject] *return_ptr):
        """Store a task return value in plasma or as an inlined object."""
        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().AllocateReturnObject(
                    return_id, data_size, metadata, contained_id, caller_address,
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
                            return_id, return_ptr[0], generator_id, caller_address))
            return True
        else:
            with nogil:
                success = (
                    CCoreWorkerProcess.GetCoreWorker().PinExistingReturnObject(
                        return_id, return_ptr, generator_id, caller_address))
            return success

    cdef store_task_outputs(self,
                            worker, outputs,
                            const CAddress &caller_address,
                            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]]
                            *returns,
                            CObjectID ref_generator_id=CObjectID.Nil()):
        cdef:
            CObjectID return_id
            size_t data_size
            shared_ptr[CBuffer] metadata
            c_vector[CObjectID] contained_id
            int64_t task_output_inlined_bytes
            int64_t num_returns = -1
            shared_ptr[CRayObject] *return_ptr

        num_outputs_stored = 0
        if not ref_generator_id.IsNil():
            # The task specified a dynamic number of return values. Determine
            # the expected number of return values.
            if returns[0].size() > 0:
                # We are re-executing the task. We should return the same
                # number of objects as before.
                num_returns = returns[0].size()
            else:
                # This is the first execution of the task, so we don't know how
                # many return objects it should have yet.
                # NOTE(swang): returns could also be empty if the task returned
                # an empty generator and was re-executed. However, this should
                # not happen because we never reconstruct empty
                # DynamicObjectRefGenerators (since these aren't stored in plasma).
                num_returns = -1
        else:
            # The task specified how many return values it should have.
            num_returns = returns[0].size()

        if num_returns == 0:
            return num_outputs_stored

        task_output_inlined_bytes = 0
        i = -1
        for i, output in enumerate(outputs):
            if num_returns >= 0 and i >= num_returns:
                raise ValueError(
                    "Task returned more than num_returns={} objects.".format(
                        num_returns))
            # TODO(sang): Remove it when the streaming generator is
            # enabled by default.
            while i >= returns[0].size():
                return_id = (CCoreWorkerProcess.GetCoreWorker()
                             .AllocateDynamicReturnId(
                                caller_address, CTaskID.Nil(), NULL_PUT_INDEX))
                returns[0].push_back(
                        c_pair[CObjectID, shared_ptr[CRayObject]](
                            return_id, shared_ptr[CRayObject]()))
            assert i < returns[0].size()
            return_id = returns[0][i].first
            if returns[0][i].second == nullptr:
                returns[0][i].second = shared_ptr[CRayObject]()
            return_ptr = &returns[0][i].second

            # Skip return values that we already created.  This can occur if
            # there were multiple return values, and we initially errored while
            # trying to create one of them.
            if (return_ptr.get() != NULL and return_ptr.get().GetData().get()
                    != NULL):
                continue

            context = worker.get_serialization_context()

            serialized_object = context.serialize(output)
            data_size = serialized_object.total_bytes
            metadata_str = serialized_object.metadata
            if ray._private.worker.global_worker.debugger_get_breakpoint:
                breakpoint = (
                    ray._private.worker.global_worker.debugger_get_breakpoint)
                metadata_str += (
                    b"," + ray_constants.OBJECT_METADATA_DEBUG_PREFIX +
                    breakpoint.encode())
                # Reset debugging context of this worker.
                ray._private.worker.global_worker.debugger_get_breakpoint = b""
            metadata = string_to_buffer(metadata_str)
            contained_id = ObjectRefsToVector(
                serialized_object.contained_object_refs)

            if not self.store_task_output(
                    serialized_object, return_id,
                    ref_generator_id,
                    data_size, metadata, contained_id, caller_address,
                    &task_output_inlined_bytes, return_ptr):
                # If the object already exists, but we fail to pin the copy, it
                # means the existing copy might've gotten evicted. Try to
                # create another copy.
                self.store_task_output(
                        serialized_object, return_id,
                        ref_generator_id,
                        data_size, metadata,
                        contained_id, caller_address, &task_output_inlined_bytes,
                        return_ptr)
            num_outputs_stored += 1

        i += 1
        if i < num_returns:
            raise ValueError(
                    "Task returned {} objects, but num_returns={}.".format(
                        i, num_returns))

        return num_outputs_stored

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
            async_thread.start()

            self.cgname_to_eventloop_dict[cg_name] = {
                "eventloop": async_eventloop,
                "thread": async_thread,
            }

            for fd in function_descriptors:
                self.fd_to_cgname_dict[fd] = cg_name

    def get_event_loop_executor(self) -> ThreadPoolExecutor:
        if self.event_loop_executor is None:
            # NOTE: We're deliberately allocating thread-pool executor with
            #       a single thread, provided that many of its use-cases are
            #       not thread-safe yet (for ex, reporting streaming generator output)
            self.event_loop_executor = ThreadPoolExecutor(max_workers=1)
        return self.event_loop_executor

    def reset_event_loop_executor(self, executor: ThreadPoolExecutor):
        self.event_loop_executor = executor

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

    def run_async_func_or_coro_in_event_loop(
          self,
          func_or_coro: Union[Callable[[Any, Any], Awaitable[Any]], Awaitable],
          function_descriptor: FunctionDescriptor,
          specified_cgname: str,
          *,
          task_id: Optional[TaskID] = None,
          func_args: Optional[Tuple] = None,
          func_kwargs: Optional[Dict] = None,
    ):
        """Run the async function or coroutine to the event loop.

        The event loop is running in a separate thread.

        Args:
            func_or_coro: Async function (not a generator) or awaitable objects.
            function_descriptor: The function descriptor.
            specified_cgname: The name of a concurrent group.
            task_id: The task ID to track the future. If None is provided
                the future is not tracked with a task ID.
                (e.g., When we deserialize the arguments, we don't want to
                track the task_id -> future mapping).
            func_args: The arguments for the async function.
            func_kwargs: The keyword arguments for the async function.

        NOTE: func_args and func_kwargs are intentionally passed as a tuple/dict and
        not unpacked to avoid collisions between system arguments and user-provided
        arguments. See https://github.com/ray-project/ray/issues/41272.
        """
        cdef:
            CFiberEvent event

        if func_args is None:
            func_args = tuple()
        if func_kwargs is None:
            func_kwargs = dict()

        # Increase recursion limit if necessary. In asyncio mode,
        # we have many parallel callstacks (represented in fibers)
        # that's suspended for execution. Python interpreter will
        # mistakenly count each callstack towards recusion limit.
        # We don't need to worry about stackoverflow here because
        # the max number of callstacks is limited in direct actor
        # transport with max_concurrency flag.
        increase_recursion_limit()

        eventloop, async_thread = self.get_event_loop(
            function_descriptor, specified_cgname)

        async def async_func():
            try:
                if task_id:
                    async_task_id.set(task_id)

                if inspect.isawaitable(func_or_coro):
                    coroutine = func_or_coro
                else:
                    coroutine = func_or_coro(*func_args, **func_kwargs)

                return await coroutine
            finally:
                event.Notify()

        future = asyncio.run_coroutine_threadsafe(async_func(), eventloop)
        if task_id:
            with self._task_id_to_future_lock:
                self._task_id_to_future[task_id] = future

        with nogil:
            (CCoreWorkerProcess.GetCoreWorker()
                .YieldCurrentFiber(event))
        try:
            result = future.result()
        except concurrent.futures.CancelledError:
            raise TaskCancelledError(task_id)
        finally:
            if task_id:
                with self._task_id_to_future_lock:
                    self._task_id_to_future.pop(task_id)
        return result

    def stop_and_join_asyncio_threads_if_exist(self):
        event_loops = []
        threads = []
        if self.event_loop_executor:
            self.event_loop_executor.shutdown(
                wait=True, cancel_futures=True)
        if self.eventloop_for_default_cg is not None:
            event_loops.append(self.eventloop_for_default_cg)
        if self.thread_for_default_cg is not None:
            threads.append(self.thread_for_default_cg)
        if self.cgname_to_eventloop_dict:
            for event_loop_and_thread in self.cgname_to_eventloop_dict.values():
                event_loops.append(event_loop_and_thread["eventloop"])
                threads.append(event_loop_and_thread["thread"])
        for event_loop in event_loops:
            event_loop.call_soon_threadsafe(
                event_loop.stop)
        for thread in threads:
            thread.join()

    def current_actor_is_asyncio(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorIsAsync())

    def current_actor_max_concurrency(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorMaxConcurrency())

    def get_current_root_detached_actor_id(self) -> ActorID:
        # This is only used in test
        return ActorID(CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                       .GetRootDetachedActorID().Binary())

    def get_queued_future(self, task_id: Optional[TaskID]) -> ConcurrentFuture:
        """Get a asyncio.Future that's queued in the event loop."""
        with self._task_id_to_future_lock:
            return self._task_id_to_future.get(task_id)

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

    def get_pending_children_task_ids(self, parent_task_id: TaskID):
        cdef:
            CTaskID c_parent_task_id = parent_task_id.native()
            c_vector[CTaskID] ret
            c_vector[CTaskID].iterator it

        result = []

        with nogil:
            ret = CCoreWorkerProcess.GetCoreWorker().GetPendingChildrenTasks(
                c_parent_task_id)

        it = ret.begin()
        while it != ret.end():
            result.append(TaskID(dereference(it).Binary()))
            postincrement(it)

        return result

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

    def set_get_async_callback(self, ObjectRef object_ref, user_callback: Callable):
        # NOTE: we need to manually increment the Python reference count to avoid the
        # callback object being garbage collected before it's called by the core worker.
        # This means we *must* guarantee that the ref is manually decremented to avoid
        # a leak.
        cpython.Py_INCREF(user_callback)
        CCoreWorkerProcess.GetCoreWorker().GetAsync(
            object_ref.native(),
            async_callback,
            <void*>user_callback
        )

    def push_error(self, JobID job_id, error_type, error_message,
                   double timestamp):
        check_status(CCoreWorkerProcess.GetCoreWorker().PushError(
            job_id.native(), error_type.encode("utf-8"),
            error_message.encode("utf-8"), timestamp))

    def get_job_config(self):
        cdef CJobConfig c_job_config
        # We can cache the deserialized job config object here because
        # the job config will not change after a job is submitted.
        if self.job_config is None:
            c_job_config = CCoreWorkerProcess.GetCoreWorker().GetJobConfig()
            self.job_config = common_pb2.JobConfig()
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

    def get_local_memory_store_bytes_used(self):
        cdef:
            int64_t num_bytes_used

        with nogil:
            num_bytes_used = (
                    CCoreWorkerProcess.GetCoreWorker().GetLocalMemoryStoreBytesUsed())
        return num_bytes_used

    def record_task_log_start(
            self, task_id: TaskID, int attempt_number,
            stdout_path, stderr_path,
            int64_t out_start_offset, int64_t err_start_offset):
        cdef:
            CTaskID c_task_id = task_id.native()
            c_string c_stdout_path = stdout_path.encode("utf-8")
            c_string c_stderr_path = stderr_path.encode("utf-8")

        with nogil:
            CCoreWorkerProcess.GetCoreWorker() \
                .RecordTaskLogStart(c_task_id, attempt_number,
                                    c_stdout_path, c_stderr_path,
                                    out_start_offset, err_start_offset)

    def record_task_log_end(
            self, task_id: TaskID, int attempt_number,
            int64_t out_end_offset, int64_t err_end_offset):
        cdef:
            CTaskID c_task_id = task_id.native()

        with nogil:
            CCoreWorkerProcess.GetCoreWorker() \
                .RecordTaskLogEnd(c_task_id, attempt_number,
                                  out_end_offset, err_end_offset)

    cdef CObjectID allocate_dynamic_return_id_for_generator(
            self,
            const CAddress &owner_address,
            const CTaskID &task_id,
            return_size,
            generator_index,
            is_async_actor):
        """Allocate a dynamic return ID for a generator task.

        NOTE: When is_async_actor is True,
        this API SHOULD NOT BE called
        within an async actor's event IO thread. The caller MUST ensure
        this for correctness. It is due to the limitation WorkerContext
        API when async actor is used.
        See https://github.com/ray-project/ray/issues/10324 for further details.

        Args:
            owner_address: The address of the owner (caller) of the
                generator task.
            task_id: The task ID of the generator task.
            return_size: The size of the static return from the task.
            generator_index: The index of dynamically generated object
                ref.
            is_async_actor: True if the allocation is for async actor.
                If async actor is used, we should calculate the
                put_index ourselves.
        """
        # Generator only has 1 static return.
        assert return_size == 1
        if is_async_actor:
            # This part of code has a couple of assumptions.
            # - This API is not called within an asyncio event loop
            #   thread.
            # - Ray object ref is generated by incrementing put_index
            #   whenever a new return value is added or ray.put is called.
            #
            # When an async actor is used, it uses its own thread to execute
            # async tasks. That means all the ray.put will use a put_index
            # scoped to a asyncio event loop thread.
            # This means the execution thread that this API will be called
            # will only create "return" objects. That means if we use
            # return_size + genreator_index as a put_index, it is guaranteed
            # to be unique.
            #
            # Why do we need it?
            #
            # We have to provide a put_index ourselves here because
            # the current implementation only has 1 worker context at any
            # given time, meaning WorkerContext::TaskID & WorkerContext::PutIndex
            # both could be incorrect (duplicated) when this API is called.
            return CCoreWorkerProcess.GetCoreWorker().AllocateDynamicReturnId(
                owner_address,
                task_id,
                # Should add 1 because put index is always incremented
                # before it is used. So if you have 1 return object
                # the next index will be 2.
                make_optional[ObjectIDIndexType](
                    <int>1 + <int>return_size + <int>generator_index)  # put_index
            )
        else:
            return CCoreWorkerProcess.GetCoreWorker().AllocateDynamicReturnId(
                owner_address,
                task_id,
                make_optional[ObjectIDIndexType](
                    <int>1 + <int>return_size + <int>generator_index))

    def async_delete_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()

        with nogil:
            CCoreWorkerProcess.GetCoreWorker().AsyncDelObjectRefStream(c_generator_id)

    def try_read_next_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            CObjectReference c_object_ref

        with nogil:
            check_status(
                CCoreWorkerProcess.GetCoreWorker().TryReadObjectRefStream(
                    c_generator_id, &c_object_ref))

        return ObjectRef(
            c_object_ref.object_id(),
            c_object_ref.owner_address().SerializeAsString(),
            "",
            # Already added when the ref is updated.
            skip_adding_local_ref=True)

    def is_object_ref_stream_finished(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            c_bool finished

        with nogil:
            finished = CCoreWorkerProcess.GetCoreWorker().StreamingGeneratorIsFinished(
                c_generator_id)
        return finished

    def peek_object_ref_stream(self, ObjectRef generator_id):
        cdef:
            CObjectID c_generator_id = generator_id.native()
            pair[CObjectReference, c_bool] c_object_ref_and_is_ready_pair

        with nogil:
            c_object_ref_and_is_ready_pair = (
                    CCoreWorkerProcess.GetCoreWorker().PeekObjectRefStream(
                        c_generator_id))

        return (ObjectRef(
                    c_object_ref_and_is_ready_pair.first.object_id(),
                    c_object_ref_and_is_ready_pair.first.owner_address().SerializeAsString()), # noqa
                c_object_ref_and_is_ready_pair.second)

cdef void async_callback(shared_ptr[CRayObject] obj,
                         CObjectID object_ref,
                         void *user_callback_ptr) with gil:
    cdef:
        c_vector[shared_ptr[CRayObject]] objects_to_deserialize

    try:
        # Object is retrieved from in memory store.
        # Here we go through the code path used to deserialize objects.
        objects_to_deserialize.push_back(obj)
        data_metadata_pairs = RayObjectsToDataMetadataPairs(
            objects_to_deserialize)
        ids_to_deserialize = [ObjectRef(object_ref.Binary())]
        result = ray._private.worker.global_worker.deserialize_objects(
            data_metadata_pairs, ids_to_deserialize)[0]

        user_callback = <object>user_callback_ptr
        user_callback(result)
    except Exception:
        # Only log the error here because this calllback is called from Cpp
        # and Cython will ignore the exception anyway
        logger.exception(f"failed to run async callback (user func)")
    finally:
        # NOTE: we manually increment the Python reference count of the callback when
        # registering it in the core worker, so we must decrement here to avoid a leak.
        cpython.Py_DECREF(user_callback)


# Note this deletes keys with prefix `RAY{key_prefix}@`
# Example: with key_prefix = `default`, we remove all `RAYdefault@...` keys.
def del_key_prefix_from_storage(host, port, password, use_ssl, key_prefix):
    return RedisDelKeyPrefixSync(host, port, password, use_ssl, key_prefix)


def get_session_key_from_storage(host, port, password, use_ssl, config, key):
    """
    Get the session key from the storage.
    Intended to be used for session_name only.
    Args:
        host: The address of the owner (caller) of the
            generator task.
        port: The task ID of the generator task.
        password: The redis password.
        use_ssl: Whether to use SSL.
        config: The Ray config. Used to get storage namespace.
        key: The key to retrieve.
    """
    cdef:
        c_string data
    result = RedisGetKeySync(host, port, password, use_ssl, config, key, &data)
    if result:
        return data
    else:
        logger.info("Could not retrieve session key from storage.")
        return None
