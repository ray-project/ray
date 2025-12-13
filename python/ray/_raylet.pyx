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
import io
import os
import pickle
import random
import sys
import threading
import time
import traceback
import _thread
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
    NamedTuple,
)

import contextvars
import concurrent.futures
import collections

from libc.stdint cimport (
    int32_t,
    int64_t,
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

from libcpp.functional cimport function
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector as c_vector
from libcpp.pair cimport pair as c_pair

from cpython.object cimport PyTypeObject
from cython.operator import dereference, postincrement
from cpython.pystate cimport (
    PyGILState_Ensure,
    PyGILState_Release,
    PyGILState_STATE,
)

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
    CFallbackOption,
    CGcsClientOptions,
    CGcsNodeInfo,
    CJobTableData,
    CLabelSelector,
    CLogBatch,
    CTaskArg,
    CTaskArgByReference,
    CTaskArgByValue,
    CTaskType,
    CPlacementStrategy,
    CSchedulingStrategy,
    CPlacementGroupSchedulingStrategy,
    CNodeAffinitySchedulingStrategy,
    CNodeLabelSchedulingStrategy,
    CLabelMatchExpressions,
    CLabelMatchExpression,
    CLabelIn,
    CLabelNotIn,
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
    RAY_ERROR_INFO_CHANNEL,
    RAY_LOG_CHANNEL,
    PythonGetLogBatchLines,
    WORKER_EXIT_TYPE_USER_ERROR,
    WORKER_EXIT_TYPE_SYSTEM_ERROR,
    WORKER_EXIT_TYPE_INTENTIONAL_SYSTEM_ERROR,
    kResourceUnitScaling,
    kImplicitResourcePrefix,
    kWorkerSetupHookKeyName,
    PythonGetNodeLabels,
    PythonGetResourcesTotal,
    kGcsPidKey,
    CTensorTransport,
    TENSOR_TRANSPORT_OBJECT_STORE,
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
    CGeneratorBackpressureWaiter,
    CReaderRefInfo,
)
from ray.includes.stream_redirection cimport (
    CStreamRedirectionOptions,
    RedirectStdoutOncePerProcess,
    RedirectStderrOncePerProcess,
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

cimport cpython

include "includes/network_util.pxi"
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
include "includes/setproctitle.pxi"
include "includes/raylet_client.pxi"
include "includes/gcs_subscriber.pxi"
include "includes/rpc_token_authentication.pxi"

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
from ray.core.generated.gcs_service_pb2 import GetAllResourceUsageReply
from ray._private.async_compat import (
    sync_to_async,
    get_new_event_loop,
    is_async_func,
    has_async_methods,
)
from ray._private.client_mode_hook import disable_client_hook
import ray.core.generated.common_pb2 as common_pb2
from ray._common.utils import decode
from ray._private.utils import DeferSigint
from ray._private.object_ref_generator import ObjectRefGenerator, DynamicObjectRefGenerator
from ray._private.custom_types import TensorTransportEnum
from ray._private.gc_collect_manager import PythonGCThread

# Expose GCC & Clang macro to report
# whether C++ optimizations were enabled during compilation.
OPTIMIZED = __OPTIMIZE__

GRPC_STATUS_CODE_UNAVAILABLE = CGrpcStatusCode.UNAVAILABLE
GRPC_STATUS_CODE_UNKNOWN = CGrpcStatusCode.UNKNOWN
GRPC_STATUS_CODE_DEADLINE_EXCEEDED = CGrpcStatusCode.DEADLINE_EXCEEDED
GRPC_STATUS_CODE_RESOURCE_EXHAUSTED = CGrpcStatusCode.RESOURCE_EXHAUSTED
GRPC_STATUS_CODE_UNIMPLEMENTED = CGrpcStatusCode.UNIMPLEMENTED

logger = logging.getLogger(__name__)

import warnings
class NumReturnsWarning(UserWarning):
    """Warning when num_returns=0 but the task returns a non-None value."""
    pass

warnings.filterwarnings("once", category=NumReturnsWarning)

# The currently running task, if any. These are used to synchronize task
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
async_task_name = contextvars.ContextVar('async_task_name', default=None)
async_task_function_name = contextvars.ContextVar('async_task_function_name',                                                  default=None)


# Update the type names of the extension type so they are
# ray.{ObjectRef, ObjectRefGenerator} instead of ray._raylet.*
# For ObjectRefGenerator that can be done directly since it is
# a full Python class. For ObjectRef we need to update the
# tp_name since it is a C extension class and not a full class.
cdef PyTypeObject* object_ref_py_type = <PyTypeObject*>ObjectRef
object_ref_py_type.tp_name = "ray.ObjectRef"
ObjectRefGenerator.__module__ = "ray"


# For backward compatibility.
StreamingObjectRefGenerator = ObjectRefGenerator

cdef c_bool is_plasma_object(shared_ptr[CRayObject] obj):
    """Return True if the given object is a plasma object."""
    assert obj.get() != NULL
    if (obj.get().GetData().get() != NULL
            and obj.get().GetData().get().IsPlasmaBuffer()):
        return True
    return False


class SerializedRayObject(NamedTuple):
    data: Optional[Buffer]
    metadata: Optional[Buffer]
    # If set to None, use the default object store transport. Data will be
    # either inlined in `data` or found in the plasma object store.
    tensor_transport: Optional[TensorTransportEnum]


cdef RayObjectsToSerializedRayObjects(
        const c_vector[shared_ptr[CRayObject]] objects, object_refs: Optional[List[ObjectRef]] = None):
    serialized_ray_objects = []
    for i in range(objects.size()):
        # core_worker will return a nullptr for objects that couldn't be
        # retrieved from the store or if an object was an exception.
        if not objects[i].get():
            serialized_ray_objects.append(SerializedRayObject(None, None, None))
        else:
            data = None
            metadata = None
            if objects[i].get().HasData():
                data = Buffer.make(objects[i].get().GetData())
            if objects[i].get().HasMetadata():
                metadata = Buffer.make(
                    objects[i].get().GetMetadata()).to_pybytes()
            tensor_transport = TensorTransportEnum(<int>(objects[i].get().GetTensorTransport()))
            if (
                tensor_transport == TensorTransportEnum.OBJECT_STORE
                and object_refs is not None
            ):
                tensor_transport = TensorTransportEnum(object_refs[i].tensor_transport())
            serialized_ray_objects.append(SerializedRayObject(data, metadata, tensor_transport))
    return serialized_ray_objects


cdef VectorToObjectRefs(const c_vector[CObjectReference] &object_refs,
                        skip_adding_local_ref):
    result = []
    for i in range(object_refs.size()):
        tensor_transport_val = <int>object_refs[i].tensor_transport()
        result.append(ObjectRef(
            object_refs[i].object_id(),
            object_refs[i].owner_address().SerializeAsString(),
            object_refs[i].call_site(),
            skip_adding_local_ref=skip_adding_local_ref,
            tensor_transport_val=tensor_transport_val))
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
    """
    Ray does some weird things with asio fibers and asyncio to run asyncio actors.
    This results in the Python interpreter thinking there's a lot of recursion depth,
    so we need to increase the limit when we start getting close.

    0x30C0000 is Python 3.12
        On 3.12, when recursion depth increases, c_recursion_remaining will decrease,
        and that's what's actually compared to raise a RecursionError. So increasing
        it by 1000 when it drops below 1000 will keep us from raising the RecursionError.
        https://github.com/python/cpython/blob/bfb9e2f4a4e690099ec2ec53c08b90f4d64fde36/Python/pystate.c#L1353
    0x30B00A4 is Python 3.11
        On 3.11, the recursion depth can be calculated with recursion_limit - recursion_remaining.
        We can get the current limit with Py_GetRecursionLimit and set it with Py_SetRecursionLimit.
        We'll double the limit when there's less than 500 remaining.
    On older versions
        There's simply a recursion_depth variable and we'll increase the max the same
        way we do for 3.11.
    """
    cdef:
        cdef extern from *:
            """
#if PY_VERSION_HEX >= 0x30C0000
    bool IncreaseRecursionLimitIfNeeded(PyThreadState *x) {
        if (x->c_recursion_remaining < 1000) {
            x->c_recursion_remaining += 1000;
            return true;
        }
        return false;
    }
#elif PY_VERSION_HEX >= 0x30B00A4
    bool IncreaseRecursionLimitIfNeeded(PyThreadState *x) {
        int current_limit = Py_GetRecursionLimit();
        int current_depth = x->recursion_limit - x->recursion_remaining;
        if (current_limit - current_depth < 500) {
            Py_SetRecursionLimit(current_limit * 2);
            return true;
        }
        return false;
    }
#else
    bool IncreaseRecursionLimitIfNeeded(PyThreadState *x) {
        int current_limit = Py_GetRecursionLimit();
        if (current_limit - x->recursion_depth < 500) {
            Py_SetRecursionLimit(current_limit * 2);
            return true;
        }
        return false;
    }
#endif
            """
            c_bool IncreaseRecursionLimitIfNeeded(CPyThreadState *x)

        CPyThreadState * s = <CPyThreadState *> PyThreadState_Get()
        c_bool increased_recursion_limit = IncreaseRecursionLimitIfNeeded(s)

    if increased_recursion_limit:
        logger.debug("Increased Python recursion limit")


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


cdef int prepare_labels(
        dict label_dict,
        unordered_map[c_string, c_string] *label_map) except -1:

    if label_dict is None:
        return 0

    label_map[0].reserve(len(label_dict))
    for key, value in label_dict.items():
        if not isinstance(key, str):
            raise ValueError(f"Label key must be string, but got {type(key)}")
        if not isinstance(value, str):
            raise ValueError(f"Label value must be string, but got {type(value)}")
        label_map[0][key.encode("utf-8")] = value.encode("utf-8")

    return 0

cdef int prepare_label_selector(
        dict label_selector_dict,
        CLabelSelector *c_label_selector) except -1:

    c_label_selector[0] = CLabelSelector()

    if label_selector_dict is None:
        return 0

    for key, value in label_selector_dict.items():
        if not isinstance(key, str):
            raise ValueError(f"Label selector key type must be string, but got {type(key)}")
        if not isinstance(value, str):
            raise ValueError(f"Label selector value must be string, but got {type(value)}")
        if key == "":
            raise ValueError("Label selector key must be a non-empty string.")
        if (value.startswith("in(") and value.endswith(")")) or \
           (value.startswith("!in(") and value.endswith(")")):
            inner = value[value.index("(")+1:-1].strip()
            if not inner:
                raise ValueError(f"No values provided for Label Selector '{value[:value.index('(')]}' operator on key '{key}'.")
        # Add key-value constraint to the LabelSelector object.
        c_label_selector[0].AddConstraint(key.encode("utf-8"), value.encode("utf-8"))

    return 0

cdef int prepare_fallback_strategy(
        list fallback_strategy,
        c_vector[CFallbackOption] *fallback_strategy_vector) except -1:

    cdef dict label_selector_dict
    cdef CLabelSelector c_label_selector

    if fallback_strategy is None:
        return 0

    for strategy_dict in fallback_strategy:
        if not isinstance(strategy_dict, dict):
            raise ValueError(
                "Fallback strategy must be a list of dicts, "
                f"but got list containing {type(strategy_dict)}")

        label_selector_dict = strategy_dict.get("label_selector")

        if label_selector_dict is not None and not isinstance(label_selector_dict, dict):
            raise ValueError("Invalid fallback strategy element: invalid 'label_selector'.")

        prepare_label_selector(label_selector_dict, &c_label_selector)

        fallback_strategy_vector.push_back(
             CFallbackOption(c_label_selector)
        )

    return 0

cdef int prepare_resources(
        dict resource_dict,
        unordered_map[c_string, double] *resource_map) except -1:
    cdef:
        list unit_resources

    if resource_dict is None:
        raise ValueError("Must provide resource map.")

    resource_map[0].reserve(len(resource_dict))
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

    fd_list.reserve(len(pyfd_list))
    for pyfd in pyfd_list:
        fd_list.push_back(CFunctionDescriptorBuilder.BuildPython(
            pyfd.module_name, pyfd.class_name, pyfd.function_name, b""))
    return fd_list


cdef int prepare_actor_concurrency_groups(
        dict concurrency_groups_dict,
        c_vector[CConcurrencyGroup] *concurrency_groups):

    cdef:
        c_vector[CFunctionDescriptor] c_fd_list

    if concurrency_groups_dict is None:
        raise ValueError("Must provide it...")

    concurrency_groups.reserve(len(concurrency_groups_dict))
    for key, value in concurrency_groups_dict.items():
        c_fd_list = prepare_function_descriptors(value["function_descriptors"])
        concurrency_groups.push_back(CConcurrencyGroup(
            key.encode("ascii"), value["max_concurrency"], move(c_fd_list)))
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
    serialization_context = worker.get_serialization_context()
    for arg in args:
        from ray.experimental.compiled_dag_ref import CompiledDAGRef
        if isinstance(arg, CompiledDAGRef):
            raise TypeError("CompiledDAGRef cannot be used as Ray task/actor argument.")
        if isinstance(arg, ObjectRef):
            c_arg = (<ObjectRef>arg).native()
            op_status = CCoreWorkerProcess.GetCoreWorker().GetOwnerAddress(
                    c_arg, &c_owner_address)
            check_status(op_status)
            c_tensor_transport = (<ObjectRef>arg).c_tensor_transport()
            args_vector.push_back(
                unique_ptr[CTaskArg](new CTaskArgByReference(
                    c_arg,
                    c_owner_address,
                    arg.call_site(),
                    c_tensor_transport)))

        else:
            try:
                serialized_arg = serialization_context.serialize(arg)
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
                            put_arg_call_site,
                            TENSOR_TRANSPORT_OBJECT_STORE
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
    e: BaseException,
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
        c_string* application_error,
        CTensorTransport c_tensor_transport=TENSOR_TRANSPORT_OBJECT_STORE):
    cdef:
        CoreWorker core_worker = worker.core_worker

    # If the debugger is enabled, drop into the remote pdb here.
    if ray.util.pdb._is_ray_debugger_post_mortem_enabled():
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
        returns,
        None,  # ref_generator_id
        c_tensor_transport)

    if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
        raise ActorDiedError.from_task_error(failure_object)
    return num_errors_stored


cdef class StreamingGeneratorExecutionContext:
    """The context to run a streaming generator function.

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
        generator_id.Binary())

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
            generator_id.Binary())
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
        int64_t generator_backpressure_num_objects,
        CTensorTransport c_tensor_transport) except *:
    worker = ray._private.worker.global_worker
    manager = worker.function_actor_manager
    actor = None
    actor_id = None
    cdef:
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        TaskID task_id = core_worker.get_current_task_id()
        uint64_t attempt_number = core_worker.get_current_task_attempt_number()

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
            func = execution_info.function

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

                if is_async_func(func.method):
                    async_function = func
                else:
                    # Just execute the method if it's ray internal method.
                    if func.name.startswith("__ray"):
                        return func(actor, *arguments, **kwarguments)
                    async_function = sync_to_async(func)

                if inspect.isasyncgenfunction(func.method):
                    # The coroutine will be handled separately by
                    # execute_dynamic_generator_and_store_task_outputs
                    return async_function(actor, *arguments, **kwarguments)
                else:
                    return core_worker.run_async_func_or_coro_in_event_loop(
                        async_function, function_descriptor,
                        name_of_concurrency_group_to_execute, task_id=task_id,
                        task_name=task_name, func_args=(actor, *arguments),
                        func_kwargs=kwarguments)

            return func(actor, *arguments, **kwarguments)

    with core_worker.profile_event(b"task::" + name, extra_data=extra_data), \
         ray._private.worker._changeproctitle(title, next_title):
        task_exception = False
        try:
            with core_worker.profile_event(b"task:deserialize_arguments"):
                if c_args.empty():
                    args, kwargs = [], {}
                else:
                    object_refs = VectorToObjectRefs(
                            c_arg_refs,
                            skip_adding_local_ref=False)
                    metadata_pairs = RayObjectsToSerializedRayObjects(c_args, object_refs)
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
                    args, kwargs = ray._common.signature.recover_args(args)

            if (<int>task_type == <int>TASK_TYPE_ACTOR_CREATION_TASK):
                actor_id = core_worker.get_actor_id()
                actor = worker.actors[actor_id]

            worker.record_task_log_start(task_id, attempt_number)

            # Execute the task.
            with core_worker.profile_event(b"task:execute"):
                task_exception = True
                task_exception_instance = None
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
                                task_id=task_id,
                                task_name=task_name)
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
                except (KeyboardInterrupt, SystemExit):
                    # Special casing these two because Ray can raise them
                    raise
                except BaseException as e:
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
                    task_exception_instance = e
                finally:
                    # Record the end of the task log.
                    worker.record_task_log_end(task_id, attempt_number)
                    if task_exception_instance is not None:
                        raise task_exception_instance
                    if core_worker.get_current_actor_should_exit():
                        raise_sys_exit_with_custom_error_message("exit_actor() is called.")

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
                    dynamic_refs = collections.deque()
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
                    returns,
                    None, # ref_generator_id
                    c_tensor_transport
                )
        except (KeyboardInterrupt, SystemExit):
            # Special casing these two because Ray can raise them
            raise
        except BaseException as e:
            num_errors_stored = store_task_errors(
                    worker, e, task_exception, actor, actor_id, function_name,
                    task_type, title, caller_address, returns, application_error, c_tensor_transport)
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
        int64_t generator_backpressure_num_objects,
        CTensorTransport c_tensor_transport):

    is_retryable_error[0] = False

    worker = ray._private.worker.global_worker
    manager = worker.function_actor_manager
    cdef:
        dict execution_infos = manager.execution_infos
        CoreWorker core_worker = worker.core_worker
        JobID job_id = core_worker.get_current_job_id()
        TaskID task_id = core_worker.get_current_task_id()

    task_name = name.decode("utf-8")
    title = f"ray::{task_name}"

    # Automatically restrict the GPUs (CUDA), neuron_core, TPU accelerator
    # runtime_ids, OMP_NUM_THREADS to restrict availability to this task.
    # Once actor is created, users can change the visible accelerator ids within
    # an actor task and we don't want to reset it.
    if (<int>task_type != <int>TASK_TYPE_ACTOR_TASK):
        original_visible_accelerator_env_vars = ray._private.utils.set_visible_accelerator_ids()
        omp_num_threads_overriden = ray._private.utils.set_omp_num_threads_if_unset()
    else:
        original_visible_accelerator_env_vars = None
        omp_num_threads_overriden = False

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
                     generator_backpressure_num_objects,
                     c_tensor_transport)

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

        if (<int>task_type == <int>TASK_TYPE_NORMAL_TASK):
            if original_visible_accelerator_env_vars:
                # Reset the visible accelerator env vars for normal tasks, since they may be reused.
                ray._private.utils.reset_visible_accelerator_env_vars(original_visible_accelerator_env_vars)
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

cdef void free_actor_object_callback(const CObjectID &c_object_id) nogil:
    # Expected to be called on the owner process. Will free on the primary copy holder.
    with gil:
        object_id = c_object_id.Hex().decode()
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        gpu_object_manager.free_object_primary_copy(object_id)

cdef shared_ptr[LocalMemoryBuffer] ray_error_to_memory_buf(ray_error):
    cdef bytes py_bytes = ray_error.to_bytes()
    return make_shared[LocalMemoryBuffer](
        <uint8_t*>py_bytes, len(py_bytes), True)

cdef void pygilstate_release(PyGILState_STATE gstate) nogil:
    with gil:
        PyGILState_Release(gstate)

cdef function[void()] initialize_pygilstate_for_thread() nogil:
    """
    This function initializes a C++ thread to make it be considered as a
    Python thread from the Python interpreter's perspective, regardless of whether
    it is executing Python code or not. This function must be called in a thread
    before executing any Ray tasks on that thread.

    Returns:
        A function that calls `PyGILState_Release` to release the GIL state.
        This function should be called in a thread before the thread exits.

    Reference: https://docs.python.org/3/c-api/init.html#non-python-created-threads
    """
    cdef function[void()] callback
    with gil:
        gstate = PyGILState_Ensure()
        callback = bind(pygilstate_release, ref(gstate))
    return callback

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
        int64_t generator_backpressure_num_objects,
        CTensorTransport c_tensor_transport) nogil:
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
                        generator_backpressure_num_objects,
                        c_tensor_transport)
            except Exception as e:
                sys_exit = SystemExit()
                if isinstance(e, RayActorError) and \
                   e.actor_init_failed:
                    traceback_str = str(e)
                    logger.error("Exception raised "
                                 f"in creation task: {traceback_str}")
                    creation_task_exception_pb_bytes = ray_error_to_memory_buf(e)
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
        except Exception as e:
            msg = "Unexpected exception raised in task execution handler: {}".format(e)
            logger.error(msg)
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
            if sys.is_finalizing():
                return CRayStatus.IntentionalSystemExit(
                    "Python is exiting.".encode("utf-8")
                )
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


cdef void gc_collect() nogil:
     with gil:
        if RayConfig.instance().start_python_gc_manager_thread():
            start = time.perf_counter()
            worker = ray._private.worker.global_worker
            worker.core_worker.trigger_gc()
            end = time.perf_counter()
            logger.debug("GC event triggered in {} seconds".format(end - start))
        else:
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


cdef c_bool cancel_async_actor_task(const CTaskID &c_task_id) nogil:
    """Attempt to cancel a task running in this asyncio actor.

    Returns True if the task was currently running and was cancelled, else False.

    Note that the underlying asyncio task may not actually have been cancelled: it
    could already have completed or else might not gracefully handle cancellation.
    The return value only indicates that the task was found and cancelled.
    """
    with gil:
        task_id = TaskID(c_task_id.Binary())
        worker = ray._private.worker.global_worker
        fut = worker.core_worker.get_future_for_running_task(task_id)
        if fut is None:
            # Either the task hasn't started executing yet or already finished.
            return False

        fut.cancel()
        return True


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
        worker.raise_errors([SerializedRayObject(data, metadata, TensorTransportEnum.OBJECT_STORE)], object_ids)


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


cdef void call_actor_shutdown() noexcept nogil:
    """C++ wrapper function that calls the Python actor shutdown callback."""
    with gil:
        core_worker = ray._private.worker.global_worker.core_worker
        if core_worker.current_actor_is_asyncio():
            core_worker.stop_and_join_asyncio_threads_if_exist()

        _call_actor_shutdown()


def _call_actor_shutdown():
    """Internal function that calls actor's __ray_shutdown__ method."""
    try:
        worker = ray._private.worker.global_worker

        if not worker.actors:
            return

        actor_id, actor_instance = next(iter(worker.actors.items()))
        if actor_instance is not None:
            # Only call __ray_shutdown__ if the method exists and is callable
            # This preserves backward compatibility: actors without __ray_shutdown__
            # use Python's normal exit flow (including atexit handlers)
            if hasattr(actor_instance, '__ray_shutdown__') and callable(getattr(actor_instance, '__ray_shutdown__')):
                try:
                    actor_instance.__ray_shutdown__()
                except Exception:
                    logger.exception("Error during actor __ray_shutdown__ method")
            # Always clean up the actor instance
            worker.actors.pop(actor_id, None)
    except Exception:
        # Catch any system-level exceptions to prevent propagation to C++
        logger.exception("System error during actor shutdown callback")


cdef class StreamRedirector:
    @staticmethod
    def redirect_stdout(const c_string &file_path, uint64_t rotation_max_size, uint64_t rotation_max_file_count, c_bool tee_to_stdout, c_bool tee_to_stderr):
        cdef CStreamRedirectionOptions opt = CStreamRedirectionOptions()
        opt.file_path = file_path
        opt.rotation_max_size = rotation_max_size
        opt.rotation_max_file_count = rotation_max_file_count
        opt.tee_to_stdout = tee_to_stdout
        opt.tee_to_stderr = tee_to_stderr
        RedirectStdoutOncePerProcess(opt)

    @staticmethod
    def redirect_stderr(const c_string &file_path, uint64_t rotation_max_size, uint64_t rotation_max_file_count, c_bool tee_to_stdout, c_bool tee_to_stderr):
        cdef CStreamRedirectionOptions opt = CStreamRedirectionOptions()
        opt.file_path = file_path
        opt.rotation_max_size = rotation_max_size
        opt.rotation_max_file_count = rotation_max_file_count
        opt.tee_to_stdout = tee_to_stdout
        opt.tee_to_stderr = tee_to_stderr
        RedirectStderrOncePerProcess(opt)

# An empty profile event context to be used when the timeline is disabled.
cdef class EmptyProfileEvent:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


cdef class GcsClient:
    """
    Client to the GCS server.

    This is a thin wrapper around InnerGcsClient with only call frequency collection.
    """

    cdef InnerGcsClient inner

    def __cinit__(self, address: str,
                  cluster_id: Optional[str] = None):
        # For timeout (DEADLINE_EXCEEDED): retries once with timeout_ms.
        #
        # For other RpcError (UNAVAILABLE, UNKNOWN): retries indefinitely until it
        # thinks GCS is down and kills the whole process.
        timeout_ms = RayConfig.instance().py_gcs_connect_timeout_s() * 1000
        self.inner = InnerGcsClient.standalone(address, cluster_id, timeout_ms)

    def __getattr__(self, name):
        # We collect the frequency of each method call.
        if "TEST_RAY_COLLECT_KV_FREQUENCY" in os.environ:
            with ray._private.utils._CALLED_FREQ_LOCK:
                ray._private.utils._CALLED_FREQ[name] += 1
        return getattr(self.inner, name)

cdef class CoreWorker:

    def __cinit__(self, worker_type, store_socket, raylet_socket,
                  JobID job_id, GcsClientOptions gcs_options, log_dir,
                  node_ip_address, node_manager_port,
                  local_mode, driver_name,
                  serialized_job_config, metrics_agent_port, runtime_env_hash,
                  startup_token, session_name, cluster_id, entrypoint,
                  worker_launch_time_ms, worker_launched_time_ms, debug_source):
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
        options.install_failure_signal_handler = (
            not ray_constants.RAY_DISABLE_FAILURE_SIGNAL_HANDLER
        )
        # https://stackoverflow.com/questions/2356399/tell-if-python-is-in-interactive-mode
        options.interactive = hasattr(sys, "ps1")
        options.node_ip_address = node_ip_address.encode("utf-8")
        options.node_manager_port = node_manager_port
        options.driver_name = driver_name
        options.initialize_thread_callback = initialize_pygilstate_for_thread
        options.task_execution_callback = task_execution_handler
        options.free_actor_object_callback = free_actor_object_callback
        options.check_signals = check_signals
        options.gc_collect = gc_collect
        options.spill_objects = spill_objects_handler
        options.restore_spilled_objects = restore_spilled_objects_handler
        options.delete_spilled_objects = delete_spilled_objects_handler
        options.unhandled_exception_handler = unhandled_exception_handler
        options.cancel_async_actor_task = cancel_async_actor_task
        options.get_lang_stack = get_py_stack
        options.is_local_mode = local_mode
        options.kill_main = kill_main_task
        options.actor_shutdown_callback = call_actor_shutdown
        options.serialized_job_config = serialized_job_config
        options.metrics_agent_port = metrics_agent_port
        options.runtime_env_hash = runtime_env_hash
        options.startup_token = startup_token
        options.session_name = session_name
        options.cluster_id = CClusterID.FromHex(cluster_id)
        options.entrypoint = entrypoint
        options.worker_launch_time_ms = worker_launch_time_ms
        options.worker_launched_time_ms = worker_launched_time_ms
        options.debug_source = debug_source
        CCoreWorkerProcess.Initialize(options)

        self.cgname_to_eventloop_dict = None
        self.fd_to_cgname_dict = None
        self.eventloop_for_default_cg = None
        self.current_runtime_env = None
        self._task_id_to_future_lock = threading.Lock()
        self._task_id_to_future = {}
        self.event_loop_executor = None

        self._gc_thread = None
        if RayConfig.instance().start_python_gc_manager_thread():
            self._gc_thread = PythonGCThread()
            self._gc_thread.start()

    def shutdown_driver(self):
        # If it's a worker, the core worker process should have been
        # shutdown. So we can't call
        # `CCoreWorkerProcess.GetCoreWorker().GetWorkerType()` here.
        # Instead, we use the cached `is_driver` flag to test if it's a
        # driver.
        assert self.is_driver
        if self._gc_thread is not None:
            self._gc_thread.stop()
            self._gc_thread = None
        with nogil:
            CCoreWorkerProcess.Shutdown()

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

    def get_current_task_name(self) -> str:
        """Return the current task name.

        If it is a normal task, it returns the task name from the main thread.
        If it is a threaded actor, it returns the task name for the current thread.
        If it is async actor, it returns the task name stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task name within asyncio task
        # via async_task_name contextvar. We try this first.
        # It is needed because the core worker's GetCurrentTask API
        # doesn't have asyncio context, thus it cannot return the
        # correct task name.
        task_name = async_task_name.get()
        if task_name is None:
            # if it is not within asyncio context, fallback to TaskName
            # obtainable from core worker.
            task_name = CCoreWorkerProcess.GetCoreWorker().GetCurrentTaskName() \
                .decode("utf-8")
        return task_name

    def get_current_task_function_name(self) -> str:
        """Return the current task function.

        If it is a normal task, it returns the task function from the main thread.
        If it is a threaded actor, it returns the task function for the current thread.
        If it is async actor, it returns the task function stored in contextVar for
        the current asyncio task.
        """
        # We can only obtain the correct task function within asyncio task
        # via async_task_function_name contextvar. We try this first.
        # It is needed because the core Worker's GetCurrentTask API
        # doesn't have asyncio context, thus it cannot return the
        # correct task function.
        task_function_name = async_task_function_name.get()
        if task_function_name is None:
            # if it is not within asyncio context, fallback to TaskName
            # obtainable from core worker.
            task_function_name = CCoreWorkerProcess.GetCoreWorker() \
                .GetCurrentTaskFunctionName().decode("utf-8")
        return task_function_name

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

    def set_actor_repr_name(self, repr_name):
        CCoreWorkerProcess.GetCoreWorker().SetActorReprName(repr_name)

    def get_objects(self, object_refs, int64_t timeout_ms=-1):
        cdef:
            c_vector[shared_ptr[CRayObject]] results
            c_vector[CObjectID] c_object_ids = ObjectRefsToVector(object_refs)
        with nogil:
            op_status = CCoreWorkerProcess.GetCoreWorker().Get(
                c_object_ids, timeout_ms, results)
        check_status(op_status)

        return RayObjectsToSerializedRayObjects(results, object_refs)

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
        return RayObjectsToSerializedRayObjects(results, object_refs)

    def object_exists(self, ObjectRef object_ref, memory_store_only=False):
        cdef:
            c_bool has_object
            c_bool is_in_plasma
            CObjectID c_object_id = object_ref.native()

        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker().Contains(
                c_object_id, &has_object, &is_in_plasma))

        return has_object and (not memory_store_only or not is_in_plasma)

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
            unique_ptr[CAddress] c_owner_address = self._convert_python_address(
                    object_ref.owner_address())

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
            CCoreWorkerProcess.GetCoreWorker().ExperimentalRegisterMutableObjectWriter(
                    c_writer_ref,
                    c_remote_reader_nodes,
            )
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

    def put_object(
            self,
            serialized_object,
            *,
            c_bool pin_object,
            owner_address,
            c_bool inline_small_object,
            c_bool _is_experimental_channel,
            int tensor_transport_val=0
    ):
        """Create an object reference with the current worker as the owner.
        """
        created_object = self.put_serialized_object_and_increment_local_ref(
            serialized_object, pin_object, owner_address, inline_small_object, _is_experimental_channel, tensor_transport_val)
        if owner_address is None:
            owner_address = CCoreWorkerProcess.GetCoreWorker().GetRpcAddress().SerializeAsString()

        # skip_adding_local_ref is True because it's already added through the call to
        # put_serialized_object_and_increment_local_ref.
        return ObjectRef(
            created_object,
            owner_address,
            skip_adding_local_ref=True,
            tensor_transport_val=tensor_transport_val
        )

    def put_serialized_object_and_increment_local_ref(
            self,
            serialized_object,
            c_bool pin_object=True,
            owner_address=None,
            c_bool inline_small_object=True,
            c_bool _is_experimental_channel=False,
            int tensor_transport_val=0
            ):
        cdef:
            CObjectID c_object_id
            shared_ptr[CBuffer] data
            c_vector[CObjectReference] contained_object_refs
            shared_ptr[CBuffer] metadata = string_to_buffer(
                serialized_object.metadata)
            unique_ptr[CAddress] c_owner_address = self._convert_python_address(
                owner_address)
            c_vector[CObjectID] contained_object_ids = ObjectRefsToVector(
                serialized_object.contained_object_refs)
            size_t total_bytes = serialized_object.total_bytes

        c_tensor_transport_val = <CTensorTransport>tensor_transport_val
        with nogil:
            check_status(CCoreWorkerProcess.GetCoreWorker()
                .CreateOwnedAndIncrementLocalRef(
                    _is_experimental_channel,
                    metadata,
                    total_bytes,
                    contained_object_ids,
                    &c_object_id,
                    &data,
                    c_owner_address,
                    inline_small_object,
                    c_tensor_transport_val))

        if (data.get() == NULL):
            # Object already exists
            return c_object_id.Binary()

        logger.debug(
            f"Serialized object size of {c_object_id.Hex()} is {total_bytes} bytes")

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
            with nogil:
                check_status(
                    CCoreWorkerProcess.GetCoreWorker().SealOwned(
                                c_object_id,
                                pin_object,
                                move(c_owner_address)))
        return c_object_id.Binary()

    def wait(self,
             object_refs_or_generators,
             int num_returns,
             int64_t timeout_ms,
             c_bool fetch_local):
        cdef:
            c_vector[CObjectID] wait_ids
            c_vector[c_bool] results

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
            check_status(CCoreWorkerProcess.GetCoreWorker().
                         Delete(free_ids, local_only))

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

    def log_plasma_usage(self):
        """Logs the current usage of the Plasma Store.
        Makes an unretriable blocking IPC to the Plasma Store.

        Raises an error if cannot connect to the Plasma Store. This should
        be fatal for the worker.
        """
        cdef:
            c_string result
        status = CCoreWorkerProcess.GetCoreWorker().GetPlasmaUsage(result)
        check_status(status)
        logger.warning("Plasma Store Usage:\n{}\n".format(
            result.decode("utf-8")))

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
                    c_bool enable_task_events,
                    labels,
                    label_selector,
                    fallback_strategy):
        cdef:
            unordered_map[c_string, double] c_resources
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_vector[CFallbackOption] c_fallback_strategy
            CRayFunction ray_function
            CTaskOptions task_options
            c_vector[unique_ptr[CTaskArg]] args_vector
            c_vector[CObjectReference] return_refs
            CSchedulingStrategy c_scheduling_strategy
            c_vector[CObjectID] incremented_put_arg_ids
            c_string serialized_retry_exception_allowlist
            CTaskID current_c_task_id
            TaskID current_task = self.get_current_task_id()
            c_string call_site

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        if RayConfig.instance().record_task_actor_creation_sites():
            # TODO(ryw): unify with get_py_stack used by record_ref_creation_sites.
            call_site = ''.join(traceback.format_stack())

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_labels(labels, &c_labels)
            prepare_label_selector(label_selector, &c_label_selector)
            prepare_fallback_strategy(fallback_strategy, &c_fallback_strategy)
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
                enable_task_events,
                c_labels,
                c_label_selector,
                # `tensor_transport` is currently only supported in Ray Actor tasks.
                # For Ray tasks, we always use `OBJECT_STORE`.
                TENSOR_TRANSPORT_OBJECT_STORE,
                c_fallback_strategy)

            current_c_task_id = current_task.native()

            with nogil:
                return_refs = CCoreWorkerProcess.GetCoreWorker().SubmitTask(
                    ray_function, args_vector, task_options,
                    max_retries, retry_exceptions,
                    c_scheduling_strategy,
                    debugger_breakpoint,
                    serialized_retry_exception_allowlist,
                    call_site,
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
                     labels,
                     label_selector,
                     c_bool allow_out_of_order_execution,
                     c_bool enable_tensor_transport,
                     fallback_strategy,
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
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_vector[CFallbackOption] c_fallback_strategy
            c_string call_site

        self.python_scheduling_strategy_to_c(
            scheduling_strategy, &c_scheduling_strategy)

        if RayConfig.instance().record_task_actor_creation_sites():
            # TODO(ryw): unify with get_py_stack used by record_ref_creation_sites.
            call_site = ''.join(traceback.format_stack())

        with self.profile_event(b"submit_task"):
            prepare_resources(resources, &c_resources)
            prepare_resources(placement_resources, &c_placement_resources)
            prepare_labels(labels, &c_labels)
            prepare_label_selector(label_selector, &c_label_selector)
            prepare_fallback_strategy(fallback_strategy, &c_fallback_strategy)
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
                        allow_out_of_order_execution,
                        max_pending_calls,
                        enable_tensor_transport,
                        enable_task_events,
                        c_labels,
                        c_label_selector,
                        c_fallback_strategy),
                    extension_data,
                    call_site,
                    &c_actor_id,
                )

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
                            soft_target_node_id,
                            c_vector[unordered_map[c_string, c_string]] bundle_label_selector):
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
                                c_soft_target_node_id,
                                bundle_label_selector),
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
                          c_bool enable_task_events,
                          int py_tensor_transport):

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
            unordered_map[c_string, c_string] c_labels
            CLabelSelector c_label_selector
            c_string call_site
            CTensorTransport c_tensor_transport_val
            c_vector[CFallbackOption] c_fallback_strategy

        serialized_retry_exception_allowlist = serialize_retry_exception_allowlist(
            retry_exception_allowlist,
            function_descriptor)

        if RayConfig.instance().record_task_actor_creation_sites():
            call_site = ''.join(traceback.format_stack())

        c_tensor_transport_val = <CTensorTransport>py_tensor_transport

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
                        enable_task_events,
                        c_labels,
                        c_label_selector,
                        c_tensor_transport_val,
                        c_fallback_strategy),
                    max_retries,
                    retry_exceptions,
                    serialized_retry_exception_allowlist,
                    call_site,
                    return_refs,
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

    def is_canceled(self):
        """Check if the current task has been canceled.

        Returns:
            True if the current task has been canceled, False otherwise.
        """
        cdef:
            CTaskID c_task_id
            c_bool is_canceled
            TaskID task_id

        # Get the current task ID
        task_id = self.get_current_task_id()
        c_task_id = task_id.native()

        with nogil:
            is_canceled = CCoreWorkerProcess.GetCoreWorker().IsTaskCanceled(c_task_id)

        return is_canceled

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
        allow_out_of_order_execution = dereference(c_actor_handle).AllowOutOfOrderExecution()
        enable_tensor_transport = dereference(c_actor_handle).EnableTensorTransport()
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
                                         enable_tensor_transport,
                                         method_meta.method_name_to_tensor_transport,
                                         actor_method_cpu,
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref,
                                         allow_out_of_order_execution=allow_out_of_order_execution)
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
                                         False,  # enable_tensor_transport
                                         None,  # method_name_to_tensor_transport
                                         0,  # actor method cpu
                                         actor_creation_function_descriptor,
                                         worker.current_cluster_and_job,
                                         weak_ref=weak_ref,
                                         allow_out_of_order_execution=allow_out_of_order_execution,
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
            # For objects that can't be inlined, return_ptr will only be set if
            # the object doesn't already exist in plasma.
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
                # Pins the object, succeeds if the object exists in plasma and is
                # sealed.
                success = (
                    CCoreWorkerProcess.GetCoreWorker().PinExistingReturnObject(
                        return_id, return_ptr, generator_id, caller_address))
            return success

    cdef store_task_outputs(self,
                            worker, outputs,
                            const CAddress &caller_address,
                            c_vector[c_pair[CObjectID, shared_ptr[CRayObject]]]
                            *returns,
                            ref_generator_id=None,
                            CTensorTransport c_tensor_transport=TENSOR_TRANSPORT_OBJECT_STORE):
        cdef:
            CObjectID return_id
            size_t data_size
            shared_ptr[CBuffer] metadata
            c_vector[CObjectID] contained_id
            int64_t task_output_inlined_bytes
            int64_t num_returns = -1
            CObjectID c_ref_generator_id = CObjectID.Nil()
            shared_ptr[CRayObject] *return_ptr

        if ref_generator_id:
            c_ref_generator_id = CObjectID.FromBinary(ref_generator_id)

        num_outputs_stored = 0
        if not c_ref_generator_id.IsNil():
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
            if outputs is not None and len(outputs) > 0:
                # Warn if num_returns=0 but the task returns a non-None value (likely unintended).
                task_name = self.get_current_task_name()
                obj_value = repr(outputs)
                warnings.warn(
                    f"Task '{task_name}' has num_returns=0 but returned a non-None value '{obj_value}'. "
                    "The return value will be ignored.",
                    NumReturnsWarning,
                    stacklevel=2
                )

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

            # TODO(kevin85421): We should consider unifying both serialization logic in the future
            # when GPU objects are more stable. We currently separate the logic to ensure
            # GPU object-related logic does not affect the normal object serialization logic.
            if <int>c_tensor_transport != <int>TENSOR_TRANSPORT_OBJECT_STORE:
                # `output` contains tensors. We need to retrieve these tensors from `output`
                # and store them in the GPUObjectManager.
                serialized_object, tensors = context.serialize_gpu_objects(output)
                context.store_gpu_objects(return_id.Hex().decode("ascii"), tensors)

            else:
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

            # It's possible for store_task_output to fail when the object already
            # exists, but we fail to pin it. We can fail to pin the object if
            # 1. it exists but isn't sealed yet because it's being written to by
            #    another worker. We'll keep looping until it's sealed.
            # 2. it existed during the allocation attempt but was evicted before
            #    the pin attempt. We'll allocate and write the second time.
            base_backoff_s = 1
            attempt = 1
            max_attempts = 6 # 6 attempts =~ 60 seconds of total backoff time
            while not self.store_task_output(
                    serialized_object,
                    return_id,
                    c_ref_generator_id,
                    data_size,
                    metadata,
                    contained_id,
                    caller_address,
                    &task_output_inlined_bytes,
                    return_ptr):
                if (attempt > max_attempts):
                    raise RaySystemError(
                        "Failed to store task output with object id {} after {} attempts.".format(
                            return_id.Hex().decode("ascii"),
                            max_attempts))
                time.sleep(base_backoff_s * (2 ** (attempt-1)))
                attempt += 1
                continue

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

    def get_event_loop_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        if self.event_loop_executor is None:
            # NOTE: We're deliberately allocating thread-pool executor with
            #       a single thread, provided that many of its use-cases are
            #       not thread-safe yet (for ex, reporting streaming generator output)
            self.event_loop_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        return self.event_loop_executor

    def reset_event_loop_executor(self, executor: concurrent.futures.ThreadPoolExecutor):
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
          task_name: Optional[str] = None,
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

        eventloop, _ = self.get_event_loop(
            function_descriptor, specified_cgname)

        async def async_func():
            try:
                if task_id:
                    async_task_id.set(task_id)
                if task_name is not None:
                    async_task_name.set(task_name)
                async_task_function_name.set(function_descriptor.repr)

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

    def set_current_actor_should_exit(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .SetCurrentActorShouldExit())

    def get_current_actor_should_exit(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .GetCurrentActorShouldExit())

    def current_actor_max_concurrency(self):
        return (CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                .CurrentActorMaxConcurrency())

    def get_current_root_detached_actor_id(self) -> ActorID:
        # This is only used in test
        return ActorID(CCoreWorkerProcess.GetCoreWorker().GetWorkerContext()
                       .GetRootDetachedActorID().Binary())

    def get_future_for_running_task(self, task_id: Optional[TaskID]) -> Optional[concurrent.futures.Future]:
        """Get the future corresponding to a running task (or None).

        The underyling asyncio task might be queued, running, or completed.
        """
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

    def trigger_gc(self):
        self._gc_thread.trigger_gc()

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
        serialized_ray_objects = RayObjectsToSerializedRayObjects(
            objects_to_deserialize)
        ids_to_deserialize = [ObjectRef(object_ref.Binary())]
        result = ray._private.worker.global_worker.deserialize_objects(
            serialized_ray_objects, ids_to_deserialize)[0]

        user_callback = <object>user_callback_ptr
        user_callback(result)
    except Exception:
        # Only log the error here because this callback is called from Cpp
        # and Cython will ignore the exception anyway
        logger.exception("failed to run async callback (user func)")
    finally:
        # NOTE: we manually increment the Python reference count of the callback when
        # registering it in the core worker, so we must decrement here to avoid a leak.
        cpython.Py_DECREF(user_callback)


# Note this deletes keys with prefix `RAY{key_prefix}@`
# Example: with key_prefix = `default`, we remove all `RAYdefault@...` keys.
def del_key_prefix_from_storage(host, port, username, password, use_ssl, key_prefix):
    return RedisDelKeyPrefixSync(host, port, username, password, use_ssl, key_prefix)


def get_session_key_from_storage(host, port, username, password, use_ssl, config, key):
    """
    Get the session key from the storage.
    Intended to be used for session_name only.
    Args:
        host: The address of the owner (caller) of the
            generator task.
        port: The task ID of the generator task.
        username: The Redis username.
        password: The Redis password.
        use_ssl: Whether to use SSL.
        config: The Ray config. Used to get storage namespace.
        key: The key to retrieve.
    """
    cdef:
        c_string data
    result = RedisGetKeySync(
        host, port, username, password, use_ssl, config, key, &data)
    if result:
        return data
    else:
        logger.info("Could not retrieve session key from storage.")
        return None
