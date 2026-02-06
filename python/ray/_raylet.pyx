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

from dataclasses import dataclass
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
    CStatusOr,
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
    CLabelSelector,
    CNodeResources,
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
    GetPortFileName,
    PersistPort,
    WaitForPersistedPort,
    CWaitForPersistedPortResult,
    SetNodeResourcesLabels,
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
    ActorHandleNotFoundError,
    ActorDiedError,
    RayActorError,
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

# It is used to indicate std::nullopt for
# AllocateDynamicReturnId.
cdef optional[ObjectIDIndexType] NULL_PUT_INDEX = nullopt
# Used to indicate std::nullopt for tensor_transport.
cdef optional[c_string] NULL_TENSOR_TRANSPORT = nullopt

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
    tensor_transport: Optional[str]


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

            c_tensor_transport = objects[i].get().GetTensorTransport()
            tensor_transport = None
            if (
                not c_tensor_transport.has_value()
                and object_refs is not None
            ):
                tensor_transport = object_refs[i].tensor_transport()
            elif c_tensor_transport.has_value():
                tensor_transport = c_tensor_transport.value().decode("utf-8")

            serialized_ray_objects.append(SerializedRayObject(data, metadata, tensor_transport))
    return serialized_ray_objects


cdef VectorToObjectRefs(const c_vector[CObjectReference] &object_refs,
                        skip_adding_local_ref):
    result = []
    for i in range(object_refs.size()):
        tensor_transport = None
        if object_refs[i].has_tensor_transport():
            tensor_transport = object_refs[i].tensor_transport().decode("utf-8")
        result.append(ObjectRef(
            object_refs[i].object_id(),
            object_refs[i].owner_address().SerializeAsString(),
            object_refs[i].call_site(),
            skip_adding_local_ref,
            tensor_transport))
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


def get_port_filename(node_id: str, port_name: str) -> str:
    cdef CNodeID c_node_id = CNodeID.FromHex(node_id)
    return GetPortFileName(c_node_id, port_name.encode()).decode()


def persist_port(dir: str, node_id: str, port_name: str, port: int) -> None:
    cdef CNodeID c_node_id = CNodeID.FromHex(node_id)
    cdef CRayStatus status = PersistPort(
        dir.encode(), c_node_id, port_name.encode(), port)
    if not status.ok():
        raise RuntimeError(status.message().decode())


def wait_for_persisted_port(
    dir: str,
    node_id: str,
    port_name: str,
    timeout_ms: int = 30000,
    poll_interval_ms: int = 100
) -> int:
    cdef CNodeID c_node_id = CNodeID.FromHex(node_id)
    cdef CWaitForPersistedPortResult result = WaitForPersistedPort(
        dir.encode(), c_node_id, port_name.encode(), timeout_ms, poll_interval_ms)
    if not result.has_value():
        raise RuntimeError(result.message().decode())
    return result.value()


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

def node_labels_match_selector(node_labels: Dict[str, str], selector: Dict[str, str]) -> bool:
    """
    Checks if the given node labels satisfy the label selector. This helper function exposes
    the C++ logic for determining if a node satisfies a label selector to the Python layer.
    """
    cdef:
        CNodeResources c_node_resources
        CLabelSelector c_label_selector
        unordered_map[c_string, c_string] c_labels_map

    prepare_labels(node_labels, &c_labels_map)
    SetNodeResourcesLabels(c_node_resources, c_labels_map)
    prepare_label_selector(selector, &c_label_selector)

    # Return whether the node resources satisfy the label constraint.
    return c_node_resources.HasRequiredLabels(c_label_selector)

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
        optional[c_string] c_tensor_transport = NULL_TENSOR_TRANSPORT

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
                    move(c_tensor_transport))))
            c_tensor_transport = NULL_TENSOR_TRANSPORT
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
                            serialized_arg, c_tensor_transport, pin_object=True,
                            owner_address=None, inline_small_object=False))
                args_vector.push_back(unique_ptr[CTaskArg](
                    new CTaskArgByReference(
                            put_id,
                            CCoreWorkerProcess.GetCoreWorker().GetRpcAddress(),
                            put_arg_call_site,
                            c_tensor_transport
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
        optional[c_string] c_tensor_transport):
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
    # MAX_APPLICATION_ERROR_LENGTH characters of the error message.
    if application_error != NULL:
        if ray_constants.MAX_APPLICATION_ERROR_LENGTH == 0:
            application_error[0] = b""
        else:
            application_error[0] = str(failure_object)[-ray_constants.MAX_APPLICATION_ERROR_LENGTH:]

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


@dataclass(frozen=True)
class StreamingGeneratorStats:
    object_creation_dur_s: float


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

    start = time.perf_counter()

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

    serialization_dur_s = time.perf_counter() - start

    with nogil:
        check_status(CCoreWorkerProcess.GetCoreWorker().ReportGeneratorItemReturns(
            return_obj,
            context.generator_id,
            context.caller_address,
            generator_index,
            context.attempt_number,
            context.waiter))


    return StreamingGeneratorStats(
        object_creation_dur_s=serialization_dur_s,
    )


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
        stats = None

        while True:
            try:
                # Send object serialization duration to the generator and retrieve
                # next output
                output = gen.send(stats)
                # Track serialization duration of the next output
                stats = report_streaming_generator_output(context, output, gen_index, None)

                gen_index += 1

            except StopIteration:
                break
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
        stats = None

        while True:
            try:
                output = await gen.asend(stats)
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
                stats = await loop.run_in_executor(
                    executor,
                    report_streaming_generator_output,
                    context,
                    output,
                    cur_generator_index,
                    interrupt_signal_event,
                )
                cur_generator_index += 1

            except StopAsyncIteration:
                break

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
                &intermediate_result,
                application_error,
                NULL_TENSOR_TRANSPORT)

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
                        dynamic_returns,
                        application_error,
                        NULL_TENSOR_TRANSPORT)
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
        c_string *actor_repr_name,
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
        optional[c_string] c_tensor_transport) except *:
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
                    actor_repr_str = repr(actor)
                    actor_magic_token = "{}{}\n".format(
                        ray_constants.LOG_PREFIX_ACTOR_NAME, actor_repr_str)
                    # Flush on both stdout and stderr.
                    print(actor_magic_token, end="")
                    print(actor_magic_token, file=sys.stderr, end="")

                    actor_repr_name[0] = actor_repr_str

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
        c_string *actor_repr_name,
        c_string *application_error,
        # This parameter is only used for actor creation task to define
        # the concurrency groups of this actor.
        const c_vector[CConcurrencyGroup] &c_defined_concurrency_groups,
        const c_string c_name_of_concurrency_group_to_execute,
        c_bool is_reattempt,
        c_bool is_streaming_generator,
        c_bool should_retry_exceptions,
        int64_t generator_backpressure_num_objects,
        optional[c_string] c_tensor_transport):

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
                     actor_repr_name,
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
                NULL,
                NULL_TENSOR_TRANSPORT)
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
        gpu_object_manager.queue_or_free_object_primary_copy(object_id)

cdef void set_direct_transport_metadata(const CObjectID &c_object_id, const c_string &c_direct_transport_metadata) nogil:
    with gil:
        object_id = c_object_id.Hex().decode()
        tensor_transport_meta = ray_pickle.loads(c_direct_transport_metadata)
        gpu_object_manager = ray._private.worker.global_worker.gpu_object_manager
        gpu_object_manager.set_tensor_transport_metadata_and_trigger_queued_operations(object_id, tensor_transport_meta)

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
        c_string *actor_repr_name,
        c_string *application_error,
        const c_vector[CConcurrencyGroup] &defined_concurrency_groups,
        const c_string name_of_concurrency_group_to_execute,
        c_bool is_reattempt,
        c_bool is_streaming_generator,
        c_bool should_retry_exceptions,
        int64_t generator_backpressure_num_objects,
        optional[c_string] c_tensor_transport) nogil:
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
                        actor_repr_name,
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
        worker.raise_errors([SerializedRayObject(data, metadata, None)], object_ids)


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

include "includes/core_worker.pxi"


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
