from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector

from ray.includes.common cimport (
    CObjectLocation,
    CGcsClientOptions,
    CPythonGcsPublisher,
    CPythonGcsSubscriber,
    kWorkerSetupHookKeyName,
    kResourceUnitScaling,
    kImplicitResourcePrefix,
    kStreamingGeneratorReturn,
    kGcsAutoscalerStateNamespace,
    kGcsAutoscalerV2EnabledKey,
    kGcsAutoscalerClusterConfigKey,
)

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
)


cdef class GcsClientOptions:
    """Cython wrapper class of C++ `ray::gcs::GcsClientOptions`."""
    cdef:
        unique_ptr[CGcsClientOptions] inner

    @classmethod
    def create(
        cls, gcs_address, cluster_id_hex, allow_cluster_id_nil, fetch_cluster_id_if_nil
    ):
        """
        Creates a GcsClientOption with a maybe-Nil cluster_id, and may fetch from GCS.
        """
        cdef CClusterID c_cluster_id = CClusterID.Nil()
        if cluster_id_hex:
            c_cluster_id = CClusterID.FromHex(cluster_id_hex)
        self = GcsClientOptions()
        try:
            ip, port = gcs_address.split(":", 2)
            port = int(port)
            self.inner.reset(
                new CGcsClientOptions(
                    ip, port, c_cluster_id, allow_cluster_id_nil, allow_cluster_id_nil))
        except Exception:
            raise ValueError(f"Invalid gcs_address: {gcs_address}")
        return self

    cdef CGcsClientOptions* native(self):
        return <CGcsClientOptions*>(self.inner.get())

cdef int check_status(const CRayStatus& status) except -1 nogil:
    if status.ok():
        return 0

    with gil:
        message = status.message().decode()

    if status.IsObjectStoreFull():
        raise ObjectStoreFullError(message)
    elif status.IsInvalidArgument():
        raise ValueError(message)
    elif status.IsAlreadyExists():
        raise ValueError(message)
    elif status.IsOutOfDisk():
        raise OutOfDiskError(message)
    elif status.IsObjectRefEndOfStream():
        raise ObjectRefStreamEndOfStreamError(message)
    elif status.IsInterrupted():
        raise KeyboardInterrupt()
    elif status.IsTimedOut():
        raise GetTimeoutError(message)
    elif status.IsNotFound():
        raise ValueError(message)
    elif status.IsObjectNotFound():
        raise ValueError(message)
    elif status.IsObjectUnknownOwner():
        raise ValueError(message)
    elif status.IsIOError():
        raise IOError(message)
    elif status.IsRpcError():
        raise RpcError(message, rpc_code=status.rpc_code())
    elif status.IsIntentionalSystemExit():
        with gil:
            raise_sys_exit_with_custom_error_message(message)
    elif status.IsUnexpectedSystemExit():
        with gil:
            raise_sys_exit_with_custom_error_message(
                message, exit_code=1)
    elif status.IsChannelError():
        raise RayChannelError(message)
    elif status.IsChannelTimeoutError():
        raise RayChannelTimeoutError(message)
    else:
        raise RaySystemError(message)

cdef int check_status_timeout_as_rpc_error(const CRayStatus& status) except -1 nogil:
    """
    Same as check_status, except that it raises RpcError for timeout. This is for
    backward compatibility: on timeout, `ray.get` raises GetTimeoutError, while
    GcsClient methods raise RpcError. So in the binding, `get_objects` use check_status
    and GcsClient methods use check_status_timeout_as_rpc_error.
    """
    if status.IsTimedOut():
        raise RpcError(status.message().decode(),
                       rpc_code=CGrpcStatusCode.DEADLINE_EXCEEDED)
    return check_status(status)


WORKER_PROCESS_SETUP_HOOK_KEY_NAME_GCS = str(kWorkerSetupHookKeyName)
RESOURCE_UNIT_SCALING = kResourceUnitScaling
IMPLICIT_RESOURCE_PREFIX = kImplicitResourcePrefix.decode()
STREAMING_GENERATOR_RETURN = kStreamingGeneratorReturn
GCS_AUTOSCALER_STATE_NAMESPACE = kGcsAutoscalerStateNamespace.decode()
GCS_AUTOSCALER_V2_ENABLED_KEY = kGcsAutoscalerV2EnabledKey.decode()
GCS_AUTOSCALER_CLUSTER_CONFIG_KEY = kGcsAutoscalerClusterConfigKey.decode()
