from ray.core.generated.common_pb2 import ErrorType
import enum
import logging
from typing import List
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    GcsNodeInfo,
    AvailableResources,
    JobTableData,
    JobConfig,
    ErrorTableData,
    GcsEntry,
    ResourceUsageBatchData,
    ResourcesData,
    ObjectTableData,
    ProfileTableData,
    TablePrefix,
    TablePubsub,
    TaskTableData,
    ResourceDemand,
    ResourceLoad,
    ResourceMap,
    ResourceTableData,
    ObjectLocationInfo,
    PubSubMessage,
    WorkerTableData,
    PlacementGroupTableData,
)

logger = logging.getLogger(__name__)

__all__ = [
    "ActorTableData",
    "GcsNodeInfo",
    "AvailableResources",
    "JobTableData",
    "JobConfig",
    "ErrorTableData",
    "ErrorType",
    "GcsEntry",
    "ResourceUsageBatchData",
    "ResourcesData",
    "ObjectTableData",
    "ProfileTableData",
    "TablePrefix",
    "TablePubsub",
    "TaskTableData",
    "ResourceDemand",
    "ResourceLoad",
    "ResourceMap",
    "ResourceTableData",
    "construct_error_message",
    "ObjectLocationInfo",
    "PubSubMessage",
    "WorkerTableData",
    "PlacementGroupTableData",
]

FUNCTION_PREFIX = "RemoteFunction:"
LOG_FILE_CHANNEL = "RAY_LOG_CHANNEL"
REPORTER_CHANNEL = "RAY_REPORTER"

# xray resource usages
XRAY_RESOURCES_BATCH_PATTERN = "RESOURCES_BATCH:".encode("ascii")

# xray job updates
XRAY_JOB_PATTERN = "JOB:*".encode("ascii")

# Actor pub/sub updates
RAY_ACTOR_PUBSUB_PATTERN = "ACTOR:*".encode("ascii")

# Reporter pub/sub updates
RAY_REPORTER_PUBSUB_PATTERN = "RAY_REPORTER.*".encode("ascii")

RAY_ERROR_PUBSUB_PATTERN = "ERROR_INFO:*".encode("ascii")

# These prefixes must be kept up-to-date with the TablePrefix enum in
# gcs.proto.
# TODO(rkn): We should use scoped enums, in which case we should be able to
# just access the flatbuffer generated values.
TablePrefix_RAYLET_TASK_string = "RAYLET_TASK"
TablePrefix_OBJECT_string = "OBJECT"
TablePrefix_PROFILE_string = "PROFILE"
TablePrefix_JOB_string = "JOB"
TablePrefix_ACTOR_string = "ACTOR"

WORKER = 0
DRIVER = 1


def construct_error_message(job_id, error_type, message, timestamp):
    """Construct a serialized ErrorTableData object.

    Args:
        job_id: The ID of the job that the error should go to. If this is
            nil, then the error will go to all drivers.
        error_type: The type of the error.
        message: The error message.
        timestamp: The time of the error.

    Returns:
        The serialized object.
    """
    data = ErrorTableData()
    data.job_id = job_id.binary()
    data.type = error_type
    data.error_message = message
    data.timestamp = timestamp
    return data.SerializeToString()


class GcsCode(enum.IntEnum):
    # corresponding to ray/src/ray/common/status.h
    OK = 0
    NotFound = 17


class GcsClient:
    MAX_MESSAGE_LENGTH = 512 * 1024 * 1024  # 512MB

    def __init__(self, address):
        from ray._private.utils import init_grpc_channel
        logger.debug(f"Connecting to gcs address: {address}")
        options = [("grpc.enable_http_proxy",
                    0), ("grpc.max_send_message_length",
                         GcsClient.MAX_MESSAGE_LENGTH),
                   ("grpc.max_receive_message_length",
                    GcsClient.MAX_MESSAGE_LENGTH)]
        channel = init_grpc_channel(address, options=options)
        self._kv_stub = gcs_service_pb2_grpc.InternalKVGcsServiceStub(channel)

    def internal_kv_get(self, key: bytes) -> bytes:
        logger.debug(f"internal_kv_get {key}")
        req = gcs_service_pb2.InternalKVGetRequest(key=key)
        reply = self._kv_stub.InternalKVGet(req)
        if reply.status.code == GcsCode.OK:
            return reply.value
        elif reply.status.code == GcsCode.NotFound:
            return None
        else:
            raise RuntimeError(f"Failed to get value for key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_put(self, key: bytes, value: bytes,
                        overwrite: bool) -> int:
        logger.debug(f"internal_kv_put {key} {value} {overwrite}")
        req = gcs_service_pb2.InternalKVPutRequest(
            key=key, value=value, overwrite=overwrite)
        reply = self._kv_stub.InternalKVPut(req)
        if reply.status.code == GcsCode.OK:
            return reply.added_num
        else:
            raise RuntimeError(f"Failed to put value {value} to key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_del(self, key: bytes) -> int:
        logger.debug(f"internal_kv_del {key}")
        req = gcs_service_pb2.InternalKVDelRequest(key=key)
        reply = self._kv_stub.InternalKVDel(req)
        if reply.status.code == GcsCode.OK:
            return reply.deleted_num
        else:
            raise RuntimeError(f"Failed to delete key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_exists(self, key: bytes) -> bool:
        logger.debug(f"internal_kv_exists {key}")
        req = gcs_service_pb2.InternalKVExistsRequest(key=key)
        reply = self._kv_stub.InternalKVExists(req)
        if reply.status.code == GcsCode.OK:
            return reply.exists
        else:
            raise RuntimeError(f"Failed to check existence of key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_keys(self, prefix: bytes) -> List[bytes]:
        logger.debug(f"internal_kv_keys {prefix}")
        req = gcs_service_pb2.InternalKVKeysRequest(prefix=prefix)
        reply = self._kv_stub.InternalKVKeys(req)
        if reply.status.code == GcsCode.OK:
            return list(reply.results)
        else:
            raise RuntimeError(f"Failed to list prefix {prefix} "
                               f"due to error {reply.status.message}")

    @staticmethod
    def create_from_redis(redis_cli):
        gcs_address = redis_cli.get("GcsServerAddress")
        if gcs_address is None:
            raise RuntimeError("Failed to look up gcs address through redis")
        return GcsClient(gcs_address.decode())

    @staticmethod
    def connect_to_gcs_by_redis_address(redis_address, redis_password):
        from ray._private.services import create_redis_client
        return GcsClient.create_from_redis(
            create_redis_client(redis_address, redis_password))
