import enum
import logging
import time
from typing import List, Callable

import grpc

from ray.core.generated.common_pb2 import ErrorType
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
    "ObjectLocationInfo",
    "PubSubMessage",
    "WorkerTableData",
    "PlacementGroupTableData",
]

LOG_FILE_CHANNEL = "RAY_LOG_CHANNEL"

# Actor pub/sub updates
RAY_ACTOR_PUBSUB_PATTERN = "ACTOR:*".encode("ascii")

RAY_ERROR_PUBSUB_PATTERN = "ERROR_INFO:*".encode("ascii")

# These prefixes must be kept up-to-date with the TablePrefix enum in
# gcs.proto.
TablePrefix_ACTOR_string = "ACTOR"

WORKER = 0
DRIVER = 1

# Cap messages at 512MB
_MAX_MESSAGE_LENGTH = 512 * 1024 * 1024
# Send keepalive every 60s
_GRPC_KEEPALIVE_TIME_MS = 60 * 1000
# Keepalive should be replied < 60s
_GRPC_KEEPALIVE_TIMEOUT_MS = 60 * 1000

# Also relying on these defaults:
# grpc.keepalive_permit_without_calls=0: No keepalive without inflight calls.
# grpc.use_local_subchannel_pool=0: Subchannels are shared.
_GRPC_OPTIONS = [("grpc.enable_http_proxy",
                  0), ("grpc.max_send_message_length", _MAX_MESSAGE_LENGTH),
                 ("grpc.max_receive_message_length", _MAX_MESSAGE_LENGTH),
                 ("grpc.keepalive_time_ms",
                  _GRPC_KEEPALIVE_TIME_MS), ("grpc.keepalive_timeout_ms",
                                             _GRPC_KEEPALIVE_TIMEOUT_MS)]


def get_gcs_address_from_redis(redis) -> str:
    """Reads GCS address from redis.

    Args:
        redis: Redis client to fetch GCS address.
    Returns:
        GCS address string.
    """
    gcs_address = redis.get("GcsServerAddress")
    if gcs_address is None:
        raise RuntimeError("Failed to look up gcs address through redis")
    return gcs_address.decode()


def create_gcs_channel(address: str, aio=False):
    """Returns a GRPC channel to GCS.

    Args:
        address: GCS address string, e.g. ip:port
        aio: Whether using grpc.aio
    Returns:
        grpc.Channel or grpc.aio.Channel to GCS
    """
    from ray._private.utils import init_grpc_channel
    return init_grpc_channel(address, options=_GRPC_OPTIONS, asynchronous=aio)


class GcsCode(enum.IntEnum):
    # corresponding to ray/src/ray/common/status.h
    OK = 0
    NotFound = 17


def auto_reconnect(f):
    def wrapper(self, *args, **kwargs):
        while True:
            try:
                return f(self, *args, **kwargs)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logger.error(
                        f"Failed to send request to gcs, reconnect. Error {e}")
                    try:
                        self._connect()
                        time.sleep(1)
                    except Exception:
                        logger.error(f"Connect to gcs failed. Error {e}")
                    continue
                raise e

    return wrapper


class GcsClient:
    """Client to GCS using GRPC"""

    def __init__(self, get_address: Callable):
        assert get_address is not None
        self._get_address = get_address
        self._connect()

    def _connect(self):
        gcs_address = self._get_address()
        logger.debug(f"Connecting to gcs address: {gcs_address}")
        self._channel = create_gcs_channel(gcs_address)
        self._kv_stub = gcs_service_pb2_grpc.InternalKVGcsServiceStub(
            self._channel)

    @auto_reconnect
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

    @auto_reconnect
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

    @auto_reconnect
    def internal_kv_del(self, key: bytes) -> int:
        logger.debug(f"internal_kv_del {key}")
        req = gcs_service_pb2.InternalKVDelRequest(key=key)
        reply = self._kv_stub.InternalKVDel(req)
        if reply.status.code == GcsCode.OK:
            return reply.deleted_num
        else:
            raise RuntimeError(f"Failed to delete key {key} "
                               f"due to error {reply.status.message}")

    @auto_reconnect
    def internal_kv_exists(self, key: bytes) -> bool:
        logger.debug(f"internal_kv_exists {key}")
        req = gcs_service_pb2.InternalKVExistsRequest(key=key)
        reply = self._kv_stub.InternalKVExists(req)
        if reply.status.code == GcsCode.OK:
            return reply.exists
        else:
            raise RuntimeError(f"Failed to check existence of key {key} "
                               f"due to error {reply.status.message}")

    @auto_reconnect
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
        return GcsClient(lambda: get_gcs_address_from_redis(redis_cli))

    @staticmethod
    def connect_to_gcs_by_redis_address(redis_address, redis_password):
        from ray._private.services import create_redis_client
        return GcsClient.create_from_redis(
            create_redis_client(redis_address, redis_password))
