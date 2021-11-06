import enum
import logging
import random
import threading
from typing import List

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
from ray.core.generated import pubsub_pb2

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

LOG_FILE_CHANNEL = "RAY_LOG_CHANNEL"

# Actor pub/sub updates
RAY_ACTOR_PUBSUB_PATTERN = "ACTOR:*".encode("ascii")

RAY_ERROR_PUBSUB_PATTERN = "ERROR_INFO:*".encode("ascii")

# These prefixes must be kept up-to-date with the TablePrefix enum in
# gcs.proto.
TablePrefix_ACTOR_string = "ACTOR"

WORKER = 0
DRIVER = 1

_gcs_connection_lock = threading.Lock()
_gcs_address = None
_gcs_channel = None
_gcs_aio_channel = None


def init_gcs_address(address: str, update=False) -> None:
    """Initializes the address of GCS.

    Args:
        address: address of GCS. Repeated initialization to the same address is
            fine, but it is an error to initialize GCS to different addresses.
        update: whether updating the current GCS address is allowed, if the GCS
            address has already been initialized and is different.
    """
    global _gcs_address
    if _gcs_address == address:
        return

    with _gcs_connection_lock:
        # if _gcs_address is None:
        #     _gcs_address = address
        #     return
        if _gcs_address == address:
            return
        if _gcs_address is not None and not update:
            raise ValueError("GCS initialized to different addresses")
        _gcs_address = address
        global _gcs_channel, _gcs_aio_channel
        _gcs_channel = None
        _gcs_aio_channel = None


def get_gcs_address() -> str:
    """Returns the address of GCS."""
    global _gcs_address
    return _gcs_address


def get_gcs_channel() -> grpc.Channel:
    """Returns a GRPC channel to GCS."""
    global _gcs_channel
    channel = _gcs_channel
    if channel:
        return channel
    if _gcs_channel:
        return _gcs_channel
    with _gcs_connection_lock:
        if _gcs_channel:
            return _gcs_channel
        from ray._private.utils import init_grpc_channel
        options = [("grpc.enable_http_proxy",
                    0), ("grpc.max_send_message_length",
                         GcsClient.MAX_MESSAGE_LENGTH),
                   ("grpc.max_receive_message_length",
                    GcsClient.MAX_MESSAGE_LENGTH)]
        _gcs_channel = init_grpc_channel(get_gcs_address(), options=options)
        return _gcs_channel


def get_gcs_aio_channel() -> grpc.aio.Channel:
    """Returns a AIO GRPC channel to GCS."""
    global _gcs_aio_channel
    channel = _gcs_aio_channel
    if channel and channel.get_state() != grpc.ChannelConnectivity.SHUTDOWN:
        return channel
    with _gcs_connection_lock:
        if _gcs_aio_channel:
            return _gcs_aio_channel
        from ray._private.utils import init_grpc_channel
        options = [("grpc.enable_http_proxy",
                    0), ("grpc.max_send_message_length",
                         GcsClient.MAX_MESSAGE_LENGTH),
                   ("grpc.max_receive_message_length",
                    GcsClient.MAX_MESSAGE_LENGTH)]
        _gcs_aio_channel = init_grpc_channel(
            get_gcs_address(), options=options, asynchronous=True)
        return _gcs_aio_channel


def construct_error_message(job_id, error_type, message, timestamp):
    """Construct an ErrorTableData object.

    Args:
        job_id: The ID of the job that the error should go to. If this is
            nil, then the error will go to all drivers.
        error_type: The type of the error.
        message: The error message.
        timestamp: The time of the error.

    Returns:
        The ErrorTableData object.
    """
    data = ErrorTableData()
    data.job_id = job_id.binary()
    data.type = error_type
    data.error_message = message
    data.timestamp = timestamp
    return data


class GcsCode(enum.IntEnum):
    # corresponding to ray/src/ray/common/status.h
    OK = 0
    NotFound = 17


class GcsClient:
    MAX_MESSAGE_LENGTH = 512 * 1024 * 1024  # 512MB

    def __init__(self, address):
        logger.debug(f"Connecting to gcs address: {address}")
        from ray._private.utils import init_grpc_channel
        options = [("grpc.enable_http_proxy",
                    0), ("grpc.max_send_message_length",
                         GcsClient.MAX_MESSAGE_LENGTH),
                   ("grpc.max_receive_message_length",
                    GcsClient.MAX_MESSAGE_LENGTH)]
        channel = init_grpc_channel(address, options=options)
        self._kv_stub = gcs_service_pb2_grpc.InternalKVGcsServiceStub(channel)

    def internal_kv_get(self, key: bytes) -> bytes:
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
        req = gcs_service_pb2.InternalKVPutRequest(
            key=key, value=value, overwrite=overwrite)
        reply = self._kv_stub.InternalKVPut(req)
        if reply.status.code == GcsCode.OK:
            return reply.added_num
        else:
            raise RuntimeError(f"Failed to put value {value} to key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_del(self, key: bytes) -> int:
        req = gcs_service_pb2.InternalKVDelRequest(key=key)
        reply = self._kv_stub.InternalKVDel(req)
        if reply.status.code == GcsCode.OK:
            return reply.deleted_num
        else:
            raise RuntimeError(f"Failed to delete key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_exists(self, key: bytes) -> bool:
        req = gcs_service_pb2.InternalKVExistsRequest(key=key)
        reply = self._kv_stub.InternalKVExists(req)
        if reply.status.code == GcsCode.OK:
            return reply.exists
        else:
            raise RuntimeError(f"Failed to check existence of key {key} "
                               f"due to error {reply.status.message}")

    def internal_kv_keys(self, prefix: bytes) -> List[bytes]:
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


class GcsPublisher:
    def __init__(self):
        if get_gcs_address() is None:
            raise "Must initialize GCS address with init_gcs_address()"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(
            get_gcs_channel())

    def publish_error(self, key_id: bytes, error_info: ErrorTableData) -> None:
        msg = pubsub_pb2.PubMessage(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            key_id=key_id,
            error_info_message=error_info)
        req = gcs_service_pb2.GcsPublishRequest(pub_messages=[msg])
        self._stub.GcsPublish(req)


class GcsSubscriber:
    def __init__(self):
        if get_gcs_address() is None:
            raise "Must initialize GCS address with init_gcs_address()"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(
            get_gcs_channel())
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))
        self._subscribed_error = False

    def subscribe_error(self) -> None:
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        self._stub.GcsSubscriberCommandBatch(req)
        self._subscribed_error = True

    def poll_error(self) -> List[ErrorTableData]:
        if not self._subscribed_error:
            self.subscribe_error()

        req = gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)
        reply = self._stub.GcsSubscriberPoll(req)
        error_info = []
        for msg in reply.pub_messages:
            error_info.append((msg.key_id, msg.error_info_message))
        return error_info


class GcsAioPublisher:
    def __init__(self):
        if get_gcs_address() is None:
            raise "Must initialize GCS address with init_gcs_address()"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(
            get_gcs_aio_channel())

    async def publish_error(self, key_id: bytes,
                            error_info: ErrorTableData) -> None:
        msg = pubsub_pb2.PubMessage(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            key_id=key_id,
            error_info_message=error_info)
        req = gcs_service_pb2.GcsPublishRequest(pub_messages=[msg])
        await self._stub.GcsPublish(req)


class GcsAioSubscriber:
    def __init__(self):
        if get_gcs_address() is None:
            raise "Must initialize GCS address with init_gcs_address()"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(
            get_gcs_aio_channel())
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))
        self._subscribed_error = False

    async def subscribe_error(self) -> None:
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        await self._stub.GcsSubscriberCommandBatch(req)
        self._subscribed_error = True

    async def poll_error(self) -> List[ErrorTableData]:
        if not self._subscribed_error:
            self.subscribe_error()

        req = gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)
        reply = await self._stub.GcsSubscriberPoll(req)
        error_info = []
        for msg in reply.pub_messages:
            error_info.append((msg.key_id, msg.error_info_message))
        return error_info
