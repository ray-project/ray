from collections import deque
import enum
import logging
import random
import threading
from typing import List, Tuple

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


def get_gcs_address_from_redis(redis) -> str:
    """Reads GCS address from redis."""
    gcs_address = redis.get("GcsServerAddress")
    if gcs_address is None:
        raise RuntimeError("Failed to look up gcs address through redis")
    return gcs_address.decode()


def create_gcs_channel(address: str) -> grpc.Channel:
    """Returns a GRPC channel to GCS."""
    from ray._private.utils import init_grpc_channel
    options = [("grpc.enable_http_proxy", 0), ("grpc.max_send_message_length",
                                               GcsClient.MAX_MESSAGE_LENGTH),
               ("grpc.max_receive_message_length",
                GcsClient.MAX_MESSAGE_LENGTH)]
    return init_grpc_channel(address, options=options)


def create_gcs_aio_channel(address: str) -> grpc.aio.Channel:
    """Returns a AIO GRPC channel to GCS."""
    from ray._private.utils import init_grpc_channel
    options = [("grpc.enable_http_proxy", 0), ("grpc.max_send_message_length",
                                               GcsClient.MAX_MESSAGE_LENGTH),
               ("grpc.max_receive_message_length",
                GcsClient.MAX_MESSAGE_LENGTH)]
    return init_grpc_channel(address, options=options, asynchronous=True)


class GcsCode(enum.IntEnum):
    # corresponding to ray/src/ray/common/status.h
    OK = 0
    NotFound = 17


class GcsClient:
    MAX_MESSAGE_LENGTH = 512 * 1024 * 1024  # 512MB

    def __init__(self, address: str = None, channel: grpc.Channel = None):
        if address:
            assert channel is None, \
                "Only one of address and channel can be specified"
            channel = create_gcs_channel(address)
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
        return GcsClient(get_gcs_address_from_redis(redis_cli))

    @staticmethod
    def connect_to_gcs_by_redis_address(redis_address, redis_password):
        from ray._private.services import create_redis_client
        return GcsClient.create_from_redis(
            create_redis_client(redis_address, redis_password))


class GcsPublisher:
    def __init__(self, address: str = None, channel: grpc.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = create_gcs_channel(address)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

    def publish_error(self, key_id: bytes, error_info: ErrorTableData) -> None:
        """Publishes error info to GCS."""
        msg = pubsub_pb2.PubMessage(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            key_id=key_id,
            error_info_message=error_info)
        req = gcs_service_pb2.GcsPublishRequest(pub_messages=[msg])
        self._stub.GcsPublish(req)


class GcsSubscriber:
    """Subscribes to GCS. Thread safe."""

    def __init__(
            self,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = create_gcs_channel(address)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        self._lock = threading.RLock()
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))
        # Whether error info has been subscribed.
        self._subscribed_error = False
        # Buffer for holding error info.
        self._errors = deque()
        # Future for indicating whether the subscriber has closed.
        self._close = threading.Event()

    def subscribe_error(self) -> None:
        """Starts a subscription for error info"""
        with self._lock:
            if self._close.is_set():
                return
            if self._subscribed_error:
                return
            cmd = pubsub_pb2.Command(
                channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
                subscribe_message={})
            req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
                subscriber_id=self._subscriber_id, commands=[cmd])
            self._stub.GcsSubscriberCommandBatch(req, timeout=30)
            self._subscribed_error = True

    def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        with self._lock:
            if self._close.is_set():
                return

            self.subscribe_error()

            if len(self._errors) == 0:
                req = gcs_service_pb2.GcsSubscriberPollRequest(
                    subscriber_id=self._subscriber_id)
                fut = self._stub.GcsSubscriberPoll.future(req, timeout=timeout)
                # Wait for result to become available, or cancel if the
                # subscriber has closed.
                while True:
                    try:
                        fut.result(timeout=1)
                        break
                    except grpc.FutureTimeoutError:
                        # Subscriber has closed. Cancel inflight the request
                        # and return from polling.
                        if self._close.is_set():
                            fut.cancel()
                            return None, None
                        # GRPC has not replied, continue waiting.
                        continue
                    except Exception:
                        # GRPC error, including deadline exceeded.
                        raise
                if fut.done():
                    for msg in fut.result().pub_messages:
                        self._errors.append((msg.key_id,
                                             msg.error_info_message))

            if len(self._errors) == 0:
                return None, None
            return self._errors.popleft()

    def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        # Mark close to terminate inflight polling and prevent future requests.
        self._close.set()
        with self._lock:
            if not self._stub:
                # Subscriber already closed.
                return
            req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
                subscriber_id=self._subscriber_id, commands=[])
            if self._subscribed_error:
                cmd = pubsub_pb2.Command(
                    channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
                    unsubscribe_message={})
                req.commands.append(cmd)
            try:
                self._stub.GcsSubscriberCommandBatch(req, timeout=30)
            except Exception:
                pass
            self._stub = None


class GcsAioPublisher:
    def __init__(self, address: str = None, channel: grpc.aio.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = create_gcs_aio_channel(address)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

    async def publish_error(self, key_id: bytes,
                            error_info: ErrorTableData) -> None:
        """Publishes error info to GCS."""
        msg = pubsub_pb2.PubMessage(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            key_id=key_id,
            error_info_message=error_info)
        req = gcs_service_pb2.GcsPublishRequest(pub_messages=[msg])
        await self._stub.GcsPublish(req)


class GcsAioSubscriber:
    def __init__(self, address: str = None, channel: grpc.aio.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = create_gcs_aio_channel(address)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))
        # Whether error info has been subscribed.
        self._subscribed_error = False
        # Buffer for holding error info.
        self._errors = deque()

    async def subscribe_error(self) -> None:
        """Starts a subscription for error info"""
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        await self._stub.GcsSubscriberCommandBatch(req)
        self._subscribed_error = True

    async def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        if not self._subscribed_error:
            self.subscribe_error()

        if len(self._errors) == 0:
            req = gcs_service_pb2.GcsSubscriberPollRequest(
                subscriber_id=self._subscriber_id)
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            for msg in reply.pub_messages:
                self._errors.append((msg.key_id, msg.error_info_message))

        if len(self._errors) == 0:
            return None, None
        return self._errors.popleft()

        req = gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)
        reply = await self._stub.GcsSubscriberPoll(req)
        error_info = []
        for msg in reply.pub_messages:
            error_info.append((msg.key_id, msg.error_info_message))
        return error_info

    async def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[])
        if self._subscribed_error:
            cmd = pubsub_pb2.Command(
                channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
                unsubscribe_message={})
            req.commands.append(cmd)
        try:
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)
        except Exception:
            pass
        self._subscribed_error = False
