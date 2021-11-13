import os
from collections import deque
import logging
import random
import threading
from typing import Tuple

import grpc
try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

import ray._private.gcs_utils as gcs_utils
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated.gcs_pb2 import (
    ErrorTableData, )
from ray.core.generated import pubsub_pb2

logger = logging.getLogger(__name__)


def gcs_pubsub_enabled():
    """Checks whether GCS pubsub feature flag is enabled."""
    return os.environ.get("RAY_gcs_grpc_based_pubsub") not in\
        [None, "0", "false"]


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


class _SubscriberBase:
    def _subscribe_error_request(self):
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        return req

    def _poll_request(self):
        return gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)

    def _unsubscribe_request(self):
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[])
        if self._subscribed_error:
            cmd = pubsub_pb2.Command(
                channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
                unsubscribe_message={})
            req.commands.append(cmd)
        return req


class GcsPublisher:
    """Publisher to GCS."""

    def __init__(self, *, address: str = None, channel: grpc.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address)
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


class GcsSubscriber(_SubscriberBase):
    """Subscriber to GCS. Thread safe.

    Usage example:
        subscriber = GcsSubscriber()
        subscriber.subscribe_error()
        while running:
            error_id, error_data = subscriber.poll_error()
            ......
        subscriber.close()
    """

    def __init__(
            self,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address)
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
        """Registers a subscription for error info.

        Before the registration, published errors will not be saved for the
        subscriber.
        """
        with self._lock:
            if self._close.is_set():
                return
            if not self._subscribed_error:
                req = self._subscribe_error_request()
                self._stub.GcsSubscriberCommandBatch(req, timeout=30)
                self._subscribed_error = True

    def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        with self._lock:
            if self._close.is_set():
                return

            if len(self._errors) == 0:
                req = self._poll_request()
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
            req = self._unsubscribe_request()
            try:
                self._stub.GcsSubscriberCommandBatch(req, timeout=30)
            except Exception:
                pass
            self._stub = None


class GcsAioPublisher:
    """Publisher to GCS. Uses async io."""

    def __init__(self, address: str = None, channel: aiogrpc.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address, aio=True)
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


class GcsAioSubscriber(_SubscriberBase):
    """Async io subscriber to GCS.

    Usage example:
        subscriber = GcsAioSubscriber()
        await subscriber.subscribe_error()
        while running:
            error_id, error_data = await subscriber.poll_error()
            ......
        await subscriber.close()
    """

    def __init__(self, address: str = None, channel: aiogrpc.Channel = None):
        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address, aio=True)
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
        """Registers a subscription for error info.

        Before the registration, published errors will not be saved for the
        subscriber.
        """
        if not self._subscribed_error:
            req = self._subscribe_error_request()
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)
            self._subscribed_error = True

    async def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        if len(self._errors) == 0:
            req = self._poll_request()
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            for msg in reply.pub_messages:
                self._errors.append((msg.key_id, msg.error_info_message))

        if len(self._errors) == 0:
            return None, None
        return self._errors.popleft()

    async def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        req = self._unsubscribe_request()
        try:
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)
        except Exception:
            pass
        self._subscribed_error = False
