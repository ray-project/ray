import os
from collections import deque
import logging
import random
import threading
from typing import Optional, Tuple

import grpc
try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

import ray._private.gcs_utils as gcs_utils
import ray._private.logging_utils as logging_utils
from ray.core.generated.gcs_pb2 import ErrorTableData
from ray.core.generated import dependency_pb2
from ray.core.generated import gcs_service_pb2_grpc
from ray.core.generated import gcs_service_pb2
from ray.core.generated import pubsub_pb2

logger = logging.getLogger(__name__)


def gcs_pubsub_enabled():
    """Checks whether GCS pubsub feature flag is enabled."""
    return os.environ.get("RAY_gcs_grpc_based_pubsub") not in \
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


class _PublisherBase:
    @staticmethod
    def _create_log_request(log_json: dict):
        job_id = log_json.get("job")
        return gcs_service_pb2.GcsPublishRequest(pub_messages=[
            pubsub_pb2.PubMessage(
                channel_type=pubsub_pb2.RAY_LOG_CHANNEL,
                key_id=job_id.encode() if job_id else None,
                log_batch_message=logging_utils.log_batch_dict_to_proto(
                    log_json))
        ])

    @staticmethod
    def _create_function_key_request(key: bytes):
        return gcs_service_pb2.GcsPublishRequest(pub_messages=[
            pubsub_pb2.PubMessage(
                channel_type=pubsub_pb2.RAY_PYTHON_FUNCTION_CHANNEL,
                python_function_message=dependency_pb2.PythonFunction(key=key))
        ])


class _SubscriberBase:
    def __init__(self):
        # self._subscriber_id needs to match the binary format of a random
        # SubscriberID / UniqueID, which is 28 (kUniqueIDSize) random bytes.
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))

    def _subscribe_request(self, channel):
        cmd = pubsub_pb2.Command(channel_type=channel, subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        return req

    def _poll_request(self):
        return gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)

    def _unsubscribe_request(self, channels):
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[])
        for channel in channels:
            req.commands.append(
                pubsub_pb2.Command(
                    channel_type=channel, unsubscribe_message={}))
        return req

    @staticmethod
    def _pop_error_info(queue):
        if len(queue) == 0:
            return None, None
        msg = queue.popleft()
        return msg.key_id, msg.error_info_message

    @staticmethod
    def _pop_log_batch(queue):
        if len(queue) == 0:
            return None
        msg = queue.popleft()
        return logging_utils.log_batch_proto_to_dict(msg.log_batch_message)

    @staticmethod
    def _pop_function_key(queue):
        if len(queue) == 0:
            return None
        msg = queue.popleft()
        return msg.python_function_message.key


class GcsPublisher(_PublisherBase):
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

    def publish_logs(self, log_batch: dict) -> None:
        """Publishes logs to GCS."""
        req = self._create_log_request(log_batch)
        self._stub.GcsPublish(req)

    def publish_function_key(self, key: bytes) -> None:
        """Publishes function key to GCS."""
        req = self._create_function_key_request(key)
        self._stub.GcsPublish(req)


class _SyncSubscriber(_SubscriberBase):
    def __init__(
            self,
            pubsub_channel_type,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        super().__init__()

        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        # GRPC stub to GCS pubsub.
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

        # Type of the channel.
        self._channel = pubsub_channel_type
        # Protects multi-threaded read and write of self._queue.
        self._lock = threading.Lock()
        # A queue of received PubMessage.
        self._queue = deque()
        # Indicates whether the subscriber has closed.
        self._close = threading.Event()

    def subscribe(self) -> None:
        """Registers a subscription for the subscriber's channel type.

        Before the registration, published messages in the channel will not be
        saved for the subscriber.
        """
        with self._lock:
            if self._close.is_set():
                return
            req = self._subscribe_request(self._channel)
            self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    def _poll_locked(self, timeout=None) -> None:
        assert self._lock.locked()

        # Poll until data becomes available.
        while len(self._queue) == 0:
            if self._close.is_set():
                return

            fut = self._stub.GcsSubscriberPoll.future(
                self._poll_request(), timeout=timeout)
            # Wait for result to become available, or cancel if the
            # subscriber has closed.
            while True:
                try:
                    # Use 1s timeout to check for subscriber closing
                    # periodically.
                    fut.result(timeout=1)
                    break
                except grpc.FutureTimeoutError:
                    # Subscriber has closed. Cancel inflight request and
                    # return from polling.
                    if self._close.is_set():
                        fut.cancel()
                        return
                    # GRPC has not replied, continue waiting.
                    continue
                except grpc.RpcError as e:
                    # Choose to not raise deadline exceeded errors to the
                    # caller. Instead return None. This can be revisited later.
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        return
                    raise

            if fut.done():
                for msg in fut.result().pub_messages:
                    if msg.channel_type != self._channel:
                        logger.warn(
                            f"Ignoring message from unsubscribed channel {msg}"
                        )
                        continue
                    self._queue.append(msg)

    def close(self) -> None:
        """Closes the subscriber and its active subscription."""

        # Mark close to terminate inflight polling and prevent future requests.
        if self._close.is_set():
            return
        self._close.set()
        req = self._unsubscribe_request(channels=[self._channel])
        try:
            self._stub.GcsSubscriberCommandBatch(req, timeout=5)
        except Exception:
            pass
        self._stub = None


class GcsErrorSubscriber(_SyncSubscriber):
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

    def __init__(
            self,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.RAY_ERROR_INFO_CHANNEL, address, channel)

    def poll(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages.

        Returns:
            A tuple of error message ID and ErrorTableData proto message,
            or None, None if polling times out or subscriber closed.
        """
        with self._lock:
            self._poll_locked(timeout=timeout)
            return self._pop_error_info(self._queue)


class GcsLogSubscriber(_SyncSubscriber):
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

    def __init__(
            self,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.RAY_LOG_CHANNEL, address, channel)

    def poll(self, timeout=None) -> Optional[dict]:
        """Polls for new log messages.

        Returns:
            A dict containing a batch of log lines and their metadata,
            or None if polling times out or subscriber closed.
        """
        with self._lock:
            self._poll_locked(timeout=timeout)
            return self._pop_log_batch(self._queue)


class GcsFunctionKeySubscriber(_SyncSubscriber):
    """Subscriber to function（and actor class) dependency keys. Thread safe.

    Usage example:
        subscriber = GcsFunctionKeySubscriber()
        # Subscribe to the function key channel.
        subscriber.subscribe()
        ...
        while running:
            key = subscriber.poll()
            ......
        # Unsubscribe from the function key channel.
        subscriber.close()
    """

    def __init__(
            self,
            address: str = None,
            channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.RAY_PYTHON_FUNCTION_CHANNEL, address,
                         channel)

    def poll(self, timeout=None) -> Optional[bytes]:
        """Polls for new function key messages.

        Returns:
            A byte string of function key.
            None if polling times out or subscriber closed.
        """
        with self._lock:
            self._poll_locked(timeout=timeout)
            return self._pop_function_key(self._queue)


class GcsAioPublisher(_PublisherBase):
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

    async def publish_logs(self, log_batch: dict) -> None:
        """Publishes logs to GCS."""
        req = self._create_log_request(log_batch)
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
        super().__init__()

        if address:
            assert channel is None, \
                "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address, aio=True)
        else:
            assert channel is not None, \
                "One of address and channel must be specified"
        # Message queue for each channel.
        self._messages = {}
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

    async def subscribe_error(self) -> None:
        """Registers a subscription for error info.

        Before the registration, published errors will not be saved for the
        subscriber.
        """
        if pubsub_pb2.RAY_ERROR_INFO_CHANNEL not in self._messages:
            self._messages[pubsub_pb2.RAY_ERROR_INFO_CHANNEL] = deque()
            req = self._subscribe_request(pubsub_pb2.RAY_ERROR_INFO_CHANNEL)
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    async def subscribe_logs(self) -> None:
        """Registers a subscription for logs.

        Before the registration, published logs will not be saved for the
        subscriber.
        """
        if pubsub_pb2.RAY_LOG_CHANNEL not in self._messages:
            self._messages[pubsub_pb2.RAY_LOG_CHANNEL] = deque()
            req = self._subscribe_request(pubsub_pb2.RAY_LOG_CHANNEL)
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    def _enqueue_poll_response(self, resp):
        for msg in resp.pub_messages:
            queue = self._messages.get(msg.channel_type)
            if queue is not None:
                queue.append(msg)
            else:
                logger.warn(
                    f"Ignoring message from unsubscribed channel {msg}")

    async def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        queue = self._messages.get(pubsub_pb2.RAY_ERROR_INFO_CHANNEL)
        while len(queue) == 0:
            req = self._poll_request()
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            self._enqueue_poll_response(reply)

        return self._pop_error_info(queue)

    async def poll_logs(self, timeout=None) -> dict:
        """Polls for new error messages."""
        queue = self._messages.get(pubsub_pb2.RAY_LOG_CHANNEL)
        while len(queue) == 0:
            req = self._poll_request()
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            self._enqueue_poll_response(reply)

        return self._pop_log_batch(queue)

    async def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        req = self._unsubscribe_request(self._messages.keys())
        try:
            await self._stub.GcsSubscriberCommandBatch(req, timeout=5)
        except Exception:
            pass
