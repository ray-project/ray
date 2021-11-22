import os
from collections import deque
import logging
import random
import threading
import time
from typing import Tuple

import grpc
try:
    from grpc import aio as aiogrpc
except ImportError:
    from grpc.experimental import aio as aiogrpc

import ray._private.gcs_utils as gcs_utils
import ray._private.logging_utils as logging_utils
from ray.core.generated.gcs_pb2 import ErrorTableData
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
    def _create_log_request(self, log_json: dict):
        job_id = log_json.get("job")
        return gcs_service_pb2.GcsPublishRequest(pub_messages=[
            pubsub_pb2.PubMessage(
                channel_type=pubsub_pb2.RAY_LOG_CHANNEL,
                key_id=job_id.encode() if job_id else None,
                log_batch_message=logging_utils.log_batch_dict_to_proto(
                    log_json))
        ])


class _SubscriberBase:
    def __init__(self):
        self._subscriber_id = bytes(
            bytearray(random.getrandbits(8) for _ in range(28)))
        # Maps channel type to a deque of received PubMessage.
        self._messages: dict = {}

    def _subscribe_error_request(self):
        if pubsub_pb2.RAY_ERROR_INFO_CHANNEL not in self._messages:
            self._messages[pubsub_pb2.RAY_ERROR_INFO_CHANNEL] = deque()
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        return req

    def _subscribe_logs_request(self):
        if pubsub_pb2.RAY_LOG_CHANNEL not in self._messages:
            self._messages[pubsub_pb2.RAY_LOG_CHANNEL] = deque()
        cmd = pubsub_pb2.Command(
            channel_type=pubsub_pb2.RAY_LOG_CHANNEL, subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[cmd])
        return req

    def _poll_request(self):
        return gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id)

    def _process_poll_response(self, resp):
        for msg in resp.pub_messages:
            queue = self._messages.get(msg.channel_type)
            if queue is not None:
                queue.append(msg)
            else:
                logger.warn(
                    f"Received message from unsubscribed channel {msg}")

    def _unsubscribe_request(self):
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, commands=[])
        for channel, _ in self._messages.items():
            req.commands.append(
                pubsub_pb2.Command(
                    channel_type=channel, unsubscribe_message={}))
        return req

    def _has_error_info(self) -> bool:
        errors = self._messages.get(pubsub_pb2.RAY_ERROR_INFO_CHANNEL)
        return len(errors) > 0

    def _pop_error_info(self) -> Tuple[bytes, ErrorTableData]:
        errors = self._messages.get(pubsub_pb2.RAY_ERROR_INFO_CHANNEL)
        if len(errors) == 0:
            return None, None
        msg = errors.popleft()
        return msg.key_id, msg.error_info_message

    def _has_log_batch(self) -> bool:
        logs = self._messages.get(pubsub_pb2.RAY_LOG_CHANNEL)
        return len(logs) > 0

    def _pop_log_batch(self) -> dict:
        logs = self._messages.get(pubsub_pb2.RAY_LOG_CHANNEL)
        if len(logs) == 0:
            return None
        msg = logs.popleft()
        return logging_utils.log_batch_proto_to_dict(msg.log_batch_message)


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


class GcsSubscriber(_SubscriberBase):
    """Subscriber to GCS. Thread safe.

    Usage example:
        subscriber = GcsSubscriber()
        # Subscribe to one or more channels.
        subscriber.subscribe_error()
        ...
        while running:
            error_id, error_data = subscriber.poll_error()
            ......
        # Unsubscribe from all channels.
        subscriber.close()
    """

    def __init__(
            self,
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
        # Protects multi-threaded read and write of self._messages.
        # Also protects self._polling_thread.
        self._lock = threading.Lock()
        # Pollers of different channels use self._cond to wait on data
        # available or subscriber closing. And on the flip side, this Condition
        # must be notified whenever self._messages or self._close changes.
        self._cond = threading.Condition(lock=self._lock)
        # Thread for making polling requests.
        self._polling_thread: threading.Thread = None
        # GRPC stub to GCS pubsub.
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)
        # Indicates whether the subscriber has closed.
        self._close = threading.Event()

    def subscribe_error(self) -> None:
        """Registers a subscription for error info.

        Before the registration, published errors will not be saved for the
        subscriber.
        """
        with self._lock:
            if self._close.is_set():
                return
            req = self._subscribe_error_request()
        self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    def subscribe_logs(self) -> None:
        """Registers a subscription for logs.

        Before the registration, published logs will not be saved for the
        subscriber.
        """
        with self._lock:
            if self._close.is_set():
                return
            req = self._subscribe_logs_request()
        self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    def _do_polling(self) -> grpc.Future:
        req = self._poll_request()
        fut = self._stub.GcsSubscriberPoll.future(req)
        while True:
            # Terminate the polling if the subscriber has closed.
            if self._close.is_set():
                break
            try:
                # Release lock and wait for at most 1s for result to
                # become available. This allows checking if the subscriber
                # has closed.
                fut.result(timeout=1)
                # Result available.
                break
            except grpc.FutureTimeoutError:
                # GRPC has not replied, continue waiting.
                continue
            except Exception:
                if fut.done():
                    # This should be a GRPC error e.g. connection unavailable.
                    # Abort and retry from the outer loop.
                    # The exception is stored in fut.
                    break
                # Unknown failure.
                raise
        return fut

    def _polling_loop(self):
        """Main loop of the polling thread.

        Polling thread is terminated when subscriber has closed.
        """
        exception_count = 0
        while True:
            # Terminate the thread if the subscriber has closed.
            if self._close.is_set():
                break

            # Run one polling request until it is done or an error occurs.
            fut = self._do_polling()

            # Happens only when the subscriber is closed.
            if fut.running():
                fut.cancel()
                break

            # Ignore failure and retry polling.
            if fut.exception():
                exception_count += 1
                if exception_count % 100 == 0:
                    logger.warn("GCS subscriber polling failed: "
                                f"{fut.exception()}. Retrying ...")
                time.sleep(1)
                continue

            with self._lock:
                # fut.result() should not throw exception here.
                self._process_poll_response(fut.result())
                self._cond.notify_all()

    def _poll(self, done=None, timeout=None):
        """Starts a polling thread if it has not started yet."""
        assert self._lock.locked()

        if not self._polling_thread:
            self._polling_thread = threading.Thread(
                target=self._polling_loop,
                name="gcs_subscriber_polling",
                daemon=True)
            self._polling_thread.start()

        def done_polling():
            return self._close.is_set() or done()

        self._cond.wait_for(done_polling, timeout=timeout)

    def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages.

        :return: A tuple of error message ID and ErrorTableData proto message,
        or None, None if polling times out or subscriber closed.
        """
        with self._lock:
            self._poll(done=self._has_error_info, timeout=timeout)
            return self._pop_error_info()

    def poll_logs(self, timeout=None) -> dict:
        """Polls for new log batch.

        :return: A dict containing a batch of log lines and their metadata,
        or None if polling times out or subscriber closed.
        """
        with self._lock:
            self._poll(done=self._has_log_batch, timeout=timeout)
            return self._pop_log_batch()

    def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        # Mark close to terminate inflight polling and prevent future requests.
        self._close.set()
        with self._lock:
            self._cond.notify_all()
            if not self._polling_thread:
                # Subscriber already closed or never started.
                return
            polling_thread = self._polling_thread
            self._polling_thread = None
            req = self._unsubscribe_request()
            try:
                self._stub.GcsSubscriberCommandBatch(req, timeout=30)
            except Exception:
                pass
            self._stub = None
        polling_thread.join()


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
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

    async def subscribe_error(self) -> None:
        """Registers a subscription for error info.

        Before the registration, published errors will not be saved for the
        subscriber.
        """
        req = self._subscribe_error_request()
        await self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    async def subscribe_logs(self) -> None:
        """Registers a subscription for logs.

        Before the registration, published logs will not be saved for the
        subscriber.
        """
        req = self._subscribe_logs_request()
        await self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    async def poll_error(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error messages."""
        while not self._has_error_info():
            req = self._poll_request()
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            self._process_poll_response(reply)

        return self._pop_error_info()

    async def poll_logs(self, timeout=None) -> dict:
        """Polls for new error messages."""
        while not self._has_log_batch():
            req = self._poll_request()
            reply = await self._stub.GcsSubscriberPoll(req, timeout=timeout)
            self._process_poll_response(reply)

        return self._pop_log_batch()

    async def close(self) -> None:
        """Closes the subscriber and its active subscriptions."""
        req = self._unsubscribe_request()
        try:
            await self._stub.GcsSubscriberCommandBatch(req, timeout=30)
        except Exception:
            pass
