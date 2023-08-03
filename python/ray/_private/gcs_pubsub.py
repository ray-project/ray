import asyncio
from collections import deque
import logging
import random
from typing import Tuple, List

import grpc
from ray._private.utils import get_or_create_event_loop

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
from ray.core.generated import common_pb2
from ray.core.generated import pubsub_pb2

logger = logging.getLogger(__name__)

# Max retries for GCS publisher connection error
MAX_GCS_PUBLISH_RETRIES = 60


class _PublisherBase:
    @staticmethod
    def _create_log_request(log_json: dict):
        job_id = log_json.get("job")
        return gcs_service_pb2.GcsPublishRequest(
            pub_messages=[
                pubsub_pb2.PubMessage(
                    channel_type=pubsub_pb2.RAY_LOG_CHANNEL,
                    key_id=job_id.encode() if job_id else None,
                    log_batch_message=logging_utils.log_batch_dict_to_proto(log_json),
                )
            ]
        )

    @staticmethod
    def _create_function_key_request(key: bytes):
        return gcs_service_pb2.GcsPublishRequest(
            pub_messages=[
                pubsub_pb2.PubMessage(
                    channel_type=pubsub_pb2.RAY_PYTHON_FUNCTION_CHANNEL,
                    python_function_message=dependency_pb2.PythonFunction(key=key),
                )
            ]
        )

    @staticmethod
    def _create_node_resource_usage_request(key: str, json: str):
        return gcs_service_pb2.GcsPublishRequest(
            pub_messages=[
                pubsub_pb2.PubMessage(
                    channel_type=pubsub_pb2.RAY_NODE_RESOURCE_USAGE_CHANNEL,
                    key_id=key.encode(),
                    node_resource_usage_message=common_pb2.NodeResourceUsage(json=json),
                )
            ]
        )


class _SubscriberBase:
    def __init__(self, worker_id: bytes = None):
        self._worker_id = worker_id
        # self._subscriber_id needs to match the binary format of a random
        # SubscriberID / UniqueID, which is 28 (kUniqueIDSize) random bytes.
        self._subscriber_id = bytes(bytearray(random.getrandbits(8) for _ in range(28)))
        self._last_batch_size = 0
        self._max_processed_sequence_id = 0
        self._publisher_id = b""

    # Batch size of the result from last poll. Used to indicate whether the
    # subscriber can keep up.
    @property
    def last_batch_size(self):
        return self._last_batch_size

    def _subscribe_request(self, channel):
        cmd = pubsub_pb2.Command(channel_type=channel, subscribe_message={})
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, sender_id=self._worker_id, commands=[cmd]
        )
        return req

    def _poll_request(self):
        return gcs_service_pb2.GcsSubscriberPollRequest(
            subscriber_id=self._subscriber_id,
            max_processed_sequence_id=self._max_processed_sequence_id,
            publisher_id=self._publisher_id,
        )

    def _unsubscribe_request(self, channels):
        req = gcs_service_pb2.GcsSubscriberCommandBatchRequest(
            subscriber_id=self._subscriber_id, sender_id=self._worker_id, commands=[]
        )
        for channel in channels:
            req.commands.append(
                pubsub_pb2.Command(channel_type=channel, unsubscribe_message={})
            )
        return req

    @staticmethod
    def _should_terminate_polling(e: grpc.RpcError) -> None:
        # Caller only expects polling to be terminated after deadline exceeded.
        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return True
        # Could be a temporary connection issue. Suppress error.
        # TODO: reconnect GRPC channel?
        if e.code() == grpc.StatusCode.UNAVAILABLE:
            return True
        return False

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

    @staticmethod
    def _pop_resource_usage(queue):
        if len(queue) == 0:
            return None, None
        msg = queue.popleft()
        return msg.key_id.decode(), msg.node_resource_usage_message.json

    @staticmethod
    def _pop_actors(queue, batch_size=100):
        if len(queue) == 0:
            return []
        popped = 0
        msgs = []
        while len(queue) > 0 and popped < batch_size:
            msg = queue.popleft()
            msgs.append((msg.key_id, msg.actor_message))
            popped += 1
        return msgs


class GcsAioPublisher(_PublisherBase):
    """Publisher to GCS. Uses async io."""

    def __init__(self, address: str = None, channel: aiogrpc.Channel = None):
        if address:
            assert channel is None, "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address, aio=True)
        else:
            assert channel is not None, "One of address and channel must be specified"
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

    async def publish_error(self, key_id: bytes, error_info: ErrorTableData) -> None:
        """Publishes error info to GCS."""
        msg = pubsub_pb2.PubMessage(
            channel_type=pubsub_pb2.RAY_ERROR_INFO_CHANNEL,
            key_id=key_id,
            error_info_message=error_info,
        )
        req = gcs_service_pb2.GcsPublishRequest(pub_messages=[msg])
        await self._stub.GcsPublish(req)

    async def publish_logs(self, log_batch: dict) -> None:
        """Publishes logs to GCS."""
        req = self._create_log_request(log_batch)
        await self._stub.GcsPublish(req)

    async def publish_resource_usage(self, key: str, json: str) -> None:
        """Publishes logs to GCS."""
        req = self._create_node_resource_usage_request(key, json)
        await self._stub.GcsPublish(req)


class _AioSubscriber(_SubscriberBase):
    """Async io subscriber to GCS.

    Usage example common to Aio subscribers:
        subscriber = GcsAioXxxSubscriber(address="...")
        await subscriber.subscribe()
        while running:
            ...... = await subscriber.poll()
            ......
        await subscriber.close()
    """

    def __init__(
        self,
        pubsub_channel_type,
        worker_id: bytes = None,
        address: str = None,
        channel: aiogrpc.Channel = None,
    ):
        super().__init__(worker_id)

        if address:
            assert channel is None, "address and channel cannot both be specified"
            channel = gcs_utils.create_gcs_channel(address, aio=True)
        else:
            assert channel is not None, "One of address and channel must be specified"
        # GRPC stub to GCS pubsub.
        self._stub = gcs_service_pb2_grpc.InternalPubSubGcsServiceStub(channel)

        # Type of the channel.
        self._channel = pubsub_channel_type
        # A queue of received PubMessage.
        self._queue = deque()
        # Indicates whether the subscriber has closed.
        self._close = asyncio.Event()

    async def subscribe(self) -> None:
        """Registers a subscription for the subscriber's channel type.

        Before the registration, published messages in the channel will not be
        saved for the subscriber.
        """
        if self._close.is_set():
            return
        req = self._subscribe_request(self._channel)
        await self._stub.GcsSubscriberCommandBatch(req, timeout=30)

    async def _poll_call(self, req, timeout=None):
        # Wrap GRPC _AioCall as a coroutine.
        return await self._stub.GcsSubscriberPoll(req, timeout=timeout)

    async def _poll(self, timeout=None) -> None:
        while len(self._queue) == 0:
            req = self._poll_request()
            poll = get_or_create_event_loop().create_task(
                self._poll_call(req, timeout=timeout)
            )
            close = get_or_create_event_loop().create_task(self._close.wait())
            done, others = await asyncio.wait(
                [poll, close], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            # Cancel the other task if needed to prevent memory leak.
            other_task = others.pop()
            if not other_task.done():
                other_task.cancel()
            if poll not in done or close in done:
                # Request timed out or subscriber closed.
                break
            try:
                self._last_batch_size = len(poll.result().pub_messages)
                if poll.result().publisher_id != self._publisher_id:
                    if self._publisher_id != "":
                        logger.debug(
                            f"replied publisher_id {poll.result().publisher_id}"
                            f"different from {self._publisher_id}, this should "
                            "only happens during gcs failover."
                        )
                    self._publisher_id = poll.result().publisher_id
                    self._max_processed_sequence_id = 0
                for msg in poll.result().pub_messages:
                    if msg.sequence_id <= self._max_processed_sequence_id:
                        logger.warn(f"Ignoring out of order message {msg}")
                        continue
                    self._max_processed_sequence_id = msg.sequence_id
                    self._queue.append(msg)
            except grpc.RpcError as e:
                if self._should_terminate_polling(e):
                    return
                raise

    async def close(self) -> None:
        """Closes the subscriber and its active subscription."""

        # Mark close to terminate inflight polling and prevent future requests.
        if self._close.is_set():
            return
        self._close.set()
        req = self._unsubscribe_request(channels=[self._channel])
        try:
            await self._stub.GcsSubscriberCommandBatch(req, timeout=5)
        except Exception:
            pass
        self._stub = None


class GcsAioErrorSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.RAY_ERROR_INFO_CHANNEL, worker_id, address, channel)

    async def poll(self, timeout=None) -> Tuple[bytes, ErrorTableData]:
        """Polls for new error message.

        Returns:
            A tuple of error message ID and ErrorTableData proto message,
            or None, None if polling times out or subscriber closed.
        """
        await self._poll(timeout=timeout)
        return self._pop_error_info(self._queue)


class GcsAioLogSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.RAY_LOG_CHANNEL, worker_id, address, channel)

    async def poll(self, timeout=None) -> dict:
        """Polls for new log message.

        Returns:
            A dict containing a batch of log lines and their metadata,
            or None if polling times out or subscriber closed.
        """
        await self._poll(timeout=timeout)
        return self._pop_log_batch(self._queue)


class GcsAioResourceUsageSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(
            pubsub_pb2.RAY_NODE_RESOURCE_USAGE_CHANNEL, worker_id, address, channel
        )

    async def poll(self, timeout=None) -> Tuple[bytes, str]:
        """Polls for new resource usage message.

        Returns:
            A tuple of string reporter ID and resource usage json string.
        """
        await self._poll(timeout=timeout)
        return self._pop_resource_usage(self._queue)


class GcsAioActorSubscriber(_AioSubscriber):
    def __init__(
        self,
        worker_id: bytes = None,
        address: str = None,
        channel: grpc.Channel = None,
    ):
        super().__init__(pubsub_pb2.GCS_ACTOR_CHANNEL, worker_id, address, channel)

    @property
    def queue_size(self):
        return len(self._queue)

    async def poll(self, timeout=None, batch_size=500) -> List[Tuple[bytes, str]]:
        """Polls for new actor message.

        Returns:
            A tuple of binary actor ID and actor table data.
        """
        await self._poll(timeout=timeout)
        return self._pop_actors(self._queue, batch_size=batch_size)
