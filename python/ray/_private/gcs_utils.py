import enum
import logging
import inspect
import os
import asyncio
from functools import wraps
from typing import Optional

import grpc

from ray._private import ray_constants

import ray._private.gcs_aio_client

from ray.core.generated.common_pb2 import ErrorType, JobConfig
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    AvailableResources,
    ErrorTableData,
    GcsEntry,
    GcsNodeInfo,
    JobTableData,
    ObjectTableData,
    PlacementGroupTableData,
    PubSubMessage,
    ResourceDemand,
    ResourceLoad,
    ResourceMap,
    ResourcesData,
    ResourceTableData,
    ResourceUsageBatchData,
    TablePrefix,
    TablePubsub,
    TaskEvents,
    WorkerTableData,
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
    "TablePrefix",
    "TablePubsub",
    "TaskEvents",
    "ResourceDemand",
    "ResourceLoad",
    "ResourceMap",
    "ResourceTableData",
    "PubSubMessage",
    "WorkerTableData",
    "PlacementGroupTableData",
]


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
_GRPC_OPTIONS = [
    *ray_constants.GLOBAL_GRPC_OPTIONS,
    ("grpc.max_send_message_length", _MAX_MESSAGE_LENGTH),
    ("grpc.max_receive_message_length", _MAX_MESSAGE_LENGTH),
    ("grpc.keepalive_time_ms", _GRPC_KEEPALIVE_TIME_MS),
    ("grpc.keepalive_timeout_ms", _GRPC_KEEPALIVE_TIMEOUT_MS),
]


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


# This global variable is used for testing only
_called_freq = {}


def _auto_reconnect(f):
    # This is for testing to count the frequence
    # of gcs call
    if inspect.iscoroutinefunction(f):

        @wraps(f)
        async def wrapper(self, *args, **kwargs):
            if "TEST_RAY_COLLECT_KV_FREQUENCY" in os.environ:
                global _called_freq
                name = f.__name__
                if name not in _called_freq:
                    _called_freq[name] = 0
                _called_freq[name] += 1

            remaining_retry = self._nums_reconnect_retry
            while True:
                try:
                    return await f(self, *args, **kwargs)
                except grpc.RpcError as e:
                    if e.code() in (
                        grpc.StatusCode.UNAVAILABLE,
                        grpc.StatusCode.UNKNOWN,
                    ):
                        if remaining_retry <= 0:
                            logger.error(
                                "Failed to connect to GCS. Please check"
                                " `gcs_server.out` for more details."
                            )
                            raise
                        logger.debug(
                            "Failed to send request to gcs, reconnecting. " f"Error {e}"
                        )
                        try:
                            self._connect()
                        except Exception:
                            logger.error(f"Connecting to gcs failed. Error {e}")
                        await asyncio.sleep(1)
                        remaining_retry -= 1
                        continue
                    raise

        return wrapper
    else:

        raise NotImplementedError(
            "This code moved to Cython, see "
            "https://github.com/ray-project/ray/pull/33769"
        )


class GcsChannel:
    def __init__(self, gcs_address: Optional[str] = None, aio: bool = False):
        self._gcs_address = gcs_address
        self._aio = aio

    @property
    def address(self):
        return self._gcs_address

    def connect(self):
        # GCS server uses a cached port, so it should use the same port after
        # restarting. This means GCS address should stay the same for the
        # lifetime of the Ray cluster.
        self._channel = create_gcs_channel(self._gcs_address, self._aio)

    def channel(self):
        return self._channel


class GcsCode(enum.IntEnum):
    # corresponding to ray/src/ray/common/status.h
    OK = 0
    NotFound = 17
    GrpcUnavailable = 26


# re-export
GcsAioClient = ray._private.gcs_aio_client.GcsAioClient


def cleanup_redis_storage(
    host: str, port: int, password: str, use_ssl: bool, storage_namespace: str
):
    """This function is used to cleanup the storage. Before we having
    a good design for storage backend, it can be used to delete the old
    data. It support redis cluster and non cluster mode.

    Args:
       host: The host address of the Redis.
       port: The port of the Redis.
       password: The password of the Redis.
       use_ssl: Whether to encrypt the connection.
       storage_namespace: The namespace of the storage to be deleted.
    """

    from ray._raylet import del_key_from_storage  # type: ignore

    if not isinstance(host, str):
        raise ValueError("Host must be a string")

    if not isinstance(password, str):
        raise ValueError("Password must be a string")

    if port < 0:
        raise ValueError(f"Invalid port: {port}")

    if not isinstance(use_ssl, bool):
        raise TypeError("use_ssl must be a boolean")

    if not isinstance(storage_namespace, str):
        raise ValueError("storage namespace must be a string")

    # Right now, GCS store all data into a hash set key by storage_namespace.
    # So we only need to delete the specific key to cleanup the cluster.
    return del_key_from_storage(host, port, password, use_ssl, storage_namespace)
