import logging
import time
from typing import Optional

from ray._private import ray_constants
from ray.core.generated.common_pb2 import ErrorType, JobConfig
from ray.core.generated.gcs_pb2 import (
    ActorTableData,
    AvailableResources,
    ErrorTableData,
    GcsEntry,
    GcsNodeInfo,
    JobTableData,
    PlacementGroupTableData,
    PubSubMessage,
    ResourceDemand,
    ResourceLoad,
    ResourcesData,
    ResourceUsageBatchData,
    TablePrefix,
    TablePubsub,
    TaskEvents,
    TotalResources,
    WorkerTableData,
)
from ray.core.generated.gcs_service_pb2 import GetAllNodeInfoRequest

logger = logging.getLogger(__name__)

__all__ = [
    "ActorTableData",
    "GcsNodeInfo",
    "AvailableResources",
    "TotalResources",
    "JobTableData",
    "JobConfig",
    "ErrorTableData",
    "ErrorType",
    "GcsEntry",
    "ResourceUsageBatchData",
    "ResourcesData",
    "TablePrefix",
    "TablePubsub",
    "TaskEvents",
    "ResourceDemand",
    "ResourceLoad",
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
    from ray._private.grpc_utils import init_grpc_channel

    return init_grpc_channel(address, options=_GRPC_OPTIONS, asynchronous=aio)


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


def cleanup_redis_storage(
    host: str,
    port: int,
    password: str,
    use_ssl: bool,
    storage_namespace: str,
    username: Optional[str] = None,
):
    """This function is used to cleanup the GCS storage in Redis.
    It supports Redis in cluster and non-cluster modes.

    Args:
       host: The Redis host address.
       port: The Redis port.
       username: The Redis username.
       password: The Redis password.
       use_ssl: Whether to encrypt the connection.
       storage_namespace: The namespace of the storage to be deleted.
    """

    from ray._raylet import del_key_prefix_from_storage  # type: ignore

    if not isinstance(host, str):
        raise ValueError("Host must be a string")

    if username is None:
        username = ""

    if not isinstance(username, str):
        raise ValueError("Username must be a string")

    if not isinstance(password, str):
        raise ValueError("Password must be a string")

    if port < 0:
        raise ValueError(f"Invalid port: {port}")

    if not isinstance(use_ssl, bool):
        raise TypeError("use_ssl must be a boolean")

    if not isinstance(storage_namespace, str):
        raise ValueError("storage namespace must be a string")

    # Right now, GCS stores all data in multiple hashes with keys prefixed by
    # storage_namespace. So we only need to delete the specific key prefix to cleanup
    # the cluster's data.
    # Note this deletes all keys with prefix `RAY{key_prefix}@`, not `{key_prefix}`.
    return del_key_prefix_from_storage(
        host, port, username, password, use_ssl, storage_namespace
    )


def get_node_info_with_retry(
    gcs_client,
    node_ip_address: str,
    timeout_s: float = 30,
    retry_interval_s: float = 1,
) -> GcsNodeInfo:
    """Get node info from GCS with retry logic.

    Keeps retrying until the node is found or timeout is reached.

    Some Ray processes (e.g., ray_client_server) start in parallel
    with the raylet. When they query GCS for node info, the raylet may not have
    registered yet. This function retries until the node info is available.

    Args:
        gcs_client: A connected GcsClient instance.
        node_ip_address: The IP address of the node to find.
        timeout_s: Total timeout in seconds. Default 30s.
        retry_interval_s: Interval between retries in seconds. Default 1s.

    Returns:
        GcsNodeInfo proto for the matching node. If multiple nodes match,
        the head node is prioritized.

    Raises:
        RuntimeError: If no matching node is found within the timeout.
    """
    end_time = time.time() + timeout_s
    node_selector = GetAllNodeInfoRequest.NodeSelector(node_ip_address=node_ip_address)

    while True:
        try:
            node_infos = gcs_client.get_all_node_info(
                timeout=retry_interval_s,
                node_selectors=[node_selector],
                state_filter=GcsNodeInfo.GcsNodeState.ALIVE,
            )
            if node_infos:
                # Prioritize head node if multiple nodes match
                for node_info in node_infos.values():
                    if node_info.is_head_node:
                        return node_info
                # Otherwise return the first one
                return next(iter(node_infos.values()))
        except Exception:
            pass

        if time.time() >= end_time:
            raise RuntimeError(
                f"Timed out waiting for node info for {node_ip_address}."
            )

        time.sleep(retry_interval_s)
