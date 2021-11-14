import logging
import threading
from typing import Any, Dict, List, Union

from anyscale_node_provider.generated import node_provider_pb2, node_provider_pb2_grpc
import grpc
from ray.autoscaler.node_provider import NodeProvider


# Note: Log handlers set up in autoscaling monitor entrypoint.
logger = logging.getLogger(__name__)

provider_exists = False


class KuberayNodeProvider(NodeProvider):  # type: ignore
    def __init__(
        self,
        provider_config: Dict[str, Any],
        cluster_name: str,
        _allow_multiple: bool = False,
    ):
        logger.info("Creating ClientNodeProvider.")
        self._grpc_channel = None

        # Disallow multiple node providers, unless explicitly allowed for testing.
        global provider_exists
        if not _allow_multiple:
            assert (
                not provider_exists
            ), "Only one ClientNodeProvider allowed per process."
        assert (
            provider_config.get("disable_node_updaters", False) is True
        ), "Must disable node updaters to use ClientNodeProvider."
        provider_exists = True

        super().__init__(provider_config, cluster_name)

        # Map of node ids (pod names) to cached tags (`metadata.labels`)
        self.tag_cache: Dict[str, Dict[str, str]] = {}
        # Map of node ids (pod names) to cached pod ips.
        self.ip_cache: Dict[str, str] = {}
        # Lock the caches, which are accessed by main autoscaler thread and node
        # launcher child thread.
        self.cache_lock = threading.RLock()

        self._connect_client_and_instantiate_stub()

    def _connect_client_and_instantiate_stub(self) -> None:
        server_address = self.provider_config.get("server_address")
        assert server_address is not None
        self._grpc_channel = grpc.insecure_channel(server_address)
        self._node_provider_stub = node_provider_pb2_grpc.NodeProviderStub(
            self._grpc_channel
        )

    def __del__(self) -> None:
        if self._grpc_channel:
            self._grpc_channel.close()

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Dict[str, Dict[str, str]]:
        """Creates a number of nodes within the namespace.
        Returns a mapping from created node ids to node metadata.
        """
        tags["ray-cluster-name"] = self.cluster_name
        request = node_provider_pb2.CreateNodeRequest(tags=tags, count=count)
        response = self._node_provider_stub.CreateNode(request)
        node_to_meta = _node_meta_response_to_dict(response)
        return node_to_meta

    def _internal_ip_rpc(self, node_id: str) -> str:
        """Returns the ip of the given node by direct RPC to the operator.
        """
        request = node_provider_pb2.InternalIpRequest(node_id=node_id)
        response = self._node_provider_stub.InternalIp(request)
        return response.node_ip

    def _node_tags_rpc(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict) by direct RPC to the operator."""
        request = node_provider_pb2.NodeTagsRequest(node_id=node_id)
        response = self._node_provider_stub.NodeTags(request)
        return response.node_tags

    def internal_ip(self, node_id: str) -> str:
        """Returns the ip of the given node from the ip cache.
        Falls back to direct RPC if necessary.
        """
        with self.cache_lock:
            ip = self.ip_cache.get(node_id, None)
            if ip is None:
                logger.warning(f"IP of node {node_id} not found in the cache.")
                ip = self._internal_ip_rpc(node_id)
                self.ip_cache[node_id] = ip
            return ip

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict) from the tag cache.
        Falls back to direct RPC if necessary.
        """
        with self.cache_lock:
            tags = self.tag_cache.get(node_id, None)
            if tags is None:
                logger.warning(f"Tags of node {node_id} not found in the cache.")
                tags = self._node_tags_rpc(node_id)
                self.tag_cache[node_id] = tags
            return tags

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.
        Also updates caches of ips and tags.
        """
        request = node_provider_pb2.NonTerminatedNodesRequest(
            tag_filters=tag_filters, cluster_name=self.cluster_name
        )
        response = self._node_provider_stub.NonTerminatedNodes(request)

        with self.cache_lock:
            # If we're not filtering on anything, we're about to extract data on
            # all Ray nodes in the cluster. We may as well take the opportunity to
            # clear the caches of stale data before re-filling the caches.
            if tag_filters == {}:
                # Clear caches. (This logic is accessed at least once per autoscaler update.)
                self.tag_cache = {}
                self.ip_cache = {}

            # Fill caches.
            for node_id, node_ip, node_tags_message in zip(
                response.node_ids, response.node_ips, response.node_tags_list
            ):
                self.tag_cache[node_id] = node_tags_message.node_tags
                self.ip_cache[node_id] = node_ip

        return list(response.node_ids)

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node."""
        self.terminate_nodes([node_id])

    def terminate_nodes(self, node_ids: List[str]) -> Dict[str, Dict[str, str]]:
        """Terminates a set of nodes.
        Returns map of deleted node ids to node metadata.
        """
        request = node_provider_pb2.TerminateNodesRequest(node_ids=node_ids)
        response = self._node_provider_stub.TerminateNodes(request)
        node_to_meta = _node_meta_response_to_dict(response)
        return node_to_meta


def _node_meta_response_to_dict(
    response: Union[
        node_provider_pb2.CreateNodeResponse, node_provider_pb2.TerminateNodesResponse
    ]
) -> Dict[str, Dict[str, str]]:
    # Convert response values to dicts
    node_to_meta = {
        key: dict(value.node_meta) for key, value in response.node_to_meta.items()
    }
    return node_to_meta