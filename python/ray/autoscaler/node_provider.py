import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.command_runner import \
    SSHCommandRunner, DockerCommandRunner

logger = logging.getLogger(__name__)


class NodeProvider:
    """Interface for getting and returning nodes from a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom node providers. It is not allowed to call into
    NodeProvider methods from any Ray package outside the autoscaler, only to
    define new implementations of NodeProvider for use with the "external" node
    provider option.

    NodeProviders are namespaced by the `cluster_name` parameter; they only
    operate on nodes within that namespace.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config: Dict[str, Any],
                 cluster_name: str) -> None:
        self.provider_config = provider_config
        self.cluster_name = cluster_name
        self._internal_ip_cache: Dict[str, str] = {}
        self._external_ip_cache: Dict[str, str] = {}

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to nodes() to
        serve single-node queries (e.g. is_running(node_id)). This means that
        nodes() must be called again to refresh results.

        Examples:
            >>> provider.non_terminated_nodes({TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]
        """
        raise NotImplementedError

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        raise NotImplementedError

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        raise NotImplementedError

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        raise NotImplementedError

    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        raise NotImplementedError

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        raise NotImplementedError

    def get_node_id(self, ip_address: str,
                    use_internal_ip: bool = False) -> str:
        """Returns the node_id given an IP address.

        Assumes ip-address is unique per node.

        Args:
            ip_address (str): Address of node.
            use_internal_ip (bool): Whether the ip address is
                public or private.

        Raises:
            ValueError if not found.
        """

        def find_node_id():
            if use_internal_ip:
                return self._internal_ip_cache.get(ip_address)
            else:
                return self._external_ip_cache.get(ip_address)

        if not find_node_id():
            all_nodes = self.non_terminated_nodes({})
            for node_id in all_nodes:
                if use_internal_ip:
                    int_ip = self.internal_ip(node_id)
                    self._internal_ip_cache[int_ip] = node_id
                else:
                    ext_ip = self.external_ip(node_id)
                    self._external_ip_cache[ext_ip] = node_id

        if not find_node_id():
            if use_internal_ip:
                known_msg = (
                    f"Worker internal IPs: {list(self._internal_ip_cache)}")
            else:
                known_msg = (
                    f"Worker external IP: {list(self._external_ip_cache)}")
            raise ValueError(f"ip {ip_address} not found. " + known_msg)

        return find_node_id()

    def create_node(self, node_config: Dict[str, Any], tags: Dict[str, str],
                    count: int) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.

        Optionally returns a mapping from created node ids to node metadata.
        """
        raise NotImplementedError

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        raise NotImplementedError

    def terminate_node(self, node_id: str) -> None:
        """Terminates the specified node."""
        raise NotImplementedError

    def terminate_nodes(self, node_ids: List[str]) -> None:
        """Terminates a set of nodes. May be overridden with a batch method."""
        for node_id in node_ids:
            logger.info("NodeProvider: "
                        "{}: Terminating node".format(node_id))
            self.terminate_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config

    def get_command_runner(self,
                           log_prefix: str,
                           node_id: str,
                           auth_config: Dict[str, Any],
                           cluster_name: str,
                           process_runner: ModuleType,
                           use_internal_ip: bool,
                           docker_config: Optional[Dict[str, Any]] = None
                           ) -> CommandRunnerInterface:
        """Returns the CommandRunner class used to perform SSH commands.

        Args:
        log_prefix(str): stores "NodeUpdater: {}: ".format(<node_id>). Used
            to print progress in the CommandRunner.
        node_id(str): the node ID.
        auth_config(dict): the authentication configs from the autoscaler
            yaml file.
        cluster_name(str): the name of the cluster.
        process_runner(module): the module to use to run the commands
            in the CommandRunner. E.g., subprocess.
        use_internal_ip(bool): whether the node_id belongs to an internal ip
            or external ip.
        docker_config(dict): If set, the docker information of the docker
            container that commands should be run on.
        """
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip
        }
        if docker_config and docker_config["container_name"] != "":
            return DockerCommandRunner(docker_config, **common_args)
        else:
            return SSHCommandRunner(**common_args)

    def prepare_for_head_node(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        return cluster_config

    @staticmethod
    def fillout_available_node_types_resources(
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        return cluster_config
