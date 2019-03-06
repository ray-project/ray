from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import redis

import ray

logger = logging.getLogger(__name__)


class Cluster(object):
    def __init__(self,
                 initialize_head=False,
                 connect=False,
                 head_node_args=None,
                 shutdown_at_exit=True):
        """Initializes the cluster.

        Args:
            initialize_head (bool): Automatically start a Ray cluster
                by initializing the head node. Defaults to False.
            connect (bool): If `initialize_head=True` and `connect=True`,
                ray.init will be called with the redis address of this cluster
                passed in.
            head_node_args (dict): Arguments to be passed into
                `start_ray_head` via `self.add_node`.
            shutdown_at_exit (bool): If True, registers an exit hook
                for shutting down all started processes.
        """
        self.head_node = None
        self.worker_nodes = set()
        self.redis_address = None
        self.connected = False
        self._shutdown_at_exit = shutdown_at_exit
        if not initialize_head and connect:
            raise RuntimeError("Cannot connect to uninitialized cluster.")

        if initialize_head:
            head_node_args = head_node_args or {}
            self.add_node(**head_node_args)
            if connect:
                self.connect()

    def connect(self):
        """Connect the driver to the cluster."""
        assert self.redis_address is not None
        assert not self.connected
        output_info = ray.init(
            ignore_reinit_error=True,
            redis_address=self.redis_address,
            redis_password=self.redis_password)
        logger.info(output_info)
        self.connected = True

    def add_node(self, **node_args):
        """Adds a node to the local Ray Cluster.

        All nodes are by default started with the following settings:
            cleanup=True,
            num_cpus=1,
            object_store_memory=100 * (2**20) # 100 MB

        Args:
            node_args: Keyword arguments used in `start_ray_head` and
                `start_ray_node`. Overrides defaults.

        Returns:
            Node object of the added Ray node.
        """
        default_kwargs = {
            "num_cpus": 1,
            "num_gpus": 0,
            "object_store_memory": 100 * (2**20),  # 100 MB
        }
        ray_params = ray.parameter.RayParams(**node_args)
        ray_params.update_if_absent(**default_kwargs)
        if self.head_node is None:
            node = ray.node.Node(
                ray_params, head=True, shutdown_at_exit=self._shutdown_at_exit)
            self.head_node = node
            self.redis_address = self.head_node.redis_address
            self.redis_password = node_args.get("redis_password")
            self.webui_url = self.head_node.webui_url
        else:
            ray_params.update_if_absent(redis_address=self.redis_address)
            node = ray.node.Node(
                ray_params,
                head=False,
                shutdown_at_exit=self._shutdown_at_exit)
            self.worker_nodes.add(node)

        # Wait for the node to appear in the client table. We do this so that
        # the nodes appears in the client table in the order that the
        # corresponding calls to add_node were made. We do this because in the
        # tests we assume that the driver is connected to the first node that
        # is added.
        self._wait_for_node(node)

        return node

    def remove_node(self, node, allow_graceful=False):
        """Kills all processes associated with worker node.

        Args:
            node (Node): Worker node of which all associated processes
                will be removed.
        """
        if self.head_node == node:
            self.head_node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful)
            self.head_node = None
            # TODO(rliaw): Do we need to kill all worker processes?
        else:
            node.kill_all_processes(
                check_alive=False, allow_graceful=allow_graceful)
            self.worker_nodes.remove(node)

        assert not node.any_processes_alive(), (
            "There are zombie processes left over after killing.")

    def _wait_for_node(self, node, timeout=30):
        """Wait until this node has appeared in the client table.

        Args:
            node (ray.node.Node): The node to wait for.
            timeout: The amount of time in seconds to wait before raising an
                exception.

        Raises:
            Exception: An exception is raised if the timeout expires before the
                node appears in the client table.
        """
        ip_address, port = self.redis_address.split(":")
        redis_client = redis.StrictRedis(
            host=ip_address, port=int(port), password=self.redis_password)

        start_time = time.time()
        while time.time() - start_time < timeout:
            clients = ray.experimental.state.parse_client_table(redis_client)
            object_store_socket_names = [
                client["ObjectStoreSocketName"] for client in clients
            ]
            if node.plasma_store_socket_name in object_store_socket_names:
                return
            else:
                time.sleep(0.1)
        raise Exception("Timed out while waiting for nodes to join.")

    def wait_for_nodes(self, timeout=30):
        """Waits for correct number of nodes to be registered.

        This will wait until the number of live nodes in the client table
        exactly matches the number of "add_node" calls minus the number of
        "remove_node" calls that have been made on this cluster. This means
        that if a node dies without "remove_node" having been called, this will
        raise an exception.

        Args:
            timeout (float): The number of seconds to wait for nodes to join
                before failing.

        Raises:
            Exception: An exception is raised if we time out while waiting for
                nodes to join.
        """
        ip_address, port = self.redis_address.split(":")
        redis_client = redis.StrictRedis(
            host=ip_address, port=int(port), password=self.redis_password)

        start_time = time.time()
        while time.time() - start_time < timeout:
            clients = ray.experimental.state.parse_client_table(redis_client)
            live_clients = [
                client for client in clients if client["IsInsertion"]
            ]

            expected = len(self.list_all_nodes())
            if len(live_clients) == expected:
                logger.debug("All nodes registered as expected.")
                return
            else:
                logger.debug(
                    "{} nodes are currently registered, but we are expecting "
                    "{}".format(len(live_clients), expected))
                time.sleep(0.1)
        raise Exception("Timed out while waiting for nodes to join.")

    def list_all_nodes(self):
        """Lists all nodes.

        TODO(rliaw): What is the desired behavior if a head node
        dies before worker nodes die?

        Returns:
            List of all nodes, including the head node.
        """
        nodes = list(self.worker_nodes)
        if self.head_node:
            nodes = [self.head_node] + nodes
        return nodes

    def remaining_processes_alive(self):
        """Returns a bool indicating whether all processes are alive or not.

        Note that this ignores processes that have been explicitly killed,
        e.g., via a command like node.kill_raylet().

        Returns:
            True if all processes are alive and false otherwise.
        """
        return all(
            node.remaining_processes_alive() for node in self.list_all_nodes())

    def shutdown(self):
        """Removes all nodes."""

        # We create a list here as a copy because `remove_node`
        # modifies `self.worker_nodes`.
        all_nodes = list(self.worker_nodes)
        for node in all_nodes:
            self.remove_node(node)

        if self.head_node is not None:
            self.remove_node(self.head_node)
