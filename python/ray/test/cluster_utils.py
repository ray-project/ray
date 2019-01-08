from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import atexit
import logging
import time

import redis

import ray
from ray.parameter import RayParams
import ray.services as services

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
        self.worker_nodes = {}
        self.redis_address = None
        self.redis_password = None
        self.connected = False
        if not initialize_head and connect:
            raise RuntimeError("Cannot connect to uninitialized cluster.")

        if initialize_head:
            head_node_args = head_node_args or {}
            self.add_node(**head_node_args)
            if connect:
                self.connect(head_node_args)
        if shutdown_at_exit:
            atexit.register(self.shutdown)

    def connect(self, head_node_args):
        assert self.redis_address is not None
        assert not self.connected
        self.redis_password = head_node_args.get("redis_password")
        output_info = ray.init(
            ignore_reinit_error=True,
            redis_address=self.redis_address,
            redis_password=self.redis_password)
        logger.info(output_info)
        self.connected = True

    def add_node(self, **override_kwargs):
        """Adds a node to the local Ray Cluster.

        All nodes are by default started with the following settings:
            cleanup=True,
            num_cpus=1,
            object_store_memory=100 * (2**20) # 100 MB

        Args:
            override_kwargs: Keyword arguments used in `start_ray_head`
                and `start_ray_node`. Overrides defaults.

        Returns:
            Node object of the added Ray node.
        """
        node_kwargs = {
            "num_cpus": 1,
            "object_store_memory": 100 * (2**20)  # 100 MB
        }
        node_kwargs.update(override_kwargs)
        ray_params = RayParams(
            node_ip_address=services.get_node_ip_address(), **node_kwargs)

        if self.head_node is None:
            ray_params.update(include_webui=False)
            address_info = services.start_ray_head(ray_params, cleanup=True)
            self.redis_address = address_info["redis_address"]
            # TODO(rliaw): Find a more stable way than modifying global state.
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(address_info, process_dict_copy)
            self.head_node = node
        else:
            ray_params.update(redis_address=self.redis_address)
            address_info = services.start_ray_node(ray_params, cleanup=True)
            # TODO(rliaw): Find a more stable way than modifying global state.
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(address_info, process_dict_copy)
            self.worker_nodes[node] = address_info
        logger.info("Starting Node with raylet socket {}".format(
            address_info["raylet_socket_name"]))

        return node

    def remove_node(self, node):
        """Kills all processes associated with worker node.

        Args:
            node (Node): Worker node of which all associated processes
                will be removed.
        """
        if self.head_node == node:
            self.head_node.kill_all_processes()
            self.head_node = None
            # TODO(rliaw): Do we need to kill all worker processes?
        else:
            node.kill_all_processes()
            self.worker_nodes.pop(node)

        assert not node.any_processes_alive(), (
            "There are zombie processes left over after killing.")

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

    def shutdown(self):
        """Removes all nodes."""

        # We create a list here as a copy because `remove_node`
        # modifies `self.worker_nodes`.
        all_nodes = list(self.worker_nodes)
        for node in all_nodes:
            self.remove_node(node)

        if self.head_node:
            self.remove_node(self.head_node)
        else:
            logger.warning("No headnode exists!")


class Node(object):
    """Abstraction for a Ray node."""

    def __init__(self, address_info, process_dict):
        # TODO(rliaw): Is there a unique identifier for a node?
        self.address_info = address_info
        self.process_dict = process_dict

    def kill_plasma_store(self):
        self.process_dict[services.PROCESS_TYPE_PLASMA_STORE][0].kill()
        self.process_dict[services.PROCESS_TYPE_PLASMA_STORE][0].wait()

    def kill_raylet(self):
        self.process_dict[services.PROCESS_TYPE_RAYLET][0].kill()
        self.process_dict[services.PROCESS_TYPE_RAYLET][0].wait()

    def kill_log_monitor(self):
        self.process_dict["log_monitor"][0].kill()
        self.process_dict["log_monitor"][0].wait()

    def kill_all_processes(self):
        for process_name, process_list in self.process_dict.items():
            logger.info("Killing all {}(s)".format(process_name))
            for process in process_list:
                # Kill the process if it is still alive.
                if process.poll() is None:
                    process.kill()

        for process_name, process_list in self.process_dict.items():
            logger.info("Waiting all {}(s)".format(process_name))
            for process in process_list:
                process.wait()

    def live_processes(self):
        return [(p_name, proc) for p_name, p_list in self.process_dict.items()
                for proc in p_list if proc.poll() is None]

    def dead_processes(self):
        return [(p_name, proc) for p_name, p_list in self.process_dict.items()
                for proc in p_list if proc.poll() is not None]

    def any_processes_alive(self):
        return any(self.live_processes())

    def all_processes_alive(self):
        return not any(self.dead_processes())

    def get_plasma_store_name(self):
        """Return the plasma store name.

        Assuming one plasma store per raylet, this may be used as a unique
        identifier for a node.
        """
        return self.address_info['object_store_address']
