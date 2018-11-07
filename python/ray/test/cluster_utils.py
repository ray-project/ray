from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import ray
import ray.services as services

logger = logging.getLogger(__name__)


class Cluster(object):
    def __init__(self,
                 initialize_head=False,
                 connect=False,
                 head_node_args=None):
        """Initializes the cluster.

        Args:
            initialize_head (bool): Automatically start a Ray cluster
                by initializing the head node. Defaults to False.
            connect (bool): If `initialize_head=True` and `connect=True`,
                ray.init will be called with the redis address of this cluster
                passed in.
            head_node_args (kwargs): Arguments to be passed into
                `start_ray_head` via `self.add_node`.
        """
        self.head_node = None
        self.worker_nodes = {}
        self.redis_address = None
        if not initialize_head and connect:
            raise RuntimeError("Cannot connect to uninitialized cluster.")

        if initialize_head:
            head_node_args = head_node_args or {}
            self.add_node(**head_node_args)
            if connect:
                redis_password = head_node_args.get("redis_password")
                ray.init(
                    redis_address=self.redis_address,
                    redis_password=redis_password)

    def add_node(self, **override_kwargs):
        """Adds a node to the local Ray Cluster.

        All nodes are by default started with the following settings:
            cleanup=True,
            resources={"CPU": 1},
            object_store_memory=100 * (2**20) # 100 MB

        Args:
            override_kwargs: Keyword arguments used in `start_ray_head`
                and `start_ray_node`. Overrides defaults.

        Returns:
            Node object of the added Ray node.
        """
        node_kwargs = {
            "cleanup": True,
            "resources": {
                "CPU": 1
            },
            "object_store_memory": 100 * (2**20)  # 100 MB
        }
        node_kwargs.update(override_kwargs)

        if self.head_node is None:
            address_info = services.start_ray_head(
                node_ip_address=services.get_node_ip_address(),
                include_webui=False,
                **node_kwargs)
            self.redis_address = address_info["redis_address"]
            # TODO(rliaw): Find a more stable way than modifying global state.
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(process_dict_copy)
            self.head_node = node
        else:
            address_info = services.start_ray_node(
                services.get_node_ip_address(), self.redis_address,
                **node_kwargs)
            # TODO(rliaw): Find a more stable way than modifying global state.
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(process_dict_copy)
            self.worker_nodes[node] = address_info
        logging.info("Starting Node with raylet socket {}".format(
            address_info["raylet_socket_names"]))

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

    def wait_for_nodes(self, retries=30):
        """Waits for all nodes to be registered with global state.

        By default, waits for 3 seconds.

        Args:
            retries (int): Number of times to retry checking client table.
        """
        for i in range(retries):
            if not ray.is_initialized() or not self._check_registered_nodes():
                time.sleep(0.1)
            else:
                break

    def _check_registered_nodes(self):
        registered = len([
            client for client in ray.global_state.client_table()
            if client["IsInsertion"]
        ])
        expected = len(self.list_all_nodes())
        if registered == expected:
            logger.info("All nodes registered as expected.")
        else:
            logger.info("Currently registering {} but expecting {}".format(
                registered, expected))
        return registered == expected

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
        # We create a list here as a copy because `remove_node`
        # modifies `self.worker_nodes`.
        all_nodes = list(self.worker_nodes)
        for node in all_nodes:
            self.remove_node(node)
        self.remove_node(self.head_node)


class Node(object):
    """Abstraction for a Ray node."""

    def __init__(self, process_dict):
        # TODO(rliaw): Is there a unique identifier for a node?
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
