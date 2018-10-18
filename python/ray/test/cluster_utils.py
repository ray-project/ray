from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

import ray
import ray.services as services

logger = logging.getLogger(__name__)


class Cluster():
    def __init__(self, initialize_head=False, connect=False, **head_node_args):
        """Initializes the cluster.

        Arguments:
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
        self.redis_address = ""
        if initialize_head:
            self.add_node(**head_node_args)
            if connect:
                ray.init(redis_address=self.redis_address)
        elif connect:
            logger.warning("Head not started; not connecting.")

    def add_node(self, **kwargs):
        """Adds a node to the local Ray Cluster.

        Arguments:
            kwargs: Keyword arguments used in `start_ray_head`
                and `start_ray_node`.

        Returns:
            Node object of the added Ray node.
        """
        if self.head_node is None:
            address_info = services.start_ray_head(
                node_ip_address=services.get_node_ip_address(),
                include_webui=False,
                cleanup=True,
                use_raylet=True,
                **kwargs)
            self.redis_address = address_info["redis_address"]
            # TODO(rliaw): Find a more stable way than modifying global state.
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(process_dict_copy)
            self.head_node = node
        else:
            address_info = services.start_ray_node(
                services.get_node_ip_address(),
                self.redis_address,
                cleanup=True,
                use_raylet=True,
                **kwargs)
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
            self.head_node.kill_allprocesses()
            self.head_node = None
            # TODO(rliaw): Do we need to kill all worker processes?
        else:
            node.kill_allprocesses()
            self.worker_nodes.pop(node)

        assert not node.any_processes_alive(), (
            "There are zombie processes left over after killing...")

    def wait_for_nodes(self):
        """Waits for all nodes to be registered with global state."""
        try:
            while True:
                registered = len([
                    client for client in ray.global_state.client_table()
                    if client["IsInsertion"]
                ])
                expected = len(self.list_all_nodes())
                if registered == expected:
                    logger.info("All nodes registered as expected!")
                    break
                else:
                    logger.info(
                        "Currently registering {} but expecting {}".format(
                            registered, expected))
                time.sleep(0.3)
        except Exception as e:
            logger.exception("Perhaps try Cluster(connect=True)?")

    def list_all_nodes(self):
        """Lists all nodes.

        TODO(rliaw): What is the desired behavior if a head node
        dies before worker nodes die?

        Returns:
            List of all nodes, including the head node."""
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


class Node():
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

    def kill_allprocesses(self):
        for process_name, process_list in self.process_dict.items():
            logger.info("Killing all {}(s)".format(process_name))
            for process in process_list:
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
