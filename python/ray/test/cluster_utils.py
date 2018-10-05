from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray.services as services

logger = logging.getLogger(__name__)


class Cluster():
    def __init__(self, initialize_head=False, **head_node_args):
        """Initializes the cluster.

        Arguments:
            initialize_head (bool): Automatically start a Ray cluster
                by initializing the head node. Defaults to False.
            head_node_args (kwargs): Arguments to be passed into
                `start_ray_head` via `self.add_node`.
        """
        self.head_node = None
        self.worker_nodes = {}
        self.redis_address = ""
        if initialize_head:
            self.add_node(**head_node_args)

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


class Node():
    """Abstraction for a Ray node."""

    def __init__(self, process_dict):
        # TODO(rliaw): Is there a unique identifier for a node?
        self.process_dict = process_dict

    def kill_plasma_store(self):
        self.process_dict["plasma_store"][0].kill()
        self.process_dict["plasma_store"][0].wait()

    def kill_raylet(self):
        self.process_dict["raylet"][0].kill()
        self.process_dict["raylet"][0].wait()

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

    def any_processes_alive(self):
        return any(
            any(process.poll() is None for process in process_list)
            for process_list in self.process_dict.values())

    def all_processes_alive(self):
        return all(
            all(process.poll() is None for process in process_list)
            for process_list in self.process_dict.values())


if __name__ == '__main__':
    g = Cluster(initialize_head=False)
    node = g.add_node()
    node2 = g.add_node()
    assert node.all_processes_alive()
    assert node2.all_processes_alive()
    g.remove_node(node2)
    g.remove_node(node)
    assert not any(node.any_processes_alive() for node in g.list_all_nodes())
