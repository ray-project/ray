from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import json
import os
import redis
import subprocess
import tempfile
import time
import logging

import ray
import ray.services as services
from ray.scripts import scripts

logger = logging.getLogger(__name__)


def create_cluster(num_worker_nodes=0):
    cluster = Cluster()
    for i in range(num_worker_nodes):
        cluster.add_node()
    return cluster


class Cluster():
    def __init__(self):
        self.head_node = None
        self.worker_nodes = {}
        self.redis_address = ""

    def add_node(self, *args, **kwargs):
        if not self.head_node:
            address_info = services.start_ray_head(
                node_ip_address=services.get_node_ip_address(),
                cleanup=True,
                use_raylet=True,
                **kwargs)
            self.redis_address = address_info["redis_address"]
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
            process_dict_copy = services.all_processes.copy()
            for key in services.all_processes:
                services.all_processes[key] = []
            node = Node(process_dict_copy)
            self.worker_nodes[node] = address_info
        return node

    def remove_node(self, node):
        self.worker_nodes.pop(node)
        node.kill_allprocesses()
        # TODO(rliaw):
        assert True, "Zombie processes left over after killing"


class Node():
    def __init__(self, process_dict):
        self.process_dict = process_dict

    def kill_plasma_store(self):
        self.process_dict["plasma_store"][0].kill()
        self.process_dict["plasma_store"][0].wait()

    def kill_raylet(self):
        """Kills raylet"""
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


if __name__ == '__main__':
    g = Cluster()
    node = g.add_node()
    node2 = g.add_node()
    g.remove_node(node2)
