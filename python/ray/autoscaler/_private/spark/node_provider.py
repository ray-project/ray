import copy
import json
import logging
import os
import subprocess
import sys
import time
import requests
from threading import RLock
from types import ModuleType
from typing import Any, Dict, Optional

import yaml

import ray
import ray._private.ray_constants as ray_constants
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_UP_TO_DATE,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)
from ray.util.spark.cluster_init import _start_ray_worker_nodes


logger = logging.getLogger(__name__)

# We generate the node ids deterministically in the fake node provider, so that
# we can associate launched nodes with their resource reports. IDs increment
# starting with fffff*00000000 for the head node, fffff*00000001, etc. for workers.
RAY_ON_SPARK_HEAD_NODE_ID = 0
RAY_ON_SPARK_HEAD_NODE_TYPE = "ray.head.default"


class RayOnSparkNodeProvider(NodeProvider):
    """A node provider that implements provider for nodes of Ray on spark."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()

        self._nodes = {
            RAY_ON_SPARK_HEAD_NODE_ID: {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: RAY_ON_SPARK_HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: RAY_ON_SPARK_HEAD_NODE_ID,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                }
            },
        }
        self._next_node_id = 0

    def get_next_node_id(self):
        with self.lock:
            self._next_node_id += 1
            return self._next_node_id

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            nodes = []
            for node_id in self._nodes:
                tags = self.node_tags(node_id)
                ok = True
                for k, v in tag_filters.items():
                    if tags.get(k) != v:
                        ok = False
                if ok:
                    nodes.append(node_id)

            return nodes

    def is_running(self, node_id):
        with self.lock:
            return node_id in self._nodes

    def is_terminated(self, node_id):
        with self.lock:
            return node_id not in self._nodes

    def node_tags(self, node_id):
        with self.lock:
            return self._nodes[node_id]["tags"]

    def _get_ip(self, node_id: str) -> Optional[str]:
        return node_id

    def external_ip(self, node_id):
        return self._get_ip(node_id)

    def internal_ip(self, node_id):
        return self._get_ip(node_id)

    def set_node_tags(self, node_id, tags):
        assert node_id in self._nodes
        self._nodes[node_id]["tags"].update(tags)

    def create_node(self, node_config: Dict[str, Any], tags: Dict[str, str], count: int) -> Optional[Dict[str, Any]]:
        raise AssertionError("This method should not be called.")

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        with self.lock:
            for _ in range(count):
                resources = resources.copy()
                node_type = tags[TAG_RAY_USER_NODE_TYPE]
                next_id = self.get_next_node_id()
                resources["NODE_ID_AS_RESOURCE"] = next_id

                """
                ray_worker_node_cmd = [
                    sys.executable,
                    "-m",
                    "ray.util.spark.start_ray_node",
                    f"--temp-dir=/tmp/test_worker1",
                    f"--num-cpus={resources.pop('CPU', 0)}",
                    f"--num-gpus={resources.pop('GPU', 0)}",
                    "--block",
                    "--address=192.168.10.116:3344",
                    f"--memory={512 * 1024 * 1024}",
                    f"--object-store-memory={resources.pop('object_store_memory', 512 * 1024 * 1024)}",
                    f"--min-worker-port={11000}",
                    f"--max-worker-port={20000}",
                    f"--resources={json.dumps(resources)}",
                    # f"--dashboard-agent-listen-port={ray_worker_node_dashboard_agent_port}",
                    # *_convert_ray_node_options(worker_node_options),
                ]
                
                from ray.util.spark.cluster_init import setup_sigterm_on_parent_death, exec_cmd, RAY_ON_SPARK_COLLECT_LOG_TO_PATH
                ray_node_proc, tail_output_deque = exec_cmd(
                    ray_worker_node_cmd,
                    synchronous=False,
                    preexec_fn=setup_sigterm_on_parent_death,
                    extra_env={
                        RAY_ON_SPARK_COLLECT_LOG_TO_PATH: "",
                        "RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER": "1",
                    },
                )
                """

                """
                node_params = {
                    "spark_job_group_desc": None,
                    "num_worker_nodes": 1,
                    using_stage_scheduling = data["using_stage_scheduling"]
                    ray_head_ip = data["ray_head_ip"]
                    ray_head_port = data["ray_head_port"]
                    ray_temp_dir = data["ray_temp_dir"]
                    num_cpus_per_node = data["num_cpus_per_node"]
                    num_gpus_per_node = data["num_gpus_per_node"]
                    heap_memory_per_node = data["heap_memory_per_node"]
                    object_store_memory_per_node = data["object_store_memory_per_node"]
                    worker_node_options = data["worker_node_options"]
                    collect_log_to_path = data["collect_log_to_path"]

                }
                """

                _start_ray_worker_nodes()

                self._nodes[next_id] = {
                    "tags": {
                        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                        TAG_RAY_USER_NODE_TYPE: node_type,
                        TAG_RAY_NODE_NAME: next_id,
                        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                    },
                    "node_proc": ray_node_proc,
                }

    def terminate_node(self, node_id):
        with self.lock:
            try:
                node = self._nodes.pop(node_id)
            except Exception as e:
                raise e

            self._terminate_node(node)

    def _terminate_node(self, node):
        # node["node"].kill_all_processes(check_alive=False, allow_graceful=True)
        node["node_proc"].terminate()

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config


