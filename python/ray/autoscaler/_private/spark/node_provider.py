import copy
import json
import logging
import os
import subprocess
import requests
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
        self.server_url = "http://127.0.0.1:8899"

        self.ray_head_ip = self.provider_config["ray_head_ip"]
        self.ray_head_port = self.provider_config["ray_head_port"]
        self.cluster_unique_id = self.provider_config["cluster_unique_id"]

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

    def _gen_spark_job_group_id(self, node_id):
        return f"ray-cluster-{self.ray_head_port}-{self.cluster_unique_id}-worker-node-{node_id}"

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        from ray.util.spark.cluster_init import _append_resources_config

        for _ in range(count):
            with self.lock:
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
                conf = self.provider_config

                num_cpus_per_node = resources.pop('CPU')
                num_gpus_per_node = resources.pop('GPU')
                heap_memory_per_node = resources.pop('memory')
                object_store_memory_per_node = resources.pop('object_store_memory')

                conf["worker_node_options"] = _append_resources_config(conf["worker_node_options"], resources)
                response = requests.post(
                    url=self.server_url + "/create_node",
                    json={
                        "num_worker_nodes": 1,
                        "spark_job_group_id": self._gen_spark_job_group_id(next_id),
                        "spark_job_group_desc":
                            "This job group is for spark job which runs the Ray "
                            f"cluster worker node {next_id} connecting to ray "
                            f"head node {self.ray_head_ip}:{self.ray_head_port}",
                        "using_stage_scheduling": conf["using_stage_scheduling"],
                        "ray_head_ip": self.ray_head_ip,
                        "ray_head_port": self.ray_head_port,
                        "ray_temp_dir": conf["ray_temp_dir"],
                        "num_cpus_per_node": num_cpus_per_node,
                        "num_gpus_per_node": num_gpus_per_node,
                        "heap_memory_per_node": heap_memory_per_node,
                        "object_store_memory_per_node": object_store_memory_per_node,
                        "worker_node_options": conf["worker_node_options"],
                        "collect_log_to_path": conf["collect_log_to_path"],
                    }
                )
                if response.status_code != 200:
                    raise RuntimeError("Starting ray worker node failed.")

                self._nodes[next_id] = {
                    "tags": {
                        TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                        TAG_RAY_USER_NODE_TYPE: node_type,
                        TAG_RAY_NODE_NAME: next_id,
                        TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                    },
                }

    def terminate_node(self, node_id):
        with self.lock:
            try:
                self._nodes.pop(node_id)
            except Exception as e:
                raise e

            requests.post(
                url=self.server_url + "/terminate_node",
                json={"spark_job_group_id": self._gen_spark_job_group_id(node_id)}
            )

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config


