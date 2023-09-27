import json
import logging
import threading
import time
from threading import RLock
from typing import Any, Dict, Optional

import requests

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (
    NODE_KIND_HEAD,
    NODE_KIND_WORKER,
    STATUS_SETTING_UP,
    STATUS_UP_TO_DATE,
    TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME,
    TAG_RAY_NODE_STATUS,
    TAG_RAY_USER_NODE_TYPE,
)

logger = logging.getLogger(__name__)

RAY_ON_SPARK_HEAD_NODE_ID = 0
RAY_ON_SPARK_HEAD_NODE_TYPE = "ray.head.default"


class SparkNodeProvider(NodeProvider):
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

        self.ray_head_ip = self.provider_config["ray_head_ip"]
        server_port = self.provider_config["spark_job_server_port"]
        self.server_url = f"http://{self.ray_head_ip}:{server_port}"
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

    def _query_node_status(self, node_id):
        spark_job_group_id = self._gen_spark_job_group_id(node_id)

        response = requests.post(
            url=self.server_url + "/query_task_status",
            json={"spark_job_group_id": spark_job_group_id},
        )
        decoded_resp = response.content.decode("utf-8")
        json_res = json.loads(decoded_resp)
        return json_res["status"]

    def is_running(self, node_id):
        with self.lock:
            return (
                node_id in self._nodes
                and self._nodes[node_id]["tags"][TAG_RAY_NODE_STATUS]
                == STATUS_UP_TO_DATE
            )

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

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        raise AssertionError("This method should not be called.")

    def _gen_spark_job_group_id(self, node_id):
        return (
            f"ray-cluster-{self.ray_head_port}-{self.cluster_unique_id}"
            f"-worker-node-{node_id}"
        )

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        from ray.util.spark.cluster_init import _append_resources_config

        # Note: even if count > 1 , we should only create 1 node in this call
        #  otherwise some issue happens in my test.
        with self.lock:
            resources = resources.copy()
            node_type = tags[TAG_RAY_USER_NODE_TYPE]
            # NOTE:
            #  "NODE_ID_AS_RESOURCE" value must be an integer,
            #  but `node_id` used by autoscaler must be a string.
            node_id = str(self.get_next_node_id())
            resources["NODE_ID_AS_RESOURCE"] = int(node_id)

            conf = self.provider_config

            num_cpus_per_node = resources.pop("CPU")
            num_gpus_per_node = resources.pop("GPU")
            heap_memory_per_node = resources.pop("memory")
            object_store_memory_per_node = resources.pop("object_store_memory")

            conf["worker_node_options"] = _append_resources_config(
                conf["worker_node_options"], resources
            )
            response = requests.post(
                url=self.server_url + "/create_node",
                json={
                    "spark_job_group_id": self._gen_spark_job_group_id(node_id),
                    "spark_job_group_desc": (
                        "This job group is for spark job which runs the Ray "
                        f"cluster worker node {node_id} connecting to ray "
                        f"head node {self.ray_head_ip}:{self.ray_head_port}"
                    ),
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
                },
            )
            if response.status_code != 200:
                raise RuntimeError("Starting ray worker node failed.")

            self._nodes[node_id] = {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                    TAG_RAY_USER_NODE_TYPE: node_type,
                    TAG_RAY_NODE_NAME: node_id,
                    TAG_RAY_NODE_STATUS: STATUS_SETTING_UP,
                },
            }

            def update_node_status(_node_id):
                while True:
                    time.sleep(5)
                    status = self._query_node_status(_node_id)
                    if status == "running":
                        with self.lock:
                            self._nodes[_node_id]["tags"][
                                TAG_RAY_NODE_STATUS
                            ] = STATUS_UP_TO_DATE
                            logger.info(f"node {_node_id} starts running.")
                    elif status == "terminated":
                        with self.lock:
                            self._nodes.pop(_node_id)
                        break

            threading.Thread(target=update_node_status, args=(node_id,)).start()

    def terminate_node(self, node_id):
        with self.lock:
            logger.info(f"Terminate spark job {self._gen_spark_job_group_id(node_id)}.")

            requests.post(
                url=self.server_url + "/terminate_node",
                json={"spark_job_group_id": self._gen_spark_job_group_id(node_id)},
            )

            try:
                self._nodes.pop(node_id)
            except Exception as e:
                raise e

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
