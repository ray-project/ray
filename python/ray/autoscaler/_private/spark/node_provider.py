import json
import logging
import sys
from threading import RLock
from typing import Any, Dict, Optional

import requests

from ray._common.network_utils import build_address
from ray.autoscaler.node_launch_exception import NodeLaunchException
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

HEAD_NODE_ID = 0
HEAD_NODE_TYPE = "ray.head.default"


class SparkNodeProvider(NodeProvider):
    """A node provider that implements provider for nodes of Ray on spark."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()

        self._nodes = {
            str(HEAD_NODE_ID): {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: HEAD_NODE_ID,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                }
            },
        }
        self._next_node_id = 0

        self.ray_head_ip = self.provider_config["ray_head_ip"]
        # The port of spark job server. We send http request to spark job server
        # to launch spark jobs, ray worker nodes are launched by spark task in
        # spark jobs.
        spark_job_server_port = self.provider_config["spark_job_server_port"]
        self.spark_job_server_url = (
            f"http://{build_address(self.ray_head_ip, spark_job_server_port)}"
        )
        self.ray_head_port = self.provider_config["ray_head_port"]
        # The unique id for the Ray on spark cluster.
        self.cluster_id = self.provider_config["cluster_unique_id"]

    def get_next_node_id(self):
        with self.lock:
            self._next_node_id += 1
            return self._next_node_id

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            nodes = []

            died_nodes = []
            for node_id in self._nodes:
                if node_id == str(HEAD_NODE_ID):
                    status = "running"
                else:
                    status = self._query_node_status(node_id)

                if status == "running":
                    if (
                        self._nodes[node_id]["tags"][TAG_RAY_NODE_STATUS]
                        == STATUS_SETTING_UP
                    ):
                        self._nodes[node_id]["tags"][
                            TAG_RAY_NODE_STATUS
                        ] = STATUS_UP_TO_DATE
                        logger.info(
                            f"Spark node provider node {node_id} starts running."
                        )

                if status == "terminated":
                    died_nodes.append(node_id)
                else:
                    tags = self.node_tags(node_id)
                    ok = True
                    for k, v in tag_filters.items():
                        if tags.get(k) != v:
                            ok = False
                    if ok:
                        nodes.append(node_id)

            for died_node_id in died_nodes:
                self._nodes.pop(died_node_id)

            return nodes

    def _query_node_status(self, node_id):
        spark_job_group_id = self._gen_spark_job_group_id(node_id)

        response = requests.post(
            url=self.spark_job_server_url + "/query_task_status",
            json={"spark_job_group_id": spark_job_group_id},
        )
        response.raise_for_status()

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
            f"ray-cluster-{self.ray_head_port}-{self.cluster_id}"
            f"-worker-node-{node_id}"
        )

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        for _ in range(count):
            self._create_node_with_resources_and_labels(
                node_config, tags, resources, labels
            )

    def _create_node_with_resources_and_labels(
        self, node_config, tags, resources, labels
    ):
        from ray.util.spark.cluster_init import _append_resources_config

        with self.lock:
            resources = resources.copy()
            node_type = tags[TAG_RAY_USER_NODE_TYPE]
            # NOTE:
            #  "NODE_ID_AS_RESOURCE" value must be an integer,
            #  but `node_id` used by autoscaler must be a string.
            node_id = str(self.get_next_node_id())
            resources["NODE_ID_AS_RESOURCE"] = int(node_id)

            conf = self.provider_config.copy()

            num_cpus_per_node = resources.pop("CPU")
            num_gpus_per_node = resources.pop("GPU")
            heap_memory_per_node = resources.pop("memory")
            object_store_memory_per_node = resources.pop("object_store_memory")

            conf["worker_node_options"] = _append_resources_config(
                conf["worker_node_options"], resources
            )
            response = requests.post(
                url=self.spark_job_server_url + "/create_node",
                json={
                    "spark_job_group_id": self._gen_spark_job_group_id(node_id),
                    "spark_job_group_desc": (
                        "This job group is for spark job which runs the Ray "
                        f"cluster worker node {node_id} connecting to ray "
                        f"head node {build_address(self.ray_head_ip, self.ray_head_port)}"
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
                    "node_id": resources["NODE_ID_AS_RESOURCE"],
                },
            )

            try:
                # Spark job server is locally launched, if spark job server request
                # failed, it is unlikely network error but probably unrecoverable
                # error, so we make it fast-fail.
                response.raise_for_status()
            except Exception:
                raise NodeLaunchException(
                    "Node creation failure",
                    f"Starting ray worker node {node_id} failed",
                    sys.exc_info(),
                )

            self._nodes[node_id] = {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                    TAG_RAY_USER_NODE_TYPE: node_type,
                    TAG_RAY_NODE_NAME: node_id,
                    TAG_RAY_NODE_STATUS: STATUS_SETTING_UP,
                },
            }
            logger.info(f"Spark node provider creates node {node_id}.")

    def terminate_node(self, node_id):
        if node_id in self._nodes:
            response = requests.post(
                url=self.spark_job_server_url + "/terminate_node",
                json={"spark_job_group_id": self._gen_spark_job_group_id(node_id)},
            )
            response.raise_for_status()

        with self.lock:
            if node_id in self._nodes:
                self._nodes.pop(node_id)

        logger.info(f"Spark node provider terminates node {node_id}")

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
