import copy
import json
import logging
import os
from threading import RLock
import time
from types import ModuleType
from typing import Any, Dict, Optional
import yaml

import ray
from ray.autoscaler._private.fake_multi_node.command_runner import \
    FakeDockerCommandRunner
from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_NODE_KIND, NODE_KIND_HEAD,
                                 NODE_KIND_WORKER, TAG_RAY_USER_NODE_TYPE,
                                 TAG_RAY_NODE_NAME, TAG_RAY_NODE_STATUS,
                                 STATUS_UP_TO_DATE)

logger = logging.getLogger(__name__)

# We generate the node ids deterministically in the fake node provider, so that
# we can associate launched nodes with their resource reports. IDs increment
# starting with fffff*00000 for the head node, fffff*00001, etc. for workers.
FAKE_HEAD_NODE_ID = "fffffffffffffffffffffffffffffffffffffffffffffffffff00000"
FAKE_HEAD_NODE_TYPE = "ray.head.default"

DOCKER_COMPOSE_SKELETON = {
    "version": "3.9",
    "services": {},
    "networks": {
        "ray_local": {}
    }
}

DOCKER_NODE_SKELETON = {
    "networks": ["ray_local"],
    "mem_limit": "3000m",
    "mem_reservation": "3000m",
    "shm_size": "1200m",
    "volumes": [
        # Todo: Remove
        "/Users/kai/coding/ray/python/ray/autoscaler:"
        "/home/ray/anaconda3/lib/python3.7/site-packages/ray/autoscaler:ro"
    ]
}

DOCKER_HEAD_CMD = (
    "bash -c \"sleep 1 "
    "&& sudo chmod 777 {volume_dir} "
    "&& RAY_FAKE_CLUSTER=1 ray start --head --port=6379 "
    "--object-manager-port=8076 --dashboard-host 0.0.0.0 "
    "--autoscaling-config={autoscaling_config} "
    "--num-cpus {num_cpus} "
    "--num-gpus {num_gpus} "
    # "--resources='{resources}' "
    "&& sleep 1000000\"")

DOCKER_WORKER_CMD = (
    "bash -c \"sleep 1 && "
    f"ray start --address={FAKE_HEAD_NODE_ID}:6379 "
    "--object-manager-port=8076 "
    "--num-cpus {num_cpus} "
    "--num-gpus {num_gpus} "
    # "--resources='{resources}' "
    "&& sleep 1000000\"")


def create_node_spec(head: bool,
                     docker_image: str,
                     mounted_cluster_dir: str,
                     mounted_node_dir: str,
                     num_cpus: int = 2,
                     num_gpus: int = 0,
                     resources: Optional[Dict] = None,
                     env_vars: Optional[Dict] = None,
                     volume_dir: Optional[str] = None,
                     status_path: Optional[str] = None,
                     docker_compose_path: Optional[str] = None,
                     bootstrap_config_path: Optional[str] = None):
    node_spec = copy.deepcopy(DOCKER_NODE_SKELETON)
    node_spec["image"] = docker_image

    bootstrap_path_on_container = "/home/ray/ray_bootstrap_config.yaml"

    resources = resources or {}

    resources_kwargs = dict(
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=json.dumps(resources, indent=None),
        volume_dir=volume_dir,
        autoscaling_config=bootstrap_path_on_container)

    if head:
        node_spec["command"] = DOCKER_HEAD_CMD.format(**resources_kwargs)
        node_spec["ports"] = [6379, 8265, 10001]
        node_spec["volumes"] += [
            f"{status_path}:{status_path}",
            f"{docker_compose_path}:{docker_compose_path}",
            f"{bootstrap_config_path}:{bootstrap_path_on_container}"
        ]
    else:
        node_spec["command"] = DOCKER_WORKER_CMD.format(**resources_kwargs)
        node_spec["depends_on"] = [FAKE_HEAD_NODE_ID]

    node_spec["volumes"] += [
        f"{mounted_cluster_dir}:/cluster/shared",
        f"{mounted_node_dir}:/cluster/node",
    ]

    env_vars = env_vars or {}
    node_spec["environment"] = [f"{k}={v}" for k, v in env_vars.items()]

    return node_spec


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()
        if "RAY_FAKE_CLUSTER" not in os.environ:
            raise RuntimeError(
                "FakeMultiNodeProvider requires ray to be started with "
                "RAY_FAKE_CLUSTER=1 ray start ...")

        self._nodes = {
            FAKE_HEAD_NODE_ID: {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: FAKE_HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: FAKE_HEAD_NODE_ID,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                }
            },
        }
        self._next_node_id = 0

        self.uses_docker = provider_config.get("docker", False)

        if self.uses_docker:
            self._project_name = self.provider_config["project_name"]
            self._docker_image = self.provider_config["image"]

            # subdirs:
            #  - ./shared (shared filesystem)
            #  - ./nodes/<node_id> (node-specific mounted filesystem)
            self._volume_dir = self.provider_config["shared_volume_dir"]
            self._mounted_cluster_dir = os.path.join(self._volume_dir,
                                                     "shared")

            os.makedirs(self._mounted_cluster_dir, mode=0o755, exist_ok=True)

            resources = copy.deepcopy(provider_config.get("head_resources"))

            self._boostrap_config_path = os.path.join(self._volume_dir,
                                                      "bootstrap_config.yaml")

            self._docker_compose_config_path = os.path.join(
                self._volume_dir, "docker-compose.yaml")
            self._docker_compose_config = None

            self._status_path = os.path.join(self._volume_dir, "status.json")

            self._nodes[FAKE_HEAD_NODE_ID][
                "node_spec"] = self._create_node_spec_with_resources(
                    head=True, node_id=FAKE_HEAD_NODE_ID, resources=resources)

            self._docker_status = {}

            self._update_docker_compose_config()
            self._update_docker_status()

    def _next_hex_node_id(self):
        self._next_node_id += 1
        base = "fffffffffffffffffffffffffffffffffffffffffffffffffff"
        return base + str(self._next_node_id).zfill(5)

    def _has_head_node(self):
        for node in self._nodes.values():
            tags = node["tags"]
            if tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD:
                return True

        return False

    def _create_node_spec_with_resources(self, head: bool, node_id: str,
                                         resources: Dict[str, Any]):
        # Create shared directory
        node_dir = os.path.join(self._volume_dir, "nodes", node_id)
        os.makedirs(node_dir, mode=0o755, exist_ok=True)

        return create_node_spec(
            head=head,
            docker_image=self._docker_image,
            mounted_cluster_dir=self._mounted_cluster_dir,
            mounted_node_dir=node_dir,
            num_cpus=resources.pop("CPU", 0),
            num_gpus=resources.pop("GPU", 0),
            resources=resources,
            env_vars={
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": node_id,
                # "RAY_OVERRIDE_RESOURCES": json.dumps(resources, indent=None),
            },
            volume_dir=self._volume_dir,
            status_path=self._status_path,
            docker_compose_path=self._docker_compose_config_path,
            bootstrap_config_path=self._boostrap_config_path)

    def _update_docker_compose_config(self):
        config = copy.deepcopy(DOCKER_COMPOSE_SKELETON)
        config["services"] = {}
        for node_id, node in self._nodes.items():
            config["services"][node_id] = node["node_spec"]

        with open(self._docker_compose_config_path, "wt") as f:
            yaml.safe_dump(config, f)

    def _update_docker_status(self):
        if not os.path.exists(self._status_path):
            return
        with open(self._status_path, "rt") as f:
            self._docker_status = json.load(f)

    def _container_name(self, node_id):
        return f"{self._project_name}_{node_id}_1"

    def get_command_runner(self,
                           log_prefix: str,
                           node_id: str,
                           auth_config: Dict[str, Any],
                           cluster_name: str,
                           process_runner: ModuleType,
                           use_internal_ip: bool,
                           docker_config: Optional[Dict[str, Any]] = None
                           ) -> CommandRunnerInterface:
        if not self.uses_docker:
            return super(FakeMultiNodeProvider, self).get_command_runner(
                log_prefix, node_id, auth_config, cluster_name, process_runner,
                use_internal_ip)

        # Else, docker:
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip
        }

        docker_config["container_name"] = self._container_name(node_id)
        docker_config["image"] = self._docker_image

        return FakeDockerCommandRunner(docker_config, **common_args)

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

    def _is_docker_running(self, node_id):
        if not self.uses_docker:
            return True
        self._update_docker_status()

        return self._docker_status.get(node_id, {}).get("status",
                                                        None) == "running"

    def is_running(self, node_id):
        with self.lock:
            return node_id in self._nodes and self._is_docker_running(node_id)

    def is_terminated(self, node_id):
        with self.lock:
            return node_id not in self._nodes and not self._is_docker_running(
                node_id)

    def node_tags(self, node_id):
        with self.lock:
            return self._nodes[node_id]["tags"]

    def _get_ip(self, node_id: str) -> Optional[str]:
        if not self.uses_docker:
            return node_id

        for i in range(3):
            self._update_docker_status()
            ip = self._docker_status.get(node_id, {}).get("IP", None)
            if ip:
                print("FOUND IP", ip)
                return ip
            time.sleep(3)
        print("FOUND NO IP", self._docker_status)
        return None

    def external_ip(self, node_id):
        if self.uses_docker:
            return self._get_ip(node_id)
        return node_id

    def internal_ip(self, node_id):
        if self.uses_docker:
            return self._get_ip(node_id)
        return node_id

    def set_node_tags(self, node_id, tags):
        assert node_id in self._nodes
        self._nodes[node_id]["tags"] = tags

    def create_node_with_resources(self, node_config, tags, count, resources):
        with self.lock:
            if not self.uses_docker:
                return self._create_node_with_resources_core(
                    node_config, tags, count, resources)
            else:
                return self._create_node_with_resources_docker(
                    node_config, tags, count, resources)

    def _create_node_with_resources_core(self, node_config, tags, count,
                                         resources):
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        next_id = self._next_hex_node_id()
        ray_params = ray._private.parameter.RayParams(
            min_worker_port=0,
            max_worker_port=0,
            dashboard_port=None,
            num_cpus=resources.pop("CPU", 0),
            num_gpus=resources.pop("GPU", 0),
            object_store_memory=resources.pop("object_store_memory", None),
            resources=resources,
            redis_address="{}:6379".format(
                ray._private.services.get_node_ip_address()),
            env_vars={
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": next_id,
                "RAY_OVERRIDE_RESOURCES": json.dumps(resources),
            })
        node = ray.node.Node(
            ray_params, head=False, shutdown_at_exit=False, spawn_reaper=False)
        self._nodes[next_id] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: node_type,
                TAG_RAY_NODE_NAME: next_id,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            "node": node
        }

    def _create_node_with_resources_docker(self, node_config, tags, count,
                                           resources):
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        is_head = tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD

        if is_head:
            next_id = FAKE_HEAD_NODE_ID
        else:
            next_id = self._next_hex_node_id()

        overwrite_tags = {
            TAG_RAY_NODE_KIND: (NODE_KIND_WORKER
                                if not is_head else NODE_KIND_HEAD),
            TAG_RAY_USER_NODE_TYPE: node_type,
            TAG_RAY_NODE_NAME: next_id,
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
        }

        self._nodes[next_id] = {
            "tags": overwrite_tags,
            "node_spec": self._create_node_spec_with_resources(
                head=is_head, node_id=next_id, resources=resources)
        }
        self._update_docker_compose_config()

    def terminate_node(self, node_id):
        with self.lock:
            try:
                node = self._nodes.pop(node_id)
            except Exception as e:
                print("EXCEPTION", str(e), "NODES", self._nodes)
                raise e

            if not self.uses_docker:
                self._kill_ray_processes(node["node"])
            else:
                self._update_docker_compose_config()

    def _kill_ray_processes(self, node):
        node.kill_all_processes(check_alive=False, allow_graceful=True)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
