import copy
import json
import logging
import os
import subprocess
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
    "shm_size": "1200m"
}

DOCKER_HEAD_CMD = (
    "bash -c \"sleep 1 && ray start --head --port=6379 "
    "--object-manager-port=8076 --dashboard-host 0.0.0.0 "
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
                     env_vars: Optional[Dict] = None):
    node_spec = copy.deepcopy(DOCKER_NODE_SKELETON)
    node_spec["image"] = docker_image

    resources = resources or {}

    resources_kwargs = dict(
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=json.dumps(resources, indent=None))

    if head:
        node_spec["command"] = DOCKER_HEAD_CMD.format(**resources_kwargs)
        node_spec["ports"] = [6379, 8265, 10001]
    else:
        node_spec["command"] = DOCKER_WORKER_CMD.format(**resources_kwargs)
        node_spec["depends_on"] = [FAKE_HEAD_NODE_ID]

    node_spec["volumes"] = [
        f"{mounted_cluster_dir}:/cluster/shared",
        f"{mounted_node_dir}:/cluster/node",
    ]

    env_vars = env_vars or {}
    node_spec["environment"] = [f"{k}='{v}'" for k, v in env_vars.items()]

    return node_spec


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

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

            self._nodes[FAKE_HEAD_NODE_ID][
                "node_spec"] = self._create_node_spec_with_resources(
                    head=True, node_id=FAKE_HEAD_NODE_ID, resources=resources)

            self._docker_compose_config_path = os.path.join(
                self._volume_dir, "docker-compose.yaml")
            self._docker_compose_config = None

            self._update_docker_compose_config()

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
            })

    def _update_docker_compose_config(self):
        config = DOCKER_COMPOSE_SKELETON.copy()
        config["services"] = {}
        for node_id, node in self._nodes.items():
            config["services"][node_id] = node["node_spec"]

        with open(self._docker_compose_config_path, "wt") as f:
            yaml.safe_dump(config, f)

        if self._has_head_node():
            update = subprocess.check_call([
                "docker-compose",
                "-f",
                self._docker_compose_config_path,
                "-p",
                self._project_name,
                "up",
                "-d",
                "--remove-orphans",
            ])
            print(f"Updated docker-compose: {update}")
        else:
            print("Waiting to update docker-compose until head node "
                  "is defined.")

    def _container_name(self, node_id):
        return f"{self._project_name}_{node_id}_1"

    def _get_ip(self,
                node_id,
                override_network: Optional[str] = None,
                retry_times: int = 3) -> str:
        if not self.uses_docker:
            return node_id

        network = override_network or f"{self._project_name}_ray_local"

        cmd = [
            "docker", "inspect", "-f", "\"{{ .NetworkSettings.Networks"
            f".{network}.IPAddress"
            " }}\"",
            self._container_name(node_id)
        ]
        for i in range(retry_times):
            try:
                ip_address = subprocess.check_output(cmd, encoding="utf-8")
            except Exception:
                time.sleep(1)
            else:
                return ip_address
        return None

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
        return node_id in self._nodes

    def is_terminated(self, node_id):
        return node_id not in self._nodes

    def node_tags(self, node_id):
        return self._nodes[node_id]["tags"]

    def external_ip(self, node_id):
        return node_id

    def internal_ip(self, node_id):
        return node_id

    def set_node_tags(self, node_id, tags):
        raise AssertionError("Readonly node provider cannot be updated")

    def create_node_with_resources(self, node_config, tags, count, resources):
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
        # node_type = tags[TAG_RAY_USER_NODE_TYPE]
        is_head = tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD

        if is_head:
            next_id = FAKE_HEAD_NODE_ID
        else:
            next_id = self._next_hex_node_id()

        # overwrite_tags = {
        #     TAG_RAY_NODE_KIND: (NODE_KIND_WORKER
        #       if not is_head else NODE_KIND_HEAD),
        #     TAG_RAY_USER_NODE_TYPE: node_type,
        #     TAG_RAY_NODE_NAME: next_id,
        #     TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
        # }

        self._nodes[next_id] = {
            "tags": tags,
            "node_spec": self._create_node_spec_with_resources(
                head=is_head, node_id=next_id, resources=resources)
        }
        self._update_docker_compose_config()

    def terminate_node(self, node_id):
        node = self._nodes.pop(node_id)["node"]
        self._kill_ray_processes(node)

    def _kill_ray_processes(self, node):
        if self.uses_docker:
            self._update_docker_compose_config()
        else:
            node.kill_all_processes(check_alive=False, allow_graceful=True)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
