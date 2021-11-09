import copy
import logging
import os
import subprocess

import yaml
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

DOCKER_HEAD_CMD = ("bash -c \"sleep 1 && ray start --head --port=6379 "
                   "--object-manager-port=8076 --dashboard-host 0.0.0.0 "
                   "--num-cpus {num_cpus} && sleep 1000000\"")

DOCKER_WORKER_CMD = (
    f"bash -c \"sleep 1 && ray start --address={FAKE_HEAD_NODE_ID}:6379 "
    "--object-manager-port=8076 --num-cpus {num_cpus} && sleep 1000000\"")


def create_node_spec(head: bool, docker_image: str, mounted_cluster_dir: str,
                     mounted_node_dir: str):
    node_spec = copy.deepcopy(DOCKER_NODE_SKELETON)
    node_spec["image"] = docker_image

    if head:
        node_spec["command"] = DOCKER_HEAD_CMD.format(num_cpus=4)
        node_spec["ports"] = [6379, 8265, 10001]
    else:
        node_spec["command"] = DOCKER_WORKER_CMD.format(num_cpus=4)

    node_spec["volumes"] = [
        f"{mounted_cluster_dir}:/cluster/shared",
        f"{mounted_node_dir}:/cluster/node",
    ]

    return node_spec


class FakeDockerProvider(NodeProvider):
    """A node provider that implements a docker cluster on a single machine.

    This is used for laptop/ci mode testing of autoscaling functionality
    while providing a realistic cluster environment. E.g. the nodes have
    their own IPs, run a linux distribution, and can ssh into each other."""

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        if "RAY_FAKE_DOCKER" not in os.environ:
            raise RuntimeError(
                "FakeDockerProvider requires ray to be started with "
                "RAY_FAKE_DOCKER=1 ray start ...")

        self._docker_image = "rayproject/ray:nightly"  # Todo

        # subdirs:
        #  - ./shared (shared filesystem)
        #  - ./nodes/<node_id> (node-specific mounted filesystem)
        self._volume_dir = "/tmp/fake_docker"  # Todo
        self._mounted_cluster_dir = os.path.join(self._volume_dir, "shared")

        os.makedirs(self._mounted_cluster_dir, mode=0o755, exist_ok=True)

        head_node_dir = os.path.join(self._volume_dir, "nodes",
                                     FAKE_HEAD_NODE_ID)
        os.makedirs(head_node_dir, mode=0o755, exist_ok=True)

        self._nodes = {
            FAKE_HEAD_NODE_ID: {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: FAKE_HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: FAKE_HEAD_NODE_ID,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                },
                "node_spec": create_node_spec(
                    head=True,
                    docker_image=self._docker_image,
                    mounted_cluster_dir=self._mounted_cluster_dir,
                    mounted_node_dir=head_node_dir,
                )
            },
        }
        self._docker_compose_config_path = \
            "/tmp/fake_docker/docker_compose.yaml"  # Todo
        self._docker_compose_config = None
        self._next_node_id = 0

        self._update_docker_compose_config()

    def _next_hex_node_id(self):
        self._next_node_id += 1
        base = "fffffffffffffffffffffffffffffffffffffffffffffffffff"
        return base + str(self._next_node_id).zfill(5)

    def _update_docker_compose_config(self):
        config = DOCKER_COMPOSE_SKELETON.copy()
        config["services"] = {}
        for node_id, node in self._nodes.items():
            config["services"][node_id] = node["node_spec"]

        with open(self._docker_compose_config_path, "wt") as f:
            yaml.safe_dump(config, f)

        update = subprocess.check_output([
            "docker-compose",
            "-f",
            self._docker_compose_config_path,
            "up",
            "-d",
            "--remove-orphans",
        ])
        print(f"Updated docker-compose: {update}")

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
        node_type = tags[TAG_RAY_USER_NODE_TYPE]
        next_id = self._next_hex_node_id()

        node_dir = os.path.join(self._volume_dir, "nodes", next_id)
        os.makedirs(node_dir, mode=0o755, exist_ok=True)

        # ray_params = ray._private.parameter.RayParams(
        #     min_worker_port=0,
        #     max_worker_port=0,
        #     dashboard_port=None,
        #     num_cpus=resources.pop("CPU", 0),
        #     num_gpus=resources.pop("GPU", 0),
        #     object_store_memory=resources.pop("object_store_memory", None),
        #     resources=resources,
        #     redis_address="{}:6379".format(
        #         ray._private.services.get_node_ip_address()),
        #     env_vars={
        #         "RAY_OVERRIDE_NODE_ID_FOR_TESTING": next_id,
        #         "RAY_OVERRIDE_RESOURCES": json.dumps(resources),
        #     })

        self._nodes[next_id] = {
            "tags": {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: node_type,
                TAG_RAY_NODE_NAME: next_id,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            },
            "node_spec": create_node_spec(
                head=False,
                docker_image=self._docker_image,
                mounted_cluster_dir=self._mounted_cluster_dir,
                mounted_node_dir=node_dir,
            )
        }
        self._update_docker_compose_config()

    def terminate_node(self, node_id):
        self._nodes.pop(node_id)
        self._update_docker_compose_config()

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
