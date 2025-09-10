import copy
import json
import logging
import os
import subprocess
import sys
import time
from threading import RLock
from types import ModuleType
from typing import Any, Dict, Optional

import yaml

import ray
import ray._private.ray_constants as ray_constants
from ray._common.network_utils import build_address
from ray.autoscaler._private.fake_multi_node.command_runner import (
    FakeDockerCommandRunner,
)
from ray.autoscaler.command_runner import CommandRunnerInterface
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
# starting with fffff*00000 for the head node, fffff*00001, etc. for workers.
FAKE_HEAD_NODE_ID = "fffffffffffffffffffffffffffffffffffffffffffffffffff00000"
FAKE_HEAD_NODE_TYPE = "ray.head.default"

FAKE_DOCKER_DEFAULT_GCS_PORT = 16379
FAKE_DOCKER_DEFAULT_OBJECT_MANAGER_PORT = 18076
FAKE_DOCKER_DEFAULT_CLIENT_PORT = 10002

DOCKER_COMPOSE_SKELETON = {
    "services": {},
    "networks": {"ray_local": {}},
}

DOCKER_NODE_SKELETON = {
    "networks": ["ray_local"],
    "mem_limit": "3000m",
    "mem_reservation": "3000m",
    "shm_size": "1200m",
    "volumes": [],
}

DOCKER_HEAD_CMD = (
    'bash -c "'
    "sudo mkdir -p {volume_dir} && "
    "sudo chmod 777 {volume_dir} && "
    "touch {volume_dir}/.in_docker && "
    "sudo chown -R ray:users /cluster/node && "
    "sudo chmod -R 777 /cluster/node && "
    "sudo chown -R ray:users /cluster/shared && "
    "sudo chmod -R 777 /cluster/shared && "
    "sudo chmod 700 ~/.ssh && "
    "sudo chmod 600 ~/.ssh/authorized_keys && "
    "sudo chmod 600 ~/ray_bootstrap_key.pem && "
    "sudo chown ray:users "
    "~/.ssh ~/.ssh/authorized_keys ~/ray_bootstrap_key.pem && "
    "{ensure_ssh} && "
    "sleep 1 && "
    "RAY_FAKE_CLUSTER=1 ray start --head "
    "--autoscaling-config=~/ray_bootstrap_config.yaml "
    "--object-manager-port=8076 "
    "--num-cpus {num_cpus} "
    "--num-gpus {num_gpus} "
    # "--resources='{resources}' "
    '--block"'
)

DOCKER_WORKER_CMD = (
    'bash -c "'
    "sudo mkdir -p {volume_dir} && "
    "sudo chmod 777 {volume_dir} && "
    "touch {volume_dir}/.in_docker && "
    "sudo chown -R ray:users /cluster/node && "
    "sudo chmod -R 777 /cluster/node && "
    "sudo chmod 700 ~/.ssh && "
    "sudo chmod 600 ~/.ssh/authorized_keys && "
    "sudo chown ray:users ~/.ssh ~/.ssh/authorized_keys && "
    "{ensure_ssh} && "
    "sleep 1 && "
    f"ray start --address={FAKE_HEAD_NODE_ID}:6379 "
    "--object-manager-port=8076 "
    "--num-cpus {num_cpus} "
    "--num-gpus {num_gpus} "
    # "--resources='{resources}' "
    '--block"'
)


def host_dir(container_dir: str):
    """Replace local dir with potentially different host dir.

    E.g. in docker-in-docker environments, the host dir might be
    different to the mounted directory in the container.

    This method will do a simple global replace to adjust the paths.
    """
    ray_tempdir = os.environ.get("RAY_TEMPDIR", None)
    ray_hostdir = os.environ.get("RAY_HOSTDIR", None)

    if not ray_tempdir or not ray_hostdir:
        return container_dir

    return container_dir.replace(ray_tempdir, ray_hostdir)


def create_node_spec(
    head: bool,
    docker_image: str,
    mounted_cluster_dir: str,
    mounted_node_dir: str,
    num_cpus: int = 2,
    num_gpus: int = 0,
    resources: Optional[Dict] = None,
    env_vars: Optional[Dict] = None,
    host_gcs_port: int = 16379,
    host_object_manager_port: int = 18076,
    host_client_port: int = 10002,
    volume_dir: Optional[str] = None,
    node_state_path: Optional[str] = None,
    docker_status_path: Optional[str] = None,
    docker_compose_path: Optional[str] = None,
    bootstrap_config_path: Optional[str] = None,
    private_key_path: Optional[str] = None,
    public_key_path: Optional[str] = None,
):
    node_spec = copy.deepcopy(DOCKER_NODE_SKELETON)
    node_spec["image"] = docker_image

    bootstrap_cfg_path_on_container = "/home/ray/ray_bootstrap_config.yaml"
    bootstrap_key_path_on_container = "/home/ray/ray_bootstrap_key.pem"

    resources = resources or {}

    ensure_ssh = (
        (
            "((sudo apt update && sudo apt install -y openssh-server && "
            "sudo service ssh start) || true)"
        )
        if not bool(int(os.environ.get("RAY_HAS_SSH", "0") or "0"))
        else "sudo service ssh start"
    )

    cmd_kwargs = dict(
        ensure_ssh=ensure_ssh,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=json.dumps(resources, indent=None),
        volume_dir=volume_dir,
        autoscaling_config=bootstrap_cfg_path_on_container,
    )

    env_vars = env_vars or {}

    # Set to "auto" to mount current autoscaler directory to nodes for dev
    fake_cluster_dev_dir = os.environ.get("FAKE_CLUSTER_DEV", "")
    if fake_cluster_dev_dir:
        if fake_cluster_dev_dir == "auto":
            local_ray_dir = os.path.dirname(ray.__file__)
        else:
            local_ray_dir = fake_cluster_dev_dir
        os.environ["FAKE_CLUSTER_DEV"] = local_ray_dir

        mj = sys.version_info.major
        mi = sys.version_info.minor

        fake_modules_str = os.environ.get("FAKE_CLUSTER_DEV_MODULES", "autoscaler")
        fake_modules = fake_modules_str.split(",")

        docker_ray_dir = f"/home/ray/anaconda3/lib/python{mj}.{mi}/site-packages/ray"

        node_spec["volumes"] += [
            f"{local_ray_dir}/{module}:{docker_ray_dir}/{module}:ro"
            for module in fake_modules
        ]
        env_vars["FAKE_CLUSTER_DEV"] = local_ray_dir
        env_vars["FAKE_CLUSTER_DEV_MODULES"] = fake_modules_str
        os.environ["FAKE_CLUSTER_DEV_MODULES"] = fake_modules_str

    if head:
        node_spec["command"] = DOCKER_HEAD_CMD.format(**cmd_kwargs)
        # Expose ports so we can connect to the cluster from outside
        node_spec["ports"] = [
            f"{host_gcs_port}:{ray_constants.DEFAULT_PORT}",
            f"{host_object_manager_port}:8076",
            f"{host_client_port}:10001",
        ]
        # Mount status and config files for the head node
        node_spec["volumes"] += [
            f"{host_dir(node_state_path)}:{node_state_path}",
            f"{host_dir(docker_status_path)}:{docker_status_path}",
            f"{host_dir(docker_compose_path)}:{docker_compose_path}",
            f"{host_dir(bootstrap_config_path)}:" f"{bootstrap_cfg_path_on_container}",
            f"{host_dir(private_key_path)}:{bootstrap_key_path_on_container}",
        ]

        # Create file if it does not exist on local filesystem
        for filename in [node_state_path, docker_status_path, bootstrap_config_path]:
            if not os.path.exists(filename):
                with open(filename, "wt") as f:
                    f.write("{}")
    else:
        node_spec["command"] = DOCKER_WORKER_CMD.format(**cmd_kwargs)
        node_spec["depends_on"] = [FAKE_HEAD_NODE_ID]

    # Mount shared directories and ssh access keys
    node_spec["volumes"] += [
        f"{host_dir(mounted_cluster_dir)}:/cluster/shared",
        f"{host_dir(mounted_node_dir)}:/cluster/node",
        f"{host_dir(public_key_path)}:/home/ray/.ssh/authorized_keys",
    ]

    # Pass these environment variables (to the head node)
    # These variables are propagated by the `docker compose` command.
    env_vars.setdefault("RAY_HAS_SSH", os.environ.get("RAY_HAS_SSH", ""))
    env_vars.setdefault("RAY_TEMPDIR", os.environ.get("RAY_TEMPDIR", ""))
    env_vars.setdefault("RAY_HOSTDIR", os.environ.get("RAY_HOSTDIR", ""))

    node_spec["environment"] = [f"{k}={v}" for k, v in env_vars.items()]

    return node_spec


class FakeMultiNodeProvider(NodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of autoscaling functionality."""

    def __init__(
        self,
        provider_config,
        cluster_name,
    ):
        """
        Args:
            provider_config: Configuration for the provider.
            cluster_name: Name of the cluster.
        """

        NodeProvider.__init__(self, provider_config, cluster_name)
        self.lock = RLock()
        if "RAY_FAKE_CLUSTER" not in os.environ:
            raise RuntimeError(
                "FakeMultiNodeProvider requires ray to be started with "
                "RAY_FAKE_CLUSTER=1 ray start ..."
            )
        # GCS address to use for the cluster
        self._gcs_address = provider_config.get("gcs_address", None)
        # Head node id
        self._head_node_id = provider_config.get("head_node_id", FAKE_HEAD_NODE_ID)
        # Whether to launch multiple nodes at once, or one by one regardless of
        # the count (default)
        self._launch_multiple = provider_config.get("launch_multiple", False)

        # These are injected errors for testing purposes. If not None,
        # these will be raised on `create_node_with_resources_and_labels`` and
        # `terminate_node``, respectively.
        self._creation_error = None
        self._termination_errors = None

        self._nodes = {
            self._head_node_id: {
                "tags": {
                    TAG_RAY_NODE_KIND: NODE_KIND_HEAD,
                    TAG_RAY_USER_NODE_TYPE: FAKE_HEAD_NODE_TYPE,
                    TAG_RAY_NODE_NAME: self._head_node_id,
                    TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
                }
            },
        }
        self._next_node_id = 0

    def _next_hex_node_id(self):
        self._next_node_id += 1
        base = "fffffffffffffffffffffffffffffffffffffffffffffffffff"
        return base + str(self._next_node_id).zfill(5)

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
        raise AssertionError("Readonly node provider cannot be updated")

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        if self._creation_error:
            raise self._creation_error

        if self._launch_multiple:
            for _ in range(count):
                self._create_node_with_resources_and_labels(
                    node_config, tags, count, resources, labels
                )
        else:
            self._create_node_with_resources_and_labels(
                node_config, tags, count, resources, labels
            )

    def _create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        # This function calls `pop`. To avoid side effects, we make a
        # copy of `resources`.
        resources = copy.deepcopy(resources)
        with self.lock:
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
                labels=labels,
                redis_address=build_address(
                    ray._private.services.get_node_ip_address(), 6379
                )
                if not self._gcs_address
                else self._gcs_address,
                gcs_address=build_address(
                    ray._private.services.get_node_ip_address(), 6379
                )
                if not self._gcs_address
                else self._gcs_address,
                env_vars={
                    "RAY_OVERRIDE_NODE_ID_FOR_TESTING": next_id,
                    "RAY_CLOUD_INSTANCE_ID": next_id,
                    "RAY_NODE_TYPE_NAME": node_type,
                    ray_constants.RESOURCES_ENVIRONMENT_VARIABLE: json.dumps(resources),
                    ray_constants.LABELS_ENVIRONMENT_VARIABLE: json.dumps(labels),
                },
            )
            node = ray._private.node.Node(
                ray_params, head=False, shutdown_at_exit=False, spawn_reaper=False
            )
            all_tags = {
                TAG_RAY_NODE_KIND: NODE_KIND_WORKER,
                TAG_RAY_USER_NODE_TYPE: node_type,
                TAG_RAY_NODE_NAME: next_id,
                TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE,
            }
            all_tags.update(tags)
            self._nodes[next_id] = {
                "tags": all_tags,
                "node": node,
            }

    def terminate_node(self, node_id):
        with self.lock:
            if self._termination_errors:
                raise self._termination_errors

            try:
                node = self._nodes.pop(node_id)
            except Exception as e:
                raise e

            self._terminate_node(node)

    def _terminate_node(self, node):
        node["node"].kill_all_processes(check_alive=False, allow_graceful=True)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config

    ############################
    # Test only methods
    ############################
    def _test_set_creation_error(self, e: Exception):
        """Set an error that will be raised on
        create_node_with_resources_and_labels."""
        self._creation_error = e

    def _test_add_termination_errors(self, e: Exception):
        """Set an error that will be raised on terminate_node."""
        self._termination_errors = e


class FakeMultiNodeDockerProvider(FakeMultiNodeProvider):
    """A node provider that implements multi-node on a single machine.

    This is used for laptop mode testing of multi node functionality
    where each node has their own FS and IP."""

    def __init__(self, provider_config, cluster_name):
        super(FakeMultiNodeDockerProvider, self).__init__(provider_config, cluster_name)

        fake_head = copy.deepcopy(self._nodes)

        self._project_name = self.provider_config["project_name"]
        self._docker_image = self.provider_config["image"]

        self._host_gcs_port = self.provider_config.get(
            "host_gcs_port", FAKE_DOCKER_DEFAULT_GCS_PORT
        )
        self._host_object_manager_port = self.provider_config.get(
            "host_object_manager_port", FAKE_DOCKER_DEFAULT_OBJECT_MANAGER_PORT
        )
        self._host_client_port = self.provider_config.get(
            "host_client_port", FAKE_DOCKER_DEFAULT_CLIENT_PORT
        )

        self._head_resources = self.provider_config["head_resources"]

        # subdirs:
        #  - ./shared (shared filesystem)
        #  - ./nodes/<node_id> (node-specific mounted filesystem)
        self._volume_dir = self.provider_config["shared_volume_dir"]
        self._mounted_cluster_dir = os.path.join(self._volume_dir, "shared")

        if not self.in_docker_container:
            # Only needed on host
            os.makedirs(self._mounted_cluster_dir, mode=0o755, exist_ok=True)

        self._boostrap_config_path = os.path.join(
            self._volume_dir, "bootstrap_config.yaml"
        )

        self._private_key_path = os.path.join(self._volume_dir, "bootstrap_key.pem")
        self._public_key_path = os.path.join(self._volume_dir, "bootstrap_key.pem.pub")

        if not self.in_docker_container:
            # Create private key
            if not os.path.exists(self._private_key_path):
                subprocess.check_call(
                    f'ssh-keygen -b 2048 -t rsa -q -N "" '
                    f"-f {self._private_key_path}",
                    shell=True,
                )

            # Create public key
            if not os.path.exists(self._public_key_path):
                subprocess.check_call(
                    f"ssh-keygen -y "
                    f"-f {self._private_key_path} "
                    f"> {self._public_key_path}",
                    shell=True,
                )

        self._docker_compose_config_path = os.path.join(
            self._volume_dir, "docker-compose.yaml"
        )
        self._docker_compose_config = None

        self._node_state_path = os.path.join(self._volume_dir, "nodes.json")
        self._docker_status_path = os.path.join(self._volume_dir, "status.json")

        self._load_node_state()
        if FAKE_HEAD_NODE_ID not in self._nodes:
            # Reset
            self._nodes = copy.deepcopy(fake_head)

        self._nodes[FAKE_HEAD_NODE_ID][
            "node_spec"
        ] = self._create_node_spec_with_resources(
            head=True, node_id=FAKE_HEAD_NODE_ID, resources=self._head_resources
        )
        self._possibly_terminated_nodes = dict()

        self._cleanup_interval = provider_config.get("cleanup_interval", 9.5)

        self._docker_status = {}

        self._update_docker_compose_config()
        self._update_docker_status()
        self._save_node_state()

    @property
    def in_docker_container(self):
        return os.path.exists(os.path.join(self._volume_dir, ".in_docker"))

    def _create_node_spec_with_resources(
        self, head: bool, node_id: str, resources: Dict[str, Any]
    ):
        resources = resources.copy()

        # Create shared directory
        node_dir = os.path.join(self._volume_dir, "nodes", node_id)
        os.makedirs(node_dir, mode=0o777, exist_ok=True)

        resource_str = json.dumps(resources, indent=None)

        return create_node_spec(
            head=head,
            docker_image=self._docker_image,
            mounted_cluster_dir=self._mounted_cluster_dir,
            mounted_node_dir=node_dir,
            num_cpus=resources.pop("CPU", 0),
            num_gpus=resources.pop("GPU", 0),
            host_gcs_port=self._host_gcs_port,
            host_object_manager_port=self._host_object_manager_port,
            host_client_port=self._host_client_port,
            resources=resources,
            env_vars={
                "RAY_OVERRIDE_NODE_ID_FOR_TESTING": node_id,
                ray_constants.RESOURCES_ENVIRONMENT_VARIABLE: resource_str,
                **self.provider_config.get("env_vars", {}),
            },
            volume_dir=self._volume_dir,
            node_state_path=self._node_state_path,
            docker_status_path=self._docker_status_path,
            docker_compose_path=self._docker_compose_config_path,
            bootstrap_config_path=self._boostrap_config_path,
            public_key_path=self._public_key_path,
            private_key_path=self._private_key_path,
        )

    def _load_node_state(self) -> bool:
        if not os.path.exists(self._node_state_path):
            return False
        try:
            with open(self._node_state_path, "rt") as f:
                nodes = json.load(f)
        except Exception:
            return False
        if not nodes:
            return False
        self._nodes = nodes
        return True

    def _save_node_state(self):
        with open(self._node_state_path, "wt") as f:
            json.dump(self._nodes, f)

        # Make sure this is always writeable from inside the containers
        if not self.in_docker_container:
            # Only chmod from the outer container
            os.chmod(self._node_state_path, 0o777)

    def _update_docker_compose_config(self):
        config = copy.deepcopy(DOCKER_COMPOSE_SKELETON)
        config["services"] = {}
        for node_id, node in self._nodes.items():
            config["services"][node_id] = node["node_spec"]

        with open(self._docker_compose_config_path, "wt") as f:
            yaml.safe_dump(config, f)

    def _update_docker_status(self):
        if not os.path.exists(self._docker_status_path):
            return
        with open(self._docker_status_path, "rt") as f:
            self._docker_status = json.load(f)

    def _update_nodes(self):
        for node_id in list(self._nodes):
            if not self._is_docker_running(node_id):
                self._possibly_terminated_nodes.setdefault(node_id, time.monotonic())
            else:
                self._possibly_terminated_nodes.pop(node_id, None)
        self._cleanup_nodes()

    def _cleanup_nodes(self):
        for node_id, timestamp in list(self._possibly_terminated_nodes.items()):
            if time.monotonic() > timestamp + self._cleanup_interval:
                if not self._is_docker_running(node_id):
                    self._nodes.pop(node_id, None)
                self._possibly_terminated_nodes.pop(node_id, None)
        self._save_node_state()

    def _container_name(self, node_id):
        node_status = self._docker_status.get(node_id, {})
        timeout = time.monotonic() + 60
        while not node_status:
            if time.monotonic() > timeout:
                raise RuntimeError(f"Container for {node_id} never became available.")
            time.sleep(1)
            self._update_docker_status()
            node_status = self._docker_status.get(node_id, {})

        return node_status["Name"]

    def _is_docker_running(self, node_id):
        self._update_docker_status()

        return self._docker_status.get(node_id, {}).get("State", None) == "running"

    def non_terminated_nodes(self, tag_filters):
        self._update_nodes()
        return super(FakeMultiNodeDockerProvider, self).non_terminated_nodes(
            tag_filters
        )

    def is_running(self, node_id):
        with self.lock:
            self._update_nodes()

            return node_id in self._nodes and self._is_docker_running(node_id)

    def is_terminated(self, node_id):
        with self.lock:
            self._update_nodes()

            return node_id not in self._nodes and not self._is_docker_running(node_id)

    def get_command_runner(
        self,
        log_prefix: str,
        node_id: str,
        auth_config: Dict[str, Any],
        cluster_name: str,
        process_runner: ModuleType,
        use_internal_ip: bool,
        docker_config: Optional[Dict[str, Any]] = None,
    ) -> CommandRunnerInterface:
        if self.in_docker_container:
            return super(FakeMultiNodeProvider, self).get_command_runner(
                log_prefix,
                node_id,
                auth_config,
                cluster_name,
                process_runner,
                use_internal_ip,
            )

        # Else, host command runner:
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip,
        }

        docker_config["container_name"] = self._container_name(node_id)
        docker_config["image"] = self._docker_image

        return FakeDockerCommandRunner(docker_config, **common_args)

    def _get_ip(self, node_id: str) -> Optional[str]:
        for i in range(3):
            self._update_docker_status()
            ip = self._docker_status.get(node_id, {}).get("IP", None)
            if ip:
                return ip
            time.sleep(3)
        return None

    def set_node_tags(self, node_id, tags):
        assert node_id in self._nodes
        self._nodes[node_id]["tags"].update(tags)

    def create_node_with_resources_and_labels(
        self, node_config, tags, count, resources, labels
    ):
        with self.lock:
            is_head = tags[TAG_RAY_NODE_KIND] == NODE_KIND_HEAD

            if is_head:
                next_id = FAKE_HEAD_NODE_ID
            else:
                next_id = self._next_hex_node_id()

            self._nodes[next_id] = {
                "tags": tags,
                "node_spec": self._create_node_spec_with_resources(
                    head=is_head, node_id=next_id, resources=resources
                ),
            }
            self._update_docker_compose_config()
            self._save_node_state()

    def create_node(
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        resources = self._head_resources
        return self.create_node_with_resources_and_labels(
            node_config, tags, count, resources, {}
        )

    def _terminate_node(self, node):
        self._update_docker_compose_config()
        self._save_node_state()

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
