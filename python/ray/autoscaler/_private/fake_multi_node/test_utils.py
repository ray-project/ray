import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Dict, Any, Optional
import yaml

import ray
from ray.autoscaler._private.fake_multi_node.node_provider import \
    FAKE_DOCKER_DEFAULT_CLIENT_PORT, FAKE_DOCKER_DEFAULT_GCS_PORT
from ray.util.ml_utils.dict import deep_update

logger = logging.getLogger(__name__)

DEFAULT_DOCKER_IMAGE = "rayproject/ray:nightly-py{major}{minor}-cpu"


class DockerCluster:
    """Docker cluster wrapper.

    Creates a directory for starting a fake multinode docker cluster.

    Includes APIs to update the cluster config as needed in tests,
    and to start and connect to the cluster.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._base_config_file = os.path.join(
            os.path.dirname(__file__), "example_docker.yaml")
        self._tempdir = None
        self._config_file = None
        self._nodes_file = None
        self._nodes = {}
        self._status_file = None
        self._status = {}
        self._partial_config = config
        self._cluster_config = None
        self._docker_image = None

        self._monitor_script = os.path.join(
            os.path.dirname(__file__), "docker_monitor.py")
        self._monitor_process = None

    @property
    def config_file(self):
        return self._config_file

    @property
    def cluster_config(self):
        return self._cluster_config

    @property
    def gcs_port(self):
        return self._cluster_config.get("provider", {}).get(
            "host_gcs_port", FAKE_DOCKER_DEFAULT_GCS_PORT)

    @property
    def client_port(self):
        return self._cluster_config.get("provider", {}).get(
            "host_client_port", FAKE_DOCKER_DEFAULT_CLIENT_PORT)

    def connect(self, client: bool = True, timeout: int = 120):
        """Connect to the docker-compose Ray cluster.

        Assumes the cluster is at RAY_TESTHOST (defaults to
        ``127.0.0.1``).

        Args:
            client (bool): If True, uses Ray client to connect to the
                cluster. If False, uses GCS to connect to the cluster.
            timeout (int): Connection timeout in seconds.

        """
        host = os.environ.get("RAY_TESTHOST", "127.0.0.1")

        if client:
            port = self.client_port
            address = f"ray://{host}:{port}"
        else:
            port = self.gcs_port
            address = f"{host}:{port}"

        timeout_at = time.monotonic() + timeout
        while time.monotonic() < timeout_at:
            try:
                ray.init(address)
                self.wait_for_resources({"CPU": 1})
            except Exception:
                time.sleep(1)
                continue
            else:
                break

        try:
            ray.cluster_resources()
        except Exception as e:
            raise RuntimeError(f"Timed out connecting to Ray: {e}")

    @staticmethod
    def wait_for_resources(resources: Dict[str, float], timeout: int = 60):
        """Wait until Ray cluster resources are available

        Args:
            resources (Dict[str, float]): Minimum resources needed before
                this function returns.
            timeout (int): Timeout in seconds.

        """
        timeout = time.monotonic() + timeout

        available = ray.cluster_resources()
        while any(available.get(k, 0.) < v for k, v in resources.items()):
            if time.monotonic() > timeout:
                raise RuntimeError(
                    f"Timed out waiting for resources: {resources}")
            time.sleep(1)
            available = ray.cluster_resources()

    def update_config(self, config: Optional[Dict[str, Any]] = None):
        """Update autoscaling config.

        Does a deep update of the base config with a new configuration.
        This can change autoscaling behavior.

        Args:
            config (Dict[str, Any]): Partial config to update current
                config with.

        """
        assert self._tempdir, "Call setup() first"

        config = config or {}

        if config:
            self._partial_config = config

        if not config.get("provider", {}).get("image"):
            # No image specified, trying to parse from buildkite
            docker_image = os.environ.get("RAY_DOCKER_IMAGE", None)

            if not docker_image:
                # If still no docker image, use one according to Python version
                mj = sys.version_info.major
                mi = sys.version_info.minor

                docker_image = DEFAULT_DOCKER_IMAGE.format(major=mj, minor=mi)

            self._docker_image = docker_image

        with open(self._base_config_file, "rt") as f:
            cluster_config = yaml.safe_load(f)

        if self._partial_config:
            deep_update(
                cluster_config, self._partial_config, new_keys_allowed=True)

        if self._docker_image:
            cluster_config["provider"]["image"] = self._docker_image

        cluster_config["provider"]["shared_volume_dir"] = self._tempdir

        self._cluster_config = cluster_config

        with open(self._config_file, "wt") as f:
            yaml.safe_dump(self._cluster_config, f)

        logging.info(f"Updated cluster config to: {self._cluster_config}")

    def maybe_pull_image(self):
        if self._docker_image:
            try:
                images_str = subprocess.check_output(
                    f"docker image inspect {self._docker_image}", shell=True)
                images = json.loads(images_str)
            except Exception as e:
                logger.error(
                    f"Error inspecting image {self._docker_image}: {e}")
                return

            if not images:
                try:
                    subprocess.check_output(
                        f"docker pull {self._docker_image}", shell=True)
                except Exception as e:
                    logger.error(
                        f"Error pulling image {self._docker_image}: {e}")

    def setup(self):
        """Setup docker compose cluster environment.

        Creates the temporary directory, writes the initial config file,
        and pulls the docker image, if required.
        """
        self._tempdir = tempfile.mkdtemp(
            dir=os.environ.get("RAY_TEMPDIR", None))
        os.chmod(self._tempdir, 0o777)
        self._config_file = os.path.join(self._tempdir, "cluster.yaml")
        self._nodes_file = os.path.join(self._tempdir, "nodes.json")
        self._status_file = os.path.join(self._tempdir, "status.json")
        self.update_config()
        self.maybe_pull_image()

    def teardown(self):
        """Tear down docker compose cluster environment."""
        shutil.rmtree(self._tempdir)
        self._tempdir = None
        self._config_file = None

    def _start_monitor(self):
        self._monitor_process = subprocess.Popen(
            ["python", self._monitor_script, self.config_file])
        time.sleep(2)

    def _stop_monitor(self):
        if self._monitor_process:
            self._monitor_process.wait(timeout=30)
            if self._monitor_process.poll() is None:
                self._monitor_process.terminate()
        self._monitor_process = None

    def start(self):
        """Start docker compose cluster.

        Starts the monitor process and runs ``ray up``.
        """
        self._start_monitor()

        subprocess.check_output(
            f"RAY_FAKE_CLUSTER=1 ray up -y {self.config_file}", shell=True)

    def stop(self):
        """Stop docker compose cluster.

        Runs ``ray down`` and stops the monitor process.
        """
        if ray.is_initialized:
            ray.shutdown()

        subprocess.check_output(
            f"RAY_FAKE_CLUSTER=1 ray down -y {self.config_file}", shell=True)

        self._stop_monitor()

    def _update_nodes(self):
        with open(self._nodes_file, "rt") as f:
            self._nodes = json.load(f)

    def _update_status(self):
        with open(self._status_file, "rt") as f:
            self._status = json.load(f)

    def _get_node(self,
                  node_id: Optional[str] = None,
                  num: Optional[int] = None,
                  rand: Optional[str] = None) -> str:
        self._update_nodes()
        if node_id:
            assert not num and not rand, (
                "Only provide either `node_id`, `num`, or `random`.")
        elif num:
            assert not node_id and not rand, (
                "Only provide either `node_id`, `num`, or `random`.")
            base = "fffffffffffffffffffffffffffffffffffffffffffffffffff"
            node_id = base + str(num).zfill(5)
        elif rand:
            assert not node_id and not num, (
                "Only provide either `node_id`, `num`, or `random`.")
            assert rand in ["worker", "any"
                            ], ("`random` must be one of ['worker', 'any']")
            choices = list(self._nodes.keys())
            if rand == "worker":
                choices.remove(
                    "fffffffffffffffffffffffffffffffffffffffffffffffffff00000")
            # Else: any
            node_id = random.choice(choices)

        assert node_id in self._nodes, (
            f"Node with ID {node_id} is not in active nodes.")
        return node_id

    def _get_docker_container(self, node_id: str) -> Optional[str]:
        self._update_status()
        node_status = self._status.get(node_id)
        if not node_status:
            return None

        return node_status["Name"]

    def kill_node(self,
                  node_id: Optional[str] = None,
                  num: Optional[int] = None,
                  rand: Optional[str] = None):
        """Kill node.

        If ``node_id`` is given, kill that node.

        If ``num`` is given, construct node_id from this number, and kill
        that node.

        If ``rand`` is given (as either ``worker`` or ``any``), kill a random
        node.
        """
        node_id = self._get_node(node_id=node_id, num=num, rand=rand)
        container = self._get_docker_container(node_id=node_id)
        subprocess.check_output(f"docker kill {container}", shell=True)
