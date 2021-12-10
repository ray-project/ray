import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from typing import Dict, Any, Optional
import yaml

import ray
from ray.autoscaler._private.fake_multi_node.node_provider import \
    FAKE_DOCKER_DEFAULT_CLIENT_PORT, FAKE_DOCKER_DEFAULT_GCS_PORT
from ray.util.ml_utils.dict import deep_update

logger = logging.getLogger(__name__)


class DockerMonitor:
    def __init__(self, config_file: str):
        self._monitor_script = os.path.join(
            os.path.dirname(__file__), "docker_monitor.py")
        self._config_file = config_file
        self._process = None

    def start(self):
        self._process = subprocess.Popen(
            ["python", self._monitor_script, self._config_file])

    def stop(self):
        if self._process:
            self._process.wait(timeout=30)
            if self._process.poll() is None:
                self._process.terminate()
        self._process = None


class DockerCluster:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self._base_config_file = os.path.join(
            os.path.dirname(__file__), "example_docker.yaml")
        self._tempdir = None
        self._config_file = None
        self._partial_config = config
        self._cluster_config = None

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

    def connect(self, client: bool = True, timeout: int = 60):
        if client:
            port = self.client_port
            address = f"ray://127.0.0.1:{port}"
        else:
            port = self.gcs_port
            address = f"127.0.0.1:{port}"

        timeout = time.monotonic() + timeout
        port_open = False
        while time.monotonic() < timeout:
            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            location = ("127.0.0.1", port)
            port_open = a_socket.connect_ex(location) == 0
            if port_open:
                break
            time.sleep(1)

        if not port_open:
            raise RuntimeError(f"Timed out waiting on port {port} to open")

        ray.init(address)
        self.wait_for_resources({"CPU": 1})

    @staticmethod
    def wait_for_resources(resources: Dict[str, float], timeout: int = 60):
        timeout = time.monotonic() + timeout

        available = ray.cluster_resources()
        while any(available.get(k, 0.) < v for k, v in resources.items()):
            if time.monotonic() > timeout:
                raise RuntimeError(
                    f"Timed out waiting for resources: {resources}")
            time.sleep(1)
            available = ray.cluster_resources()

    def update_config(self, config: Optional[Dict[str, Any]] = None):
        assert self._tempdir, "Call setup() first"

        config = config or {}

        if config:
            self._partial_config = config

        docker_image = None
        if not config.get("provider", {}).get("image"):
            # No image specified, trying to parse from buildkite
            buildkite_plugins_str = os.environ.get("BUILDKITE_PLUGINS")
            if buildkite_plugins_str:
                plugins = json.loads(buildkite_plugins_str)
                for plugin in plugins:
                    for plugin_name, data in plugin.items():
                        if ("buildkite-plugins/docker-buildkite-plugin" in
                                plugin_name):
                            docker_image = data.get("image")

        with open(self._base_config_file, "rt") as f:
            cluster_config = yaml.safe_load(f)

        if self._partial_config:
            deep_update(
                cluster_config, self._partial_config, new_keys_allowed=True)

        if docker_image:
            cluster_config["provider"]["image"] = docker_image

        cluster_config["provider"]["shared_volume_dir"] = self._tempdir

        self._cluster_config = cluster_config

        with open(self._config_file, "wt") as f:
            yaml.safe_dump(self._cluster_config, f)

        logging.info(f"Updated cluster config to: {self._cluster_config}")

    def setup(self):
        self._tempdir = tempfile.mkdtemp()
        self._config_file = os.path.join(self._tempdir, "cluster.yaml")
        self.update_config()

    def teardown(self):
        shutil.rmtree(self._tempdir)
        self._tempdir = None
        self._config_file = None

    def start(self):
        subprocess.check_output(
            f"RAY_FAKE_CLUSTER=1 ray up -y {self.config_file}", shell=True)

    def stop(self):
        if ray.is_initialized:
            ray.shutdown()

        subprocess.check_output(
            f"RAY_FAKE_CLUSTER=1 ray down -y {self.config_file}", shell=True)
