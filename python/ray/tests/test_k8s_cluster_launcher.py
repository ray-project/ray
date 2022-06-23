import os
import tempfile
import time
import unittest

import kubernetes
import pytest
import yaml

from ray.autoscaler._private._kubernetes.node_provider import KubernetesNodeProvider
from ray.autoscaler import sdk

IMAGE_ENV = "KUBERNETES_CLUSTER_LAUNCHER_TEST_IMAGE"


def fill_image_field(pod_config):
    image = os.getenv(IMAGE_ENV, "rayproject/ray:nightly")
    pod_config["spec"]["containers"][0]["image"] = image


def fill_image_fields(cluster_config):
    for key in "worker_nodes", "head_node":
        fill_image_field(cluster_config[key])


def get_config():
    here = os.path.realpath(__file__)
    parent = os.path.dirname(here)
    relative_path = "test_cli_patterns/test_k8s_cluster_launcher.yaml"
    config_path = os.path.join(parent, relative_path)
    config = yaml.safe_load(open(config_path).read())
    fill_image_fields(config)
    return config


class KubernetesTest(unittest.TestCase):
    def test_up_and_down(self):
        """(1) Runs 'ray up' with a Kubernetes config that specifies
        min_workers=1.
        (2) Runs 'ray exec' to read monitor logs and confirm that worker and
        head are connected.
        (4) Rsyncs files up and down.
        (3) Runs 'ray down' and confirms that the cluster is gone."""

        # get path to config
        config = get_config()

        # get a node provider
        provider_config = config["provider"]
        cluster_name = config["cluster_name"]
        self.provider = KubernetesNodeProvider(provider_config, cluster_name)

        # ray up
        sdk.create_or_update_cluster(config, no_config_cache=True)

        # Check for two pods (worker and head).
        while True:
            nodes = self.provider.non_terminated_nodes({})
            if len(nodes) == 2:
                break
            else:
                time.sleep(1)

        # Read logs with ray exec and check that worker and head are connected.
        # (Since the config yaml is legacy-style, we check for
        # ray-legacy-*-node_type.)
        log_cmd = "tail -n 100 /tmp/ray/session_latest/logs/monitor*"
        while True:
            monitor_output = sdk.run_on_cluster(
                config, cmd=log_cmd, with_output=True
            ).decode()
            if "head-node" in monitor_output and "worker-node" in monitor_output:
                break
            else:
                time.sleep(1)

        # rsync
        with tempfile.NamedTemporaryFile("w") as test_file:
            test_file.write("test")
            test_file.flush()
            sdk.rsync(config, source=test_file.name, target="~/in_pod", down=False)
        with tempfile.NamedTemporaryFile() as test_file:
            sdk.rsync(config, target=test_file.name, source="~/in_pod", down=True)
            contents = open(test_file.name).read()
        assert contents == "test"

        # ray down
        sdk.teardown_cluster(config)

        # Check that there are no pods left in namespace ray to confirm that
        # the cluster is gone.
        while True:
            nodes = self.provider.non_terminated_nodes({})
            if len(nodes) == 0:
                break
            else:
                time.sleep(1)

    def __del__(self):
        kubernetes.config.load_kube_config()
        core_api = kubernetes.client.CoreV1Api()
        core_api.delete_namespace(self.provider.namespace)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
