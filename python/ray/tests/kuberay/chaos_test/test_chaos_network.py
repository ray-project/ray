import copy
import logging
import os
import tempfile
import unittest
import subprocess

from typing import Any, Dict

import yaml

from ray.tests.kuberay.utils import (
    get_pod,
    get_pod_names,
    get_raycluster,
    ray_client_port_forward,
    ray_job_submit,
    switch_to_ray_parent_dir,
    kubectl_exec_python_script,
    kubectl_logs,
    kubectl_patch,
    kubectl_exec,
    kubectl_delete,
    wait_for_pods,
    wait_for_pod_to_start,
    wait_for_ray_health,
)

from ray.tests.kuberay.scripts import (
    gpu_actor_placement,
    gpu_actor_validation,
    non_terminated_nodes_count,
)

logger = logging.getLogger(__name__)

# This image will be used for both the Ray nodes and the autoscaler.
# The CI should pass an image built from the test branch.
RAY_IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:nightly-py38")
# By default, use the same image for the autoscaler and Ray containers.
AUTOSCALER_IMAGE = os.environ.get("AUTOSCALER_IMAGE", RAY_IMAGE)
# Set to IfNotPresent in kind CI.
PULL_POLICY = os.environ.get("PULL_POLICY", "IfNotPresent")
logger.info(f"Using image `{RAY_IMAGE}` for Ray containers.")
logger.info(f"Using image `{AUTOSCALER_IMAGE}` for Autoscaler containers.")
logger.info(f"Using pull policy `{PULL_POLICY}` for all images.")

# Path to example config rel RAY_PARENT
EXAMPLE_CLUSTER_PATH = (
    "ray/python/ray/autoscaler/kuberay/config/samples/ray-cluster.autoscaler.yaml"
)

HEAD_SERVICE = "raycluster-autoscaler-head-svc"
HEAD_POD_PREFIX = "raycluster-autoscaler-head"
CPU_WORKER_PREFIX = "raycluster-autoscaler-worker-small-group"
RAY_CLUSTER_NAME = "raycluster-autoscaler"
RAY_CLUSTER_NAMESPACE = "default"

CURRENT_FILE_PATH = os.path.abspath(__file__)


class KubeRayFaultInjectionTest(unittest.TestCase):
    """e2e verification of autoscaling following the steps in the Ray documentation.
    kubectl is used throughout, as that reflects the instructions in the docs.
    """

    def _get_ray_cr_config(self, min_replicas=0, max_replicas=0) -> Dict[str, Any]:
        """Get Ray CR config yaml.

        - Use configurable replica fields for a CPU workerGroup.

        - Add a GPU-annotated group for testing GPU upscaling.

        - Fill in Ray image, autoscaler image, and image pull policies from env
          variables.
        """
        with open(EXAMPLE_CLUSTER_PATH) as ray_cr_config_file:
            ray_cr_config_str = ray_cr_config_file.read()
        config = yaml.safe_load(ray_cr_config_str)

        cpu_group = config["spec"]["workerGroupSpecs"][0]
        cpu_group["minReplicas"] = min_replicas
        cpu_group["maxReplicas"] = max_replicas
        # Substitute images.
        for group_spec in config["spec"]["workerGroupSpecs"] + [
            config["spec"]["headGroupSpec"]
        ]:
            containers = group_spec["template"]["spec"]["containers"]

            ray_container = containers[0]
            # Confirm the first container in the example config is the Ray container.
            assert ray_container["name"] in ["ray-head", "ray-worker"]
            # ("machine-learning" is the name of the worker Ray container)

            ray_container["image"] = RAY_IMAGE

            for container in containers:
                container["imagePullPolicy"] = PULL_POLICY

        autoscaler_options = {
            "image": AUTOSCALER_IMAGE,
            "imagePullPolicy": PULL_POLICY,
            # Allow quick scale-down for test purposes.
            "idleTimeoutSeconds": 10,
        }
        config["spec"]["autoscalerOptions"] = autoscaler_options

        return config

    def _apply_ray_cr(self, min_replicas=0, max_replicas=0) -> None:
        """Apply Ray CR config yaml, with configurable replica fields for the cpu
        workerGroup.

        If the CR does not yet exist, `replicas` can be set as desired.
        If the CR does already exist, the recommended usage is this:
            (1) Set `cpu_replicas` and `gpu_replicas` to what we currently expect them
                to be.
            (2) Set `validate_replicas` to True. We will then check that the replicas
            set on the CR coincides with `replicas`.
        """
        with tempfile.NamedTemporaryFile("w") as config_file:
            cr_config = self._get_ray_cr_config(
                min_replicas=min_replicas,
                max_replicas=max_replicas,
            )
            yaml.dump(cr_config, config_file)
            config_file.flush()
            subprocess.check_call(["kubectl", "apply", "-f", config_file.name])

    def _non_terminated_nodes_count(self) -> int:
        with ray_client_port_forward(head_service=HEAD_SERVICE):
            return non_terminated_nodes_count.main()

    def testRoundRobinWithoutFault(self):
        """
        Test the round robin script without inject any fault. This is to make sure the
        test infra works when there's no fault.

        1. Spinning up a Ray cluster
        2. Run the Ray job, assert it works
        """

        # Cluster creation
        logger.info("Creating a RayCluster with no worker pods.")
        self._apply_ray_cr(min_replicas=5, max_replicas=5)

        logger.info("Confirming presence of head.")
        wait_for_pods(goal_num_pods=1, namespace=RAY_CLUSTER_NAMESPACE)

        logger.info("Waiting for head pod to start Running.")
        wait_for_pod_to_start(
            pod_name_filter=HEAD_POD_PREFIX, namespace=RAY_CLUSTER_NAMESPACE
        )
        logger.info("Confirming Ray is up on the head pod.")
        wait_for_ray_health(
            pod_name_filter=HEAD_POD_PREFIX, namespace=RAY_CLUSTER_NAMESPACE
        )

        head_pod = get_pod(
            pod_name_filter=HEAD_POD_PREFIX, namespace=RAY_CLUSTER_NAMESPACE
        )
        assert head_pod, "Could not find the Ray head pod."

        logger.info("Running potato passer.")
        job_abs_path = os.path.join(os.path.dirname(
            CURRENT_FILE_PATH), "potato_passer.py")

        out = kubectl_exec(["python", "-c", job_abs_path, "--num_actors", 5,
                           "--pass_times", 1000, "--sleep-secs", 0.01],
                           head_pod, "default", "ray-head")
        
        logger.info("result: ", out)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
