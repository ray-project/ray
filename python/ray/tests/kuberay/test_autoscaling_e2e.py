import os
import pathlib
import tempfile
import unittest
import subprocess

from typing import Any, Dict

import yaml

from ray.tests.kuberay.utils import (
    get_pod,
    kubectl_exec,
    wait_for_pods,
    wait_for_pod_to_start,
    wait_for_ray_health,
)


# This image will be used for both the autoscaler and Ray nodes.
RAY_IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:413fe0")
# The default "rayproject/ray:413fe0" is the currently pinned autoscaler image
# (to be replaced with rayproject/ray:1.12.0 upon 1.12.0 release).

# Parent directory of Ray repository
RAY_PARENT = str(pathlib.Path(__file__).resolve().parents[5])
# Path to example config rel RAY_PARENT
EXAMPLE_CLUSTER_PATH = "ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml"


class KubeRayAutoscalingTest(unittest.TestCase):
    """e2e verification of autoscaling following the steps in the Ray documentation.
    kubectl is used throughout, as that reflects the instructions in the docs.
    """

    def setUp(self):
        """
        Set up KubeRay operator and Ray autoscaler RBAC.
        """

        # Switch to parent of Ray repo, because that's what the doc examples do.
        print("Switching to parent of Ray directory.")
        os.chdir(RAY_PARENT)

        print("Cloning KubeRay and setting up KubeRay configuration.")
        subprocess.check_call(
            [
                "bash",
                "-c",
                "ls kuberay || ./ray/python/ray/autoscaler/kuberay/init-config.sh",
            ]
        )
        print("Creating KubeRay operator.")
        subprocess.check_call(
            [
                "kubectl",
                "apply",
                "-k",
                "ray/python/ray/autoscaler/kuberay/config/default",
            ]
        )
        print("Creating autoscaler RBAC objects.")
        subprocess.check_call(
            [
                "kubectl",
                "apply",
                "-f",
                "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml",
            ]
        )
        self.ray_cr_config_file = self._get_ray_cr_config_file()

    def _get_ray_cr_config_file(self) -> str:
        """Replace Ray node and autoscaler images in example CR with the test image.
        Write modified CR to temp file.
        Return temp file's name.
        """
        ray_cr_config_str = open(EXAMPLE_CLUSTER_PATH).read()
        ray_images = [
            word for word in ray_cr_config_str.split() if "rayproject/ray:" in word
        ]
        for ray_image in ray_images:
            ray_cr_config_str = ray_cr_config_str.replace(ray_image, RAY_IMAGE)
        for image in ray_images:
            ray_cr_config_str.replace(image, RAY_IMAGE)
        raycluster_cr_file = tempfile.NamedTemporaryFile(delete=False)
        raycluster_cr_file.write(ray_cr_config_str.encode())
        raycluster_cr_file.close()
        return raycluster_cr_file.name

    def _get_ray_cr_config(
        self, min_replicas=0, max_replicas=300, replicas=0
    ) -> Dict[str, Any]:
        """Get Ray CR config yaml, with configurable replica fields for the single
        workerGroup."""
        config = yaml.safe_load(open(self._get_ray_cr_config_file()).read())
        config["spec"]["workerGroupSpecs"][0]["replicas"] = replicas
        config["spec"]["workerGroupSpecs"][0]["minReplicas"] = min_replicas
        config["spec"]["workerGroupSpecs"][0]["maxReplicas"] = max_replicas

        return config

    def _apply_ray_cr(self, min_replicas=0, max_replicas=300, replicas=0) -> None:
        """Apply Ray CR config yaml, with configurable replica fields for the single
        workerGroup."""
        with tempfile.NamedTemporaryFile("w") as config_file:
            cr_config = self._get_ray_cr_config(
                min_replicas=min_replicas, max_replicas=max_replicas, replicas=replicas
            )
            yaml.dump(cr_config, config_file)
            config_file.flush()
            subprocess.check_call(["kubectl", "apply", "-f", config_file.name])

    def testAutoscaling(self):
        """Test the following behaviors:

        1. Spinning up a Ray cluster
        2. Scaling up a Ray worker via autoscaler.sdk.request_resources()
        3. Scaling up by updating the CRD's minReplicas
        4. Scaling down by removing the resource request and reducing maxReplicas

        Items 1. and 2. protect the example in the documentation.
        Items 3. and 4. protect the autoscaler's ability to respond to Ray CR update.
        """
        # Cluster-creation
        print("Creating a RayCluster with no worker pods.")
        self._apply_ray_cr(min_replicas=0, replicas=0)

        print("Confirming presence of head.")
        wait_for_pods(goal_num_pods=1, namespace="default")
        head_pod = get_pod(
            pod_name_filter="raycluster-complete-head", namespace="default"
        )

        print("Waiting for head pod to start Running.")
        wait_for_pod_to_start(head_pod, namespace="default")
        print("Confirming Ray is up on the head pod.")
        wait_for_ray_health(head_pod, namespace="default")

        # Scale-up
        print("Scaling up to one worker via Ray resource request.")
        scale_script = (
            "import ray;"
            'ray.init("auto");'
            "ray.autoscaler.sdk.request_resources(num_cpus=2)"
        )
        kubectl_exec(
            command=["python", "-c", scale_script], pod=head_pod, namespace="default"
        )
        print("Confirming number of workers.")
        wait_for_pods(goal_num_pods=2, namespace="default")

        print("Scaling up to two workers by editing minReplicas.")
        # (replicas=1 reflects the current number of workers)
        self._apply_ray_cr(min_replicas=2, replicas=1)
        print("Confirming number of workers.")
        wait_for_pods(goal_num_pods=3, namespace="default")

        # Scale-down
        print("Removing resource request.")
        scale_down_script = (
            "import ray;"
            'ray.init("auto");'
            "ray.autoscaler.sdk.request_resources(num_cpus=0)"
        )
        kubectl_exec(
            command=["python", "-c", scale_down_script],
            pod=head_pod,
            namespace="default",
        )
        print("Scaling down all workers by editing maxReplicas.")
        # (replicas=2 reflects the current number of workers)
        self._apply_ray_cr(min_replicas=0, max_replicas=0, replicas=2)
        print("Confirming workers are gone.")
        wait_for_pods(goal_num_pods=1, namespace="default")

        # Cluster deletion
        print("Deleting Ray cluster.")
        subprocess.check_call(
            ["kubectl", "delete", "raycluster", "raycluster-complete"]
        )
        print("Confirming Ray pods are gone.")
        wait_for_pods(goal_num_pods=0, namespace="default")

    def tearDown(self):
        """Clean resources following the instructions in the docs."""

        print("Deleting operator.")
        subprocess.check_call(
            [
                "kubectl",
                "delete",
                "-k",
                "ray/python/ray/autoscaler/kuberay/config/default",
            ]
        )

        print("Deleting autoscaler RBAC.")
        subprocess.check_call(
            [
                "kubectl",
                "delete",
                "-f",
                "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml",
            ]
        )

        print("Double-checking no pods left over.")
        wait_for_pods(goal_num_pods=0, namespace="default")
        wait_for_pods(goal_num_pods=0, namespace="ray-system")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
