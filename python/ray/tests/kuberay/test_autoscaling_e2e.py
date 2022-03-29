import logging
import os
import pathlib
import tempfile
import unittest
import subprocess

from typing import Any, Dict

import yaml

from ray.tests.kuberay.utils import (
    get_pod,
    get_raycluster,
    kubectl_exec,
    wait_for_pods,
    wait_for_pod_to_start,
    wait_for_ray_health,
    wait_for_crd,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s",
)

# This image will be used for both the Ray nodes and the autoscaler.
# The CI should pass an image built from the test branch.
RAY_IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:413fe0")
logger.info(f"Using image {RAY_IMAGE} for autoscaler and Ray nodes.")
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
        """Set up KubeRay operator and Ray autoscaler RBAC."""

        # Switch to parent of Ray repo, because that's what the doc examples do.
        logger.info("Switching to parent of Ray directory.")
        os.chdir(RAY_PARENT)

        logger.info("Cloning KubeRay and setting up KubeRay configuration.")
        subprocess.check_call(
            [
                "bash",
                "-c",
                "ls kuberay || ./ray/python/ray/autoscaler/kuberay/init-config.sh",
            ]
        )
        logger.info("Creating KubeRay operator.")
        subprocess.check_call(
            [
                "kubectl",
                "apply",
                "-k",
                "ray/python/ray/autoscaler/kuberay/config/default",
            ]
        )
        logger.info("Creating autoscaler RBAC objects.")
        subprocess.check_call(
            [
                "kubectl",
                "apply",
                "-f",
                "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml",
            ]
        )
        logger.info("Making sure RayCluster CRD has been registered.")
        wait_for_crd("rayclusters.ray.io")

    def _get_ray_cr_config_file(self) -> str:
        """Formats a RayCluster CR based on the example in the Ray documentation.

        - Replaces Ray node and autoscaler images in example CR with the test image.
        - Set image pull policies to IfNotPresent.
        - Writes modified CR to temp file.
        - Returns temp file's name.
        """
        # Set Ray and autoscaler images.
        ray_cr_config_str = open(EXAMPLE_CLUSTER_PATH).read()
        ray_images = [
            word for word in ray_cr_config_str.split() if "rayproject/ray:" in word
        ]
        for ray_image in ray_images:
            ray_cr_config_str = ray_cr_config_str.replace(ray_image, RAY_IMAGE)

        # Set pull policies to IfNotPresent to ensure no issues using a local test
        # image on kind.
        ray_cr_config_str = ray_cr_config_str.replace("Always", "IfNotPresent")

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

    def _apply_ray_cr(
        self,
        min_replicas=0,
        max_replicas=300,
        replicas=0,
        validate_replicas: bool = False,
    ) -> None:
        """Apply Ray CR config yaml, with configurable replica fields for the single
        workerGroup.

        If the CR does not yet exist, `replicas` can be set as desired.
        If the CR does already exist, the recommended usage is this:
            (1) Set `replicas` to what we currently expect it to be.
            (2) Set `validate_replicas` to True. We will then check that the replicas
            set on the CR coincides with `replicas`.
        """
        with tempfile.NamedTemporaryFile("w") as config_file:
            if validate_replicas:
                raycluster = get_raycluster("raycluster-complete", namespace="default")
                assert raycluster["spec"]["workerGroupSpecs"][0]["replicas"] == replicas
                logger.info(
                    f"Validated that worker replicas for raycluster-complete"
                    f" is currently {replicas}."
                )
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

        Resources requested by this test are safely within the bounds of an m5.xlarge
        instance.

        The resource REQUESTS are:
        - One Ray head pod
            - Autoscaler: .25 CPU, .5 Gi memory
            - Ray node: .5 CPU, .5 Gi memeory
        - Two Worker pods
            - Ray node: .5 CPU, .5 Gi memory
        Total: 1.75 CPU, 2 Gi memory.

        Including operator and system pods, the total CPU requested is around 3.

        The cpu LIMIT of each Ray container is 1.
        The `num-cpus` arg to Ray start is 1 for each Ray container; thus Ray accounts
        1 CPU for each Ray node in the test.
        """
        # Cluster-creation
        logger.info("Creating a RayCluster with no worker pods.")
        self._apply_ray_cr(min_replicas=0, replicas=0)

        logger.info("Confirming presence of head.")
        wait_for_pods(goal_num_pods=1, namespace="default")
        head_pod = get_pod(
            pod_name_filter="raycluster-complete-head", namespace="default"
        )

        logger.info("Waiting for head pod to start Running.")
        wait_for_pod_to_start(head_pod, namespace="default")
        logger.info("Confirming Ray is up on the head pod.")
        wait_for_ray_health(head_pod, namespace="default")

        # Scale-up
        logger.info("Scaling up to one worker via Ray resource request.")
        scale_script = (
            "import ray;"
            'ray.init("auto");'
            "ray.autoscaler.sdk.request_resources(num_cpus=2)"
        )
        # The request for 2 cpus should give us a 1-cpu head (already present) and a
        # 1-cpu worker (will await scale-up).
        kubectl_exec(
            command=["python", "-c", scale_script],
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        logger.info("Confirming number of workers.")
        wait_for_pods(goal_num_pods=2, namespace="default")

        logger.info("Scaling up to two workers by editing minReplicas.")
        # replicas=1 reflects the current number of workers
        # (which is what we expect to be already present in the Ray CR)
        self._apply_ray_cr(
            min_replicas=2,
            replicas=1,
            # Validate that replicas set on the Ray CR by the autoscaler
            # is indeed 1:
            validate_replicas=True,
        )
        logger.info("Confirming number of workers.")
        wait_for_pods(goal_num_pods=3, namespace="default")

        # Scale-down
        logger.info("Removing resource request.")
        scale_down_script = (
            "import ray;"
            'ray.init("auto");'
            "ray.autoscaler.sdk.request_resources(num_cpus=0)"
        )
        kubectl_exec(
            command=["python", "-c", scale_down_script],
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        logger.info("Scaling down all workers by editing maxReplicas.")
        # (replicas=2 reflects the current number of workers)
        self._apply_ray_cr(
            min_replicas=0,
            max_replicas=0,
            replicas=2,
            # Check that the replicas set on the Ray CR by the
            # autoscaler is indeed 2:
            validate_replicas=True,
        )
        logger.info("Confirming workers are gone.")
        wait_for_pods(goal_num_pods=1, namespace="default")

        # Cluster deletion
        logger.info("Deleting Ray cluster.")
        subprocess.check_call(
            ["kubectl", "delete", "raycluster", "raycluster-complete"]
        )
        logger.info("Confirming Ray pods are gone.")
        wait_for_pods(goal_num_pods=0, namespace="default")

    def tearDown(self):
        """Clean resources following the instructions in the docs."""

        logger.info("Deleting operator.")
        subprocess.check_call(
            [
                "kubectl",
                "delete",
                "-k",
                "ray/python/ray/autoscaler/kuberay/config/default",
            ]
        )

        logger.info("Deleting autoscaler RBAC.")
        subprocess.check_call(
            [
                "kubectl",
                "delete",
                "-f",
                "ray/python/ray/autoscaler/kuberay/kuberay-autoscaler-rbac.yaml",
            ]
        )

        logger.info("Double-checking no pods left over.")
        wait_for_pods(goal_num_pods=0, namespace="default")
        wait_for_pods(goal_num_pods=0, namespace="ray-system")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
