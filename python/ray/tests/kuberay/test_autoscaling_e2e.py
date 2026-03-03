import base64
import copy
import logging
import os
import subprocess
import sys
import tempfile
import unittest
from typing import Any, Dict

import pytest
import yaml

from ray.tests.kuberay.utils import (
    get_pod,
    get_pod_names,
    get_raycluster,
    kubectl_delete,
    kubectl_exec_python_script,
    kubectl_logs,
    switch_to_ray_parent_dir,
    wait_for_pod_to_start,
    wait_for_pods,
    wait_for_ray_health,
)

logger = logging.getLogger(__name__)

# This image will be used for both the Ray nodes and the autoscaler.
# The CI should pass an image built from the test branch.
RAY_IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:nightly-py38")
# By default, use the same image for the autoscaler and Ray containers.
AUTOSCALER_IMAGE = os.environ.get("AUTOSCALER_IMAGE", RAY_IMAGE)
# Set to IfNotPresent in kind CI.
PULL_POLICY = os.environ.get("PULL_POLICY", "IfNotPresent")
# Set to enable autoscaler v2
AUTOSCALER_V2 = os.environ.get("AUTOSCALER_V2", "False")
logger.info(f"Using image `{RAY_IMAGE}` for Ray containers.")
logger.info(f"Using image `{AUTOSCALER_IMAGE}` for Autoscaler containers.")
logger.info(f"Using pull policy `{PULL_POLICY}` for all images.")
logger.info(f"Using autoscaler v2: {AUTOSCALER_V2}")

# Path to example config inside the rayci container.
EXAMPLE_CLUSTER_PATH = (
    "rayci/python/ray/tests/kuberay/test_files/ray-cluster.autoscaler-template.yaml"
)
EXAMPLE_CLUSTER_PATH_V2 = (
    "rayci/python/ray/tests/kuberay/test_files/ray-cluster.autoscaler-v2-template.yaml"
)

HEAD_SERVICE = "raycluster-autoscaler-head-svc"
HEAD_POD_PREFIX = "raycluster-autoscaler-head"
CPU_WORKER_PREFIX = "raycluster-autoscaler-worker-small-group"
RAY_CLUSTER_NAME = "raycluster-autoscaler"
RAY_CLUSTER_NAMESPACE = "default"

# Test runs longer than the default timeout.
pytestmark = pytest.mark.timeout(300)


class KubeRayAutoscalingTest(unittest.TestCase):
    """e2e verification of autoscaling following the steps in the Ray documentation.
    kubectl is used throughout, as that reflects the instructions in the docs.
    """

    def _get_ray_cr_config(
        self, min_replicas=0, cpu_replicas=0, gpu_replicas=0
    ) -> Dict[str, Any]:
        """Get Ray CR config yaml.

        - Use configurable replica fields for a CPU workerGroup.

        - Add a GPU-annotated group for testing GPU upscaling.

        - Fill in Ray image, autoscaler image, and image pull policies from env
          variables.
        """
        if AUTOSCALER_V2 == "True":
            with open(EXAMPLE_CLUSTER_PATH_V2) as ray_cr_config_file:
                ray_cr_config_str = ray_cr_config_file.read()
        else:
            with open(EXAMPLE_CLUSTER_PATH) as ray_cr_config_file:
                ray_cr_config_str = ray_cr_config_file.read()

        for k8s_object in yaml.safe_load_all(ray_cr_config_str):
            if k8s_object["kind"] in ["RayCluster", "RayJob", "RayService"]:
                config = k8s_object
                break
        head_group = config["spec"]["headGroupSpec"]
        if "rayStartParams" not in head_group:
            head_group["rayStartParams"] = {}
        head_group["rayStartParams"][
            "resources"
        ] = '"{\\"Custom1\\": 1, \\"Custom2\\": 5}"'

        cpu_group = config["spec"]["workerGroupSpecs"][0]
        cpu_group["replicas"] = cpu_replicas
        cpu_group["minReplicas"] = min_replicas
        # Keep maxReplicas big throughout the test.
        cpu_group["maxReplicas"] = 300
        if "rayStartParams" not in cpu_group:
            cpu_group["rayStartParams"] = {}
        cpu_group["rayStartParams"][
            "resources"
        ] = '"{\\"Custom1\\": 1, \\"Custom2\\": 5}"'

        # Add a GPU-annotated group.
        # (We're not using real GPUs, just adding a GPU annotation for the autoscaler
        # and Ray scheduler.)
        gpu_group = copy.deepcopy(cpu_group)
        if "rayStartParams" not in gpu_group:
            gpu_group["rayStartParams"] = {}
        gpu_group["rayStartParams"]["num-gpus"] = "1"
        gpu_group["replicas"] = gpu_replicas
        gpu_group["minReplicas"] = 0
        gpu_group["maxReplicas"] = 1
        gpu_group["groupName"] = "fake-gpu-group"
        config["spec"]["workerGroupSpecs"].append(gpu_group)

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

    def _apply_ray_cr(
        self,
        min_replicas=0,
        cpu_replicas=0,
        gpu_replicas=0,
        validate_replicas: bool = False,
    ) -> None:
        """Apply Ray CR config yaml, with configurable replica fields for the cpu
        workerGroup.

        If the CR does not yet exist, `replicas` can be set as desired.
        If the CR does already exist, the recommended usage is this:
            (1) Set `cpu_replicas` and `gpu_replicas` to what we currently expect them
                to be.
            (2) Set `validate_replicas` to True. We will then check that the replicas
            set on the CR coincides with `replicas`.
        """
        if validate_replicas:
            raycluster = get_raycluster(
                RAY_CLUSTER_NAME, namespace=RAY_CLUSTER_NAMESPACE
            )
            assert raycluster["spec"]["workerGroupSpecs"][0]["replicas"] == cpu_replicas
            assert raycluster["spec"]["workerGroupSpecs"][1]["replicas"] == gpu_replicas
            logger.info(
                f"Validated that cpu and gpu worker replicas for "
                f"{RAY_CLUSTER_NAME} are currently {cpu_replicas} and"
                f" {gpu_replicas}, respectively."
            )
        cr_config = self._get_ray_cr_config(
            min_replicas=min_replicas,
            cpu_replicas=cpu_replicas,
            gpu_replicas=gpu_replicas,
        )

        with tempfile.NamedTemporaryFile("w") as config_file:
            yaml.dump(cr_config, config_file)
            config_file.flush()

            subprocess.check_call(
                ["kubectl", "apply", "-f", config_file.name],
                stdout=sys.stdout,
                stderr=sys.stderr,
            )

    def testAutoscaling(self):
        """Test the following behaviors:

        1. Spinning up a Ray cluster
        2. Scaling up Ray workers via autoscaler.sdk.request_resources()
        3. Scaling up by updating the CRD's minReplicas
        4. Scaling down by removing the resource request and reducing maxReplicas
        5. Autoscaler recognizes GPU annotations and Ray custom resources.
        6. Autoscaler and operator ignore pods marked for deletion.
        7. Autoscaler logs work. Autoscaler events are piped to the driver.
        8. Ray utils show correct resource limits in the head container.

        TODO (Dmitri): Split up the test logic.
        Too much is stuffed into this one test case.

        Resources requested by this test are safely within the bounds of an m5.xlarge
        instance.

        The resource REQUESTS are:
        - One Ray head pod
            - Autoscaler: .25 CPU, .5 Gi memory
            - Ray node: .5 CPU, .5 Gi memeory
        - Three Worker pods
            - Ray node: .5 CPU, .5 Gi memory
        Total: 2.25 CPU, 2.5 Gi memory.

        Including operator and system pods, the total CPU requested is around 3.

        The cpu LIMIT of each Ray container is 1.
        The `num-cpus` arg to Ray start is 1 for each Ray container; thus Ray accounts
        1 CPU for each Ray node in the test.
        """
        switch_to_ray_parent_dir()

        # Cluster creation
        logger.info("Creating a RayCluster with no worker pods.")
        self._apply_ray_cr(min_replicas=0, cpu_replicas=0, gpu_replicas=0)

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

        # Confirm head pod resource allocation.
        # (This is a misplaced test of Ray's resource detection in containers.
        # See the TODO in the docstring.)
        logger.info("Confirming head pod resource allocation.")
        out = kubectl_exec_python_script(  # Interaction mode #1: `kubectl exec`
            script_name="check_cpu_and_memory.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )

        # Scale-up
        logger.info("Scaling up to one worker via Ray resource request.")
        # The request for 2 cpus should give us a 1-cpu head (already present) and a
        # 1-cpu worker (will await scale-up).
        kubectl_exec_python_script(  # Interaction mode #1: `kubectl exec`
            script_name="scale_up.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        # Check that stdout autoscaler logging is working.
        logs = kubectl_logs(head_pod, namespace="default", container="autoscaler")
        assert "Adding 1 node(s) of type small-group." in logs
        logger.info("Confirming number of workers.")
        wait_for_pods(goal_num_pods=2, namespace=RAY_CLUSTER_NAMESPACE)

        # Ray CR updates.
        logger.info("Scaling up to two workers by editing minReplicas.")
        # replicas=1 reflects the current number of workers
        # (which is what we expect to be already present in the Ray CR)
        self._apply_ray_cr(
            min_replicas=2,
            cpu_replicas=1,
            gpu_replicas=0,
            # Confirm CPU, GPU replicas set on the Ray CR by the autoscaler are 1, 0:
            validate_replicas=True,
        )
        logger.info("Confirming number of workers.")
        wait_for_pods(goal_num_pods=3, namespace=RAY_CLUSTER_NAMESPACE)

        # GPU upscaling.
        # 1. Check we haven't spuriously already started a fake GPU node.
        assert not any(
            "gpu" in pod_name
            for pod_name in get_pod_names(namespace=RAY_CLUSTER_NAMESPACE)
        )
        # 2. Trigger GPU upscaling by requesting placement of a GPU actor.
        logger.info("Scheduling an Actor with GPU demands.")
        kubectl_exec_python_script(
            script_name="gpu_actor_placement.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )

        # 3. Confirm new pod number and presence of fake GPU worker.
        logger.info("Confirming fake GPU worker up-scaling.")
        wait_for_pods(goal_num_pods=4, namespace=RAY_CLUSTER_NAMESPACE)

        gpu_workers = [
            pod_name
            for pod_name in get_pod_names(namespace=RAY_CLUSTER_NAMESPACE)
            if "gpu" in pod_name
        ]
        assert len(gpu_workers) == 1
        # 4. Confirm that the GPU actor is up and that Ray believes
        # the node the actor is on has a GPU.
        logger.info("Confirming GPU actor placement.")
        out = kubectl_exec_python_script(
            script_name="gpu_actor_validation.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )

        # Confirms the actor was placed on a GPU-annotated node.
        # (See gpu_actor_validation.py for details.)
        assert "on-a-gpu-node" in out

        # Scale-down
        logger.info("Reducing min workers to 0.")
        # Max workers remains 300.
        self._apply_ray_cr(
            min_replicas=0,
            cpu_replicas=2,
            gpu_replicas=1,
            # Confirm CPU, GPU replicas set on the Ray CR by the autoscaler are 2, 1:
            validate_replicas=True,
        )
        logger.info("Removing resource demands.")
        kubectl_exec_python_script(
            script_name="scale_down.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        # Autoscaler should trigger scale-down after resource demands are removed.
        logger.info("Confirming workers are gone.")
        # Check that stdout autoscaler logging is working.
        logs = kubectl_logs(head_pod, namespace="default", container="autoscaler")
        assert "Removing 1 nodes of type fake-gpu-group (idle)." in logs
        wait_for_pods(goal_num_pods=1, namespace=RAY_CLUSTER_NAMESPACE, tries=120)

        # Check custom resource upscaling.

        # Submit two {"Custom2": 3} bundles to upscale two workers with 5
        # Custom2 capacity each.
        logger.info("Scaling up workers with request for custom resources.")
        out = kubectl_exec_python_script(
            script_name="scale_up_custom.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        assert "Submitted custom scale request!" in out, out

        logger.info("Confirming two workers have scaled up.")
        wait_for_pods(goal_num_pods=3, namespace=RAY_CLUSTER_NAMESPACE)

        # Cluster deletion
        logger.info("Deleting Ray cluster.")
        kubectl_delete(
            kind="raycluster", name=RAY_CLUSTER_NAME, namespace=RAY_CLUSTER_NAMESPACE
        )
        logger.info("Confirming Ray pods are gone.")
        wait_for_pods(goal_num_pods=0, namespace=RAY_CLUSTER_NAMESPACE)


if __name__ == "__main__":
    kubeconfig_base64 = os.environ.get("KUBECONFIG_BASE64")
    if kubeconfig_base64:
        kubeconfig_file = os.environ.get("KUBECONFIG")
        if not kubeconfig_file:
            raise ValueError("When KUBECONFIG_BASE64 is set, KUBECONFIG must be set.")

        with open(kubeconfig_file, "wb") as f:
            f.write(base64.b64decode(kubeconfig_base64))

    sys.exit(pytest.main(["-vv", __file__]))
