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
    setup_logging,
    switch_to_ray_parent_dir,
    kubectl_exec_python_script,
    kubectl_logs,
    kubectl_patch,
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
PULL_POLICY = os.environ.get("PULL_POLICY", "Always")
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
        with open(EXAMPLE_CLUSTER_PATH) as ray_cr_config_file:
            ray_cr_config_str = ray_cr_config_file.read()
        config = yaml.safe_load(ray_cr_config_str)
        head_group = config["spec"]["headGroupSpec"]
        head_group["rayStartParams"][
            "resources"
        ] = '"{\\"Custom1\\": 1, \\"Custom2\\": 5}"'

        cpu_group = config["spec"]["workerGroupSpecs"][0]
        cpu_group["replicas"] = cpu_replicas
        cpu_group["minReplicas"] = min_replicas
        # Keep maxReplicas big throughout the test.
        cpu_group["maxReplicas"] = 300
        cpu_group["rayStartParams"][
            "resources"
        ] = '"{\\"Custom1\\": 1, \\"Custom2\\": 5}"'

        # Add a GPU-annotated group.
        # (We're not using real GPUs, just adding a GPU annotation for the autoscaler
        # and Ray scheduler.)
        gpu_group = copy.deepcopy(cpu_group)
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
            assert ray_container["name"] in ["ray-head", "machine-learning"]
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
        with tempfile.NamedTemporaryFile("w") as config_file:
            if validate_replicas:
                raycluster = get_raycluster(
                    RAY_CLUSTER_NAME, namespace=RAY_CLUSTER_NAMESPACE
                )
                assert (
                    raycluster["spec"]["workerGroupSpecs"][0]["replicas"]
                    == cpu_replicas
                )
                assert (
                    raycluster["spec"]["workerGroupSpecs"][1]["replicas"]
                    == gpu_replicas
                )
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
            yaml.dump(cr_config, config_file)
            config_file.flush()
            subprocess.check_call(["kubectl", "apply", "-f", config_file.name])

    def _non_terminated_nodes_count(self) -> int:
        with ray_client_port_forward(head_service=HEAD_SERVICE):
            return non_terminated_nodes_count.main()

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

        Tests the following modes of interaction with a Ray cluster on K8s:
        1. kubectl exec
        2. Ray Client
        3. Ray Job Submission

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

        # Pods marked for deletion are ignored.
        logger.info(
            "Confirming that the operator and autoscaler ignore pods marked for "
            "termination."
        )
        worker_pod = get_pod(
            pod_name_filter=CPU_WORKER_PREFIX, namespace=RAY_CLUSTER_NAMESPACE
        )
        logger.info("Patching finalizer onto worker pod to block termination.")
        add_finalizer = {"metadata": {"finalizers": ["ray.io/test"]}}
        kubectl_patch(
            kind="pod",
            name=worker_pod,
            namespace=RAY_CLUSTER_NAMESPACE,
            patch=add_finalizer,
        )
        logger.info("Marking worker for deletion.")
        kubectl_delete(
            kind="pod", name=worker_pod, namespace=RAY_CLUSTER_NAMESPACE, wait=False
        )
        # Deletion of the worker hangs forever because of the finalizer.
        # We expect another pod to come up to replace it.
        logger.info(
            "Confirming another worker is up to replace the one marked for deletion."
        )
        wait_for_pods(goal_num_pods=3, namespace=RAY_CLUSTER_NAMESPACE)
        logger.info("Confirming NodeProvider ignores terminating nodes.")
        # 3 pods, 2 of which are not marked for deletion.
        assert self._non_terminated_nodes_count() == 2
        remove_finalizer = {"metadata": {"finalizers": []}}
        logger.info("Removing finalizer to allow deletion.")
        kubectl_patch(
            kind="pod",
            name=worker_pod,
            namespace="default",
            patch=remove_finalizer,
            patch_type="merge",
        )
        logger.info("Confirming worker deletion.")
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
        # Use Ray Client to validate that it works against KubeRay.
        with ray_client_port_forward(  # Interaction mode #2: Ray Client
            head_service=HEAD_SERVICE, ray_namespace="gpu-test"
        ):
            gpu_actor_placement.main()
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
        with ray_client_port_forward(
            head_service=HEAD_SERVICE, ray_namespace="gpu-test"
        ):
            out = gpu_actor_validation.main()
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
        wait_for_pods(goal_num_pods=1, namespace=RAY_CLUSTER_NAMESPACE)

        # Check custom resource upscaling.

        # Submit two {"Custom2": 3} bundles to upscale two workers with 5
        # Custom2 capacity each.
        logger.info("Scaling up workers with request for custom resources.")
        job_logs = ray_job_submit(  # Interaction mode #3: Ray Job Submission
            script_name="scale_up_custom.py",
            head_service=HEAD_SERVICE,
        )
        assert "Submitted custom scale request!" in job_logs, job_logs

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
    import pytest
    import sys

    setup_logging()
    sys.exit(pytest.main(["-vv", __file__]))
