import copy
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
    get_pod_names,
    get_raycluster,
    ray_client_port_forward,
    ray_job_submit,
    kubectl_exec_python_script,
    kubectl_patch,
    kubectl_delete,
    wait_for_pods,
    wait_for_pod_to_start,
    wait_for_ray_health,
    wait_for_crd,
)

from ray.tests.kuberay.scripts import (
    gpu_actor_placement,
    gpu_actor_validation,
    non_terminated_nodes_count,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s] " "%(filename)s: %(lineno)d  " "%(message)s",
)

# This image will be used for both the Ray nodes and the autoscaler.
# The CI should pass an image built from the test branch.
RAY_IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:c6d3ff")
# By default, use the same image for the autoscaler and Ray containers.
AUTOSCALER_IMAGE = os.environ.get("AUTOSCALER_IMAGE", RAY_IMAGE)
# Set to IfNotPresent in kind CI.
PULL_POLICY = os.environ.get("PULL_POLICY", "Always")
logger.info(f"Using image `{RAY_IMAGE}` for Ray containers.")
logger.info(f"Using image `{AUTOSCALER_IMAGE}` for Autoscaler containers.")
logger.info(f"Using pull policy `{PULL_POLICY}` for all images.")
# The default "rayproject/ray:413fe0" is the currently pinned autoscaler image
# (to be replaced with rayproject/ray:1.12.0 upon 1.12.0 release).

# Parent directory of Ray repository
RAY_PARENT = str(pathlib.Path(__file__).resolve().parents[5])
# Path to example config rel RAY_PARENT
EXAMPLE_CLUSTER_PATH = "ray/python/ray/autoscaler/kuberay/ray-cluster.complete.yaml"

HEAD_SERVICE = "raycluster-complete-head-svc"
HEAD_POD_PREFIX = "raycluster-complete-head"
CPU_WORKER_PREFIX = "raycluster-complete-worker-small-group"
RAY_CLUSTER_NAME = "raycluster-complete"
RAY_CLUSTER_NAMESPACE = "default"


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

    def _get_ray_cr_config(
        self, min_replicas=0, max_replicas=300, replicas=0
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
        cpu_group = config["spec"]["workerGroupSpecs"][0]
        cpu_group["replicas"] = replicas
        cpu_group["minReplicas"] = min_replicas
        cpu_group["maxReplicas"] = max_replicas

        # Add a GPU-annotated group.
        # (We're not using real GPUs, just adding a GPU annotation for the autoscaler
        # and Ray scheduler.)
        gpu_group = copy.deepcopy(cpu_group)
        gpu_group["rayStartParams"]["num-gpus"] = "1"
        gpu_group["replicas"] = 0
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

            ray_container["image"] = RAY_IMAGE

            for container in containers:
                container["imagePullPolicy"] = PULL_POLICY

        head_containers = config["spec"]["headGroupSpec"]["template"]["spec"][
            "containers"
        ]
        autoscaler_container = [
            container
            for container in head_containers
            if container["name"] == "autoscaler"
        ].pop()
        autoscaler_container["image"] = AUTOSCALER_IMAGE

        return config

    def _apply_ray_cr(
        self,
        min_replicas=0,
        max_replicas=300,
        replicas=0,
        validate_replicas: bool = False,
    ) -> None:
        """Apply Ray CR config yaml, with configurable replica fields for the cpu
        workerGroup.

        If the CR does not yet exist, `replicas` can be set as desired.
        If the CR does already exist, the recommended usage is this:
            (1) Set `replicas` to what we currently expect it to be.
            (2) Set `validate_replicas` to True. We will then check that the replicas
            set on the CR coincides with `replicas`.
        """
        with tempfile.NamedTemporaryFile("w") as config_file:
            if validate_replicas:
                raycluster = get_raycluster(
                    RAY_CLUSTER_NAME, namespace=RAY_CLUSTER_NAMESPACE
                )
                assert raycluster["spec"]["workerGroupSpecs"][0]["replicas"] == replicas
                logger.info(
                    f"Validated that worker replicas for {RAY_CLUSTER_NAME}"
                    f" is currently {replicas}."
                )
            cr_config = self._get_ray_cr_config(
                min_replicas=min_replicas, max_replicas=max_replicas, replicas=replicas
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

        Items 1. and 2. protect the example in the documentation.
        Items 3. and 4. protect the autoscaler's ability to respond to Ray CR update.

        Tests the following modes of interaction with a Ray cluster on K8s:
        1. kubectl exec
        2. Ray Client
        3. Ray Job Submission

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
        # Cluster creation
        logger.info("Creating a RayCluster with no worker pods.")
        self._apply_ray_cr(min_replicas=0, replicas=0, max_replicas=3)

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
        logger.info("Confirming number of workers.")
        wait_for_pods(goal_num_pods=2, namespace=RAY_CLUSTER_NAMESPACE)

        # Pods marked for deletion are ignored.
        logger.info(
            "Confirming that operator and autoscaler ignore pods marked for"
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
            replicas=1,
            # Validate that replicas set on the Ray CR by the autoscaler
            # is indeed 1:
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
        logger.info("Removing resource demands.")
        kubectl_exec_python_script(
            script_name="scale_down.py",
            pod=head_pod,
            container="ray-head",
            namespace="default",
        )
        logger.info("Scaling down all workers by editing maxReplicas.")
        # TODO (Dmitri) Expose worker idleTimeout in KubeRay CRD, set it low,
        # and validate autoscaler-initiated idle timeout, instead of modifying the CR.
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
        wait_for_pods(goal_num_pods=1, namespace=RAY_CLUSTER_NAMESPACE)

        # Check custom resource upscaling.
        # First, restore max replicas to allow worker upscaling.
        self._apply_ray_cr(
            min_replicas=0,
            max_replicas=10,
            replicas=0,
            # Check that the replicas set on the Ray CR by the
            # autoscaler is indeed 2:
            validate_replicas=True,
        )

        # Submit two {"Custom2": 3} bundles to upscale two workers with 5
        # Custom2 capacity each.
        logger.info("Scaling up workers with request for custom resources.")
        job_logs = ray_job_submit(  # Interaction mode #3: Ray Job Submission
            script_name="scale_up_custom.py",
            head_service=HEAD_SERVICE,
        )
        assert job_logs == "Submitted custom scale request!\n"

        logger.info("Confirming two workers have scaled up.")
        wait_for_pods(goal_num_pods=3, namespace=RAY_CLUSTER_NAMESPACE)

        # Cluster deletion
        logger.info("Deleting Ray cluster.")
        kubectl_delete(
            kind="raycluster", name=RAY_CLUSTER_NAME, namespace=RAY_CLUSTER_NAMESPACE
        )
        logger.info("Confirming Ray pods are gone.")
        wait_for_pods(goal_num_pods=0, namespace=RAY_CLUSTER_NAMESPACE)

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
        wait_for_pods(goal_num_pods=0, namespace=RAY_CLUSTER_NAMESPACE)
        wait_for_pods(goal_num_pods=0, namespace="ray-system")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-vv", __file__]))
