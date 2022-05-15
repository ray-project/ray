"""Utilities for e2e tests of KubeRay/Ray integration.
For consistency, all K8s interactions use kubectl through subprocess calls.
"""
import atexit
import contextlib
import logging
import pathlib
import subprocess
import tempfile
import time
from typing import Any, Dict, Generator, List, Optional
import yaml

import ray
from ray.job_submission import JobStatus, JobSubmissionClient


logger = logging.getLogger(__name__)

SCRIPTS_DIR = pathlib.Path(__file__).resolve().parent / "scripts"


def wait_for_crd(crd_name: str, tries=60, backoff_s=5):
    """CRD creation can take a bit of time after the client request.
    This function waits until the crd with the provided name is registered.
    """
    for i in range(tries):
        get_crd_output = subprocess.check_output(["kubectl", "get", "crd"]).decode()
        if crd_name in get_crd_output:
            logger.info(f"Confirmed existence of CRD {crd_name}.")
            return
        elif i < tries - 1:
            logger.info(f"Still waiting to register CRD {crd_name}")
            time.sleep(backoff_s)
        else:
            raise Exception(f"Failed to register CRD {crd_name}")


def wait_for_pods(goal_num_pods: int, namespace: str, tries=60, backoff_s=5) -> None:
    """Wait for the number of pods in the `namespace` to be exactly `num_pods`.

    Raise an exception after exceeding `tries` attempts with `backoff_s` second waits.
    """
    for i in range(tries):

        cur_num_pods = _get_num_pods(namespace)
        if cur_num_pods == goal_num_pods:
            logger.info(f"Confirmed {goal_num_pods} pod(s) in namespace {namespace}.")
            return
        elif i < tries - 1:
            logger.info(
                f"The number of pods in namespace {namespace} is {cur_num_pods}."
                f" Waiting until the number of pods is {goal_num_pods}."
            )
            time.sleep(backoff_s)
        else:
            raise Exception(
                f"Failed to scale to {goal_num_pods} pod(s) in namespace {namespace}."
            )


def _get_num_pods(namespace: str) -> int:
    return len(get_pod_names(namespace))


def get_pod_names(namespace: str) -> List[str]:
    """Get the list of pod names in the namespace."""
    get_pods_output = (
        subprocess.check_output(
            [
                "kubectl",
                "-n",
                namespace,
                "get",
                "pods",
                "-o",
                "custom-columns=POD:metadata.name",
                "--no-headers",
            ]
        )
        .decode()
        .strip()
    )

    # If there aren't any pods, the output is any empty string.
    if not get_pods_output:
        return []
    else:
        return get_pods_output.split("\n")


def wait_for_pod_to_start(
    pod_name_filter: str, namespace: str, tries=60, backoff_s=5
) -> None:
    """Waits for a pod to have Running status.phase.

    More precisely, waits until there is a pod with name containing `pod_name_filter`
    and the pod has Running status.phase."""
    for i in range(tries):
        pod = get_pod(pod_name_filter=pod_name_filter, namespace=namespace)
        if not pod:
            # We didn't get a matching pod.
            continue
        pod_status = (
            subprocess.check_output(
                [
                    "kubectl",
                    "-n",
                    namespace,
                    "get",
                    "pod",
                    pod,
                    "-o",
                    "custom-columns=POD:status.phase",
                    "--no-headers",
                ]
            )
            .decode()
            .strip()
        )
        # "not found" is part of the kubectl output if the pod's not there.
        if "not found" in pod_status:
            raise Exception(f"Pod {pod} not found.")
        elif pod_status == "Running":
            logger.info(f"Confirmed pod {pod} is Running.")
            return
        elif i < tries - 1:
            logger.info(
                f"Pod {pod} has status {pod_status}. Waiting for the pod to enter "
                "Running status."
            )
            time.sleep(backoff_s)
        else:
            raise Exception(f"Timed out waiting for pod {pod} to enter Running status.")


def wait_for_ray_health(
    pod_name_filter: str,
    namespace: str,
    tries=60,
    backoff_s=5,
    ray_container="ray-head",
) -> None:
    """Waits until a Ray pod passes `ray health-check`.

    More precisely, waits until a Ray pod whose name includes the string
    `pod_name_filter` passes `ray health-check`.
    (Ensures Ray has completely started in the pod.)

    Use case: Wait until there is a Ray head pod with Ray running on it.
    """
    for i in range(tries):
        try:
            pod = get_pod(pod_name_filter=pod_name_filter, namespace="default")
            assert pod, f"Couldn't find a pod matching {pod_name_filter}."
            # `ray health-check` yields 0 exit status iff it succeeds
            kubectl_exec(
                ["ray", "health-check"], pod, namespace, container=ray_container
            )
            logger.info(f"ray health check passes for pod {pod}")
            return
        except subprocess.CalledProcessError as e:
            logger.info(f"Failed ray health check for pod {pod}.")
            if i < tries - 1:
                logger.info("Trying again.")
                time.sleep(backoff_s)
            else:
                logger.info("Giving up.")
                raise e from None


def get_pod(pod_name_filter: str, namespace: str) -> Optional[str]:
    """Gets pods in the `namespace`.

    Returns the first pod that has `pod_name_filter` as a
    substring of its name. Returns None if there are no matches.
    """
    pod_names = get_pod_names(namespace)
    matches = [pod_name for pod_name in pod_names if pod_name_filter in pod_name]
    if not matches:
        logger.warning(f"No match for `{pod_name_filter}` in namespace `{namespace}`.")
        return None
    return matches[0]


def kubectl_exec(
    command: List[str],
    pod: str,
    namespace: str,
    container: Optional[str] = None,
) -> str:
    """kubectl exec the `command` in the given `pod` in the given `namespace`.
    If a `container` is specified, will specify that container for kubectl.

    Prints and return kubectl's output as a string.
    """
    container_option = ["-c", container] if container else []
    kubectl_exec_command = (
        ["kubectl", "exec", "-it", pod] + container_option + ["--"] + command
    )
    out = subprocess.check_output(kubectl_exec_command).decode().strip()
    # Print for debugging convenience.
    print(out)
    return out


def kubectl_exec_python_script(
    script_name: str,
    pod: str,
    namespace: str,
    container: Optional[str] = None,
) -> str:
    """
    Runs a python script in a container via `kubectl exec`.
    Scripts live in `tests/kuberay/scripts`.

    Prints and return kubectl's output as a string.
    """
    script_path = SCRIPTS_DIR / script_name
    with open(script_path) as script_file:
        script_string = script_file.read()
    return kubectl_exec(["python", "-c", script_string], pod, namespace, container)


def get_raycluster(raycluster: str, namespace: str) -> Dict[str, Any]:
    """Gets the Ray CR with name `raycluster` in namespace `namespace`.

    Returns the CR as a nested Dict.
    """
    get_raycluster_output = (
        subprocess.check_output(
            ["kubectl", "-n", namespace, "get", "raycluster", raycluster, "-o", "yaml"]
        )
        .decode()
        .strip()
    )
    return yaml.safe_load(get_raycluster_output)


def _get_service_port(service: str, namespace: str, target_port: int) -> int:
    """Given a K8s service and a port targetted by the service, returns the
    corresponding port exposed by the service.

    Args:
        service: Name of a K8s service.
        namespace: Namespace to which the service belongs.
        target_port: Port targeted by the service.

    Returns:
        service_port: The port exposed by the service.
    """
    service_str = (
        subprocess.check_output(
            ["kubectl", "-n", namespace, "get", "service", service, "-o", "yaml"]
        )
        .decode()
        .strip()
    )
    service_dict = yaml.safe_load(service_str)
    service_ports: List = service_dict["spec"]["ports"]
    matching_ports = [
        port for port in service_ports if port["targetPort"] == target_port
    ]
    assert matching_ports
    service_port = matching_ports[0]["port"]
    return service_port


@contextlib.contextmanager
def _kubectl_port_forward(
    service: str, namespace: str, target_port: int, local_port: Optional[int] = None
) -> Generator[int, None, None]:
    """Context manager which creates a kubectl port-forward process targeting a
    K8s service.

    Terminates the port-forwarding process upon exit.

    Args:
        service: Name of a K8s service.
        namespace: Namespace to which the service belongs.
        target_port: The port targeted by the service.
        local_port: Forward from this port. Optional. By default, uses the port exposed
        by the service.

    Yields:
        The local port. The service can then be accessed at 127.0.0.1:<local_port>.
    """
    # First, figure out which port the service exposes for the given target port.
    service_port = _get_service_port(service, namespace, target_port)
    if not local_port:
        local_port = service_port

    process = subprocess.Popen(
        [
            "kubectl",
            "-n",
            namespace,
            "port-forward",
            f"service/{service}",
            f"{local_port}:{service_port}",
        ]
    )

    def terminate_process():
        process.terminate()
        # Wait 10 seconds for the process to terminate.
        # This cleans up the zombie entry from the process table.
        # 10 seconds is a deliberately excessive amount of time to wait.
        process.wait(timeout=10)

    # Ensure clean-up in case of interrupt.
    atexit.register(terminate_process)
    # terminate_process is ok to execute multiple times.

    try:
        yield local_port
    finally:
        terminate_process()


@contextlib.contextmanager
def ray_client_port_forward(
    head_service: str,
    k8s_namespace: str = "default",
    ray_namespace: Optional[str] = None,
    ray_client_port: int = 10001,
):
    """Context manager which manages a Ray client connection using kubectl port-forward.

    Args:
        head_service: The name of the Ray head K8s service.
        k8s_namespace: K8s namespace the Ray cluster belongs to.
        ray_namespace: The Ray namespace to connect to.
        ray_client_port: The port on which the Ray head is running the Ray client
            server.
    """
    with _kubectl_port_forward(
        service=head_service, namespace=k8s_namespace, target_port=ray_client_port
    ) as local_port:
        with ray.init(f"ray://127.0.0.1:{local_port}", namespace=ray_namespace):
            yield


def ray_job_submit(
    script_name: str,
    head_service: str,
    k8s_namespace: str = "default",
    ray_dashboard_port: int = 8265,
) -> str:
    """Submits a Python script via the Ray Job Submission API, using the Python SDK.
    Waits for successful completion of the job and returns the job logs as a string.

    Uses `kubectl port-forward` to access the Ray head's dashboard port.

    Scripts live in `tests/kuberay/scripts`. This directory is used as the working
    dir for the job.

    Args:
        script_name: The name of the script to submit.
        head_service: The name of the Ray head K8s service.
        k8s_namespace: K8s namespace the Ray cluster belongs to.
        ray_dashboard_port: The port on which the Ray head is running the Ray dashboard.
    """
    with _kubectl_port_forward(
        service=head_service, namespace=k8s_namespace, target_port=ray_dashboard_port
    ) as local_port:
        # It takes a bit of time to establish the connection.
        # Try a few times to instantiate the JobSubmissionClient, as the client's
        # instantiation does not retry on connection errors.
        for trie in range(1, 7):
            time.sleep(5)
            try:
                client = JobSubmissionClient(f"http://127.0.0.1:{local_port}")
            except ConnectionError as e:
                if trie < 6:
                    logger.info("Job client connection failed. Retrying in 5 seconds.")
                else:
                    raise e from None
        job_id = client.submit_job(
            entrypoint=f"python {script_name}",
            runtime_env={
                "working_dir": SCRIPTS_DIR,
                # Throw in some extra data for fun, to validate runtime envs.
                "pip": ["pytest==6.0.0"],
                "env_vars": {"key_foo": "value_bar"},
            },
        )
        # Wait for the job to complete successfully.
        # This logic is copied from the Job Submission docs.
        start = time.time()
        timeout = 60
        while time.time() - start <= timeout:
            status = client.get_job_status(job_id)
            print(f"status: {status}")
            if status in {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}:
                break
            time.sleep(5)

        assert status == JobStatus.SUCCEEDED
        return client.get_job_logs(job_id)


def kubectl_patch(
    kind: str,
    name: str,
    namespace: str,
    patch: Dict[str, Any],
    patch_type: str = "strategic",
):
    """Wrapper for kubectl patch.

    Args:
        kind: Kind of the K8s resource (e.g. pod)
        name: Name of the K8s resource.
        namespace: Namespace of the K8s resource.
        patch: The patch to apply, as a dict.
        patch_type: json, merge, or strategic
    """
    with tempfile.NamedTemporaryFile("w") as patch_file:
        yaml.dump(patch, patch_file)
        patch_file.flush()
        subprocess.check_call(
            [
                "kubectl",
                "-n",
                f"{namespace}",
                "patch",
                f"{kind}",
                f"{name}",
                "--patch-file",
                f"{patch_file.name}",
                "--type",
                f"{patch_type}",
            ]
        )


def kubectl_delete(kind: str, name: str, namespace: str, wait: bool = True):
    """Wrapper for kubectl delete.

    Args:
        kind: Kind of the K8s resource (e.g. pod)
        name: Name of the K8s resource.
        namespace: Namespace of the K8s resource.
    """
    wait_str = "true" if wait else "false"
    subprocess.check_output(
        [
            "kubectl",
            "-n",
            f"{namespace}",
            "delete",
            f"{kind}",
            f"{name}",
            f"--wait={wait_str}",
        ]
    )
