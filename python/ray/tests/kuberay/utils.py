"""Utilities for e2e tests of KubeRay/Ray integration.
For consistency, all K8s interactions use kubectl through subprocess calls.
"""
import logging
from pathlib import Path
import subprocess
import time
from typing import Any, Dict, List, Optional
import yaml

logger = logging.getLogger(__name__)


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
    get_pod_output = (
        subprocess.check_output(
            ["kubectl", "-n", namespace, "get", "pods", "--no-headers"]
        )
        .decode()
        .strip()
    )

    # If there aren't any pods, the output is any empty string.
    if not get_pod_output:
        return []
    else:
        return get_pod_output.split("\n")
    pass


def wait_for_pod_to_start(pod: str, namespace: str, tries=60, backoff_s=5) -> None:
    """Waits for the pod to enter running Running status.phase."""
    for i in range(tries):
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
    ray_pod: str, namespace: str, tries=60, backoff_s=5, ray_container="ray-head"
) -> None:
    """Waits for the Ray pod to pass `ray health-check`.
    (Ensures Ray has completely started in the pod.)
    """
    for i in range(tries):
        try:
            # `ray health-check` yields 0 exit status iff it succeeds
            kubectl_exec(
                ["ray", "health-check"], ray_pod, namespace, container=ray_container
            )
            logger.info(f"ray health check passes for pod {ray_pod}")
            return
        except subprocess.CalledProcessError as e:
            logger.info(f"Failed ray health check for pod {ray_pod}.")
            if i < tries - 1:
                logger.info("Trying again.")
                time.sleep(backoff_s)
            else:
                logger.info("Giving up.")
                raise e from None


def get_pod(pod_name_filter: str, namespace: str) -> str:
    """Gets pods in the `namespace`.

    Returns the first pod that has `pod_name_filter` as a
    substring of its name. Raises an assertion error if there are no matches.
    """
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
        .split()
    )
    matches = [item for item in get_pods_output if pod_name_filter in item]
    assert matches, f"No match for `{pod_name_filter}` in namespace `{namespace}`."
    return matches[0]


def kubectl_exec(
    command: List[str],
    pod: str,
    namespace: str,
    container: Optional[str] = None,
    return_out: bool = False,
) -> Optional[str]:
    """kubectl exec the `command` in the given `pod` in the given `namespace`.
    If a `container` is specified, will specify that container for kubectl.

    Args:
        return_out: If True, stdout will be captured, printed, and returned as a string.
            Otherwise, stdout will not be redirected and None will be returned.
    """
    container_option = ["-c", container] if container else []
    kubectl_exec_command = (
        ["kubectl", "exec", "-it", pod] + container_option + ["--"] + command
    )
    if return_out:
        out = subprocess.check_output(kubectl_exec_command).decode().strip()
        # Print for debugging convenience.
        print(out)
        return out
    else:
        subprocess.check_call(kubectl_exec_command)
        return None


def kubectl_exec_python_script(
    script_name: str,
    pod: str,
    namespace: str,
    container: Optional[str] = None,
    return_out: bool = False,
) -> Optional[str]:
    """
    Runs a python script in a container via `kubectl exec`.
    Scripts live in `tests/kuberay/scripts`.

    Args:
        script_name: The name of a script in tests/kuberay/scripts.
        return_out: If True, stdout will be redirected to the function's output.
            Otherwise, stdout will not be redirected and `None` will be returned.
    """
    script_path = Path(__file__).resolve().parent / "scripts" / script_name
    script_string = open(script_path).read()
    return kubectl_exec(
        ["python", "-c", script_string], pod, namespace, container, return_out
    )


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
