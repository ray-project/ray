"""Utilities for e2e tests of KubeRay/Ray integration.
For consistency, all K8s interactions use kubectl through subprocess calls.
"""
import logging
import subprocess
import time
from typing import List

logger = logging.getLogger(__name__)


def wait_for_pods(goal_num_pods: int, namespace: str, tries=60, backoff_s=5) -> None:
    """Wait for the number of pods in the `namespace` to be exactly `num_pods`.

    Raise an exception after exceeding `tries` attempts with `backoff_s` second waits.
    """
    for i in range(tries):

        cur_num_pods = _get_num_pods(namespace)
        if cur_num_pods == goal_num_pods:
            logger.info(f"Confirmed {goal_num_pods} pods.")
            return
        elif i < tries - 1:
            logger.info(
                f"Have {cur_num_pods} pods in namespace {namespace}."
                f" Waiting to have {goal_num_pods} pods."
            )
            time.sleep(backoff_s)
            continue
        else:
            raise Exception(
                f"Failed to scale to {goal_num_pods} pod(s) in namespace {namespace}."
            )


def _get_num_pods(namespace: str) -> int:
    get_pod_output = (
        subprocess.check_output(
            ["kubectl", "-n", namespace, "get", "pods", "--no-headers"]
        )
        .decode()
        .strip()
    )

    # If there aren't any pods, the output is any empty string.
    if not get_pod_output:
        return 0
    else:
        return len(get_pod_output.split("\n"))


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


def wait_for_ray_health(ray_pod: str, namespace: str, tries=60, backoff_s=5) -> None:
    """Waits for the Ray pod to pass `ray health-check`.
    (Ensures Ray has completely started in the pod.)
    """
    for i in range(tries):
        try:
            kubectl_exec(["ray", "health-check"], ray_pod, namespace)
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


def kubectl_exec(command: List[str], pod: str, namespace: str) -> None:
    """kubectl exec the `command` in the given `pod` in the given `namespace`"""
    subprocess.check_call(["kubectl", "exec", "-it", pod, "--"] + command)
