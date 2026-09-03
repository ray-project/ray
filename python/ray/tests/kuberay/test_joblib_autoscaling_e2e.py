import base64
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import uuid

import pytest
import yaml

TEST_DIR = pathlib.Path(__file__).resolve().parent
CLUSTER_TEMPLATE = TEST_DIR / "test_files" / "ray-cluster.autoscaler-v2-template.yaml"
WORKLOAD_SCRIPT = TEST_DIR / "scripts" / "joblib_pool_autoscaling.py"
CLUSTER_NAME = "joblib-autoscale"
IMAGE = os.environ.get("RAY_IMAGE", "rayproject/ray:nightly-py310")
PULL_POLICY = os.environ.get("PULL_POLICY", "IfNotPresent")
AUTOSCALER_V2 = os.environ.get("AUTOSCALER_V2", "False")

pytestmark = [
    pytest.mark.timeout(600),
    pytest.mark.skipif(
        AUTOSCALER_V2 != "True",
        reason="the Joblib autoscaling E2E test requires Autoscaler v2",
    ),
]


def kubectl(*args):
    return subprocess.run(
        ["kubectl", *args],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ).stdout


def load_cluster_config(namespace):
    with CLUSTER_TEMPLATE.open() as config_file:
        config = yaml.safe_load(config_file)

    config["metadata"].update({"name": CLUSTER_NAME, "namespace": namespace})
    config["spec"]["enableInTreeAutoscaling"] = True
    config["spec"]["autoscalerOptions"].update(
        {
            "version": "v2",
            "upscalingMode": "Aggressive",
            "idleTimeoutSeconds": 10,
            "image": IMAGE,
            "imagePullPolicy": PULL_POLICY,
        }
    )

    head_group = config["spec"]["headGroupSpec"]
    head_group.setdefault("rayStartParams", {}).update(
        {
            "num-cpus": "0",
            "dashboard-host": "0.0.0.0",
            "disable-usage-stats": "true",
        }
    )
    worker_group = config["spec"]["workerGroupSpecs"][0]
    worker_group.update(
        {
            "groupName": "cpu-worker",
            "replicas": 0,
            "minReplicas": 0,
            "maxReplicas": 2,
        }
    )
    worker_group.setdefault("rayStartParams", {}).update(
        {"num-cpus": "1", "disable-usage-stats": "true"}
    )

    groups = [config["spec"]["headGroupSpec"], *config["spec"]["workerGroupSpecs"]]
    for group in groups:
        for container in group["template"]["spec"]["containers"]:
            container["image"] = IMAGE
            container["imagePullPolicy"] = PULL_POLICY
    return config


def worker_state(namespace):
    cluster = json.loads(
        kubectl("get", "raycluster", CLUSTER_NAME, "-n", namespace, "-o", "json")
    )
    replicas = cluster["spec"]["workerGroupSpecs"][0]["replicas"]
    pods = json.loads(
        kubectl(
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            "ray.io/node-type=worker",
            "-o",
            "json",
        )
    )["items"]
    ready = sum(
        any(
            condition["type"] == "Ready" and condition["status"] == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
        for pod in pods
    )
    return replicas, len(pods), ready


def read_workload_output(output_file):
    output_file.flush()
    output_file.seek(0)
    return output_file.read()


def wait_for_worker_state(
    namespace, expected, timeout_s, workload=None, output_file=None
):
    deadline = time.monotonic() + timeout_s
    last_state = None
    while time.monotonic() < deadline:
        last_state = worker_state(namespace)
        if last_state == expected:
            return
        if workload is not None and workload.poll() is not None:
            workload.wait()
            output = read_workload_output(output_file)
            raise AssertionError(
                f"Workload exited before worker state {expected}; "
                f"last state was {last_state}:\n{output}"
            )
        time.sleep(2)
    raise TimeoutError(
        f"Worker state did not converge to {expected}; last state was {last_state}"
    )


def head_pod(namespace):
    pods = json.loads(
        kubectl(
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            "ray.io/node-type=head",
            "-o",
            "json",
        )
    )["items"]
    return pods[0]["metadata"]["name"] if pods else ""


def wait_for_head_pod(namespace, timeout_s):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        pod = head_pod(namespace)
        if pod:
            return pod
        time.sleep(2)
    raise TimeoutError(f"KubeRay did not create a head Pod in {namespace}")


def run_workload(namespace, mode):
    script = WORKLOAD_SCRIPT.read_text()
    command = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        head_pod(namespace),
        "-c",
        "ray-head",
        "--",
        "python",
        "-c",
        script,
        mode,
        "--tasks",
        "4",
        "--delay-s",
        "15",
    ]
    # A file-backed stream cannot deadlock if kubectl emits more output than an
    # unread subprocess pipe can buffer while this process observes worker Pods.
    with tempfile.TemporaryFile(mode="w+") as output_file:
        workload = subprocess.Popen(
            command,
            stdout=output_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            wait_for_worker_state(
                namespace,
                expected=(2, 2, 2),
                timeout_s=150,
                workload=workload,
                output_file=output_file,
            )
            workload.wait(timeout=240)
        except BaseException:
            if workload.poll() is None:
                workload.terminate()
                try:
                    workload.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    workload.kill()
                    workload.wait(timeout=30)
            raise
        output = read_workload_output(output_file)

    assert workload.returncode == 0, output
    result_lines = [
        line.removeprefix("JOBLIB_AUTOSCALING_E2E=")
        for line in output.splitlines()
        if line.startswith("JOBLIB_AUTOSCALING_E2E=")
    ]
    assert len(result_lines) == 1, output
    result = json.loads(result_lines[0])
    assert result["mode"] == mode
    assert result["task_count"] == 4
    assert result["worker_node_count"] == 2
    assert sorted(output["item"] for output in result["outputs"]) == list(range(4))

    wait_for_worker_state(namespace, expected=(0, 0, 0), timeout_s=30)
    return output


def test_joblib_pool_drives_kuberay_autoscaling():
    namespace = f"joblib-autoscale-e2e-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    namespace_created = False
    try:
        kubectl("create", "namespace", namespace)
        namespace_created = True
        config = load_cluster_config(namespace)
        with tempfile.NamedTemporaryFile("w") as config_file:
            yaml.safe_dump(config, config_file)
            config_file.flush()
            kubectl("apply", "-f", config_file.name)

        pod = wait_for_head_pod(namespace, timeout_s=60)
        kubectl(
            "wait",
            "--for=condition=Ready",
            f"pod/{pod}",
            "-n",
            namespace,
            "--timeout=180s",
        )
        wait_for_worker_state(namespace, expected=(0, 0, 0), timeout_s=30)

        pool_output = run_workload(namespace, "pool")
        joblib_output = run_workload(namespace, "joblib")

        assert "JOBLIB_AUTOSCALING_E2E=" in pool_output
        assert "JOBLIB_AUTOSCALING_E2E=" in joblib_output
    finally:
        if namespace_created:
            kubectl(
                "delete",
                "namespace",
                namespace,
                "--wait=true",
                "--timeout=180s",
            )


if __name__ == "__main__":
    kubeconfig_base64 = os.environ.get("KUBECONFIG_BASE64")
    if kubeconfig_base64:
        kubeconfig_file = os.environ.get("KUBECONFIG")
        if not kubeconfig_file:
            raise ValueError("When KUBECONFIG_BASE64 is set, KUBECONFIG must be set.")

        with open(kubeconfig_file, "wb") as file:
            file.write(base64.b64decode(kubeconfig_base64))

    sys.exit(pytest.main(["-vv", __file__]))
