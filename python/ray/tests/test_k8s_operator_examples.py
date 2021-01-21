"""Tests launch and teardown of multiple Ray clusters using Kubernetes
operator."""
import sys
import os
import subprocess
import tempfile
import time
import unittest

import kubernetes
import pytest
import yaml

IMAGE_ENV = "KUBERNETES_OPERATOR_TEST_IMAGE"
IMAGE = os.getenv(IMAGE_ENV, "rayproject/ray:nightly")
NAMESPACE = "test-k8s-operator-examples"


def retry_until_true(f):
    # Retry 60 times with 1 second delay between attempts.
    def f_with_retries(*args, **kwargs):
        for _ in range(60):
            if f(*args, **kwargs):
                return
            else:
                time.sleep(1)
        pytest.fail("The condition wasn't met before the timeout expired.")

    return f_with_retries


@retry_until_true
def wait_for_pods(n):
    client = kubernetes.client.CoreV1Api()
    pods = client.list_namespaced_pod(namespace=NAMESPACE).items
    # Double-check that the correct image is use.
    for pod in pods:
        assert pod.spec.containers[0].image == IMAGE
    return len(pods) == n


@retry_until_true
def wait_for_logs():
    """Check if logs indicate presence of nodes of types "head-node" and
    "worker-nodes" in the "example-cluster" cluster."""
    cmd = f"kubectl -n {NAMESPACE} logs ray-operator-pod"\
        "| grep ^example-cluster: | tail -n 100"
    log_tail = subprocess.check_output(cmd, shell=True).decode()
    return ("head-node" in log_tail) and ("worker-nodes" in log_tail)


def operator_configs_directory():
    here = os.path.realpath(__file__)
    ray_python_root = os.path.dirname(os.path.dirname(here))
    relative_path = "autoscaler/kubernetes/operator_configs"
    return os.path.join(ray_python_root, relative_path)


def get_operator_config_path(file_name):
    return os.path.join(operator_configs_directory(), file_name)


class KubernetesOperatorTest(unittest.TestCase):
    def test_examples(self):
        with tempfile.NamedTemporaryFile("w+") as example_cluster_file, \
                tempfile.NamedTemporaryFile("w+") as example_cluster2_file,\
                tempfile.NamedTemporaryFile("w+") as operator_file:

            # Get paths to operator configs
            example_cluster_config_path = get_operator_config_path(
                "example_cluster.yaml")
            example_cluster2_config_path = get_operator_config_path(
                "example_cluster2.yaml")
            operator_config_path = get_operator_config_path("operator.yaml")
            self.crd_path = get_operator_config_path("cluster_crd.yaml")

            # Load operator configs
            example_cluster_config = yaml.safe_load(
                open(example_cluster_config_path).read())
            example_cluster2_config = yaml.safe_load(
                open(example_cluster2_config_path).read())
            operator_config = list(
                yaml.safe_load_all(open(operator_config_path).read()))

            # Fill image fields
            podTypes = example_cluster_config["spec"]["podTypes"]
            podTypes2 = example_cluster2_config["spec"]["podTypes"]
            pod_configs = ([operator_config[-1]] + [
                podType["podConfig"] for podType in podTypes
            ] + [podType["podConfig"] for podType in podTypes2])
            for pod_config in pod_configs:
                pod_config["spec"]["containers"][0]["image"] = IMAGE

            # Dump to temporary files
            yaml.dump(example_cluster_config, example_cluster_file)
            yaml.dump(example_cluster2_config, example_cluster2_file)
            yaml.dump_all(operator_config, operator_file)
            files = [
                example_cluster_file, example_cluster2_file, operator_file
            ]
            for file in files:
                file.flush()

            # Apply CR
            cmd = f"kubectl apply -f {self.crd_path}"
            subprocess.check_call(cmd, shell=True)

            # Create namespace
            cmd = f"kubectl create namespace {NAMESPACE}"
            subprocess.check_call(cmd, shell=True)

            # Start operator and two clusters
            for file in files:
                cmd = f"kubectl -n {NAMESPACE} apply -f {file.name}"
                subprocess.check_call(cmd, shell=True)

            # Check that autoscaling respects minWorkers by waiting for
            # six pods in the namespace.
            wait_for_pods(6)

            # Check that logging output looks normal (two workers connected to
            # ray cluster example-cluster.)
            wait_for_logs()

            # Delete the second cluster
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster2_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Four pods remain
            wait_for_pods(4)

            # Delete the first cluster
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Only operator pod remains.
            wait_for_pods(1)

    def __del__(self):
        cmd = f"kubectl delete -f {self.crd_path}"
        subprocess.check_call(cmd, shell=True)
        cmd = f"kubectl delete namespace {NAMESPACE}"
        subprocess.check_call(cmd, shell=True)


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-v", __file__]))
