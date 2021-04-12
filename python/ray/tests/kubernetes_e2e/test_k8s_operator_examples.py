"""Tests launch, teardown, and update of multiple Ray clusters using Kubernetes
operator. Also tests submission of jobs via Ray client."""
import copy
import sys
import os
import subprocess
import tempfile
import time
import unittest

import kubernetes
import pytest
import yaml

from ray.autoscaler._private._kubernetes.node_provider import\
    KubernetesNodeProvider

IMAGE_ENV = "KUBERNETES_OPERATOR_TEST_IMAGE"
IMAGE = os.getenv(IMAGE_ENV, "rayproject/ray:nightly")

NAMESPACE_ENV = "KUBERNETES_OPERATOR_TEST_NAMESPACE"
NAMESPACE = os.getenv(NAMESPACE_ENV, "test-k8s-operator")

PULL_POLICY_ENV = "KUBERNETES_OPERATOR_TEST_PULL_POLICY"
PULL_POLICY = os.getenv(PULL_POLICY_ENV, "Always")

RAY_PATH = os.path.abspath(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))))))


def retry_until_true(f):
    # Retry 60 times with 1 second delay between attempts.
    def f_with_retries(*args, **kwargs):
        for _ in range(240):
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
    return ("head-node" in log_tail) and ("worker-node" in log_tail)


@retry_until_true
def wait_for_job(job_pod):
    print(">>>Checking job logs.")
    cmd = f"kubectl -n {NAMESPACE} logs {job_pod}"
    try:
        out = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        print(">>>Failed to check job logs.")
        print(e.output.decode())
        return False
    success = "success" in out.lower()
    if success:
        print(">>>Job submission succeeded.")
    else:
        print(">>>Job logs do not indicate job sucess:")
        print(out)
    return success


def kubernetes_configs_directory():
    relative_path = "python/ray/autoscaler/kubernetes"
    return os.path.join(RAY_PATH, relative_path)


def get_kubernetes_config_path(name):
    return os.path.join(kubernetes_configs_directory(), name)


def get_operator_config_path(file_name):
    operator_configs = get_kubernetes_config_path("operator_configs")
    return os.path.join(operator_configs, file_name)


class KubernetesOperatorTest(unittest.TestCase):
    def test_examples(self):

        # Validate terminate_node error handling
        provider = KubernetesNodeProvider({
            "namespace": NAMESPACE
        }, "default_cluster_name")
        # 404 caught, no error
        provider.terminate_node("no-such-node")

        with tempfile.NamedTemporaryFile("w+") as example_cluster_file, \
                tempfile.NamedTemporaryFile("w+") as example_cluster2_file,\
                tempfile.NamedTemporaryFile("w+") as operator_file,\
                tempfile.NamedTemporaryFile("w+") as job_file:

            # Get paths to operator configs
            example_cluster_config_path = get_operator_config_path(
                "example_cluster.yaml")
            example_cluster2_config_path = get_operator_config_path(
                "example_cluster2.yaml")
            operator_config_path = get_operator_config_path("operator.yaml")
            job_path = os.path.join(RAY_PATH,
                                    "doc/kubernetes/job-example.yaml")

            # Load operator configs
            example_cluster_config = yaml.safe_load(
                open(example_cluster_config_path).read())
            example_cluster2_config = yaml.safe_load(
                open(example_cluster2_config_path).read())
            operator_config = list(
                yaml.safe_load_all(open(operator_config_path).read()))
            job_config = yaml.safe_load(open(job_path).read())

            # Fill image fields
            podTypes = example_cluster_config["spec"]["podTypes"]
            podTypes2 = example_cluster2_config["spec"]["podTypes"]
            pod_specs = ([operator_config[-1]["spec"]] + [
                job_config["spec"]["template"]["spec"]
            ] + [podType["podConfig"]["spec"] for podType in podTypes
                 ] + [podType["podConfig"]["spec"] for podType in podTypes2])
            for pod_spec in pod_specs:
                pod_spec["containers"][0]["image"] = IMAGE
                pod_spec["containers"][0]["imagePullPolicy"] = PULL_POLICY

            # Dump to temporary files
            yaml.dump(example_cluster_config, example_cluster_file)
            yaml.dump(example_cluster2_config, example_cluster2_file)
            yaml.dump(job_config, job_file)
            yaml.dump_all(operator_config, operator_file)
            files = [
                example_cluster_file, example_cluster2_file, operator_file
            ]
            for file in files:
                file.flush()

            # Start operator and two clusters
            print(">>>Starting operator and two clusters.")
            for file in files:
                cmd = f"kubectl -n {NAMESPACE} apply -f {file.name}"
                subprocess.check_call(cmd, shell=True)

            # Check that autoscaling respects minWorkers by waiting for
            # six pods in the namespace.
            print(">>>Waiting for pods to join clusters.")
            wait_for_pods(6)

            # Check that logging output looks normal (two workers connected to
            # ray cluster example-cluster.)
            print(">>>Checking monitor logs for head and workers.")
            wait_for_logs()

            # Delete the second cluster
            print(">>>Deleting example-cluster2.")
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster2_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Four pods remain
            print(">>>Checking that example-cluster2 pods are gone.")
            wait_for_pods(4)

            # Check job submission
            print(">>>Submitting a job to test Ray client connection.")
            cmd = f"kubectl -n {NAMESPACE} create -f {job_file.name}"
            subprocess.check_call(cmd, shell=True)

            cmd = f"kubectl -n {NAMESPACE} get pods --no-headers -o"\
                " custom-columns=\":metadata.name\""
            pods = subprocess.check_output(cmd, shell=True).decode().split()
            job_pod = [pod for pod in pods if "job" in pod].pop()
            time.sleep(10)
            wait_for_job(job_pod)
            cmd = f"kubectl -n {NAMESPACE} delete jobs --all"
            subprocess.check_call(cmd, shell=True)

            # Check that cluster updates work: increase minWorkers to 3
            # and check that one worker is created.
            print(">>>Updating cluster size.")
            example_cluster_edit = copy.deepcopy(example_cluster_config)
            example_cluster_edit["spec"]["podTypes"][1]["minWorkers"] = 3
            yaml.dump(example_cluster_edit, example_cluster_file)
            example_cluster_file.flush()
            cm = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cm, shell=True)
            print(">>>Checking that new cluster size is respected.")
            wait_for_pods(5)

            # Delete the first cluster
            print(">>>Deleting second cluster.")
            cmd = f"kubectl -n {NAMESPACE} delete -f"\
                f"{example_cluster_file.name}"
            subprocess.check_call(cmd, shell=True)

            # Only operator pod remains.
            print(">>>Checking that all Ray cluster pods are gone.")
            wait_for_pods(1)


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-sv", __file__]))
