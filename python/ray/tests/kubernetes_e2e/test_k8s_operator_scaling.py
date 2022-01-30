"""
Tests scaling behavior of Kubernetes operator.


(1) Start a cluster with minWorkers = 30 and verify scale-up
(2) Edit minWorkers to 0, verify scale-down
(3) Submit a task requiring 14 workers using Ray client
(4) Verify scale-up, task execution, and scale down.
"""
import copy
import kubernetes
import subprocess
import sys
import tempfile
import time
import unittest

import pytest
import yaml

import ray
from test_k8s_operator_basic import client_connect_to_k8s
from test_k8s_operator_basic import get_crd_path
from test_k8s_operator_basic import get_component_config_path
from test_k8s_operator_basic import retry_until_true
from test_k8s_operator_basic import wait_for_pods
from test_k8s_operator_basic import IMAGE
from test_k8s_operator_basic import PULL_POLICY
from test_k8s_operator_basic import NAMESPACE


def submit_scaling_job(num_actors):
    @ray.remote(num_cpus=1)
    class A:
        def __init__(self, index):
            self.index = index

        def report_index(self):
            return self.index

    print(">>>Scheduling actors with Ray client.")
    actors = [A.remote(i) for i in range(num_actors)]
    futures = [actor.report_index.remote() for actor in actors]

    print(">>>Verifying scale-up.")
    # Expect as many pods as actors.
    # (each Ray pod has 1 CPU)
    wait_for_pods(num_actors)

    print(">>>Waiting for task output.")
    task_output = ray.get(futures, timeout=360)

    assert task_output == list(range(num_actors)), (
        "Tasks did not" "complete with expected output."
    )


@retry_until_true
def wait_for_operator():
    cmd = "kubectl get pods"
    out = subprocess.check_output(cmd, shell=True).decode()
    for line in out.splitlines():
        if "ray-operator" in line and "Running" in line:
            return True
    return False


class KubernetesScaleTest(unittest.TestCase):
    def test_scaling(self):
        with tempfile.NamedTemporaryFile(
            "w+"
        ) as example_cluster_file, tempfile.NamedTemporaryFile(
            "w+"
        ) as example_cluster_file2, tempfile.NamedTemporaryFile(
            "w+"
        ) as operator_file:

            example_cluster_config_path = get_component_config_path(
                "example_cluster.yaml"
            )
            operator_config_path = get_component_config_path(
                "operator_cluster_scoped.yaml"
            )

            operator_config = list(
                yaml.safe_load_all(open(operator_config_path).read())
            )
            example_cluster_config = yaml.safe_load(
                open(example_cluster_config_path).read()
            )

            # Set image and pull policy
            podTypes = example_cluster_config["spec"]["podTypes"]
            pod_specs = [operator_config[-1]["spec"]["template"]["spec"]] + [
                podType["podConfig"]["spec"] for podType in podTypes
            ]
            for pod_spec in pod_specs:
                pod_spec["containers"][0]["image"] = IMAGE
                pod_spec["containers"][0]["imagePullPolicy"] = PULL_POLICY

            # Config set-up for this test.
            example_cluster_config["spec"]["maxWorkers"] = 100
            example_cluster_config["spec"]["idleTimeoutMinutes"] = 1
            worker_type = podTypes[1]
            # Make sure we have the right type
            assert "worker" in worker_type["name"]
            worker_type["maxWorkers"] = 100
            # Key for the first part of this test:
            worker_type["minWorkers"] = 30

            # Config for a small cluster with the same name to be launched
            # in another namespace.
            example_cluster_config2 = copy.deepcopy(example_cluster_config)
            example_cluster_config2["spec"]["podTypes"][1]["minWorkers"] = 1

            # Test overriding default client port.
            example_cluster_config["spec"]["headServicePorts"] = [
                {"name": "client", "port": 10002, "targetPort": 10001}
            ]

            yaml.dump(example_cluster_config, example_cluster_file)
            yaml.dump(example_cluster_config2, example_cluster_file2)
            yaml.dump_all(operator_config, operator_file)

            files = [example_cluster_file, operator_file]
            for file in files:
                file.flush()

            # Must create CRD before operator.
            print("\n>>>Creating RayCluster CRD.")
            cmd = f"kubectl apply -f {get_crd_path()}"
            subprocess.check_call(cmd, shell=True)
            # Takes a bit of time for CRD to register.
            time.sleep(10)

            print(">>>Creating operator.")
            cmd = f"kubectl apply -f {operator_file.name}"
            subprocess.check_call(cmd, shell=True)

            print(">>>Waiting for Ray operator to enter running state.")
            wait_for_operator()

            # Start a 30-pod cluster.
            print(">>>Starting a cluster.")
            cd = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cd, shell=True)

            print(">>>Starting a cluster with same name in another namespace")
            # Assumes a namespace called {NAMESPACE}2 has been created.
            cd = f"kubectl -n {NAMESPACE}2 apply -f " f"{example_cluster_file2.name}"
            subprocess.check_call(cd, shell=True)

            # Check that autoscaling respects minWorkers by waiting for
            # 32 pods in one namespace and 2 pods in the other.
            print(">>>Waiting for pods to join cluster.")
            wait_for_pods(31)
            wait_for_pods(2, namespace=f"{NAMESPACE}2")

            # Check scale-down.
            print(">>>Decreasing min workers to 0.")
            example_cluster_edit = copy.deepcopy(example_cluster_config)
            # Set minWorkers to 0:
            example_cluster_edit["spec"]["podTypes"][1]["minWorkers"] = 0
            yaml.dump(example_cluster_edit, example_cluster_file)
            example_cluster_file.flush()
            cm = f"kubectl -n {NAMESPACE} apply -f {example_cluster_file.name}"
            subprocess.check_call(cm, shell=True)
            print(">>>Sleeping for a minute while workers time-out.")
            time.sleep(60)
            print(">>>Verifying scale-down.")
            wait_for_pods(1)

            with client_connect_to_k8s(port="10002"):
                # Test scale up and scale down after task submission.
                submit_scaling_job(num_actors=15)

            print(">>>Sleeping for a minute while workers time-out.")
            time.sleep(60)
            print(">>>Verifying scale-down.")
            wait_for_pods(1)


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-sv", __file__]))
