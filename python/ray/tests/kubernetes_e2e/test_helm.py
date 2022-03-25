import kubernetes
import os
import subprocess
import sys
import unittest

import pytest

from test_k8s_operator_basic import get_kubernetes_config_path
from test_k8s_operator_basic import IMAGE
from test_k8s_operator_basic import NAMESPACE
from test_k8s_operator_basic import wait_for_pods


def helm(namespace, command, release, **options):
    options.update(
        {"image": IMAGE, "operatorImage": IMAGE, "operatorNamespace": f"{NAMESPACE}2"}
    )
    cmd = ["helm"]
    if namespace:
        cmd.append(f"-n {namespace}")
    cmd.append(command)
    cmd.append(release)
    if command in ["install", "upgrade"]:
        for key, value in options.items():
            cmd.append(f"--set {key}={value}")
        cmd.append("./ray")
    elif command == "uninstall":
        pass  # Don't need options or chart path to delete.
    else:
        raise ValueError("Unrecognized helm command.")
    final_cmd = " ".join(cmd)
    try:
        subprocess.check_output(
            final_cmd, shell=True, stderr=subprocess.STDOUT
        ).decode()
    except subprocess.CalledProcessError as e:
        assert False, "returncode: {}, stdout: {}".format(e.returncode, e.stdout)


def delete_rayclusters(namespace):
    cmd = f"kubectl -n {namespace} delete rayclusters --all"
    try:
        subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
        assert False, "returncode: {}, stdout: {}".format(e.returncode, e.stdout)


class HelmTest(unittest.TestCase):
    def test_helm(self):
        charts_dir = get_kubernetes_config_path("charts")
        os.chdir(charts_dir)

        # Basic install: operator and default cluster.
        print("\n>>>Testing default Helm install.")
        print(">>>Installing...")
        helm(NAMESPACE, "install", "example-cluster")
        wait_for_pods(3, namespace=NAMESPACE)
        wait_for_pods(1, namespace=f"{NAMESPACE}2")
        print(">>>Uninstalling...")
        delete_rayclusters(namespace=NAMESPACE)
        wait_for_pods(0, namespace=NAMESPACE)
        helm(NAMESPACE, "uninstall", "example-cluster")
        wait_for_pods(0, namespace=f"{NAMESPACE}2")

        # Install operator and two clusters in separate releases.
        print(">>>Testing installation of multiple Ray clusters.")
        print(">>>Installing...")
        helm(NAMESPACE, "install", "ray-operator", operatorOnly="true")
        wait_for_pods(1, namespace=f"{NAMESPACE}2")
        helm(NAMESPACE, "install", "example-cluster", clusterOnly="true")
        wait_for_pods(3, namespace=f"{NAMESPACE}")
        helm(
            NAMESPACE,
            "install",
            "example-cluster2",
            **{"clusterOnly": "true", "podTypes.rayWorkerType.minWorkers": "0"},
        )
        wait_for_pods(4, namespace=f"{NAMESPACE}")
        print(">>>Uninstalling...")
        helm(NAMESPACE, "uninstall", "example-cluster")
        helm(NAMESPACE, "uninstall", "example-cluster2")
        wait_for_pods(0, namespace=NAMESPACE)
        helm(NAMESPACE, "uninstall", "ray-operator")
        wait_for_pods(0, namespace=f"{NAMESPACE}2")

        # namespacedOperator
        print(">>>Testing installation of namespaced Ray operator.")
        print(">>>Installing...")
        helm(NAMESPACE, "install", "example-cluster", namespacedOperator="true")
        wait_for_pods(4, namespace=f"{NAMESPACE}")
        print(">>>Uninstalling...")
        delete_rayclusters(namespace=NAMESPACE)
        wait_for_pods(1, namespace=NAMESPACE)
        helm(NAMESPACE, "uninstall", "example-cluster")
        wait_for_pods(0, namespace=NAMESPACE)


if __name__ == "__main__":
    kubernetes.config.load_kube_config()
    sys.exit(pytest.main(["-sv", __file__]))
