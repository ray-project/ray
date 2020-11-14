"""
Ray operator for Kubernetes.

Reads ray cluster config from a k8s ConfigMap, starts a ray head node pod using
create_or_update_cluster(), then runs an autoscaling loop in the operator pod
executing this script. Writes autoscaling logs to the directory
/root/ray-operator-logs.

In this setup, the ray head node does not run an autoscaler. It is important
NOT to supply an --autoscaling-config argument to head node's ray start command
in the cluster config when using this operator.

To run, first create a ConfigMap named ray-operator-configmap from a ray
cluster config. Then apply the manifest at python/ray/autoscaler/kubernetes/operator_configs/operator_config.yaml

For example:
kubectl create namespace raytest
kubectl -n raytest create configmap ray-operator-configmap --from-file=python/ray/autoscaler/kubernetes/operator_configs/test_cluster_config.yaml
kubectl -n raytest apply -f python/ray/autoscaler/kubernetes/operator_configs/operator_config.yaml
""" # noqa
import os
from typing import Any, Dict, IO, Tuple

import kubernetes
import yaml

from ray._private import services
from ray.autoscaler._private.commands import create_or_update_cluster
from ray.autoscaler._private.kubernetes import core_api
from ray.utils import open_log
from ray import ray_constants

RAY_CLUSTER_NAMESPACE = os.environ.get("RAY_OPERATOR_POD_NAMESPACE")
RAY_CONFIG_MAP = "ray-operator-configmap"
RAY_CONFIG_DIR = "/root"

LOG_DIR = "/root/ray-operator-logs"
ERR_NAME, OUT_NAME = "ray-operator.err", "ray-operator.out"


def prepare_ray_cluster_config() -> str:
    config_map = core_api().read_namespaced_config_map(
        name=RAY_CONFIG_MAP, namespace=RAY_CLUSTER_NAMESPACE)

    # config_map.data consists of a single key:value pair
    for config_file_name, config_string in config_map.data.items():
        config = yaml.safe_load(config_string)
        config["provider"]["namespace"] = RAY_CLUSTER_NAMESPACE
        cluster_config_path = os.path.join(RAY_CONFIG_DIR, config_file_name)
        with open(cluster_config_path, "w") as file:
            yaml.dump(config, file)

    return cluster_config_path


def get_ray_head_pod_ip(config: Dict[str, Any]) -> str:
    cluster_name = config["cluster_name"]
    label_selector = f"component=ray-head,ray-cluster-name={cluster_name}"
    pods = core_api().list_namespaced_pod(
        namespace=RAY_CLUSTER_NAMESPACE, label_selector=label_selector).items
    assert (len(pods)) == 1
    head_pod = pods.pop()
    return head_pod.status.pod_ip


def get_logs() -> Tuple[IO, IO]:
    try:
        os.makedirs(LOG_DIR)
    except OSError:
        pass

    err_path = os.path.join(LOG_DIR, ERR_NAME)
    out_path = os.path.join(LOG_DIR, OUT_NAME)

    return open_log(err_path), open_log(out_path)


def main():
    kubernetes.config.load_incluster_config()
    cluster_config_path = prepare_ray_cluster_config()

    config = create_or_update_cluster(
        cluster_config_path,
        override_min_workers=None,
        override_max_workers=None,
        no_restart=False,
        restart_only=False,
        yes=True,
        no_config_cache=True)
    with open(cluster_config_path, "w") as file:
        yaml.dump(config, file)

    ray_head_pod_ip = get_ray_head_pod_ip(config)
    # TODO: Add support for user-specified redis port and password
    redis_address = services.address(ray_head_pod_ip,
                                     ray_constants.DEFAULT_PORT)
    stderr_file, stdout_file = get_logs()

    services.start_monitor(
        redis_address,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        autoscaling_config=cluster_config_path,
        redis_password=ray_constants.REDIS_DEFAULT_PASSWORD)


if __name__ == "__main__":
    main()
