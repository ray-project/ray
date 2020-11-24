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

import yaml

from ray._private.services import address
from ray.autoscaler._private.commands import create_or_update_cluster
from ray.monitor import Monitor
from ray.operator.util import (
    CLUSTER_CONFIG_PATH,
    get_ray_head_pod_ip,
    prepare_ray_cluster_config,
)
from ray.ray_constants import (DEFAULT_PORT, LOGGER_FORMAT,
                               REDIS_DEFAULT_PASSWORD)
from ray.ray_logging import setup_logger


def main():
    prepare_ray_cluster_config()

    config = create_or_update_cluster(
        CLUSTER_CONFIG_PATH,
        override_min_workers=None,
        override_max_workers=None,
        no_restart=False,
        restart_only=False,
        yes=True,
        no_config_cache=True)
    with open(CLUSTER_CONFIG_PATH, "w") as file:
        yaml.dump(config, file)

    ray_head_pod_ip = get_ray_head_pod_ip(config)
    # TODO: Add support for user-specified redis port and password
    redis_address = address(ray_head_pod_ip, DEFAULT_PORT)
    monitor = Monitor(redis_address, CLUSTER_CONFIG_PATH,
                      REDIS_DEFAULT_PASSWORD)
    monitor.run()
    # stderr_file, stdout_file = get_logs()
    """start_monitor(
        redis_address,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        autoscaling_config=CLUSTER_CONFIG_PATH,
        redis_password=ray_constants.REDIS_DEFAULT_PASSWORD)"""


if __name__ == "__main__":
    setup_logger("DEBUG", LOGGER_FORMAT)
    main()
