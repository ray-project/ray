import argparse
import logging
import os

import ray
from ray import ray_constants
from ray.autoscaler._private.providers import _get_node_provider
from ray._private.ray_logging import setup_component_logger
from ray._private.services import get_node_ip_address
from ray.autoscaler._private.monitor import Monitor
import yaml

AUTOSCALING_CONFIG_PATH = "/autoscaler/ray_bootstrap_config.yaml"


# The name of this Ray cluster. Should coincide with the `metadata.name` field of the
# Ray CR.
RAY_CLUSTER_NAME = os.getenv("RAY_CLUSTER_NAME")
assert RAY_CLUSTER_NAME

# The Kubernetes namespace in which this Ray cluster runs.
RAY_CLUSTER_NAMESPACE = os.getenv("RAY_CLUSTER_NAMESPACE")
assert RAY_CLUSTER_NAMESPACE


def _generate_provider_config() -> dict:
    """Generates the `provider` field of the autoscaling config, which carries data required
    to instantiate the KubeRay node provider.
    """
    return {
        "type": "kuberay",
        "namespace": RAY_CLUSTER_NAMESPACE,
        "disable_node_updaters": True,
        "disable_launch_config_check": True
    }


def _generate_autoscaling_config() -> dict:
    """Generates an autoscaling config by reading the RayCluster CR and translating it into
    a format readable by the autoscaler.
    """
    provider_config = _generate_provider_config()
    assert RAY_CLUSTER_NAME, ("The RAY_CLUSTER_NAME environment variable is unset. "
                              "Check the RayCluster CR.")

    # Get the KubeRay node provider by instantiating it (on first call) or retrieving it
    # from cache (subsequent calls).
    provider = _get_node_provider(
        provider_config=provider_config,
        cluster_name=RAY_CLUSTER_NAME
    )

    # Fetch the Ray CR from K8s.
    ray_cr = provider.get("rayclusters/{}".format(RAY_CLUSTER_NAME))
    # This could just as well be done using a function defined in this file, but it
    # makes enough sense to have KubeRayNodeProvider handle all interactions with K8s.

    autoscaling_config = _generate_autoscaling_config_from_ray_cr(ray_cr)

    return autoscaling_config


def _generate_autoscaling_config_from_ray_cr(ray_cr: dict) -> dict:
    """Translates a Ray custom resource to an autoscaling config.
    """
    return ray_cr


def setup_logging() -> None:
    """Log to standard autoscaler log file
    (typically, /tmp/ray/session_latest/logs/monitor.*).

    Also log to pod stdout (logs viewable with `kubectl logs <head-pod> -c autoscaler`).
    """
    # Write logs at info level to monitor.log.
    setup_component_logger(
        logging_level=ray_constants.LOGGER_LEVEL,  # info
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir=os.path.join(
            ray._private.utils.get_ray_temp_dir(), ray.node.SESSION_LATEST, "logs"
        ),
        filename=ray_constants.MONITOR_LOG_FILE_NAME,  # monitor.log
        max_bytes=ray_constants.LOGGING_ROTATE_BYTES,
        backup_count=ray_constants.LOGGING_ROTATE_BACKUP_COUNT,
    )

    # Also log to stdout for debugging with `kubectl logs`.
    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.INFO)

    root_handler = logging.StreamHandler()
    root_handler.setLevel(logging.INFO)
    root_handler.setFormatter(logging.Formatter(ray_constants.LOGGER_FORMAT))

    root_logger.addHandler(root_handler)


if __name__ == "__main__":
    setup_logging()

    parser = argparse.ArgumentParser(description="Kuberay Autoscaler")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="The password to use for Redis",
    )
    args = parser.parse_args()

    cluster_name = yaml.safe_load(open(AUTOSCALING_CONFIG_PATH).read())["cluster_name"]
    head_ip = get_node_ip_address()
    print(_generate_autoscaling_config())
    Monitor(
        address=f"{head_ip}:6379",
        redis_password=args.redis_password,
        autoscaling_config=AUTOSCALING_CONFIG_PATH,
        monitor_ip=head_ip,
    ).run()
