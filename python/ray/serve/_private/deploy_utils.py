from typing import Dict, Tuple, Union, Callable, Type, Optional, Any
import logging
from ray.serve.config import ReplicaConfig, DeploymentConfig
from ray.serve._private.constants import SERVE_LOGGER_NAME

import ray
import ray.util.serialization_addons

logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_deploy_args(
    name: str,
    deployment_def: Union[Callable, Type[Callable], str],
    init_args: Tuple[Any],
    init_kwargs: Dict[Any, Any],
    ray_actor_options: Optional[Dict] = None,
    config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
    version: Optional[str] = None,
    route_prefix: Optional[str] = None,
    is_driver_deployment: Optional[str] = None,
    docs_path: Optional[str] = None,
) -> Dict:
    """
    Takes a deployment's configuration, and returns the arguments needed
    for the controller to deploy it.
    """

    if config is None:
        config = {}
    if ray_actor_options is None:
        ray_actor_options = {}

    curr_job_env = ray.get_runtime_context().runtime_env
    if "runtime_env" in ray_actor_options:
        # It is illegal to set field working_dir to None.
        if curr_job_env.get("working_dir") is not None:
            ray_actor_options["runtime_env"].setdefault(
                "working_dir", curr_job_env.get("working_dir")
            )
    else:
        ray_actor_options["runtime_env"] = curr_job_env

    replica_config = ReplicaConfig.create(
        deployment_def,
        init_args=init_args,
        init_kwargs=init_kwargs,
        ray_actor_options=ray_actor_options,
    )

    if isinstance(config, dict):
        deployment_config = DeploymentConfig.parse_obj(config)
    elif isinstance(config, DeploymentConfig):
        deployment_config = config
    else:
        raise TypeError("config must be a DeploymentConfig or a dictionary.")

    deployment_config.version = version

    if (
        deployment_config.autoscaling_config is not None
        and deployment_config.max_concurrent_queries
        < deployment_config.autoscaling_config.target_num_ongoing_requests_per_replica  # noqa: E501
    ):
        logger.warning(
            "Autoscaling will never happen, "
            "because 'max_concurrent_queries' is less than "
            "'target_num_ongoing_requests_per_replica' now."
        )

    controller_deploy_args = {
        "name": name,
        "deployment_config_proto_bytes": deployment_config.to_proto_bytes(),
        "replica_config_proto_bytes": replica_config.to_proto_bytes(),
        "route_prefix": route_prefix,
        "deployer_job_id": ray.get_runtime_context().get_job_id(),
        "is_driver_deployment": is_driver_deployment,
        "docs_path": docs_path,
    }

    return controller_deploy_args
