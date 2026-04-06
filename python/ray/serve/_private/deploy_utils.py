import hashlib
import inspect
import json
import logging
import time
from typing import Any, Dict, Optional, Union

import ray
import ray.util.serialization_addons
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
    RAY_SERVE_ENABLE_HA_PROXY,
    RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.schema import ServeApplicationSchema

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _deployment_uses_multiplexed(deployment_def) -> bool:
    """Check if a deployment class or function uses @serve.multiplexed.

    Multiplexing is not supported on ingress deployments. The multiplexed
    decorator sets _serve_multiplexed=True on wrapped functions.
    """
    if not callable(deployment_def):
        return False
    if getattr(deployment_def, "_serve_multiplexed", False):
        return True
    if inspect.isclass(deployment_def):
        for name in dir(deployment_def):
            if name.startswith("__") and name.endswith("__"):
                continue
            try:
                attr = getattr(deployment_def, name)
                if getattr(attr, "_serve_multiplexed", False):
                    return True
            except (AttributeError, TypeError):
                continue
    return False


def get_deploy_args(
    name: str,
    replica_config: ReplicaConfig,
    ingress: bool = False,
    deployment_config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
    version: Optional[str] = None,
    route_prefix: Optional[str] = None,
    serialized_autoscaling_policy_def: Optional[bytes] = None,
    serialized_request_router_cls: Optional[bytes] = None,
    serialized_deployment_actors: Optional[Dict[str, bytes]] = None,
) -> Dict:
    """
    Takes a deployment's configuration, and returns the arguments needed
    for the controller to deploy it.
    """
    if deployment_config is None:
        deployment_config = {}

    if isinstance(deployment_config, dict):
        deployment_config = DeploymentConfig.model_validate(deployment_config)
    elif not isinstance(deployment_config, DeploymentConfig):
        raise TypeError("config must be a DeploymentConfig or a dictionary.")

    deployment_def = replica_config.deployment_def

    if ingress and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        if _deployment_uses_multiplexed(deployment_def):
            # HAProxy mode turns on direct ingress; prefer the HAProxy-specific error
            # below so callers/tests see RAY_SERVE_ENABLE_HA_PROXY when applicable.
            if not RAY_SERVE_ENABLE_HA_PROXY:
                raise ValueError(
                    "Model multiplexing (@serve.multiplexed) is not supported on "
                    "ingress deployments when direct ingress is enabled. "
                    "Multiplexing should only be used on downstream replicas composed "
                    "via DeploymentHandle. The ingress is the routing entry point, "
                    "not a model-serving leaf."
                )

    if _deployment_uses_multiplexed(deployment_def):
        if RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING:
            raise ValueError(
                "Model multiplexing (@serve.multiplexed) is disallowed because "
                "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING=1 is set. "
                "Remove multiplexed model loaders from your deployments or unset "
                "this variable."
            )
        if RAY_SERVE_ENABLE_HA_PROXY:
            raise ValueError(
                "Model multiplexing (@serve.multiplexed) is not supported when "
                "RAY_SERVE_ENABLE_HA_PROXY=1. Disable HAProxy or remove "
                "@serve.multiplexed from your deployments."
            )
        logger.warning(
            "This deployment uses @serve.multiplexed. Model multiplexing will be "
            "disallowed in a future Ray Serve release. If you rely on this feature, "
            "please reach out to the Ray team on GitHub or the Ray Slack. To fail "
            "deployments that use multiplexing immediately, set "
            "RAY_SERVE_STRICT_DISALLOW_MODEL_MULTIPLEXING=1."
        )

    deployment_config.version = version

    controller_deploy_args = {
        "deployment_name": name,
        "deployment_config_proto_bytes": deployment_config.to_proto_bytes(),
        "replica_config_proto_bytes": replica_config.to_proto_bytes(),
        "route_prefix": route_prefix,
        "deployer_job_id": ray.get_runtime_context().get_job_id(),
        "ingress": ingress,
        "serialized_autoscaling_policy_def": serialized_autoscaling_policy_def,
        "serialized_request_router_cls": serialized_request_router_cls,
        "serialized_deployment_actors": serialized_deployment_actors,
    }

    return controller_deploy_args


def deploy_args_to_deployment_info(
    deployment_name: str,
    deployment_config_proto_bytes: bytes,
    replica_config_proto_bytes: bytes,
    deployer_job_id: Union[str, bytes],
    app_name: Optional[str] = None,
    ingress: bool = False,
    route_prefix: Optional[str] = None,
    **kwargs,
) -> DeploymentInfo:
    """Takes deployment args passed to the controller after building an application and
    constructs a DeploymentInfo object.
    """

    deployment_config = DeploymentConfig.from_proto_bytes(deployment_config_proto_bytes)
    version = deployment_config.version
    replica_config = ReplicaConfig.from_proto_bytes(
        replica_config_proto_bytes, deployment_config.needs_pickle()
    )

    # Java API passes in JobID as bytes
    if isinstance(deployer_job_id, bytes):
        deployer_job_id = ray.JobID.from_int(
            int.from_bytes(deployer_job_id, "little")
        ).hex()

    return DeploymentInfo(
        actor_name=DeploymentID(
            name=deployment_name, app_name=app_name
        ).to_replica_actor_class_name(),
        version=version,
        deployment_config=deployment_config,
        replica_config=replica_config,
        deployer_job_id=deployer_job_id,
        start_time_ms=int(time.time() * 1000),
        route_prefix=route_prefix,
        ingress=ingress,
    )


def get_app_code_version(app_config: ServeApplicationSchema) -> str:
    """Returns the code version of an application.

    Args:
        app_config: The application config.

    Returns:
        str: A hash of the import path and (application level) runtime env
            representing the code version of the application.
    """
    request_router_configs = [
        deployment.request_router_config
        for deployment in app_config.deployments
        if isinstance(deployment.request_router_config, dict)
    ]
    deployment_autoscaling_policies = [
        deployment_config.autoscaling_config.get("policy", None)
        for deployment_config in app_config.deployments
        if isinstance(deployment_config.autoscaling_config, dict)
    ]
    deployment_actors_configs = [
        deployment.deployment_actors
        for deployment in app_config.deployments
        if isinstance(deployment.deployment_actors, list)
    ]

    encoded = json.dumps(
        {
            "import_path": app_config.import_path,
            "runtime_env": app_config.runtime_env,
            "args": app_config.args,
            # NOTE: trigger a change in the code version when
            # application level autoscaling policy is changed or
            # any one of the deployment level autoscaling policy is changed
            "autoscaling_policy": app_config.autoscaling_policy,
            "deployment_autoscaling_policies": deployment_autoscaling_policies,
            "request_router_configs": request_router_configs,
            "deployment_actors": deployment_actors_configs,
        },
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
