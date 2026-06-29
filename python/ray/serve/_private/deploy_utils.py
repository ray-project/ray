import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional, Union

import ray
import ray.util.serialization_addons
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import (
    RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S,
    RAY_SERVE_DIRECT_INGRESS_SHUTDOWN_BUFFER_S,
    RAY_SERVE_ENABLE_DIRECT_INGRESS,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.exceptions import RayServeException
from ray.serve.schema import ServeApplicationSchema

logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_deploy_args(
    name: str,
    replica_config: ReplicaConfig,
    ingress: bool = False,
    ingress_request_router: bool = False,
    deployment_config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
    version: Optional[str] = None,
    route_prefix: Optional[str] = None,
    serialized_autoscaling_policy_def: Optional[bytes] = None,
    serialized_request_router_cls: Optional[bytes] = None,
    serialized_deployment_actors: Optional[Dict[str, bytes]] = None,
    uses_multiplexing: bool = False,
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

    deployment_config.version = version

    controller_deploy_args = {
        "deployment_name": name,
        "deployment_config_proto_bytes": deployment_config.to_proto_bytes(),
        "replica_config_proto_bytes": replica_config.to_proto_bytes(),
        "route_prefix": route_prefix,
        "deployer_job_id": ray.get_runtime_context().get_job_id(),
        "ingress": ingress,
        "ingress_request_router": ingress_request_router,
        "serialized_autoscaling_policy_def": serialized_autoscaling_policy_def,
        "serialized_request_router_cls": serialized_request_router_cls,
        "serialized_deployment_actors": serialized_deployment_actors,
        "uses_multiplexing": uses_multiplexing,
    }

    return controller_deploy_args


def deploy_args_to_deployment_info(
    deployment_name: str,
    deployment_config_proto_bytes: bytes,
    replica_config_proto_bytes: bytes,
    deployer_job_id: Union[str, bytes],
    app_name: Optional[str] = None,
    ingress: bool = False,
    ingress_request_router: bool = False,
    route_prefix: Optional[str] = None,
    uses_multiplexing: bool = False,
    **kwargs,
) -> DeploymentInfo:
    """Takes deployment args passed to the controller after building an application and
    constructs a DeploymentInfo object.
    """

    # Model multiplexing relies on the multiplexed model ID being propagated through
    # the proxy, which direct ingress bypasses (the model ID is never populated).
    # This runs in the controller, so RAY_SERVE_ENABLE_DIRECT_INGRESS is the cluster's
    # authoritative view. Only the *statically* detectable case is caught here;
    # dynamically-initialized multiplexing is caught at replica initialization.
    if ingress and uses_multiplexing and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        raise RayServeException(
            f'Ingress deployment "{deployment_name}" in application "{app_name}" uses '
            "model multiplexing (`@serve.multiplexed`), which is not supported on the "
            "ingress deployment when direct ingress or HAProxy is enabled."
        )

    deployment_config = DeploymentConfig.from_proto_bytes(deployment_config_proto_bytes)

    # Floor the timeout so the controller's force-kill can't cut the
    # direct-ingress drain (min draining period) short.
    if ingress and RAY_SERVE_ENABLE_DIRECT_INGRESS:
        floor_s = (
            RAY_SERVE_DIRECT_INGRESS_MIN_DRAINING_PERIOD_S
            + RAY_SERVE_DIRECT_INGRESS_SHUTDOWN_BUFFER_S
        )
        if deployment_config.graceful_shutdown_timeout_s < floor_s:
            logger.info(
                f"Raising graceful_shutdown_timeout_s for ingress deployment "
                f"'{deployment_name}' from "
                f"{deployment_config.graceful_shutdown_timeout_s}s to {floor_s}s so "
                f"the force-kill deadline covers the direct-ingress drain period."
            )
            deployment_config.graceful_shutdown_timeout_s = floor_s

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
        ingress_request_router=ingress_request_router,
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
