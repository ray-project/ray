import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional, Union

import ray
import ray.util.serialization_addons
from ray.serve._private.common import DeploymentID
from ray.serve._private.config import DeploymentConfig, ReplicaConfig
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.deployment_info import DeploymentInfo
from ray.serve.schema import ServeApplicationSchema

logger = logging.getLogger(SERVE_LOGGER_NAME)


def get_deploy_args(
    name: str,
    replica_config: ReplicaConfig,
    ingress: bool = False,
    deployment_config: Optional[Union[DeploymentConfig, Dict[str, Any]]] = None,
    version: Optional[str] = None,
    route_prefix: Optional[str] = None,
) -> Dict:
    """
    Takes a deployment's configuration, and returns the arguments needed
    for the controller to deploy it.
    """
    if deployment_config is None:
        deployment_config = {}

    if isinstance(deployment_config, dict):
        deployment_config = DeploymentConfig.parse_obj(deployment_config)
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

    Returns: a hash of the import path and (application level) runtime env representing
            the code version of the application.
    """
    encoded = json.dumps(
        {
            "import_path": app_config.import_path,
            "runtime_env": app_config.runtime_env,
            "args": app_config.args,
        },
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha1(encoded).hexdigest()
