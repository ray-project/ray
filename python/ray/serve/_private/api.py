from ray.serve.context import get_global_client
from ray.serve.deployment import Deployment
from typing import Dict


def get_deployment(name: str):
    """Dynamically fetch a handle to a Deployment object.

    Args:
        name(str): name of the deployment. This must have already been
        deployed.

    Returns:
        Deployment
    """
    try:
        (
            deployment_info,
            route_prefix,
        ) = get_global_client().get_deployment_info(name)
    except KeyError:
        raise KeyError(
            f"Deployment {name} was not found. Did you call Deployment.deploy()?"
        )
    return Deployment(
        deployment_info.replica_config.deployment_def,
        name,
        deployment_info.deployment_config,
        version=deployment_info.version,
        init_args=deployment_info.replica_config.init_args,
        init_kwargs=deployment_info.replica_config.init_kwargs,
        route_prefix=route_prefix,
        ray_actor_options=deployment_info.replica_config.ray_actor_options,
        _internal=True,
    )


def list_deployments() -> Dict[str, Deployment]:
    """Returns a dictionary of all active deployments.

    Dictionary maps deployment name to Deployment objects.
    """
    infos = get_global_client().list_deployments()

    deployments = {}
    for name, (deployment_info, route_prefix) in infos.items():
        deployments[name] = Deployment(
            deployment_info.replica_config.deployment_def,
            name,
            deployment_info.deployment_config,
            version=deployment_info.version,
            init_args=deployment_info.replica_config.init_args,
            init_kwargs=deployment_info.replica_config.init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=deployment_info.replica_config.ray_actor_options,
            _internal=True,
        )

    return deployments
