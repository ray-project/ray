from typing import (
    Dict,
    Optional,
    List,
)

from ray.serve.deployment import Deployment
from ray.serve._private.deploy_utils import get_deploy_args
from ray.util.annotations import Deprecated


@Deprecated(message="Only used with `BuiltApplication` which has been deprecated.")
class ImmutableDeploymentDict(dict):
    def __init__(self, deployments: Dict[str, Deployment]):
        super().__init__()
        self.update(deployments)

    def __setitem__(self, *args):
        """Not allowed. Modify deployment options using set_options instead."""
        raise RuntimeError(
            "Setting deployments in a built app is not allowed. Modify the "
            'options using app.deployments["deployment"].set_options instead.'
        )


@Deprecated(message="Use the `serve build` CLI command instead.")
class BuiltApplication:
    """A static, pre-built Serve application.

    An application consists of a number of Serve deployments that can send
    requests to each other. One of the deployments acts as the "ingress,"
    meaning that it receives external traffic and is the entrypoint to the
    application.

    The ingress deployment can be accessed via app.ingress and a dictionary of
    all deployments can be accessed via app.deployments.

    The config options of each deployment can be modified using set_options:
    app.deployments["name"].set_options(...).

    This application object can be written to a config file and later deployed
    to production using the Serve CLI or REST API.
    """

    def __init__(self, deployments: List[Deployment], ingress: str):
        deployment_dict = {}
        for d in deployments:
            if not isinstance(d, Deployment):
                raise TypeError(f"Got {type(d)}. Expected deployment.")
            elif d.name in deployment_dict:
                raise ValueError(f"App got multiple deployments named '{d.name}'.")

            deployment_dict[d.name] = d

        self._deployments = ImmutableDeploymentDict(deployment_dict)

        if ingress not in self._deployments:
            raise ValueError(
                f"Requested ingress deployment '{ingress}' was not found in the "
                f"deployments passed in: {self._deployments.keys()}. Ingress must be "
                "one of the deployments passed in."
            )

        self._ingress = ingress

    @property
    def deployments(self) -> ImmutableDeploymentDict:
        return self._deployments

    @property
    def ingress(self) -> Optional[Deployment]:
        return self._deployments[self._ingress]


def _get_deploy_args_from_built_app(app: BuiltApplication):
    """Get list of deploy args from a BuiltApplication."""
    deploy_args_list = []
    for deployment in list(app.deployments.values()):
        is_ingress = deployment.name == app.ingress.name
        deploy_args_list.append(
            get_deploy_args(
                name=deployment._name,
                replica_config=deployment._replica_config,
                ingress=is_ingress,
                deployment_config=deployment._deployment_config,
                version=deployment.version,
                route_prefix=deployment.route_prefix,
                is_driver_deployment=deployment._is_driver_deployment,
                docs_path=deployment._docs_path,
            )
        )
    return deploy_args_list
