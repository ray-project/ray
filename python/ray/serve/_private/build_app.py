import logging
from dataclasses import dataclass
from typing import Any, Generic, List, TypeVar

from ray.dag.py_obj_scanner import _PyObjScanner
from ray.serve._private.common import DeploymentHandleSource
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.deployment import Application, Deployment
from ray.serve.handle import DeploymentHandle, _HandleOptions

logger = logging.getLogger(SERVE_LOGGER_NAME)

K = TypeVar("K")
V = TypeVar("V")

class IDDict(dict, Generic[K, V]):
    """Dictionary that uses id() for keys instead of hash().

    This is necessary because Application objects aren't hashable and we want each
    instance to map to a unique key.
    """

    def __getitem__(self, key: K) -> Any:
        return super().__getitem__(id(key))

    def __setitem__(self, key: K, value: V):
        return super().__setitem__(id(key), value)

    def __delitem__(self, key: K):
        return super().__delitem__(id(key))

    def __contains__(self, key: object):
        return super().__contains__(id(key))


@dataclass(frozen=True)
class BuiltApplication:
    # Name of the application.
    name: str
    # Name of the application's 'ingress' deployment
    # (the one exposed over gRPC/HTTP/handle).
    ingress_deployment_name: str
    # List of unique deployments comprising the app.
    deployments: List[Deployment]


def build_app(
    app: Application,
    *,
    name: str,
) -> BuiltApplication:
    """Builds the application into a list of finalized deployments.

    The following transformations are made:
        - Application objects in constructor args/kwargs are converted to
          DeploymentHandles for injection at runtime.
        - Name conflicts from deployments that use the same class are handled
          by appending a monotonically increasing suffix (e.g., SomeClass_1).

    Returns: BuiltApplication
    """
    deployments = _build_app_recursive(
        app,
        app_name=name,
        handles=IDDict(),
        deployment_names=IDDict(),
    )
    return BuiltApplication(
        name=name,
        ingress_deployment_name=app._bound_deployment.name,
        deployments=deployments,
    )


def _build_app_recursive(
    app: Application,
    *,
    app_name: str,
    deployment_names: IDDict[Application, str],
    handles: IDDict[Application, DeploymentHandle],
) -> List[Deployment]:
    """Recursively traverses the graph of Application objects.

    Each Application will have an associated DeploymentHandle created that will replace
    it in any occurrences in other Applications' args or kwargs.

    Also collects a list of the unique Applications encountered and returns them as
    deployable Deployment objects.
    """
    # This application has already been encountered.
    # There's no need to recurse into its child args and we don't want to create
    # a duplicate entry for it in the list of deployments.
    if app in handles:
        return []

    # Create the DeploymentHandle that will be used to replace this application
    # in the arguments of its parent(s).
    handles[app] = DeploymentHandle(
        _get_unique_deployment_name_memoized(app, deployment_names),
        app_name=app_name,
        handle_options=_HandleOptions(_source=DeploymentHandleSource.REPLICA),
    )

    deployments = []
    scanner = _PyObjScanner(source_type=Application)
    try:
        # Recursively traverse any Application objects bound to init args/kwargs.
        child_apps = scanner.find_nodes(
            (app._bound_deployment.init_args, app._bound_deployment.init_kwargs)
        )
        for child_app in child_apps:
            deployments.extend(
                _build_app_recursive(
                    child_app,
                    app_name=app_name,
                    handles=handles,
                    deployment_names=deployment_names,
                )
            )

        # Replace Application objects with their corresponding DeploymentHandles.
        new_init_args, new_init_kwargs = scanner.replace_nodes(handles)
        deployments.append(
            app._bound_deployment.options(
                name=_get_unique_deployment_name_memoized(app, deployment_names),
                _init_args=new_init_args,
                _init_kwargs=new_init_kwargs,
            )
        )
        return deployments
    finally:
        scanner.clear()


def _get_unique_deployment_name_memoized(
    app: Application, deployment_names: IDDict[Application, str]
) -> str:
    """Generates a name for the deployment.

    This is used to handle collisions when the user does not specify a name
    explicitly, so typically we'd use the class name as the default.

    In that case, we append a monotonically increasing suffix to the name, e.g.,
    Deployment, then Deployment_1, then Deployment_2, ...

    Names are memoized in the `deployment_names` dict, which should be passed to
    subsequent calls to this function.
    """
    if app in deployment_names:
        return deployment_names[app]

    idx = 1
    name = app._bound_deployment.name
    while name in deployment_names.values():
        name = f"{app._bound_deployment.name}_{idx}"
        idx += 1

    if idx != 1:
        logger.warning(
            "There are multiple deployments with the same name "
            f"'{app._bound_deployment.name}'. Renaming one to '{name}'."
        )

    deployment_names[app] = name
    return name
