from typing import Any, List, Tuple

from ray.dag.py_obj_scanner import _PyObjScanner
from ray.serve._private.common import DeploymentHandleSource
from ray.serve.deployment import Application, Deployment
from ray.serve.handle import DeploymentHandle, _HandleOptions


class IDDict(dict):
    """TODO"""

    def __getitem__(self, key: Any) -> Any:
        return super().__getitem__(id(key))

    def __setitem__(self, key: Any, value: Any):
        return super().__setitem__(id(key), value)

    def __delitem__(self, key: Any):
        return super().__delitem__(id(key))

    def __contains__(self, key: Any):
        return super().__contains__(id(key))


def build_app(
    app: Application,
    *,
    name: str,
) -> Tuple[str, List[Deployment]]:
    """Builds the application into a list of finalized deployments.

    The following transformations are made:
        - Application objects in constructor args/kwargs are converted to
          DeploymentHandles for injection at runtime.
        - Name conflicts from deployments that use the same class are handled
          by appending a monotonically increasing suffix (e.g., SomeClass_1).

    Returns:
        (ingress_deployment_name, deployments)
    """
    deployments = _build_app_recursive(
        app,
        app_name=name,
        handles=IDDict(),
        deployment_names=IDDict(),
    )
    return app._bound_deployment.name, deployments


def _build_app_recursive(
    app: Application,
    *,
    app_name: str,
    deployment_names: IDDict[Application, str],
    handles: IDDict[Application, DeploymentHandle],
) -> List[Deployment]:
    # This application has already been encountered.
    # There's no need to recurse into its child args and we don't want to create
    # a duplicate entry for it in the list of deployments.
    if app in handles:
        return []

    # Create the DeploymentHandle that will be used to replace this application
    # in the arguments of its parent(s).
    handles[app] = DeploymentHandle(
        _get_unique_name_memoized(app, deployment_names),
        app_name=app_name,
        handle_options=_HandleOptions(_source=DeploymentHandleSource.REPLICA),
    )

    deployments = []
    scanner = _PyObjScanner(source_type=Application)
    try:
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

        new_init_args, new_init_kwargs = scanner.replace_nodes(handles)
        deployments.append(
            app._bound_deployment.options(
                name=_get_unique_name_memoized(app, deployment_names),
                _init_args=new_init_args,
                _init_kwargs=new_init_kwargs,
            )
        )
        return deployments
    finally:
        scanner.clear()


def _get_unique_name_memoized(
    app: Application, deployment_names: IDDict[Application, str]
) -> str:
    if app in deployment_names:
        return deployment_names[app]

    name = app._bound_deployment.name
    idx = 1
    while name in deployment_names.values():
        name = f"{app._bound_deployment.name}_{idx}"
        idx += 1

    deployment_names[app] = name
    return name
