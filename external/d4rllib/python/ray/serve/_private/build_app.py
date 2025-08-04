import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union

from ray.dag.py_obj_scanner import _PyObjScanner
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve.deployment import Application, Deployment
from ray.serve.handle import DeploymentHandle
from ray.serve.schema import LoggingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)

K = TypeVar("K")
V = TypeVar("V")


class IDDict(dict, Generic[K, V]):
    """Dictionary that uses id() for keys instead of hash().

    This is necessary because Application objects aren't hashable and we want each
    instance to map to a unique key.
    """

    def __getitem__(self, key: K) -> V:
        if not isinstance(key, int):
            key = id(key)
        return super().__getitem__(key)

    def __setitem__(self, key: K, value: V):
        if not isinstance(key, int):
            key = id(key)
        return super().__setitem__(key, value)

    def __delitem__(self, key: K):
        if not isinstance(key, int):
            key = id(key)
        return super().__delitem__(key)

    def __contains__(self, key: object):
        if not isinstance(key, int):
            key = id(key)
        return super().__contains__(key)


@dataclass(frozen=True)
class BuiltApplication:
    # Name of the application.
    name: str
    route_prefix: Optional[str]
    logging_config: Optional[LoggingConfig]
    # Name of the application's 'ingress' deployment
    # (the one exposed over gRPC/HTTP/handle).
    ingress_deployment_name: str
    # List of unique deployments comprising the app.
    deployments: List[Deployment]
    # Dict[name, DeploymentHandle] mapping deployment names to the handles that replaced
    # them in other deployments' init args/kwargs.
    deployment_handles: Dict[str, DeploymentHandle]


def _make_deployment_handle_default(
    deployment: Deployment, app_name: str
) -> DeploymentHandle:
    return DeploymentHandle(
        deployment.name,
        app_name=app_name,
    )


def build_app(
    app: Application,
    *,
    name: str,
    route_prefix: Optional[str] = None,
    logging_config: Optional[Union[Dict, LoggingConfig]] = None,
    default_runtime_env: Optional[Dict[str, Any]] = None,
    make_deployment_handle: Optional[
        Callable[[Deployment, str], DeploymentHandle]
    ] = None,
) -> BuiltApplication:
    """Builds the application into a list of finalized deployments.

    The following transformations are made:
        - Application objects in constructor args/kwargs are converted to
          DeploymentHandles for injection at runtime.
        - Name conflicts from deployments that use the same class are handled
          by appending a monotonically increasing suffix (e.g., SomeClass_1).

    Returns: BuiltApplication
    """
    if make_deployment_handle is None:
        make_deployment_handle = _make_deployment_handle_default

    handles = IDDict()
    deployment_names = IDDict()
    deployments = _build_app_recursive(
        app,
        app_name=name,
        handles=handles,
        deployment_names=deployment_names,
        default_runtime_env=default_runtime_env,
        make_deployment_handle=make_deployment_handle,
    )
    return BuiltApplication(
        name=name,
        route_prefix=route_prefix,
        logging_config=logging_config,
        ingress_deployment_name=app._bound_deployment.name,
        deployments=deployments,
        deployment_handles={
            deployment_names[app]: handle for app, handle in handles.items()
        },
    )


def _build_app_recursive(
    app: Application,
    *,
    app_name: str,
    deployment_names: IDDict[Application, str],
    handles: IDDict[Application, DeploymentHandle],
    default_runtime_env: Optional[Dict[str, Any]] = None,
    make_deployment_handle: Callable[[Deployment, str], DeploymentHandle],
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
                    make_deployment_handle=make_deployment_handle,
                    default_runtime_env=default_runtime_env,
                )
            )

        # Replace Application objects with their corresponding DeploymentHandles.
        new_init_args, new_init_kwargs = scanner.replace_nodes(handles)
        final_deployment = app._bound_deployment.options(
            name=_get_unique_deployment_name_memoized(app, deployment_names),
            _init_args=new_init_args,
            _init_kwargs=new_init_kwargs,
        )
        final_deployment = _set_default_runtime_env(
            final_deployment, default_runtime_env
        )

        # Create the DeploymentHandle that will be used to replace this application
        # in the arguments of its parent(s).
        handles[app] = make_deployment_handle(
            final_deployment,
            app_name,
        )

        return deployments + [final_deployment]
    finally:
        scanner.clear()


def _set_default_runtime_env(
    d: Deployment, default_runtime_env: Optional[Dict[str, Any]]
) -> Deployment:
    """Configures the deployment with the provided default runtime_env.

    If the deployment does not have a runtime_env configured, the default will be set.

    If it does have a runtime_env configured but that runtime_env does not have a
    working_dir, only the working_dir field will be set.

    Else the deployment's runtime_env will be left untouched.
    """
    if not default_runtime_env:
        return d

    ray_actor_options = deepcopy(d.ray_actor_options or {})
    default_working_dir = default_runtime_env.get("working_dir", None)
    if "runtime_env" not in ray_actor_options:
        ray_actor_options["runtime_env"] = default_runtime_env
    elif default_working_dir is not None:
        ray_actor_options["runtime_env"].setdefault("working_dir", default_working_dir)

    return d.options(ray_actor_options=ray_actor_options)


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
