from copy import copy
import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
)

from ray.serve.context import get_global_client
from ray.experimental.dag.class_node import ClassNode
from ray.experimental.dag.function_node import FunctionNode
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentConfig,
)
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve.utils import DEFAULT, get_deployment_import_path
from ray.util.annotations import PublicAPI
from ray.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
)


@PublicAPI
class Deployment:
    def __init__(
        self,
        func_or_class: Union[Callable, str],
        name: str,
        config: DeploymentConfig,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Tuple[Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        ray_actor_options: Optional[Dict] = None,
        _internal=False,
    ) -> None:
        """Construct a Deployment. CONSTRUCTOR SHOULDN'T BE USED DIRECTLY.

        Deployments should be created, retrieved, and updated using
        `@serve.deployment`, `serve.get_deployment`, and `Deployment.options`,
        respectively.
        """

        if not _internal:
            raise RuntimeError(
                "The Deployment constructor should not be called "
                "directly. Use `@serve.deployment` instead."
            )
        if not callable(func_or_class) and not isinstance(func_or_class, str):
            raise TypeError("@serve.deployment must be called on a class or function.")
        if not isinstance(name, str):
            raise TypeError("name must be a string.")
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")
        if not (prev_version is None or isinstance(prev_version, str)):
            raise TypeError("prev_version must be a string.")
        if not (init_args is None or isinstance(init_args, (tuple, list))):
            raise TypeError("init_args must be a tuple.")
        if not (init_kwargs is None or isinstance(init_kwargs, dict)):
            raise TypeError("init_kwargs must be a dict.")
        if route_prefix is not DEFAULT.VALUE and route_prefix is not None:
            if not isinstance(route_prefix, str):
                raise TypeError("route_prefix must be a string.")
            if not route_prefix.startswith("/"):
                raise ValueError("route_prefix must start with '/'.")
            if route_prefix != "/" and route_prefix.endswith("/"):
                raise ValueError(
                    "route_prefix must not end with '/' unless it's the root."
                )
            if "{" in route_prefix or "}" in route_prefix:
                raise ValueError("route_prefix may not contain wildcards.")
        if not (ray_actor_options is None or isinstance(ray_actor_options, dict)):
            raise TypeError("ray_actor_options must be a dict.")

        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}

        # TODO(architkulkarni): Enforce that autoscaling_config and
        # user-provided num_replicas should be mutually exclusive.
        if version is None and config.autoscaling_config is not None:
            # TODO(architkulkarni): Remove this restriction.
            raise ValueError(
                "Currently autoscaling is only supported for "
                "versioned deployments. Try @serve.deployment(version=...)."
            )

        self._func_or_class = func_or_class
        self._name = name
        self._version = version
        self._prev_version = prev_version
        self._config = config
        self._init_args = init_args
        self._init_kwargs = init_kwargs
        self._route_prefix = route_prefix
        self._ray_actor_options = ray_actor_options

    @property
    def name(self) -> str:
        """Unique name of this deployment."""
        return self._name

    @property
    def version(self) -> Optional[str]:
        """Version of this deployment.

        If None, will be redeployed every time `.deploy()` is called.
        """
        return self._version

    @property
    def prev_version(self) -> Optional[str]:
        """Existing version of deployment to target.

        If prev_version does not match with existing deployment
        version, the deployment will fail to be deployed.
        """
        return self._prev_version

    @property
    def func_or_class(self) -> Union[Callable, str]:
        """Underlying class or function that this deployment wraps."""
        return self._func_or_class

    @property
    def num_replicas(self) -> int:
        """Current target number of replicas."""
        return self._config.num_replicas

    @property
    def user_config(self) -> Any:
        """Current dynamic user-provided config options."""
        return self._config.user_config

    @property
    def max_concurrent_queries(self) -> int:
        """Current max outstanding queries from each handle."""
        return self._config.max_concurrent_queries

    @property
    def route_prefix(self) -> Optional[str]:
        """HTTP route prefix that this deployment is exposed under."""
        if self._route_prefix is DEFAULT.VALUE:
            return f"/{self._name}"
        return self._route_prefix

    @property
    def ray_actor_options(self) -> Optional[Dict]:
        """Actor options such as resources required for each replica."""
        return self._ray_actor_options

    @property
    def init_args(self) -> Tuple[Any]:
        """Positional args passed to the underlying class's constructor."""
        return self._init_args

    @property
    def init_kwargs(self) -> Tuple[Any]:
        """Keyword args passed to the underlying class's constructor."""
        return self._init_kwargs

    @property
    def url(self) -> Optional[str]:
        """Full HTTP url for this deployment."""
        if self._route_prefix is None:
            # this deployment is not exposed over HTTP
            return None

        return get_global_client().root_url + self.route_prefix

    def __call__(self):
        raise RuntimeError(
            "Deployments cannot be constructed directly. "
            "Use `deployment.deploy() instead.`"
        )

    @PublicAPI(stability="alpha")
    def bind(self, *args, **kwargs) -> Union[ClassNode, FunctionNode]:
        """Bind the provided arguments and return a class or function node.

        The returned bound deployment can be deployed or bound to other
        deployments to create a deployment graph.
        """

        copied_self = copy(self)
        copied_self._init_args = []
        copied_self._init_kwargs = {}
        copied_self._func_or_class = "dummpy.module"
        schema_shell = deployment_to_schema(copied_self)

        if inspect.isfunction(self._func_or_class):
            return FunctionNode(
                self._func_or_class,
                args,  # Used to bind and resolve DAG only, can take user input
                kwargs,  # Used to bind and resolve DAG only, can take user input
                self._ray_actor_options or dict(),
                other_args_to_resolve={
                    "deployment_schema": schema_shell,
                    "is_from_serve_deployment": True,
                },
            )
        else:
            return ClassNode(
                self._func_or_class,
                args,
                kwargs,
                cls_options=self._ray_actor_options or dict(),
                other_args_to_resolve={
                    "deployment_schema": schema_shell,
                    "is_from_serve_deployment": True,
                },
            )

    @PublicAPI
    def deploy(self, *init_args, _blocking=True, **init_kwargs):
        """Deploy or update this deployment.

        Args:
            init_args (optional): args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
            init_kwargs (optional): kwargs to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        if len(init_args) == 0 and self._init_args is not None:
            init_args = self._init_args
        if len(init_kwargs) == 0 and self._init_kwargs is not None:
            init_kwargs = self._init_kwargs

        return get_global_client().deploy(
            self._name,
            self._func_or_class,
            init_args,
            init_kwargs,
            ray_actor_options=self._ray_actor_options,
            config=self._config,
            version=self._version,
            prev_version=self._prev_version,
            route_prefix=self.route_prefix,
            url=self.url,
            _blocking=_blocking,
        )

    @PublicAPI
    def delete(self):
        """Delete this deployment."""

        return get_global_client().delete_deployments([self._name])

    @PublicAPI
    def get_handle(
        self, sync: Optional[bool] = True
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Get a ServeHandle to this deployment to invoke it from Python.

        Args:
            sync (bool): If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return an
                asyncio-optimized ServeHandle that's only usable in an asyncio
                loop.

        Returns:
            ServeHandle
        """

        return get_global_client().get_handle(self._name, missing_ok=True, sync=sync)

    @PublicAPI
    def options(
        self,
        func_or_class: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None,
        _health_check_period_s: Optional[float] = None,
        _health_check_timeout_s: Optional[float] = None,
    ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.
        """
        new_config = self._config.copy()
        if num_replicas is not None:
            new_config.num_replicas = num_replicas
        if user_config is not None:
            new_config.user_config = user_config
        if max_concurrent_queries is not None:
            new_config.max_concurrent_queries = max_concurrent_queries

        if func_or_class is None:
            func_or_class = self._func_or_class

        if name is None:
            name = self._name

        if version is None:
            version = self._version

        if prev_version is None:
            prev_version = self._prev_version

        if init_args is None:
            init_args = self._init_args

        if init_kwargs is None:
            init_kwargs = self._init_kwargs

        if route_prefix is DEFAULT.VALUE:
            # Default is to keep the previous value
            route_prefix = self._route_prefix

        if ray_actor_options is None:
            ray_actor_options = self._ray_actor_options

        if _autoscaling_config is not None:
            new_config.autoscaling_config = _autoscaling_config

        if _graceful_shutdown_wait_loop_s is not None:
            new_config.graceful_shutdown_wait_loop_s = _graceful_shutdown_wait_loop_s

        if _graceful_shutdown_timeout_s is not None:
            new_config.graceful_shutdown_timeout_s = _graceful_shutdown_timeout_s

        if _health_check_period_s is not None:
            new_config.health_check_period_s = _health_check_period_s

        if _health_check_timeout_s is not None:
            new_config.health_check_timeout_s = _health_check_timeout_s

        return Deployment(
            func_or_class,
            name,
            new_config,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            ray_actor_options=ray_actor_options,
            _internal=True,
        )

    @PublicAPI(stability="alpha")
    def set_options(
        self,
        func_or_class: Optional[Callable] = None,
        name: Optional[str] = None,
        version: Optional[str] = None,
        prev_version: Optional[str] = None,
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        _autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        _graceful_shutdown_wait_loop_s: Optional[float] = None,
        _graceful_shutdown_timeout_s: Optional[float] = None,
        _health_check_period_s: Optional[float] = None,
        _health_check_timeout_s: Optional[float] = None,
    ) -> None:
        """Overwrite this deployment's options. Mutates the deployment.

        Only those options passed in will be updated, all others will remain
        unchanged.
        """

        validated = self.options(
            func_or_class=func_or_class,
            name=name,
            version=version,
            prev_version=prev_version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            num_replicas=num_replicas,
            ray_actor_options=ray_actor_options,
            user_config=user_config,
            max_concurrent_queries=max_concurrent_queries,
            _autoscaling_config=_autoscaling_config,
            _graceful_shutdown_wait_loop_s=_graceful_shutdown_wait_loop_s,
            _graceful_shutdown_timeout_s=_graceful_shutdown_timeout_s,
            _health_check_period_s=_health_check_period_s,
            _health_check_timeout_s=_health_check_timeout_s,
        )

        self._func_or_class = validated._func_or_class
        self._name = validated._name
        self._version = validated._version
        self._prev_version = validated._prev_version
        self._init_args = validated._init_args
        self._init_kwargs = validated._init_kwargs
        self._route_prefix = validated._route_prefix
        self._ray_actor_options = validated._ray_actor_options
        self._config = validated._config

    def __eq__(self, other):
        return all(
            [
                self._name == other._name,
                self._version == other._version,
                self._config == other._config,
                self._init_args == other._init_args,
                self._init_kwargs == other._init_kwargs,
                # compare route prefix with default value resolved
                self.route_prefix == other.route_prefix,
                self._ray_actor_options == self._ray_actor_options,
            ]
        )

    def __str__(self):
        return (
            f"Deployment(name={self._name},"
            f"version={self._version},"
            f"route_prefix={self.route_prefix})"
        )

    def __repr__(self):
        return str(self)


def deployment_to_schema(d: Deployment) -> DeploymentSchema:
    """Converts a live deployment object to a corresponding structured schema.

    If the deployment has a class or function, it will be attemptetd to be
    converted to a valid corresponding import path.

    init_args and init_kwargs must also be JSON-serializable or this call will
    fail.
    """
    from ray.serve.pipeline.json_serde import convert_to_json_safe_obj

    if d.ray_actor_options is not None:
        ray_actor_options_schema = RayActorOptionsSchema.parse_obj(d.ray_actor_options)
    else:
        ray_actor_options_schema = None

    return DeploymentSchema(
        name=d.name,
        import_path=get_deployment_import_path(
            d, enforce_importable=True, replace_main=True
        ),
        init_args=convert_to_json_safe_obj(d.init_args, err_key="init_args"),
        init_kwargs=convert_to_json_safe_obj(d.init_kwargs, err_key="init_kwargs"),
        num_replicas=d.num_replicas,
        route_prefix=d.route_prefix,
        max_concurrent_queries=d.max_concurrent_queries,
        user_config=d.user_config,
        autoscaling_config=d._config.autoscaling_config,
        graceful_shutdown_wait_loop_s=d._config.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=d._config.graceful_shutdown_timeout_s,
        health_check_period_s=d._config.health_check_period_s,
        health_check_timeout_s=d._config.health_check_timeout_s,
        ray_actor_options=ray_actor_options_schema,
    )


def schema_to_deployment(s: DeploymentSchema) -> Deployment:
    from ray.serve.pipeline.json_serde import convert_from_json_safe_obj

    if s.ray_actor_options is None:
        ray_actor_options = None
    else:
        ray_actor_options = s.ray_actor_options.dict(exclude_unset=True)

    config = DeploymentConfig.from_default(
        ignore_none=True,
        num_replicas=s.num_replicas,
        user_config=s.user_config,
        max_concurrent_queries=s.max_concurrent_queries,
        autoscaling_config=s.autoscaling_config,
        graceful_shutdown_wait_loop_s=s.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=s.graceful_shutdown_timeout_s,
        health_check_period_s=s.health_check_period_s,
        health_check_timeout_s=s.health_check_timeout_s,
    )

    return Deployment(
        func_or_class=s.import_path,
        name=s.name,
        config=config,
        init_args=convert_from_json_safe_obj(s.init_args, err_key="init_args"),
        init_kwargs=convert_from_json_safe_obj(s.init_kwargs, err_key="init_kwargs"),
        route_prefix=s.route_prefix,
        ray_actor_options=ray_actor_options,
        _internal=True,
    )
