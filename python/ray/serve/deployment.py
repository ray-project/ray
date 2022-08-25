from copy import copy
import inspect
import logging
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
)
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

from ray.serve.context import get_global_client
from ray.dag.class_node import ClassNode
from ray.dag.function_node import FunctionNode
from ray.serve.config import (
    AutoscalingConfig,
    DeploymentConfig,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME, MIGRATION_MESSAGE
from ray.serve.handle import RayServeHandle, RayServeSyncHandle
from ray.serve._private.utils import DEFAULT, guarded_deprecation_warning
from ray.util.annotations import Deprecated, PublicAPI
from ray.serve.schema import (
    RayActorOptionsSchema,
    DeploymentSchema,
)


logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI
class Deployment:
    def __init__(
        self,
        func_or_class: Union[Callable, str],
        name: str,
        config: DeploymentConfig,
        version: Optional[str] = None,
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

        self._func_or_class = func_or_class
        self._name = name
        self._version = version
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

    @PublicAPI(stability="beta")
    def bind(self, *args, **kwargs) -> Union[ClassNode, FunctionNode]:
        """Bind the provided arguments and return a class or function node.

        The returned bound deployment can be deployed or bound to other
        deployments to create a deployment graph.
        """

        copied_self = copy(self)
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

    @guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
    @Deprecated(message=MIGRATION_MESSAGE)
    def deploy(self, *init_args, _blocking=True, **init_kwargs):
        """Deploy or update this deployment.

        Args:
            init_args: args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
            init_kwargs: kwargs to pass to the class __init__
                method. Not valid if this deployment wraps a function.
        """
        record_extra_usage_tag(TagKey.SERVE_API_VERSION, "v1")
        self._deploy(*init_args, _blocking=_blocking, **init_kwargs)

    # TODO(Sihan) Promote the _deploy to deploy after we fully deprecate the API
    def _deploy(self, *init_args, _blocking=True, **init_kwargs):
        """Deploy or update this deployment.

        Args:
            init_args: args to pass to the class __init__
                method. Not valid if this deployment wraps a function.
            init_kwargs: kwargs to pass to the class __init__
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
            route_prefix=self.route_prefix,
            url=self.url,
            _blocking=_blocking,
        )

    @guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
    @Deprecated(message=MIGRATION_MESSAGE)
    def delete(self):
        """Delete this deployment."""

        return self._delete()

    # TODO(Sihan) Promote the _delete to delete after we fully deprecate the API
    def _delete(self):
        """Delete this deployment."""

        return get_global_client().delete_deployments([self._name])

    @guarded_deprecation_warning(instructions=MIGRATION_MESSAGE)
    @Deprecated(message=MIGRATION_MESSAGE)
    def get_handle(
        self, sync: Optional[bool] = True
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Get a ServeHandle to this deployment to invoke it from Python.

        Args:
            sync: If true, then Serve will return a ServeHandle that
                works everywhere. Otherwise, Serve will return an
                asyncio-optimized ServeHandle that's only usable in an asyncio
                loop.

        Returns:
            ServeHandle
        """
        return self._get_handle(sync)

    # TODO(Sihan) Promote the _get_handle to get_handle after we fully deprecate the API
    def _get_handle(
        self, sync: Optional[bool] = True
    ) -> Union[RayServeHandle, RayServeSyncHandle]:
        """Get a ServeHandle to this deployment to invoke it from Python.

        Args:
            sync: If true, then Serve will return a ServeHandle that
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
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        graceful_shutdown_wait_loop_s: Optional[float] = None,
        graceful_shutdown_timeout_s: Optional[float] = None,
        health_check_period_s: Optional[float] = None,
        health_check_timeout_s: Optional[float] = None,
        _internal: bool = False,
    ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.
        """
        new_config = self._config.copy()

        if num_replicas is not None and autoscaling_config is not None:
            raise ValueError(
                "Manually setting num_replicas is not allowed when "
                "autoscaling_config is provided."
            )

        if num_replicas == 0:
            raise ValueError("num_replicas is expected to larger than 0")

        if not _internal and version is not None:
            logger.warning(
                "DeprecationWarning: `version` in `Deployment.options()` has been "
                "deprecated. Explicitly specifying version will raise an error in the "
                "future!"
            )

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

        if init_args is None:
            init_args = self._init_args

        if init_kwargs is None:
            init_kwargs = self._init_kwargs

        if route_prefix is DEFAULT.VALUE:
            # Default is to keep the previous value
            route_prefix = self._route_prefix

        if ray_actor_options is None:
            ray_actor_options = self._ray_actor_options

        if autoscaling_config is not None:
            new_config.autoscaling_config = autoscaling_config

        if graceful_shutdown_wait_loop_s is not None:
            new_config.graceful_shutdown_wait_loop_s = graceful_shutdown_wait_loop_s

        if graceful_shutdown_timeout_s is not None:
            new_config.graceful_shutdown_timeout_s = graceful_shutdown_timeout_s

        if health_check_period_s is not None:
            new_config.health_check_period_s = health_check_period_s

        if health_check_timeout_s is not None:
            new_config.health_check_timeout_s = health_check_timeout_s

        return Deployment(
            func_or_class,
            name,
            new_config,
            version=version,
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
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        route_prefix: Union[str, None, DEFAULT] = DEFAULT.VALUE,
        num_replicas: Optional[int] = None,
        ray_actor_options: Optional[Dict] = None,
        user_config: Optional[Any] = None,
        max_concurrent_queries: Optional[int] = None,
        autoscaling_config: Optional[Union[Dict, AutoscalingConfig]] = None,
        graceful_shutdown_wait_loop_s: Optional[float] = None,
        graceful_shutdown_timeout_s: Optional[float] = None,
        health_check_period_s: Optional[float] = None,
        health_check_timeout_s: Optional[float] = None,
        _internal: bool = False,
    ) -> None:
        """Overwrite this deployment's options. Mutates the deployment.

        Only those options passed in will be updated, all others will remain
        unchanged.
        """

        validated = self.options(
            func_or_class=func_or_class,
            name=name,
            version=version,
            init_args=init_args,
            init_kwargs=init_kwargs,
            route_prefix=route_prefix,
            num_replicas=num_replicas,
            ray_actor_options=ray_actor_options,
            user_config=user_config,
            max_concurrent_queries=max_concurrent_queries,
            autoscaling_config=autoscaling_config,
            graceful_shutdown_wait_loop_s=graceful_shutdown_wait_loop_s,
            graceful_shutdown_timeout_s=graceful_shutdown_timeout_s,
            health_check_period_s=health_check_period_s,
            health_check_timeout_s=health_check_timeout_s,
            _internal=_internal,
        )

        self._func_or_class = validated._func_or_class
        self._name = validated._name
        self._version = validated._version
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
    if d.ray_actor_options is not None:
        ray_actor_options_schema = RayActorOptionsSchema.parse_obj(d.ray_actor_options)
    else:
        ray_actor_options_schema = None

    return DeploymentSchema(
        name=d.name,
        # TODO(Sihan) DeploymentConfig num_replicas and auto_config can be set together
        # because internally we use these two field for autoscale and deploy.
        # We can improve the code after we separate the user faced deployment config and
        # internal deployment config.
        num_replicas=None if d._config.autoscaling_config else d.num_replicas,
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
    """Creates a deployment with parameters specified in schema.

    The returned deployment CANNOT be deployed immediately. It's func_or_class
    value is an empty string (""), which is not a valid import path. The
    func_or_class value must be overwritten with a valid function or class
    before the deployment can be deployed.
    """

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
        func_or_class="",
        name=s.name,
        config=config,
        init_args=(),
        init_kwargs={},
        route_prefix=s.route_prefix,
        ray_actor_options=ray_actor_options,
        _internal=True,
    )
