import logging
import warnings
from copy import deepcopy
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ray.serve._private.config import (
    DeploymentConfig,
    ReplicaConfig,
    RequestRouterConfig,
    handle_num_replicas_auto,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.usage import ServeUsageTag
from ray.serve._private.utils import DEFAULT, Default
from ray.serve.config import AutoscalingConfig
from ray.serve.schema import DeploymentSchema, LoggingConfig, RayActorOptionsSchema
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


@PublicAPI(stability="stable")
class Application:
    """One or more deployments bound with arguments that can be deployed together.

    Can be passed into another `Deployment.bind()` to compose multiple deployments in a
    single application, passed to `serve.run`, or deployed via a Serve config file.

    For example, to define an Application and run it in Python:

        .. code-block:: python

            from ray import serve
            from ray.serve import Application

            @serve.deployment
            class MyDeployment:
                pass

            app: Application = MyDeployment.bind(OtherDeployment.bind())
            serve.run(app)

    To run the same app using the command line interface (CLI):

        .. code-block:: bash

            serve run python_file:app

    To deploy the same app via a config file:

        .. code-block:: yaml

            applications:
                my_app:
                    import_path: python_file:app

    """

    def __init__(self, bound_deployment: "Deployment"):
        # This is used by `build_app`, but made private so users don't use it.
        self._bound_deployment = bound_deployment


@PublicAPI(stability="stable")
class Deployment:
    """Class (or function) decorated with the `@serve.deployment` decorator.

    This is run on a number of replica actors. Requests to those replicas call
    this class.

    One or more deployments can be composed together into an `Application` which is
    then run via `serve.run` or a config file.

    Example:

    .. code-block:: python

        @serve.deployment
        class MyDeployment:
            def __init__(self, name: str):
                self._name = name

            def __call__(self, request):
                return "Hello world!"

        app = MyDeployment.bind()
        # Run via `serve.run` or the `serve run` CLI command.
        serve.run(app)

    """

    def __init__(
        self,
        name: str,
        deployment_config: DeploymentConfig,
        replica_config: ReplicaConfig,
        version: Optional[str] = None,
        _internal=False,
    ) -> None:
        if not _internal:
            raise RuntimeError(
                "The Deployment constructor should not be called "
                "directly. Use `@serve.deployment` instead."
            )
        self._validate_name(name)
        if not (version is None or isinstance(version, str)):
            raise TypeError("version must be a string.")

        self._name = name
        self._version = version
        self._deployment_config = deployment_config
        self._replica_config = replica_config

    def _validate_name(self, name: str):
        if not isinstance(name, str):
            raise TypeError("name must be a string.")

        # name does not contain #
        if "#" in name:
            warnings.warn(
                f"Deployment names should not contain the '#' character, this will raise an error starting in Ray 2.46.0. "
                f"Current name: {name}."
            )

    @property
    def name(self) -> str:
        """Unique name of this deployment."""
        return self._name

    @property
    def version(self) -> Optional[str]:
        return self._version

    @property
    def func_or_class(self) -> Union[Callable, str]:
        """Underlying class or function that this deployment wraps."""
        return self._replica_config.deployment_def

    @property
    def num_replicas(self) -> int:
        """Target number of replicas."""
        return self._deployment_config.num_replicas

    @property
    def user_config(self) -> Any:
        """Dynamic user-provided config options."""
        return self._deployment_config.user_config

    @property
    def max_ongoing_requests(self) -> int:
        """Max number of requests a replica can handle at once."""
        return self._deployment_config.max_ongoing_requests

    @property
    def max_queued_requests(self) -> int:
        """Max number of requests that can be queued in each deployment handle."""
        return self._deployment_config.max_queued_requests

    @property
    def route_prefix(self):
        raise ValueError(
            "`route_prefix` can no longer be specified at the deployment level. "
            "Pass it to `serve.run` or in the application config instead."
        )

    @property
    def ray_actor_options(self) -> Optional[Dict]:
        """Actor options such as resources required for each replica."""
        return self._replica_config.ray_actor_options

    @property
    def init_args(self) -> Tuple[Any]:
        return self._replica_config.init_args

    @property
    def init_kwargs(self) -> Tuple[Any]:
        return self._replica_config.init_kwargs

    @property
    def url(self) -> Optional[str]:
        logger.warning(
            "DeprecationWarning: `Deployment.url` is deprecated "
            "and will be removed in the future."
        )
        return None

    @property
    def logging_config(self) -> Dict:
        return self._deployment_config.logging_config

    def set_logging_config(self, logging_config: Dict):
        self._deployment_config.logging_config = logging_config

    def __call__(self):
        raise RuntimeError(
            "Deployments cannot be constructed directly. "
            "Use `deployment.deploy() instead.`"
        )

    def bind(self, *args, **kwargs) -> Application:
        """Bind the arguments to the deployment and return an Application.

        The returned Application can be deployed using `serve.run` (or via
        config file) or bound to another deployment for composition.
        """
        return Application(self.options(_init_args=args, _init_kwargs=kwargs))

    def options(
        self,
        func_or_class: Optional[Callable] = None,
        name: Default[str] = DEFAULT.VALUE,
        version: Default[str] = DEFAULT.VALUE,
        num_replicas: Default[Optional[Union[int, str]]] = DEFAULT.VALUE,
        route_prefix: Default[Union[str, None]] = DEFAULT.VALUE,
        ray_actor_options: Default[Optional[Dict]] = DEFAULT.VALUE,
        placement_group_bundles: Default[List[Dict[str, float]]] = DEFAULT.VALUE,
        placement_group_strategy: Default[str] = DEFAULT.VALUE,
        max_replicas_per_node: Default[int] = DEFAULT.VALUE,
        user_config: Default[Optional[Any]] = DEFAULT.VALUE,
        max_ongoing_requests: Default[int] = DEFAULT.VALUE,
        max_queued_requests: Default[int] = DEFAULT.VALUE,
        autoscaling_config: Default[
            Union[Dict, AutoscalingConfig, None]
        ] = DEFAULT.VALUE,
        graceful_shutdown_wait_loop_s: Default[float] = DEFAULT.VALUE,
        graceful_shutdown_timeout_s: Default[float] = DEFAULT.VALUE,
        health_check_period_s: Default[float] = DEFAULT.VALUE,
        health_check_timeout_s: Default[float] = DEFAULT.VALUE,
        logging_config: Default[Union[Dict, LoggingConfig, None]] = DEFAULT.VALUE,
        request_router_config: Default[
            Union[Dict, RequestRouterConfig, None]
        ] = DEFAULT.VALUE,
        _init_args: Default[Tuple[Any]] = DEFAULT.VALUE,
        _init_kwargs: Default[Dict[Any, Any]] = DEFAULT.VALUE,
        _internal: bool = False,
        max_constructor_retry_count: Default[int] = DEFAULT.VALUE,
    ) -> "Deployment":
        """Return a copy of this deployment with updated options.

        Only those options passed in will be updated, all others will remain
        unchanged from the existing deployment.

        Refer to the `@serve.deployment` decorator docs for available arguments.
        """
        if route_prefix is not DEFAULT.VALUE:
            raise ValueError(
                "`route_prefix` can no longer be specified at the deployment level. "
                "Pass it to `serve.run` or in the application config instead."
            )

        # Modify max_ongoing_requests and autoscaling_config if
        # `num_replicas="auto"`
        if max_ongoing_requests is None:
            raise ValueError("`max_ongoing_requests` must be non-null, got None.")
        if num_replicas == "auto":
            num_replicas = None
            max_ongoing_requests, autoscaling_config = handle_num_replicas_auto(
                max_ongoing_requests, autoscaling_config
            )

            ServeUsageTag.AUTO_NUM_REPLICAS_USED.record("1")

        # NOTE: The user_configured_option_names should be the first thing that's
        # defined in this method. It depends on the locals() dictionary storing
        # only the function args/kwargs.
        # Create list of all user-configured options from keyword args
        user_configured_option_names = [
            option
            for option, value in locals().items()
            if option not in {"self", "func_or_class", "_internal"}
            and value is not DEFAULT.VALUE
        ]

        new_deployment_config = deepcopy(self._deployment_config)
        if not _internal:
            new_deployment_config.user_configured_option_names.update(
                user_configured_option_names
            )

        if num_replicas not in [
            DEFAULT.VALUE,
            None,
            "auto",
        ] and autoscaling_config not in [
            DEFAULT.VALUE,
            None,
        ]:
            raise ValueError(
                "Manually setting num_replicas is not allowed when "
                "autoscaling_config is provided."
            )

        if num_replicas == 0:
            raise ValueError("num_replicas is expected to larger than 0")

        if not _internal and version is not DEFAULT.VALUE:
            logger.warning(
                "DeprecationWarning: `version` in `Deployment.options()` has been "
                "deprecated. Explicitly specifying version will raise an error in the "
                "future!"
            )

        elif num_replicas not in [DEFAULT.VALUE, None]:
            new_deployment_config.num_replicas = num_replicas

        if user_config is not DEFAULT.VALUE:
            new_deployment_config.user_config = user_config

        if max_ongoing_requests is not DEFAULT.VALUE:
            new_deployment_config.max_ongoing_requests = max_ongoing_requests

        if max_queued_requests is not DEFAULT.VALUE:
            new_deployment_config.max_queued_requests = max_queued_requests

        if max_constructor_retry_count is not DEFAULT.VALUE:
            new_deployment_config.max_constructor_retry_count = (
                max_constructor_retry_count
            )

        if func_or_class is None:
            func_or_class = self._replica_config.deployment_def

        if name is DEFAULT.VALUE:
            name = self._name

        if version is DEFAULT.VALUE:
            version = self._version

        if _init_args is DEFAULT.VALUE:
            _init_args = self._replica_config.init_args

        if _init_kwargs is DEFAULT.VALUE:
            _init_kwargs = self._replica_config.init_kwargs

        if ray_actor_options is DEFAULT.VALUE:
            ray_actor_options = self._replica_config.ray_actor_options

        if placement_group_bundles is DEFAULT.VALUE:
            placement_group_bundles = self._replica_config.placement_group_bundles

        if placement_group_strategy is DEFAULT.VALUE:
            placement_group_strategy = self._replica_config.placement_group_strategy

        if max_replicas_per_node is DEFAULT.VALUE:
            max_replicas_per_node = self._replica_config.max_replicas_per_node

        if autoscaling_config is not DEFAULT.VALUE:
            new_deployment_config.autoscaling_config = autoscaling_config

        if request_router_config is not DEFAULT.VALUE:
            new_deployment_config.request_router_config = request_router_config

        if graceful_shutdown_wait_loop_s is not DEFAULT.VALUE:
            new_deployment_config.graceful_shutdown_wait_loop_s = (
                graceful_shutdown_wait_loop_s
            )

        if graceful_shutdown_timeout_s is not DEFAULT.VALUE:
            new_deployment_config.graceful_shutdown_timeout_s = (
                graceful_shutdown_timeout_s
            )

        if health_check_period_s is not DEFAULT.VALUE:
            new_deployment_config.health_check_period_s = health_check_period_s

        if health_check_timeout_s is not DEFAULT.VALUE:
            new_deployment_config.health_check_timeout_s = health_check_timeout_s

        if logging_config is not DEFAULT.VALUE:
            if isinstance(logging_config, LoggingConfig):
                logging_config = logging_config.dict()
            new_deployment_config.logging_config = logging_config

        new_replica_config = ReplicaConfig.create(
            func_or_class,
            init_args=_init_args,
            init_kwargs=_init_kwargs,
            ray_actor_options=ray_actor_options,
            placement_group_bundles=placement_group_bundles,
            placement_group_strategy=placement_group_strategy,
            max_replicas_per_node=max_replicas_per_node,
        )

        return Deployment(
            name,
            new_deployment_config,
            new_replica_config,
            version=version,
            _internal=True,
        )

    def __eq__(self, other):
        return all(
            [
                self._name == other._name,
                self._version == other._version,
                self._deployment_config == other._deployment_config,
                self._replica_config.init_args == other._replica_config.init_args,
                self._replica_config.init_kwargs == other._replica_config.init_kwargs,
                self._replica_config.ray_actor_options
                == other._replica_config.ray_actor_options,
            ]
        )

    def __str__(self):
        return f"Deployment(name={self._name})"

    def __repr__(self):
        return str(self)


def deployment_to_schema(d: Deployment) -> DeploymentSchema:
    """Converts a live deployment object to a corresponding structured schema.

    Args:
        d: Deployment object to convert
    """

    if d.ray_actor_options is not None:
        ray_actor_options_schema = RayActorOptionsSchema.parse_obj(d.ray_actor_options)
    else:
        ray_actor_options_schema = None

    deployment_options = {
        "name": d.name,
        "num_replicas": None
        if d._deployment_config.autoscaling_config
        else d.num_replicas,
        "max_ongoing_requests": d.max_ongoing_requests,
        "max_queued_requests": d.max_queued_requests,
        "user_config": d.user_config,
        "autoscaling_config": d._deployment_config.autoscaling_config,
        "graceful_shutdown_wait_loop_s": d._deployment_config.graceful_shutdown_wait_loop_s,  # noqa: E501
        "graceful_shutdown_timeout_s": d._deployment_config.graceful_shutdown_timeout_s,
        "health_check_period_s": d._deployment_config.health_check_period_s,
        "health_check_timeout_s": d._deployment_config.health_check_timeout_s,
        "ray_actor_options": ray_actor_options_schema,
        "placement_group_strategy": d._replica_config.placement_group_strategy,
        "placement_group_bundles": d._replica_config.placement_group_bundles,
        "max_replicas_per_node": d._replica_config.max_replicas_per_node,
        "logging_config": d._deployment_config.logging_config,
        "request_router_config": d._deployment_config.request_router_config,
    }

    # Let non-user-configured options be set to defaults. If the schema
    # is converted back to a deployment, this lets Serve continue tracking
    # which options were set by the user. Name is a required field in the
    # schema, so it should be passed in explicitly.
    for option in list(deployment_options.keys()):
        if (
            option != "name"
            and option not in d._deployment_config.user_configured_option_names
        ):
            del deployment_options[option]

    # TODO(Sihan) DeploymentConfig num_replicas and auto_config can be set together
    # because internally we use these two field for autoscale and deploy.
    # We can improve the code after we separate the user faced deployment config and
    # internal deployment config.
    return DeploymentSchema(**deployment_options)


def schema_to_deployment(s: DeploymentSchema) -> Deployment:
    """Creates a deployment with parameters specified in schema.

    The returned deployment CANNOT be deployed immediately. It's func_or_class
    value is an empty string (""), which is not a valid import path. The
    func_or_class value must be overwritten with a valid function or class
    before the deployment can be deployed.
    """

    if s.ray_actor_options is DEFAULT.VALUE:
        ray_actor_options = None
    else:
        ray_actor_options = s.ray_actor_options.dict(exclude_unset=True)

    if s.placement_group_bundles is DEFAULT.VALUE:
        placement_group_bundles = None
    else:
        placement_group_bundles = s.placement_group_bundles

    if s.placement_group_strategy is DEFAULT.VALUE:
        placement_group_strategy = None
    else:
        placement_group_strategy = s.placement_group_strategy

    if s.max_replicas_per_node is DEFAULT.VALUE:
        max_replicas_per_node = None
    else:
        max_replicas_per_node = s.max_replicas_per_node

    deployment_config = DeploymentConfig.from_default(
        num_replicas=s.num_replicas,
        user_config=s.user_config,
        max_ongoing_requests=s.max_ongoing_requests,
        max_queued_requests=s.max_queued_requests,
        autoscaling_config=s.autoscaling_config,
        graceful_shutdown_wait_loop_s=s.graceful_shutdown_wait_loop_s,
        graceful_shutdown_timeout_s=s.graceful_shutdown_timeout_s,
        health_check_period_s=s.health_check_period_s,
        health_check_timeout_s=s.health_check_timeout_s,
        logging_config=s.logging_config,
        request_router_config=s.request_router_config,
    )
    deployment_config.user_configured_option_names = (
        s._get_user_configured_option_names()
    )

    replica_config = ReplicaConfig.create(
        deployment_def="",
        init_args=(),
        init_kwargs={},
        ray_actor_options=ray_actor_options,
        placement_group_bundles=placement_group_bundles,
        placement_group_strategy=placement_group_strategy,
        max_replicas_per_node=max_replicas_per_node,
    )

    return Deployment(
        name=s.name,
        deployment_config=deployment_config,
        replica_config=replica_config,
        _internal=True,
    )
