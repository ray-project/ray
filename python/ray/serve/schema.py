import json
from pydantic import BaseModel, Field, Extra, root_validator, validator
from typing import Union, List, Dict, Set, Optional
from ray._private.runtime_env.packaging import parse_uri
from ray.serve._private.common import (
    DeploymentStatusInfo,
    ApplicationStatusInfo,
    ApplicationStatus,
    DeploymentStatus,
    StatusOverview,
    DeploymentInfo,
)
from ray.serve._private.constants import DEPLOYMENT_NAME_PREFIX_SEPARATOR
from ray.serve._private.utils import DEFAULT, dict_keys_snake_to_camel_case
from ray.util.annotations import DeveloperAPI, PublicAPI


def _route_prefix_format(cls, v):
    """
    The route_prefix
    1. must start with a / character
    2. must not end with a / character (unless the entire prefix is just /)
    3. cannot contain wildcards (must not have "{" or "}")
    """

    if v is None:
        return v

    if not v.startswith("/"):
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix must start with "/".'
        )
    if len(v) > 1 and v.endswith("/"):
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix '
            'cannot end with "/" unless the '
            'entire prefix is just "/".'
        )
    if "{" in v or "}" in v:
        raise ValueError(
            f'Got "{v}" for route_prefix. Route prefix '
            "cannot contain wildcards, so it cannot "
            'contain "{" or "}".'
        )

    return v


@PublicAPI(stability="beta")
class RayActorOptionsSchema(BaseModel, extra=Extra.forbid):
    runtime_env: dict = Field(
        default={},
        description=(
            "This deployment's runtime_env. working_dir and "
            "py_modules may contain only remote URIs."
        ),
    )
    num_cpus: float = Field(
        default=None,
        description=(
            "The number of CPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_cpus. Uses a default if null."
        ),
        ge=0,
    )
    num_gpus: float = Field(
        default=None,
        description=(
            "The number of GPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_gpus. Uses a default if null."
        ),
        ge=0,
    )
    memory: float = Field(
        default=None,
        description=(
            "Restrict the heap memory usage of each replica. Uses a default if null."
        ),
        ge=0,
    )
    object_store_memory: float = Field(
        default=None,
        description=(
            "Restrict the object store memory used per replica when "
            "creating objects. Uses a default if null."
        ),
        ge=0,
    )
    resources: Dict = Field(
        default={},
        description=("The custom resources required by each replica."),
    )
    accelerator_type: str = Field(
        default=None,
        description=(
            "Forces replicas to run on nodes with the specified accelerator type."
        ),
    )

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris.append(v["working_dir"])

        for uri in uris:
            if uri is not None:
                parse_uri(uri)

        return v


@PublicAPI(stability="beta")
class DeploymentSchema(
    BaseModel, extra=Extra.forbid, allow_population_by_field_name=True
):
    name: str = Field(
        ..., description=("Globally-unique name identifying this deployment.")
    )
    num_replicas: Optional[int] = Field(
        default=DEFAULT.VALUE,
        description=(
            "The number of processes that handle requests to this "
            "deployment. Uses a default if null."
        ),
        gt=0,
    )
    # route_prefix of None means the deployment is not exposed over HTTP.
    route_prefix: Union[str, None] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Requests to paths under this HTTP path "
            "prefix will be routed to this deployment. When null, no HTTP "
            "endpoint will be created. When omitted, defaults to "
            "the deployment's name. Routing is done based on "
            "longest-prefix match, so if you have deployment A with "
            'a prefix of "/a" and deployment B with a prefix of "/a/b", '
            'requests to "/a", "/a/", and "/a/c" go to A and requests '
            'to "/a/b", "/a/b/", and "/a/b/c" go to B. Routes must not '
            'end with a "/" unless they\'re the root (just "/"), which '
            "acts as a catch-all."
        ),
    )
    max_concurrent_queries: int = Field(
        default=DEFAULT.VALUE,
        description=(
            "The max number of pending queries in a single replica. "
            "Uses a default if null."
        ),
        gt=0,
    )
    user_config: Optional[Dict] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Config to pass into this deployment's "
            "reconfigure method. This can be updated dynamically "
            "without restarting replicas"
        ),
    )
    autoscaling_config: Optional[Dict] = Field(
        default=DEFAULT.VALUE,
        description=(
            "Config specifying autoscaling "
            "parameters for the deployment's number of replicas. "
            "If null, the deployment won't autoscale its number of "
            "replicas; the number of replicas will be fixed at "
            "num_replicas."
        ),
    )
    graceful_shutdown_wait_loop_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Duration that deployment replicas will wait until there "
            "is no more work to be done before shutting down. Uses a "
            "default if null."
        ),
        ge=0,
    )
    graceful_shutdown_timeout_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Serve controller waits for this duration before "
            "forcefully killing the replica for shutdown. Uses a "
            "default if null."
        ),
        ge=0,
    )
    health_check_period_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Frequency at which the controller will health check "
            "replicas. Uses a default if null."
        ),
        gt=0,
    )
    health_check_timeout_s: float = Field(
        default=DEFAULT.VALUE,
        description=(
            "Timeout that the controller will wait for a response "
            "from the replica's health check before marking it "
            "unhealthy. Uses a default if null."
        ),
        gt=0,
    )
    ray_actor_options: RayActorOptionsSchema = Field(
        default=DEFAULT.VALUE, description="Options set for each replica actor."
    )

    is_driver_deployment: bool = Field(
        default=DEFAULT.VALUE,
        description="Indicate Whether the deployment is driver deployment "
        "Driver deployments are spawned one per node.",
    )

    @root_validator
    def num_replicas_and_autoscaling_config_mutually_exclusive(cls, values):
        if values.get("num_replicas", None) not in [DEFAULT.VALUE, None] and values.get(
            "autoscaling_config", None
        ) not in [DEFAULT.VALUE, None]:
            raise ValueError(
                "Manually setting num_replicas is not allowed "
                "when autoscaling_config is provided."
            )

        return values

    deployment_schema_route_prefix_format = validator("route_prefix", allow_reuse=True)(
        _route_prefix_format
    )

    def get_user_configured_option_names(self) -> Set[str]:
        """Get set of names for all user-configured options.

        Any field not set to DEFAULT.VALUE is considered a user-configured option.
        """

        return {
            field for field, value in self.dict().items() if value is not DEFAULT.VALUE
        }


def _deployment_info_to_schema(name: str, info: DeploymentInfo) -> DeploymentSchema:
    """Converts a DeploymentInfo object to DeploymentSchema.

    Route_prefix will not be set in the returned DeploymentSchema, since starting in 2.x
    route_prefix is an application-level concept. (This should only be used on the 2.x
    codepath)
    """

    return DeploymentSchema(
        name=name,
        num_replicas=info.deployment_config.num_replicas,
        max_concurrent_queries=info.deployment_config.max_concurrent_queries,
        user_config=info.deployment_config.user_config,
        autoscaling_config=info.deployment_config.autoscaling_config,
        graceful_shutdown_wait_loop_s=(
            info.deployment_config.graceful_shutdown_wait_loop_s
        ),
        graceful_shutdown_timeout_s=info.deployment_config.graceful_shutdown_timeout_s,
        health_check_period_s=info.deployment_config.health_check_period_s,
        health_check_timeout_s=info.deployment_config.health_check_timeout_s,
        ray_actor_options=info.replica_config.ray_actor_options,
        is_driver_deployment=info.is_driver_deployment,
    )


@PublicAPI(stability="beta")
class ServeApplicationSchema(BaseModel, extra=Extra.forbid):
    name: str = Field(
        # TODO(cindy): eventually we should set the default app name to a non-empty
        # string and forbid empty app names.
        default="",
        description=(
            "Application name, the name should be unique within the serve instance"
        ),
    )
    route_prefix: Optional[str] = Field(
        default="/",
        description=(
            "Route prefix for HTTP requests. If not provided, it will use"
            "route_prefix of the ingress deployment. By default, the ingress route"
            "prefix is '/'."
        ),
    )
    import_path: str = Field(
        description=(
            "An import path to a bound deployment node. Should be of the "
            'form "module.submodule_1...submodule_n.'
            'dag_node". This is equivalent to '
            '"from module.submodule_1...submodule_n import '
            'dag_node". Only works with Python '
            "applications. This field is REQUIRED when deploying Serve config "
            "to a Ray cluster."
        ),
    )
    runtime_env: dict = Field(
        default={},
        description=(
            "The runtime_env that the deployment graph will be run in. "
            "Per-deployment runtime_envs will inherit from this. working_dir "
            "and py_modules may contain only remote URIs."
        ),
    )
    host: str = Field(
        default="0.0.0.0",
        description=(
            "Host for HTTP servers to listen on. Defaults to "
            '"0.0.0.0", which exposes Serve publicly. Cannot be updated once '
            "your Serve application has started running. The Serve application "
            "must be shut down and restarted with the new host instead."
        ),
    )
    port: int = Field(
        default=8000,
        description=(
            "Port for HTTP server. Defaults to 8000. Cannot be updated once "
            "your Serve application has started running. The Serve application "
            "must be shut down and restarted with the new port instead."
        ),
    )
    deployments: List[DeploymentSchema] = Field(
        default=[],
        description=("Deployment options that override options specified in the code."),
    )

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris.append(v["working_dir"])

        for uri in uris:
            if uri is not None:
                parse_uri(uri)

        return v

    @validator("import_path")
    def import_path_format_valid(cls, v: str):

        if v is None:
            return

        if ":" in v:
            if v.count(":") > 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may have at most one colon."
                )
            if v.rfind(":") == 0 or v.rfind(":") == len(v) - 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may not start or end with a colon."
                )
            return v
        else:
            if v.count(".") < 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path must contain at least on dot or colon "
                    "separating the module (and potentially submodules) from "
                    'the deployment graph. E.g.: "module.deployment_graph".'
                )
            if v.rfind(".") == 0 or v.rfind(".") == len(v) - 1:
                raise ValueError(
                    f'Got invalid import path "{v}". An '
                    "import path may not start or end with a dot."
                )

        return v

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty app schema dictionary.

        Schema can be used as a representation of an empty Serve application config.
        """

        return {
            "import_path": "",
            "runtime_env": {},
            "deployments": [],
        }

    def kubernetes_dict(self, **kwargs) -> Dict:
        """Returns dictionary in Kubernetes format.

        Dictionary can be yaml-dumped to a Serve config file directly and then
        copy-pasted into a RayService Kubernetes config.

        Args: all kwargs are passed directly into schema's dict() function.
        """

        config = self.dict(**kwargs)
        for idx, deployment in enumerate(config["deployments"]):

            if isinstance(deployment.get("ray_actor_options"), dict):

                # JSON-serialize ray_actor_options' resources dictionary
                if isinstance(deployment["ray_actor_options"].get("resources"), dict):
                    deployment["ray_actor_options"]["resources"] = json.dumps(
                        deployment["ray_actor_options"]["resources"]
                    )

                # JSON-serialize ray_actor_options' runtime_env dictionary
                if isinstance(deployment["ray_actor_options"].get("runtime_env"), dict):
                    deployment["ray_actor_options"]["runtime_env"] = json.dumps(
                        deployment["ray_actor_options"]["runtime_env"]
                    )

                # Convert ray_actor_options' keys
                deployment["ray_actor_options"] = dict_keys_snake_to_camel_case(
                    deployment["ray_actor_options"]
                )

            # JSON-serialize user_config dictionary
            if isinstance(deployment.get("user_config"), dict):
                deployment["user_config"] = json.dumps(deployment["user_config"])

            # Convert deployment's keys
            config["deployments"][idx] = dict_keys_snake_to_camel_case(deployment)

        # Convert top-level runtime_env
        if isinstance(config.get("runtime_env"), dict):
            config["runtime_env"] = json.dumps(config["runtime_env"])

        # Convert top-level option's keys
        config = dict_keys_snake_to_camel_case(config)

        return config

    def prepend_app_name_to_deployment_names(self):
        """Prepend the app name to all deployment names listed in the config."""
        app_config = self.dict(exclude_unset=True)

        if "deployments" in app_config and not self.name == "":
            for idx, deployment in enumerate(app_config["deployments"]):
                deployment["name"] = (
                    self.name + DEPLOYMENT_NAME_PREFIX_SEPARATOR + deployment["name"]
                )
                app_config["deployments"][idx] = deployment

        return ServeApplicationSchema.parse_obj(app_config)

    def remove_app_name_from_deployment_names(self):
        """Remove the app name prefix from all deployment names listed in the config.

        This method should only be called on configs that have been processed internally
        through prepend_app_name_to_deployment_names, which appends the app name prefix
        to every deployment name.
        """
        app_config = self.dict(exclude_unset=True)
        if "deployments" in app_config and not self.name == "":
            for idx, deployment in enumerate(app_config["deployments"]):
                prefix = self.name + DEPLOYMENT_NAME_PREFIX_SEPARATOR
                # This method should not be called on any config that's not processed
                # internally & returned by prepend_app_name_to_deployment_names
                assert deployment["name"].startswith(prefix)

                deployment["name"] = deployment["name"][len(prefix) :]
                app_config["deployments"][idx] = deployment

        return ServeApplicationSchema.parse_obj(app_config)

    def to_deploy_schema(self):
        return ServeDeploySchema(
            host=self.host,
            port=self.port,
            applications=[self],
        )


@PublicAPI(stability="alpha")
class ServeDeploySchema(BaseModel, extra=Extra.forbid):
    host: str = Field(
        default="0.0.0.0",
        description=(
            "Host for HTTP servers to listen on. Defaults to "
            '"0.0.0.0", which exposes Serve publicly. Cannot be updated once '
            "Serve has started running. Serve must be shut down and restarted "
            "with the new host instead."
        ),
    )
    port: int = Field(
        default=8000,
        description=(
            "Port for HTTP server. Defaults to 8000. Cannot be updated once "
            "Serve has started running. Serve must be shut down and restarted "
            "with the new port instead."
        ),
    )
    applications: List[ServeApplicationSchema] = Field(
        default=[],
        description=("The set of Serve applications to run on the Ray cluster."),
    )

    @validator("applications")
    def application_names_unique(cls, v):
        # Ensure there are no duplicate applications listed
        names = [app.name for app in v]
        duplicates = {f'"{name}"' for name in names if names.count(name) > 1}
        if len(duplicates):
            apps_str = ("application " if len(duplicates) == 1 else "applications ") + (
                ", ".join(duplicates)
            )
            raise ValueError(
                f"Found multiple configs for {apps_str}. Please remove all duplicates."
            )
        return v

    @validator("applications")
    def application_routes_unique(cls, v):
        # Ensure each application with a non-null route prefix has unique route prefixes
        routes = [app.route_prefix for app in v if app.route_prefix is not None]
        duplicates = {f'"{route}"' for route in routes if routes.count(route) > 1}
        if len(duplicates):
            routes_str = (
                "route prefix " if len(duplicates) == 1 else "route prefixes "
            ) + (", ".join(duplicates))
            raise ValueError(
                f"Found duplicate applications for {routes_str}. Please ensure each "
                "application's route_prefix is unique."
            )
        return v

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty deploy schema dictionary.

        Schema can be used as a representation of an empty Serve deploy config.
        """

        return {"applications": []}


@PublicAPI(stability="alpha")
class DeploymentDetails(BaseModel, extra=Extra.forbid):
    name: str = Field(description="Deployment name.")
    status: DeploymentStatus = Field(
        description="The current status of the deployment."
    )
    message: str = Field(
        description=(
            "If there are issues with the deployment, this will describe the issue in "
            "more detail."
        )
    )
    deployment_config: DeploymentSchema = Field(
        description=(
            "The set of deployment config options that are currently applied to this "
            "deployment. These options may come from the user's code, config file "
            "options, or Serve default values."
        )
    )

    @validator("deployment_config")
    def deployment_route_prefix_not_set(cls, v: DeploymentSchema):
        # Route prefix should not be set at the deployment level. Deployment-level route
        # prefix is outdated, there should be one route prefix per application
        if "route_prefix" in v.dict(exclude_unset=True):
            raise ValueError(
                "Unexpectedly found a deployment-level route_prefix in the "
                f'deployment_config for deployment "{cls.name}". The route_prefix in '
                "deployment_config within DeploymentDetails should not be set; please "
                "set it at the application level."
            )
        return v


@PublicAPI(stability="alpha")
class ApplicationDetails(BaseModel, extra=Extra.forbid):
    name: str = Field(description="Application name.")
    route_prefix: Optional[str] = Field(
        ...,
        description=(
            "This is the `route_prefix` of the ingress deployment in the application. "
            "Requests to paths under this HTTP path prefix will be routed to this "
            "application. This value may be null if the application is deploying "
            "and app information has not yet fully propagated in the backend; or "
            "if the user explicitly set the prefix to `None`, so the application isn't "
            "exposed over HTTP. Routing is done based on longest-prefix match, so if "
            'you have deployment A with a prefix of "/a" and deployment B with a '
            'prefix of "/a/b", requests to "/a", "/a/", and "/a/c" go to A and '
            'requests to "/a/b", "/a/b/", and "/a/b/c" go to B. Routes must not end '
            'with a "/" unless they\'re the root (just "/"), which acts as a catch-all.'
        ),
    )
    docs_path: Optional[str] = Field(
        ...,
        description=(
            "The path at which the docs for this application is served, for instance "
            "the `docs_url` for FastAPI-integrated applications."
        ),
    )
    status: ApplicationStatus = Field(
        description="The current status of the application."
    )
    message: str = Field(
        description="A message that gives more insight into the application status."
    )
    last_deployed_time_s: float = Field(
        description="The time at which the application was deployed."
    )
    deployed_app_config: ServeApplicationSchema = Field(
        description=(
            "The exact copy of the application config that was submitted to the "
            "cluster. This will include all of, and only, the options that were "
            "explicitly specified in the submitted config. Default values for "
            "unspecified options will not be displayed, and deployments that are part "
            "of the application but unlisted in the config will also not be displayed. "
            "Note that default values for unspecified options are applied to the "
            "cluster under the hood, and deployments that were unlisted will still be "
            "deployed. This config simply avoids cluttering with unspecified fields "
            "for readability."
        )
    )
    deployments: Dict[str, DeploymentDetails] = Field(
        description="Details about the deployments in this application."
    )

    application_details_route_prefix_format = validator(
        "route_prefix", allow_reuse=True
    )(_route_prefix_format)


@PublicAPI(stability="alpha")
class ServeInstanceDetails(BaseModel, extra=Extra.forbid):
    host: Optional[str] = Field(
        description="The host on which the HTTP server is listening for requests."
    )
    port: Optional[int] = Field(
        description="The port on which the HTTP server is listening for requests."
    )
    applications: Dict[str, ApplicationDetails] = Field(
        description="Details about all live applications running on the cluster."
    )

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Empty Serve instance details dictionary.

        Represents no Serve instance running on the cluster.
        """

        return {"applications": {}}


@PublicAPI(stability="beta")
class ServeStatusSchema(BaseModel, extra=Extra.forbid):
    name: str = Field(description="Application name", default="")
    app_status: ApplicationStatusInfo = Field(
        ...,
        description=(
            "Describes if the Serve application is DEPLOYING, if the "
            "DEPLOY_FAILED, or if the app is RUNNING. Includes a timestamp of "
            "when the application was deployed."
        ),
    )
    deployment_statuses: List[DeploymentStatusInfo] = Field(
        default=[],
        description=(
            "List of statuses for all the deployments running in this Serve "
            "application. Each status contains the deployment name, the "
            "deployment's status, and a message providing extra context on "
            "the status."
        ),
    )

    @staticmethod
    def get_empty_schema_dict() -> Dict:
        """Returns an empty status schema dictionary.

        Schema represents Serve status for a Ray cluster where Serve hasn't
        started yet.
        """

        return {
            "app_status": {
                "status": ApplicationStatus.NOT_STARTED.value,
                "message": "",
                "deployment_timestamp": 0,
            },
            "deployment_statuses": [],
        }


@DeveloperAPI
def serve_status_to_schema(serve_status: StatusOverview) -> ServeStatusSchema:

    return ServeStatusSchema(
        name=serve_status.name,
        app_status=serve_status.app_status,
        deployment_statuses=serve_status.deployment_statuses,
    )
