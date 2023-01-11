import json
from pydantic import BaseModel, Field, Extra, root_validator, validator
from typing import Union, List, Dict, Set, Optional
from ray._private.runtime_env.packaging import parse_uri
from ray.serve._private.common import (
    DeploymentStatusInfo,
    ApplicationStatusInfo,
    ApplicationStatus,
    StatusOverview,
)
from ray.serve._private.utils import DEFAULT, dict_keys_snake_to_camel_case
from ray.util.annotations import DeveloperAPI, PublicAPI


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

    @validator("route_prefix")
    def route_prefix_format(cls, v):
        """
        The route_prefix
        1. must start with a / character
        2. must not end with a / character (unless the entire prefix is just /)
        3. cannot contain wildcards (must not have "{" or "}")
        """

        # route_prefix of None means the deployment is not exposed
        # over HTTP.
        if v is None or v == DEFAULT.VALUE:
            return v

        if len(v) < 1 or v[0] != "/":
            raise ValueError(
                f'Got "{v}" for route_prefix. Route prefix ' 'must start with "/".'
            )
        if v[-1] == "/" and len(v) > 1:
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

    def get_user_configured_option_names(self) -> Set[str]:
        """Get set of names for all user-configured options.

        Any field not set to DEFAULT.VALUE is considered a user-configured option.
        """

        return {
            field for field, value in self.dict().items() if value is not DEFAULT.VALUE
        }


@PublicAPI(stability="beta")
class ServeApplicationSchema(BaseModel, extra=Extra.forbid):
    app_name: str = Field(
        default="",
        description=(
            "Application name, the name should be unique within the serve instance"
        ),
    )
    route_prefix: str = Field(
        default="/",
        description=(
            "route prefix to route different requests to different app. "
            "If not set, it will by default to use ingress deployment route prefix. "
            "By default, the route prefix is '/' "
        ),
    )
    import_path: str = Field(
        default=None,
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

        Schema can be used as a representation of an empty Serve config.
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


@PublicAPI(stability="beta")
class ServeStatusSchema(BaseModel, extra=Extra.forbid):
    app_name: str = Field(description="Application name", default="")
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
        app_name=serve_status.app_name,
        app_status=serve_status.app_status,
        deployment_statuses=serve_status.deployment_statuses,
    )
