from pydantic import BaseModel, Field, Extra, root_validator, validator
from typing import Union, Tuple, List, Dict
from ray._private.runtime_env.packaging import parse_uri
from ray.serve.common import DeploymentStatus, DeploymentStatusInfo
from ray.serve.utils import DEFAULT


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
        default={}, description=("The custom resources required by each replica.")
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


class DeploymentSchema(BaseModel, extra=Extra.forbid):
    name: str = Field(
        ..., description=("Globally-unique name identifying this deployment.")
    )
    import_path: str = Field(
        default=None,
        description=(
            "The application's full import path. Should be of the "
            'form "module.submodule_1...submodule_n.'
            'MyClassOrFunction." This is equivalent to '
            '"from module.submodule_1...submodule_n import '
            'MyClassOrFunction". Only works with Python '
            "applications."
        ),
        # This regex checks that there is at least one character, followed by
        # a dot, followed by at least one more character.
        regex=r".+\..+",
    )
    init_args: Union[Tuple, List] = Field(
        default=None,
        description=(
            "The application's init_args. Only works with Python applications."
        ),
    )
    init_kwargs: Dict = Field(
        default=None,
        description=(
            "The application's init_args. Only works with Python applications."
        ),
    )
    num_replicas: int = Field(
        default=None,
        description=(
            "The number of processes that handle requests to this "
            "deployment. Uses a default if null."
        ),
        gt=0,
    )
    route_prefix: Union[str, None, DEFAULT] = Field(
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
        default=None,
        description=(
            "The max number of pending queries in a single replica. "
            "Uses a default if null."
        ),
        gt=0,
    )
    user_config: Dict = Field(
        default=None,
        description=(
            "[EXPERIMENTAL] Config to pass into this deployment's "
            "reconfigure method. This can be updated dynamically "
            "without restarting replicas"
        ),
    )
    autoscaling_config: Dict = Field(
        default=None,
        description=(
            "[EXPERIMENTAL] Config specifying autoscaling "
            "parameters for the deployment's number of replicas. "
            "If null, the deployment won't autoscale its number of "
            "replicas; the number of replicas will be fixed at "
            "num_replicas."
        ),
    )
    graceful_shutdown_wait_loop_s: float = Field(
        default=None,
        description=(
            "Duration that deployment replicas will wait until there "
            "is no more work to be done before shutting down. Uses a "
            "default if null."
        ),
        ge=0,
    )
    graceful_shutdown_timeout_s: float = Field(
        default=None,
        description=(
            "Serve controller waits for this duration before "
            "forcefully killing the replica for shutdown. Uses a "
            "default if null."
        ),
        ge=0,
    )
    health_check_period_s: float = Field(
        default=None,
        description=(
            "Frequency at which the controller will health check "
            "replicas. Uses a default if null."
        ),
        gt=0,
    )
    health_check_timeout_s: float = Field(
        default=None,
        description=(
            "Timeout that the controller will wait for a response "
            "from the replica's health check before marking it "
            "unhealthy. Uses a default if null."
        ),
        gt=0,
    )
    ray_actor_options: RayActorOptionsSchema = Field(
        default=None, description="Options set for each replica actor."
    )

    @root_validator
    def application_sufficiently_specified(cls, values):
        """
        Some application information, such as the path to the function or class
        must be specified. Additionally, some attributes only work in specific
        languages (e.g. init_args and init_kwargs make sense in Python but not
        Java). Specifying attributes that belong to different languages is
        invalid.
        """

        # Ensure that an application path is set
        application_paths = {"import_path"}

        specified_path = None
        for path in application_paths:
            if path in values and values[path] is not None:
                specified_path = path

        if specified_path is None:
            raise ValueError(
                "A path to the application's class or function must be specified."
            )

        # Ensure that only attributes belonging to the application path's
        # language are specified.

        # language_attributes contains all attributes in this schema related to
        # the application's language
        language_attributes = {"import_path", "init_args", "init_kwargs"}

        # corresponding_attributes maps application_path attributes to all the
        # attributes that may be set in that path's language
        corresponding_attributes = {
            # Python
            "import_path": {"import_path", "init_args", "init_kwargs"}
        }

        possible_attributes = corresponding_attributes[specified_path]
        for attribute in values:
            if (
                attribute not in possible_attributes
                and attribute in language_attributes
            ):
                raise ValueError(
                    f'Got "{values[specified_path]}" for '
                    f"{specified_path} and {values[attribute]} "
                    f"for {attribute}. {specified_path} and "
                    f"{attribute} do not belong to the same "
                    f"language and cannot be specified at the "
                    f"same time. Expected one of these to be "
                    f"null."
                )

        return values

    @root_validator
    def num_replicas_and_autoscaling_config_mutually_exclusive(cls, values):
        if (
            values.get("num_replicas", None) is not None
            and values.get("autoscaling_config", None) is not None
        ):
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


class ServeApplicationSchema(BaseModel, extra=Extra.forbid):
    deployments: List[DeploymentSchema] = Field(...)


class DeploymentStatusSchema(BaseModel, extra=Extra.forbid):
    name: str = Field(..., description="The deployment's name.")
    status: DeploymentStatus = Field(
        default=None, description="The deployment's status."
    )
    message: str = Field(
        default="", description="Information about the deployment's status."
    )


class ServeApplicationStatusSchema(BaseModel, extra=Extra.forbid):
    statuses: List[DeploymentStatusSchema] = Field(...)


def status_info_to_schema(
    deployment_name: str, status_info: Union[DeploymentStatusInfo, Dict]
) -> DeploymentStatusSchema:
    if isinstance(status_info, DeploymentStatusInfo):
        return DeploymentStatusSchema(
            name=deployment_name, status=status_info.status, message=status_info.message
        )
    elif isinstance(status_info, dict):
        return DeploymentStatusSchema(
            name=deployment_name,
            status=status_info["status"],
            message=status_info["message"],
        )
    else:
        raise TypeError(
            f"Got {type(status_info)} as status_info's "
            "type. Expected status_info to be either a "
            "DeploymentStatusInfo or a dictionary."
        )


def serve_application_status_to_schema(
    status_infos: Dict[str, Union[DeploymentStatusInfo, Dict]]
) -> ServeApplicationStatusSchema:
    schemas = [
        status_info_to_schema(deployment_name, status_info)
        for deployment_name, status_info in status_infos.items()
    ]
    return ServeApplicationStatusSchema(statuses=schemas)
