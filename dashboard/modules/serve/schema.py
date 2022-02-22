from enum import Enum
from pydantic import BaseModel, Field, validator
from typing import Union, Tuple, List, Dict
from ray._private.runtime_env.packaging import parse_uri


class SupportedLanguage(str, Enum):
    python36 = "python_3.6"
    python37 = "python_3.7"
    python38 = "python_3.8"
    python39 = "python_3.9"
    python310 = "python_3.10"


class AppConfig(BaseModel):
    init_args: Union[Tuple, List] = Field(
        default=None,
        description=(
            "The application's init_args. Only works with Python 3 applications."
        ),
    )
    init_kwargs: Dict = Field(
        default=None,
        description=(
            "The application's init_args. Only works with Python 3 applications."
        ),
    )
    import_path: str = Field(
        default=None,
        description=(
            "The application's full import path. Should be of the "
            'form "module.submodule_1...submodule_n.'
            'MyClassOrFunction." This is equivalent to '
            '"from module.submodule_1...submodule_n import '
            'MyClassOrFunction". Only works with Python 3 '
            "applications."
        ),
        # This regex checks that there is at least one character, followed by
        # a dot, followed by at least one more character.
        regex=r".+\..+",
    )
    language: SupportedLanguage = Field(
        ..., description="The application's coding language."
    )

    @validator("language")
    def language_supports_specified_attributes(cls, v, values):
        required_attributes = {
            SupportedLanguage.python36: {"import_path"},
            SupportedLanguage.python37: {"import_path"},
            SupportedLanguage.python38: {"import_path"},
            SupportedLanguage.python39: {"import_path"},
            SupportedLanguage.python310: {"import_path"},
        }

        optional_attributes = {
            SupportedLanguage.python36: {"init_args", "init_kwargs"},
            SupportedLanguage.python37: {"init_args", "init_kwargs"},
            SupportedLanguage.python38: {"init_args", "init_kwargs"},
            SupportedLanguage.python39: {"init_args", "init_kwargs"},
            SupportedLanguage.python310: {"init_args", "init_kwargs"},
        }

        for attribute in required_attributes[v]:
            if attribute not in values or values[attribute] is None:
                raise ValueError(
                    f"{attribute} must be specified in the " f"{v.value} language."
                )

        supported_attributes = required_attributes[v].union(optional_attributes[v])
        for attribute, value in values.items():
            if attribute not in supported_attributes and value is not None:
                raise ValueError(
                    f"Got {value} as {attribute}. However, "
                    f"{attribute} is not supported in the "
                    f"{v.value} language."
                )


class DeploymentConfig(BaseModel):
    num_replicas: int = Field(
        default=None,
        description=(
            "The number of processes that handle requests to this "
            "deployment. Uses a default if null."
        ),
        gt=0,
    )
    route_prefix: str = Field(
        default=None,
        description=(
            "Requests to paths under this HTTP path "
            "prefix will be routed to this deployment. When null, no HTTP "
            "endpoint will be created. Routing is done based on "
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
        if v is None:
            return

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


class ReplicaResources(BaseModel):
    num_cpus: float = Field(
        default=None,
        description=(
            "The number of CPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_cpus. Uses a default if null."
        ),
        gt=0,
    )
    num_gpus: float = Field(
        default=None,
        description=(
            "The number of GPUs required by the deployment's "
            "application per replica. This is the same as a ray "
            "actor's num_gpus. Uses a default if null."
        ),
        gt=0,
    )
    memory: float = Field(
        default=None,
        description=(
            "Restrict the heap memory usage of each replica. Uses a default if null."
        ),
        gt=0,
    )
    object_store_memory: float = Field(
        default=None,
        description=(
            "Restrict the object store memory used per replica when "
            "creating objects. Uses a default if null."
        ),
        gt=0,
    )
    resources: Dict = Field(
        default=None, description=("The custom resources required by each replica.")
    )
    accelerator_type: str = Field(
        default=None,
        description=(
            "Forces replicas to run on nodes with the specified accelerator type."
        ),
    )
    max_concurrency: int = Field(default=None, description=(""))
    max_task_retries: int = Field(default=None, description=(""))
    max_pending_calls: int = Field(default=None, description=(""))
    placement_group_bundle_index: int = Field(default=None, description=(""))
    placement_group_capture_child_tasks: bool = Field(default=None, description=(""))


class FullDeploymentConfig(BaseModel):
    name: str = Field(
        ..., description=("Globally-unique name identifying this deployment.")
    )
    runtime_env: dict = Field(
        default=None,
        description=(
            "This deployment's runtime_env. working_dir and "
            "py_modules may contain only remote URIs."
        ),
    )
    namespace: str = Field(
        default="serve", description=("This deployment's namespace.")
    )
    app_config: AppConfig = Field(...)
    deployment_config: DeploymentConfig = Field(...)
    replica_resources: ReplicaResources = Field(...)

    @validator("runtime_env")
    def runtime_env_contains_remote_uris(cls, v):
        # Ensure that all uris in py_modules and working_dir are remote

        if v is None:
            return

        uris = v.get("py_modules", [])
        if "working_dir" in v:
            uris.append(v["working_dir"])

        for uri in uris:
            parse_uri(uri)


class ServeInstanceConfig(BaseModel):
    deployments: List[FullDeploymentConfig] = Field(...)
