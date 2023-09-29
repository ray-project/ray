import inspect
import json
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from google.protobuf.json_format import MessageToDict
from pydantic import Field, PrivateAttr

from ray import cloudpickle
from ray._private import ray_option_utils
from ray._private.serialization import pickle_dumps
from ray._private.utils import resources_from_ray_options
from ray.serve._private.constants import MAX_REPLICAS_PER_NODE_MAX_VALUE
from ray.serve._private.utils import DEFAULT, DeploymentOptionUpdateType
from ray.serve.config import AutoscalingConfig, BaseDeploymentModel
from ray.serve.generated.serve_pb2 import AutoscalingConfig as AutoscalingConfigProto
from ray.serve.generated.serve_pb2 import DeploymentConfig as DeploymentConfigProto
from ray.serve.generated.serve_pb2 import DeploymentLanguage
from ray.serve.generated.serve_pb2 import ReplicaConfig as ReplicaConfigProto


def _needs_pickle(deployment_language: DeploymentLanguage, is_cross_language: bool):
    """From Serve client API's perspective, decide whether pickling is needed."""
    if deployment_language == DeploymentLanguage.PYTHON and not is_cross_language:
        # Python client deploying Python replicas.
        return True
    elif deployment_language == DeploymentLanguage.JAVA and is_cross_language:
        # Python client deploying Java replicas,
        # using xlang serialization via cloudpickle.
        return True
    else:
        return False


class InternalDeploymentConfig(BaseDeploymentModel):
    """Internal datastructure wrapping config options for a deployment."""

    num_replicas: Optional[int] = 1
    _resource_dict: Dict = PrivateAttr(default=None)

    # This flag is used to let replica know they are deployed from
    # a different language.
    is_cross_language: bool = False

    # This flag is used to let controller know which language does
    # the deploymnent use.
    deployment_language: Any = DeploymentLanguage.PYTHON

    version: Optional[str] = Field(
        default=None,
        update_type=DeploymentOptionUpdateType.HeavyWeight,
    )

    # Contains the names of deployment options manually set by the user
    user_configured_option_names: Set[str] = set()

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        self._resource_dict = resources_from_ray_options(self.ray_actor_options.dict())
        # ray_option_utils.validate_actor_options(self.ray_actor_options, in_options=True)

    # @validator("user_config", always=True)
    # def user_config_json_serializable(cls, v):
    #     if isinstance(v, bytes):
    #         return v
    #     if v is not None:
    #         try:
    #             json.dumps(v)
    #         except TypeError as e:
    #             raise ValueError(f"user_config is not JSON-serializable: {str(e)}.")

    #     return v

    def needs_pickle(self):
        return _needs_pickle(self.deployment_language, self.is_cross_language)

    def to_proto(self):
        data = self.dict()
        if data.get("user_config") is not None:
            if self.needs_pickle():
                data["user_config"] = cloudpickle.dumps(data["user_config"])
        if data.get("autoscaling_config"):
            data["autoscaling_config"] = AutoscalingConfigProto(
                **data["autoscaling_config"]
            )
        data["user_configured_option_names"] = list(
            data["user_configured_option_names"]
        )
        del data["ray_actor_options"]
        del data["placement_group_bundles"]
        del data["placement_group_strategy"]
        del data["max_replicas_per_node"]
        print("to proto data", data)
        return DeploymentConfigProto(**data)

    def to_proto_bytes(self):
        return self.to_proto().SerializeToString()

    @classmethod
    def from_proto(cls, proto: DeploymentConfigProto):
        data = MessageToDict(
            proto,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
        )
        print("from proto data", data)
        if "user_config" in data:
            if data["user_config"] != "":
                deployment_language = (
                    data["deployment_language"]
                    if "deployment_language" in data
                    else DeploymentLanguage.PYTHON
                )
                is_cross_language = (
                    data["is_cross_language"] if "is_cross_language" in data else False
                )
                needs_pickle = _needs_pickle(deployment_language, is_cross_language)
                if needs_pickle:
                    data["user_config"] = cloudpickle.loads(proto.user_config)
                else:
                    # after MessageToDict, bytes data has been deal with base64
                    data["user_config"] = proto.user_config
            else:
                data["user_config"] = None
        if "autoscaling_config" in data:
            if not data["autoscaling_config"].get("upscale_smoothing_factor"):
                data["autoscaling_config"]["upscale_smoothing_factor"] = None
            if not data["autoscaling_config"].get("downscale_smoothing_factor"):
                data["autoscaling_config"]["downscale_smoothing_factor"] = None
            data["autoscaling_config"] = AutoscalingConfig(**data["autoscaling_config"])
        if "version" in data:
            if data["version"] == "":
                data["version"] = None
        if "user_configured_option_names" in data:
            data["user_configured_option_names"] = set(
                data["user_configured_option_names"]
            )
        return cls(**data)

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes):
        proto = DeploymentConfigProto.FromString(proto_bytes)
        return cls.from_proto(proto)

    @classmethod
    def create_from_default(cls, **kwargs):
        """Creates a default DeploymentConfig and overrides it with kwargs.

        Ignores any kwargs set to DEFAULT.VALUE.

        Raises:
            TypeError: when a keyword that's not an argument to the class is
                passed in.
        """

        # config = cls()

        # valid_config_options = set(config.dict().keys())

        # # Friendly error if a non-DeploymentConfig kwarg was passed in
        # for key, val in kwargs.items():
        #     if key not in valid_config_options:
        #         raise TypeError(
        #             f'Got invalid Deployment config option "{key}" '
        #             f"(with value {val}) as keyword argument. All Deployment "
        #             "config options must come from this list: "
        #             f"{list(valid_config_options)}."
        #         )

        # for key, val in kwargs.items():
        #     config.__setattr__(key, val)

        kwargs = {key: val for key, val in kwargs.items() if val != DEFAULT.VALUE}
        return cls(**kwargs)

    @classmethod
    def create_from(cls, config, **kwargs):
        """Creates a default DeploymentConfig and overrides it with kwargs.

        Ignores any kwargs set to DEFAULT.VALUE.

        Raises:
            TypeError: when a keyword that's not an argument to the class is
                passed in.
        """
        valid_config_options = set(config.dict().keys())

        # Friendly error if a non-DeploymentConfig kwarg was passed in
        for key, val in kwargs.items():
            if key not in valid_config_options:
                raise TypeError(
                    f'Got invalid Deployment config option "{key}" '
                    f"(with value {val}) as keyword argument. All Deployment "
                    "config options must come from this list: "
                    f"{list(valid_config_options)}."
                )

        kwargs = {key: val for key, val in kwargs.items() if val != DEFAULT.VALUE}

        for key, val in kwargs.items():
            config.__setattr__(key, val)

        return config


class ReplicaInitConfig:
    """Internal datastructure for initializing replicas.

    Provides three main properties (see property docstrings for more info):
        deployment_def: the code, or a reference to the code, that this
            replica should run.
        init_args: the deployment_def's init_args.
        init_kwargs: the deployment_def's init_kwargs.

    Offers a serialized equivalent (e.g. serialized_deployment_def) for
    deployment_def, init_args, and init_kwargs. Deserializes these properties
    when they're first accessed, if they were not passed in directly through
    create().

    Use the classmethod create() to make a ReplicaInitConfig with the
    deserialized properties.
    """

    def __init__(
        self,
        deployment_def_name: str,
        serialized_deployment_def: bytes,
        serialized_init_args: bytes,
        serialized_init_kwargs: bytes,
        needs_pickle: bool = True,
    ):
        """Construct a ReplicaConfig with serialized properties.

        All parameters are required. See classmethod create() for defaults.
        """
        self.deployment_def_name = deployment_def_name

        # Store serialized versions of code properties.
        self.serialized_deployment_def = serialized_deployment_def
        self.serialized_init_args = serialized_init_args
        self.serialized_init_kwargs = serialized_init_kwargs

        # Deserialize properties when first accessed. See @property methods.
        self._deployment_def = None
        self._init_args = None
        self._init_kwargs = None

        self.needs_pickle = needs_pickle

    @property
    def deployment_def(self) -> Union[Callable, str]:
        """The code, or a reference to the code, that this replica runs."""
        if self._deployment_def is None:
            if self.needs_pickle:
                self._deployment_def = cloudpickle.loads(self.serialized_deployment_def)
            else:
                self._deployment_def = self.serialized_deployment_def.decode(
                    encoding="utf-8"
                )

        return self._deployment_def

    @property
    def init_args(self) -> Optional[Union[Tuple[Any], bytes]]:
        """The init_args for a Python class."""
        if self._init_args is None:
            if self.needs_pickle:
                self._init_args = cloudpickle.loads(self.serialized_init_args)
            else:
                self._init_args = self.serialized_init_args

        return self._init_args

    @property
    def init_kwargs(self) -> Optional[Tuple[Any]]:
        """The init_kwargs for a Python class."""

        if self._init_kwargs is None:
            self._init_kwargs = cloudpickle.loads(self.serialized_init_kwargs)

        return self._init_kwargs

    @classmethod
    def create(
        cls,
        deployment_def: Union[Callable, str],
        init_args: Optional[Tuple[Any]] = None,
        init_kwargs: Optional[Dict[Any, Any]] = None,
        deployment_def_name: Optional[str] = None,
    ):
        """Create a ReplicaInitConfig from deserialized parameters."""

        if not callable(deployment_def) and not isinstance(deployment_def, str):
            raise TypeError("@serve.deployment must be called on a class or function.")

        if not (init_args is None or isinstance(init_args, (tuple, list))):
            raise TypeError("init_args must be a tuple.")

        if not (init_kwargs is None or isinstance(init_kwargs, dict)):
            raise TypeError("init_kwargs must be a dict.")

        if inspect.isfunction(deployment_def):
            if init_args:
                raise ValueError("init_args not supported for function deployments.")
            elif init_kwargs:
                raise ValueError("init_kwargs not supported for function deployments.")

        if not isinstance(deployment_def, (Callable, str)):
            raise TypeError(
                f'Got invalid type "{type(deployment_def)}" for '
                "deployment_def. Expected deployment_def to be a "
                "class, function, or string."
            )
        # Set defaults
        if init_args is None:
            init_args = ()
        if init_kwargs is None:
            init_kwargs = {}
        if deployment_def_name is None:
            if isinstance(deployment_def, str):
                deployment_def_name = deployment_def
            else:
                deployment_def_name = deployment_def.__name__

        config = cls(
            deployment_def_name,
            pickle_dumps(
                deployment_def,
                f"Could not serialize the deployment {repr(deployment_def)}",
            ),
            pickle_dumps(init_args, "Could not serialize the deployment init args"),
            pickle_dumps(init_kwargs, "Could not serialize the deployment init kwargs"),
        )

        config._deployment_def = deployment_def
        config._init_args = init_args
        config._init_kwargs = init_kwargs

        return config

    @classmethod
    def from_proto(cls, proto: ReplicaConfigProto, needs_pickle: bool = True):
        return ReplicaInitConfig(
            proto.deployment_def_name,
            proto.deployment_def,
            proto.init_args if proto.init_args != b"" else None,
            proto.init_kwargs if proto.init_kwargs != b"" else None,
        )

    @classmethod
    def from_proto_bytes(cls, proto_bytes: bytes, needs_pickle: bool = True):
        proto = ReplicaConfigProto.FromString(proto_bytes)
        return cls.from_proto(proto, needs_pickle)

    def to_proto(self):
        return ReplicaConfigProto(
            deployment_def_name=self.deployment_def_name,
            deployment_def=self.serialized_deployment_def,
            init_args=self.serialized_init_args,
            init_kwargs=self.serialized_init_kwargs,
        )

    def to_proto_bytes(self):
        return self.to_proto().SerializeToString()
