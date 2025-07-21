import os
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import pydantic
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    PrivateAttr,
    field_validator,
    model_validator,
)

import ray
import ray.util.accelerators.accelerators as accelerators
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.utils.cloud_utils import (
    CloudMirrorConfig,
    is_remote_path,
)
from ray.llm._internal.common.utils.import_utils import try_import
from ray.llm._internal.serve.configs.constants import (
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
    DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
    ENABLE_WORKER_PROCESS_SETUP_HOOK,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
)
from ray.llm._internal.serve.observability.logging import get_logger
from ray.serve._private.config import DeploymentConfig

transformers = try_import("transformers")


GPUType = Enum("GPUType", vars(accelerators))
ModelT = TypeVar("ModelT", bound=BaseModel)


logger = get_logger(__name__)


class ServeMultiplexConfig(BaseModelExtended):
    max_num_models_per_replica: PositiveInt = Field(
        ..., description="The maximum number of models to be loaded on each replica."
    )
    download_timeout_s: Optional[float] = Field(
        DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
        description="How much time the download subprocess has to download a single LoRA before a timeout. None means no timeout.",
    )
    max_download_tries: int = Field(
        DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
        description="The maximum number of download retries.",
    )


class InputModality(str, Enum):
    text = "text"
    image = "image"


class LLMEngine(str, Enum):
    """Enum that represents an LLMEngine."""

    vLLM = "vLLM"


class LoraConfig(BaseModelExtended):
    dynamic_lora_loading_path: Optional[str] = Field(
        default=None,
        description="Cloud storage path where LoRA adapter weights are stored.",
    )
    max_num_adapters_per_replica: PositiveInt = Field(
        default=16,
        description="The maximum number of adapters load on each replica.",
    )
    download_timeout_s: Optional[float] = Field(
        DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
        description=(
            "How much time the download subprocess has to download a single "
            "LoRA before a timeout. None means no timeout."
        ),
    )
    max_download_tries: int = Field(
        DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
        description="The maximum number of download retries.",
    )

    @field_validator("dynamic_lora_loading_path")
    def validate_dynamic_lora_loading_path(cls, value: Optional[str]):
        if value is None:
            return value

        assert is_remote_path(value), (
            "Only AWS S3 and Google Cloud Storage are supported. The "
            'dynamic_lora_loading_path must start with "s3://" or "gs://". '
            f'Got "{value}" instead.'
        )
        return value.rstrip("/")


class ModelLoadingConfig(BaseModelExtended):
    model_id: str = Field(
        description="The ID that should be used by end users to access this model.",
    )
    model_source: Optional[Union[str, CloudMirrorConfig]] = Field(
        default=None,
        description=(
            "Where to obtain the model weights from. "
            "Should be a HuggingFace model ID, S3 mirror config, GCS mirror config, "
            "or a local path. When omitted, defaults to the model_id as a "
            "HuggingFace model ID."
        ),
    )
    tokenizer_source: Optional[str] = Field(
        default=None,
        description=(
            "Where to obtain the tokenizer from. If None, tokenizer is "
            "obtained from the model source. Only HuggingFace IDs are "
            "supported for now."
        ),
    )


EngineConfigType = Union[None, "VLLMEngineConfig"]  # noqa: F821


class LLMConfig(BaseModelExtended):
    # model_config is a Pydantic setting. This setting merges with
    # model_configs in parent classes.
    model_config = ConfigDict(
        extra="forbid",
    )

    runtime_env: Optional[Dict[str, Any]] = Field(
        None,
        description=(
            "The runtime_env to use for the model deployment replica "
            "and the engine workers."
        ),
    )

    model_loading_config: ModelLoadingConfig = Field(
        description="The settings for how to download and expose the model."
    )

    llm_engine: str = Field(
        default=LLMEngine.vLLM.value,
        description=f"The LLMEngine that should be used to run the model. Only the following values are supported: {str([t.value for t in LLMEngine])}",
    )

    engine_kwargs: Dict[str, Any] = Field(
        default={},
        description=(
            "Additional keyword arguments for the engine. In case of vLLM, "
            "this will include all the configuration knobs they provide out "
            "of the box, except for tensor-parallelism which is set "
            "automatically from Ray Serve configs."
        ),
    )

    resources_per_bundle: Optional[Dict[str, float]] = Field(
        default=None,
        description="This will override the default resource bundles for placement groups. "
        "You can specify a custom device label e.g. {'NPU': 1}. "
        "The default resource bundle for LLM Stage is always a GPU resource i.e. {'GPU': 1}.",
    )

    accelerator_type: Optional[str] = Field(
        default=None,
        description=f"The type of accelerator runs the model on. Only the following values are supported: {str([t.value for t in GPUType])}",
    )

    lora_config: Optional[LoraConfig] = Field(
        default=None, description="Settings for LoRA adapter."
    )

    deployment_config: Dict[str, Any] = Field(
        default_factory=dict,
        description="""
            The Ray @server.deployment options.
            Supported fields are:
            `name`, `num_replicas`, `ray_actor_options`, `max_ongoing_requests`,
            `autoscaling_config`, `max_queued_requests`, `user_config`,
            `health_check_period_s`, `health_check_timeout_s`,
            `graceful_shutdown_wait_loop_s`, `graceful_shutdown_timeout_s`,
            `logging_config`.
            For more details, see the `Ray Serve Documentation <https://docs.ray.io/en/latest/serve/configure-serve-deployment.html>`_.
        """,
    )

    experimental_configs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Experimental configurations for Ray Serve LLM. This is a "
        "dictionary of key-value pairs. Current supported keys are:\n"
        "- `stream_batching_interval_ms`: Ray Serve LLM batches streaming "
        "requests together. This config decides how long to wait for the "
        "batch before processing the requests. Defaults to "
        f"{MODEL_RESPONSE_BATCH_TIMEOUT_MS}.\n"
        "- `num_router_replicas`: The number of replicas for the router. Ray "
        "Serve will take the max amount all the replicas. Default would be 2 "
        "router replicas per model replica.\n",
    )

    log_engine_metrics: Optional[bool] = Field(
        False,
        description="Enable additional engine metrics via Ray Prometheus port. Only compatible with V1 vLLM engine. NOTE: once v1 is fully rolled out, we will remove this flag and turn it on by default.",
    )

    _supports_vision: bool = PrivateAttr(False)
    _model_architecture: str = PrivateAttr("UNSPECIFIED")
    _engine_config: EngineConfigType = PrivateAttr(None)

    def _infer_supports_vision(self, model_id_or_path: str) -> None:
        """Called in llm node initializer together with other transformers calls. It
        loads the model config from huggingface and sets the supports_vision
        attribute based on whether the config has `vision_config`. All LVM models has
        `vision_config` setup.
        """
        hf_config = transformers.PretrainedConfig.from_pretrained(model_id_or_path)
        self._supports_vision = hasattr(hf_config, "vision_config")

    def _set_model_architecture(
        self,
        model_id_or_path: Optional[str] = None,
        model_architecture: Optional[str] = None,
    ) -> None:
        """Called in llm node initializer together with other transformers calls. It
        loads the model config from huggingface and sets the model_architecture
        attribute based on whether the config has `architectures`.
        """
        if model_id_or_path:
            hf_config = transformers.PretrainedConfig.from_pretrained(model_id_or_path)
            if hasattr(hf_config, "architectures") and hf_config.architectures:
                self._model_architecture = hf_config.architectures[0]

        if model_architecture:
            self._model_architecture = model_architecture

    def apply_checkpoint_info(
        self, model_id_or_path: str, trust_remote_code: bool = False
    ) -> None:
        """Apply the checkpoint info to the model config."""
        self._infer_supports_vision(model_id_or_path)
        self._set_model_architecture(model_id_or_path)

    @property
    def supports_vision(self) -> bool:
        return self._supports_vision

    @property
    def model_architecture(self) -> str:
        return self._model_architecture

    @property
    def input_modality(self) -> str:
        """Returns the input modality of the model. There could be more types in the
        future. Right now assumes if the model doesn't support version, it'll be text.
        """
        if self.supports_vision:
            return InputModality.image.value

        return InputModality.text.value

    @property
    def model_id(self) -> str:
        return self.model_loading_config.model_id

    @property
    def max_request_context_length(self) -> Optional[int]:
        return self.engine_kwargs.get("max_model_len")

    @field_validator("accelerator_type")
    def validate_accelerator_type(cls, value: Optional[str]):
        if value is None:
            return value

        # Ensure A10 is converted to A10G.
        if value == "A10":
            value = "A10G"

        if value not in [t.value for t in GPUType]:
            raise ValueError(f"Unsupported accelerator type: {value}")

        return value

    @field_validator("llm_engine")
    def validate_llm_engine(cls, value: str) -> str:
        """Validates the llm_engine string value."""
        try:
            # Validate the engine
            LLMEngine(value)
        except ValueError as e:
            raise ValueError(f"Unsupported engine: {value}") from e
        return value

    @field_validator("deployment_config")
    def validate_deployment_config(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """Validates the deployment config dictionary."""
        try:
            DeploymentConfig(**value)
        except Exception as e:
            raise ValueError(f"Invalid deployment config: {value}") from e

        return value

    @model_validator(mode="after")
    def _check_log_stats_with_metrics(self):
        # Require disable_log_stats is not set to True when log_engine_metrics is enabled.
        if self.log_engine_metrics and self.engine_kwargs.get("disable_log_stats"):
            raise ValueError(
                "disable_log_stats cannot be set to True when log_engine_metrics is enabled. "
                "Engine metrics require log stats to be enabled."
            )

        return self

    def multiplex_config(self) -> ServeMultiplexConfig:
        multiplex_config = None
        if self.lora_config:
            multiplex_config = ServeMultiplexConfig(
                max_num_models_per_replica=self.lora_config.max_num_adapters_per_replica,
                download_timeout_s=self.lora_config.download_timeout_s,
                max_download_tries=self.lora_config.max_download_tries,
            )
        return multiplex_config

    def get_engine_config(self) -> EngineConfigType:
        """Returns the engine config for the given LLM config.

        LLMConfig not only has engine config but also deployment config, etc.
        """
        # Note (genesu): This is important that we cache the engine config as the
        # `hf_model_id` attribute on the engine config will be set based on whether
        #  the model is downloaded from a remote storage and will be set to the
        #  local path of the model. This is important for vLLM not going to Hugging
        #  Face to download the model again after it's already downloaded during node
        #  initialization step.
        if self._engine_config:
            return self._engine_config

        if self.llm_engine == LLMEngine.vLLM:
            from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
                VLLMEngineConfig,
            )

            self._engine_config = VLLMEngineConfig.from_llm_config(self)
        else:
            # Note (genesu): This should never happen because we validate the engine
            # in the config.
            raise ValueError(f"Unsupported engine: {self.llm_engine}")

        return self._engine_config

    def _set_deployment_placement_options(self) -> Dict[str, Any]:
        deployment_config = self.deployment_config
        engine_config = self.get_engine_config()

        ray_actor_options = deployment_config.get("ray_actor_options", {})
        deployment_config["ray_actor_options"] = ray_actor_options

        replica_actor_resources = {
            "CPU": ray_actor_options.get("num_cpus", 1),
            "GPU": ray_actor_options.get("num_gpus", 0),
            **ray_actor_options.get("resources", {}),
        }
        if "memory" in ray_actor_options:
            replica_actor_resources["memory"] = ray_actor_options["memory"]

        if (
            "placement_group_bundles" in deployment_config
            or "placement_group_strategy" in deployment_config
        ):
            raise ValueError(
                "placement_group_bundles and placement_group_strategy must not be specified in deployment_config. "
                "Use scaling_config to configure replica placement group."
            )

        try:
            bundles = engine_config.placement_bundles
        except ValueError:
            # May happen if all bundles are empty.
            bundles = []

        bundles = [replica_actor_resources] + bundles
        deployment_config.update(
            {
                "placement_group_bundles": bundles,
                "placement_group_strategy": engine_config.placement_strategy,
            }
        )

        return deployment_config

    def _get_deployment_name(self) -> str:
        return self.model_id.replace("/", "--").replace(".", "_")

    def get_serve_options(
        self,
        *,
        name_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get the Serve options for the given LLM config.

        This method is used to generate the Serve options for the given LLM config.


        Examples:
            .. testcode::
                :skipif: True

                from ray import serve
                from ray.serve.llm import LLMConfig, LLMServer

                llm_config = LLMConfig(
                    model_loading_config=dict(model_id="test_model"),
                    accelerator_type="L4",
                    runtime_env={"env_vars": {"FOO": "bar"}},
                )
                serve_options = llm_config.get_serve_options(name_prefix="Test:")
                llm_app = LLMServer.as_deployment().options(**serve_options).bind(llm_config)
                serve.run(llm_app)

        Args:
            name_prefix: Optional prefix to be used for the deployment name.

        Returns:
            The dictionary to use in .options() when creating the deployment.
        """

        deployment_config = self._set_deployment_placement_options()

        default_runtime_env = ray.get_runtime_context().runtime_env
        if ENABLE_WORKER_PROCESS_SETUP_HOOK:
            default_runtime_env[
                "worker_process_setup_hook"
            ] = "ray.llm._internal.serve._worker_process_setup_hook"

        ray_actor_options = deployment_config.get("ray_actor_options", {})
        ray_actor_options["runtime_env"] = {
            **default_runtime_env,
            # Existing runtime_env should take precedence over the default.
            **ray_actor_options.get("runtime_env", {}),
            **(self.runtime_env if self.runtime_env else {}),
        }
        deployment_config["ray_actor_options"] = ray_actor_options

        # Set the name of the deployment config to map to the model ID.
        if "name" not in deployment_config:
            deployment_config["name"] = self._get_deployment_name()
        if name_prefix:
            deployment_config["name"] = name_prefix + deployment_config["name"]

        return deployment_config


def _is_yaml_file(filename: str) -> bool:
    yaml_extensions = [".yml", ".yaml", ".json"]
    for s in yaml_extensions:
        if filename.endswith(s):
            return True
    return False


def _parse_path_args(path: str) -> List[LLMConfig]:
    assert os.path.exists(
        path
    ), f"Could not load model from {path}, as it does not exist."
    if os.path.isfile(path):
        with open(path, "r") as f:
            llm_config = LLMConfig.parse_yaml(f)
            return [llm_config]
    elif os.path.isdir(path):
        apps = []
        for root, _dirs, files in os.walk(path):
            for p in files:
                if _is_yaml_file(p):
                    with open(os.path.join(root, p), "r") as f:
                        llm_config = LLMConfig.parse_yaml(f)
                        apps.append(llm_config)
        return apps
    else:
        raise ValueError(
            f"Could not load model from {path}, as it is not a file or directory."
        )


def parse_args(
    args: Union[str, LLMConfig, Any, Sequence[Union[LLMConfig, str, Any]]],
) -> List[LLMConfig]:
    """Parse the input args and return a standardized list of LLMConfig objects

    Supported args format:
    1. The path to a yaml file defining your LLMConfig
    2. The path to a folder containing yaml files, which define your LLMConfigs
    3. A list of yaml files defining multiple LLMConfigs
    4. A dict or LLMConfig object
    5. A list of dicts or LLMConfig objects
    """

    raw_models = [args]
    if isinstance(args, list):
        raw_models = args

    # For each
    models: List[LLMConfig] = []
    for raw_model in raw_models:
        if isinstance(raw_model, str):
            if os.path.exists(raw_model):
                parsed_models = _parse_path_args(raw_model)
            else:
                try:
                    llm_config = LLMConfig.parse_yaml(raw_model)
                    parsed_models = [llm_config]
                except pydantic.ValidationError as e:
                    raise ValueError(
                        f"Could not parse string as yaml. If you are "
                        "specifying a path, make sure it exists and can be "
                        f"reached. raw_model: {raw_model}"
                    ) from e
        else:
            try:
                llm_config = LLMConfig.model_validate(raw_model)
                parsed_models = [llm_config]
            except pydantic.ValidationError:
                parsed_models = [LLMConfig.model_validate(raw_model)]
        models += parsed_models

    return models


class LLMServingArgs(BaseModel):
    llm_configs: List[Union[str, LLMConfig]] = Field(
        description="A list of LLMConfigs, or paths to LLMConfigs, to run.",
    )

    def parse_args(self) -> "LLMServingArgs":
        """Converts this LLMServingArgs object into an DeployArgs object."""

        llm_configs = []
        for config in self.llm_configs:
            parsed_config = parse_args(config)[0]
            if not isinstance(parsed_config, LLMConfig):
                raise ValueError(
                    "When using the new Serve config format, all model "
                    "configs must also use the new model config format. Got "
                    "a model config that doesn't match new format. Type: "
                    f"{type(parsed_config)}. Contents: {parsed_config}."
                )
            llm_configs.append(parsed_config)

        return LLMServingArgs(llm_configs=llm_configs)


class DiskMultiplexConfig(BaseModelExtended):
    model_id: str
    max_total_tokens: Optional[int]
    local_path: str

    # this is a per process id assigned to the model
    lora_assigned_int_id: int
