from enum import Enum
from typing import (
    Any,
    Dict,
    Optional,
    TypeVar,
    Union,
)

from pydantic import (
    BaseModel,
    Field,
    PositiveInt,
    PrivateAttr,
    field_validator,
    model_validator,
)

import ray.util.accelerators.accelerators as accelerators
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.callbacks.base import (
    CallbackBase,
    CallbackConfig,
)
from ray.llm._internal.common.utils.cloud_utils import (
    CloudMirrorConfig,
    is_remote_path,
)
from ray.llm._internal.common.utils.download_utils import (
    STREAMING_LOAD_FORMATS,
    NodeModelDownloadable,
)
from ray.llm._internal.common.utils.import_utils import load_class, try_import
from ray.llm._internal.serve.constants import (
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
    DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
)
from ray.llm._internal.serve.engines.vllm.kv_transfer.factory import (
    KVConnectorBackendFactory,
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
            "Only AWS S3, Google Cloud Storage, and Azure Storage are supported. The "
            'dynamic_lora_loading_path must start with "s3://", "gs://", "abfss://", or "azure://". '
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

    runtime_env: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "The runtime_env to use for the model deployment replica "
            "and the engine workers."
        ),
    )

    model_loading_config: Union[Dict[str, Any], ModelLoadingConfig] = Field(
        description="The settings for how to download and expose the model. Validated against ModelLoadingConfig."
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

    accelerator_type: Optional[str] = Field(
        default=None,
        description=f"The type of accelerator runs the model on. Only the following values are supported: {str([t.value for t in GPUType])}",
    )

    placement_group_config: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Ray placement group configuration for scheduling vLLM engine workers. "
            "Defines resource bundles and placement strategy for multi-node deployments. "
            "Should contain 'bundles' (list of resource dicts) and optionally 'strategy' "
            "(defaults to 'PACK'). Example: {'bundles': [{'GPU': 1, 'CPU': 2}], 'strategy': 'PACK'}"
        ),
    )

    lora_config: Optional[Union[Dict[str, Any], LoraConfig]] = Field(
        default=None,
        description="Settings for LoRA adapter. Validated against LoraConfig.",
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
            `logging_config`, `request_router_config`.
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
        "- `num_ingress_replicas`: The number of replicas for the router. Ray "
        "Serve will take the max amount all the replicas. Default would be 2 "
        "router replicas per model replica.\n",
    )

    log_engine_metrics: Optional[bool] = Field(
        default=True,
        description="Enable additional engine metrics via Ray Prometheus port.",
    )

    callback_config: CallbackConfig = Field(
        default_factory=CallbackConfig,
        description="Callback configuration to use for model initialization. Can be a string path to a class or a Callback subclass.",
    )

    _supports_vision: bool = PrivateAttr(False)
    _model_architecture: str = PrivateAttr("UNSPECIFIED")
    _engine_config: EngineConfigType = PrivateAttr(None)
    _callback_instance: Optional[CallbackBase] = PrivateAttr(None)

    def _infer_supports_vision(self, model_id_or_path: str) -> None:
        """Called in llm node initializer together with other transformers calls. It
        loads the model config from huggingface and sets the supports_vision
        attribute based on whether the config has `vision_config`. All LVM models has
        `vision_config` setup.
        """
        try:
            hf_config = transformers.PretrainedConfig.from_pretrained(model_id_or_path)
            self._supports_vision = hasattr(hf_config, "vision_config")
        except Exception as e:
            raise ValueError(
                f"Failed to load Hugging Face config for model_id='{model_id_or_path}'.\
                        Ensure `model_id` is a valid Hugging Face repo or a local path that \
                        contains a valid `config.json` file. "
                f"Original error: {repr(e)}"
            ) from e

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
            try:
                hf_config = transformers.PretrainedConfig.from_pretrained(
                    model_id_or_path
                )
                if (
                    hf_config
                    and hasattr(hf_config, "architectures")
                    and hf_config.architectures
                ):
                    self._model_architecture = hf_config.architectures[0]
            except Exception as e:
                raise ValueError(
                    f"Failed to load Hugging Face config for model_id='{model_id_or_path}'.\
                        Ensure `model_id` is a valid Hugging Face repo or a local path that \
                        contains a valid `config.json` file. "
                    f"Original error: {repr(e)}"
                ) from e

        if model_architecture:
            self._model_architecture = model_architecture

    def apply_checkpoint_info(
        self, model_id_or_path: str, trust_remote_code: bool = False
    ) -> None:
        """Apply the checkpoint info to the model config."""
        self._infer_supports_vision(model_id_or_path)
        self._set_model_architecture(model_id_or_path)

    def get_or_create_callback(self) -> Optional[CallbackBase]:
        """Get or create the callback instance for this process.

        This ensures one callback instance per process (singleton pattern).
        The instance is cached so the same object is used across all hooks.

        Returns:
            Instance of class that implements Callback
        """  # Return cached instance if exists
        if self._callback_instance is not None:
            return self._callback_instance

        engine_config = self.get_engine_config()
        assert engine_config is not None
        pg = engine_config.get_or_create_pg()
        runtime_env = engine_config.get_runtime_env_with_local_env_vars()
        if self.engine_kwargs.get("load_format", None) in STREAMING_LOAD_FORMATS:
            worker_node_download_model = NodeModelDownloadable.NONE
        else:
            worker_node_download_model = NodeModelDownloadable.MODEL_AND_TOKENIZER

        # Create new instance
        if isinstance(self.callback_config.callback_class, str):
            callback_class = load_class(self.callback_config.callback_class)
        else:
            callback_class = self.callback_config.callback_class

        self._callback_instance = callback_class(
            raise_error_on_callback=self.callback_config.raise_error_on_callback,
            llm_config=self,
            ctx_kwargs={
                "worker_node_download_model": worker_node_download_model,
                "placement_group": pg,
                "runtime_env": runtime_env,
            },
            **self.callback_config.callback_kwargs,
        )
        return self._callback_instance

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

    @field_validator("model_loading_config")
    def validate_model_loading_config(
        cls, value: Union[Dict[str, Any], ModelLoadingConfig]
    ) -> ModelLoadingConfig:
        """Validates the model loading config dictionary."""
        if isinstance(value, ModelLoadingConfig):
            return value

        try:
            model_loading_config = ModelLoadingConfig(**value)
        except Exception as e:
            raise ValueError(f"Invalid model_loading_config: {value}") from e

        return model_loading_config

    @field_validator("lora_config")
    def validate_lora_config(
        cls, value: Optional[Union[Dict[str, Any], LoraConfig]]
    ) -> Optional[LoraConfig]:
        """Validates the lora config dictionary."""
        if value is None or isinstance(value, LoraConfig):
            return value

        try:
            lora_config = LoraConfig(**value)
        except Exception as e:
            raise ValueError(f"Invalid lora_config: {value}") from e

        return lora_config

    @field_validator("experimental_configs")
    def validate_experimental_configs(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        """Validates the experimental configs dictionary."""
        # TODO(Kourosh): Remove this deprecation check after users have
        # migrated.
        if "num_router_replicas" in value:
            raise ValueError(
                "The 'num_router_replicas' key in experimental_configs has "
                "been renamed to 'num_ingress_replicas'. Please update "
                "your configuration to use 'num_ingress_replicas' instead."
            )
        return value

    @model_validator(mode="after")
    def _check_log_stats_with_metrics(self):
        """Validate that disable_log_stats isn't enabled when log_engine_metrics is enabled."""
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
            from ray.llm._internal.serve.engines.vllm.vllm_models import (
                VLLMEngineConfig,
            )

            self._engine_config = VLLMEngineConfig.from_llm_config(self)
        else:
            # Note (genesu): This should never happen because we validate the engine
            # in the config.
            raise ValueError(f"Unsupported engine: {self.llm_engine}")

        return self._engine_config

    def update_engine_kwargs(self, **kwargs: Any) -> None:
        """Update the engine_kwargs and the engine_config engine_kwargs.

        This is typically called during engine starts, when certain engine_kwargs
        (e.g., data_parallel_rank) become available.
        """
        self.engine_kwargs.update(kwargs)
        # engine_config may be created before engine starts, this makes sure
        # the engine_config is updated with the latest engine_kwargs.
        if self._engine_config:
            self._engine_config.engine_kwargs.update(kwargs)

    def setup_engine_backend(self):
        self._setup_kv_connector_backend()

    def _setup_kv_connector_backend(self):
        """Private method to setup kv connector depending on the local deployment state"""
        # 1. validate that the backend is one of the backends supported (Nixl or LMCache)
        kv_transfer_config = self.engine_kwargs.get("kv_transfer_config")
        if not kv_transfer_config:
            return

        kv_connector = kv_transfer_config.get("kv_connector")
        if not kv_connector:
            raise ValueError("Connector type is not specified.")

        # 2. Setup the backend using factory
        kv_connector_backend = KVConnectorBackendFactory.create_backend(
            kv_connector, self
        )
        kv_connector_backend.setup()


class DiskMultiplexConfig(BaseModelExtended):
    model_id: str
    max_total_tokens: Optional[int]
    local_path: str

    # this is a per process id assigned to the model
    lora_assigned_int_id: int
