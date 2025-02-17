import yaml

from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    PrivateAttr,
    field_validator,
    model_validator,
)
from ray.util.placement_group import (
    PlacementGroup,
    get_current_placement_group,
    placement_group,
    placement_group_table,
)

from transformers import PretrainedConfig

from ray.llm._internal.serve.observability.logging import get_logger
import ray.util.accelerators.accelerators as accelerators

from ray.llm._internal.serve.configs.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
    DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
    DEFAULT_TARGET_ONGOING_REQUESTS,
    FALLBACK_MAX_ONGOING_REQUESTS,
)


GPUType = Enum("GPUType", vars(accelerators))
ModelT = TypeVar("ModelT", bound=BaseModel)


class BaseModelExtended(BaseModel):
    # NOTE(edoakes): Pydantic protects the namespace `model_` by default and prints
    # warnings if you define fields with that prefix. However, we added such fields
    # before this behavior existed. To avoid spamming user-facing logs, we mark the
    # namespace as not protected. This means we need to be careful about overriding
    # internal attributes starting with `model_`.
    # See: https://github.com/anyscale/ray-llm/issues/1425
    model_config = ConfigDict(protected_namespaces=tuple())

    @classmethod
    def parse_yaml(cls: Type[ModelT], file, **kwargs) -> ModelT:
        kwargs.setdefault("Loader", yaml.SafeLoader)
        dict_args = yaml.load(file, **kwargs)
        return cls.model_validate(dict_args)


logger = get_logger(__name__)


class ExtraFiles(BaseModelExtended):
    bucket_uri: str
    destination_path: str


class MirrorConfig(BaseModelExtended):
    bucket_uri: Optional[str] = None
    extra_files: List[ExtraFiles] = Field(default_factory=list)


class S3AWSCredentials(BaseModelExtended):
    create_aws_credentials_url: str
    auth_token_env_variable: Optional[str] = None


class GCSMirrorConfig(MirrorConfig):
    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if not value.startswith("gs://"):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "gs://".'
            )
        return value


class S3MirrorConfig(MirrorConfig):
    s3_sync_args: Optional[List[str]] = None
    s3_aws_credentials: Optional[S3AWSCredentials] = None

    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if value and not value.startswith("s3://"):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://".'
            )
        return value


class AutoscalingConfig(BaseModel, extra="allow"):
    """
    The model here provides reasonable defaults for llm model serving.

    Please note that field descriptions may be exposed to the end users.
    """

    min_replicas: int = Field(
        1,
        description="min_replicas is the minimum number of replicas for the deployment.",
    )
    initial_replicas: int = Field(
        1,
        description="The number of replicas that are started initially for the deployment.",
    )
    max_replicas: int = Field(
        100,
        description="max_replicas is the maximum number of replicas for the deployment.",
    )
    target_ongoing_requests: Optional[int] = Field(
        None,
        description="target_ongoing_requests is the maximum number of queries that are sent to a replica of this deployment without receiving a response.",
    )
    target_num_ongoing_requests_per_replica: Optional[int] = Field(
        None,
        description="target_num_ongoing_requests_per_replica is the deprecated field."
        "If it is set, the model will set target_ongoing_requests to that value too."
        "If neither field is set, DEFAULT_TARGET_ONGOING_REQUESTS will be used.",
        exclude=True,
    )

    metrics_interval_s: float = Field(
        10.0, description="How often to scrape for metrics in seconds."
    )
    look_back_period_s: float = Field(
        30.0, description="Time window to average over for metrics, in seconds."
    )
    downscale_delay_s: float = Field(
        300.0, description="How long to wait before scaling down replicas, in seconds."
    )
    upscale_delay_s: float = Field(
        10.0, description="How long to wait before scaling up replicas, in seconds."
    )

    @model_validator(mode="before")
    def sync_target_ongoing_requests(cls, values):
        target_ongoing_requests = values.get("target_ongoing_requests", None)
        target_num_ongoing_requests_per_replica = values.get(
            "target_num_ongoing_requests_per_replica", None
        )

        final_val = (
            target_ongoing_requests
            or target_num_ongoing_requests_per_replica
            or DEFAULT_TARGET_ONGOING_REQUESTS
        )
        values["target_ongoing_requests"] = final_val
        values["target_num_ongoing_requests_per_replica"] = final_val

        return values


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


# See: https://docs.ray.io/en/latest/serve/configure-serve-deployment.html
class DeploymentConfig(BaseModelExtended):
    autoscaling_config: Optional[AutoscalingConfig] = Field(
        AutoscalingConfig(),
        description="Configuration for autoscaling the number of workers",
    )
    max_ongoing_requests: Optional[int] = Field(
        None,
        description="Sets the maximum number of queries in flight that are sent to a single replica.",
    )
    # max_concurrent_queries is the deprecated field
    # max_ongoing_requests should be used instead
    max_concurrent_queries: Optional[int] = Field(
        None,
        description="This field is deprecated. max_ongoing_requests should be used instead.",
        exclude=True,
    )
    ray_actor_options: Optional[Dict[str, Any]] = Field(
        None, description="the Ray actor options to pass into the replica's actor."
    )
    graceful_shutdown_timeout_s: int = Field(
        300,
        description="Controller waits for this duration to forcefully kill the replica for shutdown, in seconds.",
    )  # XXX: hardcoded

    @model_validator(mode="before")
    def populate_max_ongoing_requests(cls, values):
        max_ongoing_requests = values.get("max_ongoing_requests", None)
        max_concurrent_queries = values.get("max_concurrent_queries", None)
        # max_concurrent_queries takes priority because users may have set this value
        # before max_ongoing_requests exists
        final_value = (
            max_ongoing_requests
            or max_concurrent_queries
            or FALLBACK_MAX_ONGOING_REQUESTS
        )
        values["max_ongoing_requests"] = final_value
        values["max_concurrent_queries"] = final_value

        return values


class InputModality(str, Enum):
    text = "text"
    image = "image"


class LLMEngine(str, Enum):
    """Enum that represents an LLMEngine."""

    VLLMEngine = "VLLMEngine"


class ParallelismConfig(BaseModelExtended):
    degree: int = Field(
        default=1,
        description=(
            "The degree of tensor parallelism. Must be greater than or equal "
            "to 1. When set to 1, the model does not use tensor parallelism."
        ),
    )


class JSONModeOptions(BaseModelExtended):
    num_processes: int = Field(
        default=8,
        description="The number of background processes for each replica.",
    )
    recreate_failed_actors: bool = Field(
        default=True, description="Whether to restart failed JSON mode actors."
    )


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

        assert value.startswith("s3://") or value.startswith("gs://"), (
            "Only AWS S3 and Google Cloud Storage are supported. The "
            'dynamic_lora_loading_path must start with "s3://" or "gs://". '
            f'Got "{value}" instead.'
        )
        return value.rstrip("/")


class ModelLoadingConfig(BaseModelExtended):
    model_id: str = Field(
        default=...,
        description="The ID that should be used by end users to access this model.",
    )
    model_source: Optional[Union[str, S3MirrorConfig, GCSMirrorConfig]] = Field(
        default=None,
        description=(
            "Where to obtain the model weights from. "
            "Should be a HuggingFace model ID, S3 mirror config, or GCS "
            "mirror config. When omitted, defaults to the model_id as a "
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


class LLMConfig(BaseModelExtended):
    # model_config is a Pydantic setting. This setting merges with
    # model_configs in parent classes.
    model_config = ConfigDict(
        use_enum_values=True,
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

    llm_engine: LLMEngine = Field(
        default=LLMEngine.VLLMEngine,
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

    accelerator_type: GPUType = Field(
        description=f"The type of accelerator runs the model on. Only the following values are supported: {str([t.value for t in GPUType])}",
    )

    tensor_parallelism: ParallelismConfig = Field(
        default_factory=ParallelismConfig,
        description="The tensor parallelism settings for the model.",
    )

    pipeline_parallelism: ParallelismConfig = Field(
        default_factory=ParallelismConfig,
        description="The pipeline parallelism settings for the model.",
    )

    lora_config: Optional[LoraConfig] = Field(
        default=None, description="Settings for LoRA adapter."
    )

    deployment_config: DeploymentConfig = Field(
        default_factory=DeploymentConfig,
        description="The Ray Serve deployment settings for the model deployment.",
    )

    _supports_vision: bool = PrivateAttr(False)

    def _infer_supports_vision(self, model_id_or_path: str) -> None:
        """Called in llm node initializer together with other transformers calls. It
        loads the model config from huggingface and sets the supports_vision
        attribute based on whether the config has `vision_config`. All LVM models has
        `vision_config` setup.
        """
        hf_config = PretrainedConfig.from_pretrained(model_id_or_path)
        self._supports_vision = hasattr(hf_config, "vision_config")

    def apply_checkpoint_info(self, model_id_or_path: str) -> None:
        """Apply the checkpoint info to the model config."""
        self._infer_supports_vision(model_id_or_path)

    @property
    def supports_vision(self) -> bool:
        return self._supports_vision

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
    def validate_accelerator_type(cls, value: str):
        # Ensure A10 is converted to A10G.
        if value == "A10":
            return "A10G"

        return value

    def ray_accelerator_type(self) -> str:
        """Converts the accelerator type to the Ray Core format."""

        # Ray uses a hyphen instead of an underscore for
        # accelerator_type.
        return f"accelerator_type:{self.accelerator_type.replace('_', '-')}"

    def multiplex_config(self) -> ServeMultiplexConfig:
        multiplex_config = None
        if self.lora_config:
            multiplex_config = ServeMultiplexConfig(
                max_num_models_per_replica=self.lora_config.max_num_adapters_per_replica,
                download_timeout_s=self.lora_config.download_timeout_s,
                max_download_tries=self.lora_config.max_download_tries,
            )
        return multiplex_config

    @property
    def placement_strategy(self) -> str:
        # If pp <= 1, it's TP so we should make sure all replicas are on the same node.
        if self.pipeline_parallelism.degree > 1:
            return "PACK"
        return "STRICT_PACK"

    @property
    def placement_bundles(self) -> List[Dict[str, float]]:
        num_workers = self.tensor_parallelism.degree * self.pipeline_parallelism.degree
        bundles = [
            {"GPU": 1, self.ray_accelerator_type(): 0.001} for _ in range(num_workers)
        ]

        return bundles

    def get_or_create_pg(self) -> PlacementGroup:
        """Gets or a creates a placement group.

        If we are already in a placement group, return the existing placement group.
        Else, create a new placement group based on the scaling config.
        """
        pg = get_current_placement_group()
        if pg:
            logger.debug(
                "Using existing placement group %s, details: %s",
                pg.id,
                placement_group_table(pg),
            )
        else:
            if not ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT:
                raise RuntimeError(
                    "Creating new placement groups is not allowed. "
                    "Change RAYLLM_ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT "
                    "if this is not intended."
                )

            pg = placement_group(
                self.placement_bundles, strategy=self.placement_strategy
            )
            logger.info(f"Using new placement group {pg}. {placement_group_table(pg)}")
        return pg
