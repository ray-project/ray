import copy

import yaml

from enum import Enum
from abc import ABC, abstractmethod
from vllm.sampling_params import GuidedDecodingParams
import json
from ray.llm._internal.serve.deployments.error_handling import TooManyStoppingSequences

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    Literal,
    Tuple,
)
import time
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
from typing_extensions import Annotated

from transformers import PretrainedConfig

from ray.llm._internal.serve.observability.logging import get_logger
import ray.util.accelerators.accelerators as accelerators

from ray.llm._internal.serve.configs.constants import (
    ALLOW_NEW_PLACEMENT_GROUPS_IN_DEPLOYMENT,
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
    DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
    DEFAULT_TARGET_ONGOING_REQUESTS,
    FALLBACK_MAX_ONGOING_REQUESTS,
    MAX_NUM_STOPPING_SEQUENCES,
)
from ray.llm._internal.serve.configs.prompt_formats import Prompt

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


class ResponseFormat(BaseModel, ABC):
    # make allow extra fields false
    model_config = ConfigDict(extra="forbid")

    @abstractmethod
    def to_guided_decoding_params(self, backend: str) -> Optional[GuidedDecodingParams]:
        """Convert the response format to a vLLM guided decoding params.

        Args:
            backend: The backend to use for the guided decoding. (e.g. "xgrammar", "outlines")

        Returns:
            A vLLM guided decoding params object. It can also return None if the response format is not supported. (e.g. "text")
        """
        pass


class ResponseFormatText(ResponseFormat):
    type: Literal["text"]

    def to_guided_decoding_params(self, backend: str) -> Optional[GuidedDecodingParams]:
        return None


class JSONSchemaBase(ResponseFormat, ABC):
    @property
    @abstractmethod
    def json_schema_str(self) -> str:
        pass

    @abstractmethod
    def to_dict(self):
        pass


class ResponseFormatJsonObject(JSONSchemaBase):
    model_config = ConfigDict(populate_by_name=True)

    # Support either keywords because it makes it more robust.
    type: Literal["json_object", "json_schema"]
    # Can use `schema` or `json_schema` interchangeably.
    # `schema` is allowed for backwards compatibility
    # (We released docs with `schema` field name)
    json_schema: Optional[Union[Dict[str, Any], str]] = Field(
        default={}, alias="schema", description="Schema for the JSON response format"
    )

    @model_validator(mode="after")
    def read_and_validate_json_schema(self):
        from ray.llm._internal.serve.configs.json_mode_utils import try_load_json_schema

        # Make sure the json schema is valid and dereferenced.
        self.json_schema = try_load_json_schema(self.json_schema)
        return self

    @property
    def json_schema_str(self) -> str:
        return json.dumps(self.json_schema)

    def to_guided_decoding_params(self, backend: str) -> Optional[GuidedDecodingParams]:
        kwargs = {}

        if self.json_schema:
            kwargs["json"] = self.json_schema_str
        else:
            kwargs["json_object"] = True

        return GuidedDecodingParams.from_optional(
            backend=backend,
            **kwargs,
        )

    def to_dict(self):
        return {
            "type": self.type,
            "schema": self.json_schema_str,
        }


# TODO(Kourosh): Grammar has this known issue that if there is a syntax error in the grammar
# The engine will die. We need to fix this from vLLM side.
# For now avoiding documenting this approach in the docs.
class ResponseFormatGrammar(ResponseFormat):
    type: Literal["grammar", "grammar_gbnf"]
    grammar: str

    def to_guided_decoding_params(self, backend: str) -> Optional[GuidedDecodingParams]:
        return GuidedDecodingParams.from_optional(
            backend=backend,
            grammar=self.grammar,
        )


ResponseFormatType = Union[
    ResponseFormatText,
    ResponseFormatGrammar,
    ResponseFormatJsonObject,
]


class ModelData(BaseModel):
    model_config = ConfigDict(protected_namespaces=tuple())

    id: str
    object: str
    owned_by: str
    permission: List[str]
    rayllm_metadata: Dict[str, Any]

    @property
    def model_type(self) -> str:
        return self.rayllm_metadata["engine_config"]["model_type"]


class FinishReason(str, Enum):
    LENGTH = "length"
    STOP = "stop"
    ERROR = "error"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_vllm_finish_reason(
        cls, finish_reason: Optional[str]
    ) -> Optional["FinishReason"]:
        if finish_reason is None:
            return None
        if finish_reason == "stop":
            return cls.STOP
        if finish_reason == "length":
            return cls.LENGTH
        if finish_reason == "abort":
            return cls.CANCELLED
        return cls.STOP


# TODO (genesu): remove GenerationConfig
class GenerationConfig(BaseModelExtended):
    # prompt_format: Optional[
    #     Union[HuggingFacePromptFormat]
    # ] = Field(
    #     default=HuggingFacePromptFormat(use_hugging_face_chat_template=True),
    #     description="Handles chat template formatting and tokenization. If None, prompt formatting will be disabled and the model can be only queried in the completion mode.",
    # )
    generate_kwargs: Dict[str, Any] = Field(
        default={},
        description="Extra generation kwargs that needs to be passed into the sampling stage for the deployment (this includes things like temperature, etc.)",
    )
    stopping_sequences: Optional[List[str]] = Field(
        default=None,
        description="Stopping sequences (applied after detokenization) to propagate for inference.",
    )
    stopping_tokens: Optional[List[int]] = Field(
        default=[],
        description="Stopping tokens (applied before detokenization) to propagate for inference. By default, we use EOS/UNK tokens at inference.",
    )

    # @field_validator("prompt_format")
    # @classmethod
    # def default_prompt_format(cls, prompt_format):
    #     return prompt_format if prompt_format is not None else DisabledPromptFormat()
    #
    # @property
    # def all_generate_kwargs(self) -> Dict[str, Any]:
    #     return {
    #         "stopping_sequences": self.stopping_sequences,
    #         "stopping_tokens": self.stopping_tokens,
    #         **self.generate_kwargs,
    #     }


class LoraMirrorConfig(BaseModelExtended):
    lora_model_id: str
    bucket_uri: str
    max_total_tokens: Optional[int]
    generation: Optional[GenerationConfig]
    sync_args: Optional[List[str]] = None

    @field_validator("bucket_uri")
    @classmethod
    def validate_bucket_uri(cls, value: str):
        # TODO(tchordia): remove this. this is a short term fix.
        # We should fix this on the LLM-forge side
        if not value.startswith("s3://") and not value.startswith("gs://"):
            value = "s3://" + value
        return value

    @property
    def _bucket_name_and_path(self) -> str:
        for prefix in ["s3://", "gs://"]:
            if self.bucket_uri.startswith(prefix):
                return self.bucket_uri[len(prefix) :]
        return self.bucket_uri

    @property
    def bucket_name(self) -> str:
        return self._bucket_name_and_path.split("/")[0]

    @property
    def bucket_path(self) -> str:
        return "/".join(self._bucket_name_and_path.split("/")[1:])


class DiskMultiplexConfig(BaseModelExtended):
    model_id: str
    generation: Optional[GenerationConfig]
    max_total_tokens: Optional[int]
    local_path: str

    # this is a per process id assigned to the model
    lora_assigned_int_id: int

class ErrorResponse(BaseModel):
    message: str
    internal_message: str
    code: int
    type: str
    param: Dict[str, Any] = {}
    # We use `Any` here since pydantic doesn't have a validator for exceptions.
    # This is fine since the field is excluded.
    original_exception: Annotated[Optional[Any], Field(exclude=True)] = None


class ComputedPropertyMixin:
    """
    Include properties in the dict and json representations of the model.
    """

    # Replace with pydantic.computed_field once it's available
    @classmethod
    def get_properties(cls):
        return [prop for prop in dir(cls) if isinstance(getattr(cls, prop), property)]

    def model_dump(self, *args, **kwargs):
        self.__dict__.update(
            {prop: getattr(self, prop) for prop in self.get_properties()}
        )
        return super().model_dump(*args, **kwargs)  # type: ignore

    def model_dump_json(
        self,
        *args,
        **kwargs,
    ) -> str:
        self.__dict__.update(
            {prop: getattr(self, prop) for prop in self.get_properties()}
        )

        return super().model_dump_json(*args, **kwargs)  # type: ignore


class LogProb(BaseModel):
    logprob: float
    token: str
    bytes: List[int]


class LogProbs(BaseModel):
    token: str
    logprob: float
    bytes: List[int]
    top_logprobs: List[LogProb]

    @classmethod
    def create(cls, logprobs: List[LogProb], top_logprobs: Optional[int] = None):
        assert len(logprobs) > 0, "logprobs must be a non-empty list"
        token = logprobs[0].token
        logprob = logprobs[0].logprob
        bytes = logprobs[0].bytes
        all_logprobs = logprobs if top_logprobs else []
        ret = cls(token=token, logprob=logprob, bytes=bytes, top_logprobs=all_logprobs)
        return ret


class LLMRawResponse(ComputedPropertyMixin, BaseModelExtended):
    """The response from a query to a RayLLM Model.

    Args:
        generated_text: The generated text.
        logprobs: Log probabilities of each token and possibly some of the unchosen tokens.
        num_input_tokens: The number of input tokens.
        num_generated_tokens: The number of generated tokens.
        num_input_tokens_batch: The number of input tokens in the batch.
        num_generated_tokens_batch: The number of generated tokens in the batch.
        preprocessing_time: The time spent preprocessing the request.
        generation_time: The time spent generating the response.
        timestamp: The timestamp of the response.
        finish_reason: The reason the generation finished.
        error: The error, if any.

    """

    generated_text: Optional[str] = None
    logprobs: Optional[List[LogProbs]] = None
    num_input_tokens: Optional[int] = None
    num_input_tokens_batch: Optional[int] = None
    num_generated_tokens: Optional[int] = None
    num_generated_tokens_batch: Optional[int] = None
    preprocessing_time: Optional[float] = None
    generation_time: Optional[float] = None
    timestamp: Optional[float] = Field(default_factory=time.time)
    finish_reason: Optional[str] = None
    error: Optional[ErrorResponse] = None

    @model_validator(mode="before")
    @classmethod
    def text_or_error_or_finish_reason(cls, values):
        if (
            values.get("generated_text") is None
            and values.get("error") is None
            and values.get("finish_reason") is None
        ):
            raise ValueError(
                "'generated_text', 'error', or 'finish_reason' must be set."
            )
        return values

    @classmethod
    def merge_stream(cls, *responses: "LLMRawResponse") -> "LLMRawResponse":
        """
        Merge a stream of responses into a single response.

        The generated text is concatenated. Fields are maxed, except for
        num_generated_tokens and generation_time, which are summed.
        """
        if len(responses) == 1:
            return responses[0]

        generated_text = (
            None
            if responses[0].generated_text is None
            else "".join([response.generated_text or "" for response in responses])
        )
        num_input_tokens = [
            response.num_input_tokens
            for response in responses
            if response.num_input_tokens is not None
        ]
        max_num_input_tokens = max(num_input_tokens) if num_input_tokens else None
        num_input_tokens_batch = [
            response.num_input_tokens_batch
            for response in responses
            if response.num_input_tokens_batch is not None
        ]
        max_num_input_tokens_batch = (
            max(num_input_tokens_batch) if num_input_tokens_batch else None
        )
        num_generated_tokens = [
            response.num_generated_tokens
            for response in responses
            if response.num_generated_tokens is not None
        ]
        total_generated_tokens = (
            sum(num_generated_tokens) if num_generated_tokens else None
        )
        num_generated_tokens_batch = [
            response.num_generated_tokens_batch
            for response in responses
            if response.num_generated_tokens_batch is not None
        ]
        total_generated_tokens_batch = (
            sum(num_generated_tokens_batch) if num_generated_tokens_batch else None
        )
        preprocessing_time = [
            response.preprocessing_time
            for response in responses
            if response.preprocessing_time is not None
        ]
        max_preprocessing_time = max(preprocessing_time) if preprocessing_time else None
        generation_time = [
            response.generation_time
            for response in responses
            if response.generation_time is not None
        ]
        total_generation_time = sum(generation_time) if generation_time else None
        error = next(
            (response.error for response in reversed(responses) if response.error), None
        )
        logprobs = []
        for response in responses:
            if response.logprobs:
                logprobs.extend(response.logprobs)

        return cls(
            generated_text=generated_text,
            logprobs=logprobs,
            num_input_tokens=max_num_input_tokens,
            num_input_tokens_batch=max_num_input_tokens_batch,
            num_generated_tokens=total_generated_tokens,
            num_generated_tokens_batch=total_generated_tokens_batch,
            preprocessing_time=max_preprocessing_time,
            generation_time=total_generation_time,
            timestamp=responses[-1].timestamp,
            finish_reason=responses[-1].finish_reason,
            error=error,
        )

    @property
    def total_time(self) -> Optional[float]:
        if self.generation_time is None and self.preprocessing_time is None:
            return None
        return (self.preprocessing_time or 0) + (self.generation_time or 0)

    @property
    def num_total_tokens(self) -> Optional[float]:
        try:
            return (self.num_input_tokens or 0) + (self.num_generated_tokens or 0)
        except Exception:
            return None

    @property
    def num_total_tokens_batch(self) -> Optional[float]:
        try:
            return (self.num_input_tokens_batch or 0) + (
                self.num_generated_tokens_batch or 0
            )
        except Exception:
            return None

    def unpack(self) -> Tuple["LLMRawResponse", ...]:
        return (self,)


class BatchedLLMRawResponse(LLMRawResponse):
    # Same as LLMRawResponse, but persists the individual responses
    # that were batched together to produce this response.

    _individual_responses: Optional[List[LLMRawResponse]] = PrivateAttr(None)

    @classmethod
    def merge_stream(cls, *responses: LLMRawResponse) -> LLMRawResponse:
        if len(responses) == 1:
            return responses[0]
        obj = super().merge_stream(*responses)
        obj._individual_responses = list(responses)  # type: ignore
        return obj

    def unpack(self) -> Tuple[LLMRawResponse]:
        return tuple(self._individual_responses or [])


def merge_dicts(base: Dict, overwrite: Dict) -> Dict:
    """
    Merge overwrite into base. Modify base inplace.
    """

    for key in overwrite:
        if (
            key in base
            and isinstance(base[key], dict)
            and isinstance(overwrite[key], dict)
        ):
            merge_dicts(base[key], overwrite[key])
        else:
            base[key] = overwrite[key]
    return base


class SamplingParams(BaseModelExtended):
    """
    Args:
        max_tokens: The maximum number of tokens to generate. Defaults to inf.
        temperature: What sampling temperature to use.
        top_p: An alternative to sampling with temperature, called nucleus sampling.
        n: How many completions to generate for each prompt.
        logprobs: Include the log probabilities on the `logprobs` most likely
            tokens, as well the chosen tokens.
        top_logprobs: The number of logprobs to return. Defaults to 1. `logprobs`
            must be set to `True` in order to use top_logprobs.
        stop: Up to 4 sequences where the API will stop generating further tokens.
            The returned text will not contain the stop sequence.
        stop_tokens: Tokens to stop on (applied before detokenization).
        presence_penalty: Number between -2.0 and 2.0.
            Positive values penalize new tokens based on whether they appear in
            the text so far, increasing the model's likelihood to talk about
            new topics.
        frequency_penalty: Number between -2.0 and 2.0. Positive values penalize
            new tokens based on their existing frequency in the text so far,
            decreasing the model's likelihood to repeat the same line verbatim.
        best_of: Generates `best_of` completions server-side and returns the "best".
        logit_bias: Modify the likelihood of specified tokens appearing in
            the completion.
        response_format: Format to return the final response in. Can be for ex:
            response_format={"type": "json", "schema": "{...}"}

    """

    _ignored_fields: Set[str] = set()

    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    n: int = 1
    logprobs: Optional[bool] = None
    top_logprobs: Optional[int] = None
    logit_bias: Optional[Dict[str, float]] = None
    stop: Optional[List[str]] = None
    stop_tokens: Optional[List[int]] = None
    ignore_eos: Optional[bool] = None
    presence_penalty: Optional[float] = None
    frequency_penalty: Optional[float] = None
    best_of: int = 1
    response_format: Optional[ResponseFormatType] = None

    def model_dump(self, **kwargs):
        if kwargs.get("exclude", None) is None:
            kwargs["exclude"] = self._ignored_fields
        return super().model_dump(**kwargs)

    @field_validator("stop", mode="before")
    @classmethod
    def validate_stopping_sequences(cls, values):
        if not values:
            return values

        unique_val = sorted(list(set(values)))

        if len(unique_val) > MAX_NUM_STOPPING_SEQUENCES:
            TooManyStoppingSequences(
                len(unique_val), MAX_NUM_STOPPING_SEQUENCES
            ).raise_exception()

        return unique_val

    @classmethod
    def merge_generation_params(
        cls: Type[ModelT], prompt: Prompt, generation: GenerationConfig
    ) -> ModelT:
        # Extract parameters object from prompt
        parameters = prompt.parameters or {}
        if not isinstance(parameters, dict):
            parameters = parameters.model_dump(exclude_unset=True)

        # Merge in the generate kwargs
        generate_kwargs_copy = copy.deepcopy(generation.generate_kwargs)
        generate_kwargs = merge_dicts(
            generate_kwargs_copy,
            parameters,
        )

        # The stoppping sequence needs to be merged manually
        generate_kwargs["stop"] = list(
            set((parameters.get("stop") or []) + (generation.stopping_sequences or []))
        )
        generate_kwargs["stop_tokens"] = list(
            set(
                (parameters.get("stop_tokens") or [])
                + (generation.stopping_tokens or [])
            )
        )

        return cls.model_validate(generate_kwargs)


class GenerationRequest(BaseModelExtended):
    prompt: Union[str, List[int], List[str]]
    request_id: Union[str, List[str]]
    sampling_params: Optional[Union[SamplingParams, List[SamplingParams]]] = None
