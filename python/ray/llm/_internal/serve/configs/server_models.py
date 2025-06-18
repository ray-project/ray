import time
from enum import Enum
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
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
    MAX_NUM_STOPPING_SEQUENCES,
    MODEL_RESPONSE_BATCH_TIMEOUT_MS,
)
from ray.llm._internal.serve.configs.error_handling import TooManyStoppingSequences
from ray.llm._internal.serve.configs.openai_api_models_patch import (
    ErrorResponse,
    ResponseFormatType,
)
from ray.llm._internal.serve.configs.prompt_formats import (
    Prompt,
)
from ray.llm._internal.serve.observability.logging import get_logger

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


class JSONModeOptions(BaseModelExtended):
    num_processes: int = Field(
        default=8,
        description="The number of background processes for each replica.",
    )
    recreate_failed_actors: bool = Field(
        default=True,
        description="Whether to recreate failed actors.",
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
            "Where to obtain the tokenizer from. "
            "Should be a HuggingFace model ID or a local path. "
            "When omitted, defaults to the model_source."
        ),
    )
    trust_remote_code: bool = Field(
        default=False,
        description="Whether to trust remote code when loading the model.",
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
        description="Enable additional engine metrics via Ray Prometheus port. Only compatible with V1 vLLM engine.",
    )

    _supports_vision: bool = PrivateAttr(False)
    _model_architecture: str = PrivateAttr("")
    _engine_config: EngineConfigType = PrivateAttr(None)

    def __init__(self, **data):
        super().__init__(**data)
        self._infer_supports_vision(self.model_loading_config.model_id)

    def _infer_supports_vision(self, model_id_or_path: str) -> None:
        """Infer whether the model supports vision based on the model ID."""
        # TODO: Implement vision support inference
        pass

    def _set_model_architecture(
        self,
        model_id_or_path: Optional[str] = None,
        model_architecture: Optional[str] = None,
    ) -> None:
        """Set the model architecture."""
        if model_architecture is not None:
            self._model_architecture = model_architecture
        elif model_id_or_path is not None:
            # TODO: Implement model architecture inference
            pass

    def apply_checkpoint_info(
        self, model_id_or_path: str, trust_remote_code: bool = False
    ) -> None:
        """Apply checkpoint information to the model."""
        # TODO: Implement checkpoint info application
        pass

    @property
    def supports_vision(self) -> bool:
        return self._supports_vision

    @property
    def model_architecture(self) -> str:
        return self._model_architecture

    @property
    def input_modality(self) -> str:
        if self.supports_vision:
            return InputModality.image
        return InputModality.text

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

        if value not in [t.value for t in GPUType]:
            raise ValueError(
                f"accelerator_type must be one of {str([t.value for t in GPUType])}, "
                f"got {value}"
            )
        return value

    @field_validator("llm_engine")
    def validate_llm_engine(cls, value: str) -> str:
        if value not in [t.value for t in LLMEngine]:
            raise ValueError(
                f"llm_engine must be one of {str([t.value for t in LLMEngine])}, "
                f"got {value}"
            )
        return value

    @field_validator("deployment_config")
    def validate_deployment_config(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        # TODO: Add validation for deployment config
        return value

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
        """Set deployment placement options."""
        # TODO: Implement deployment placement options
        return {}

    def _get_deployment_name(self) -> str:
        return self.model_loading_config.model_id.replace("/", "--")

    def get_serve_options(
        self,
        *,
        name_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get the serve options for this config."""
        deployment_config = self.deployment_config.copy()

        # Set default values if not provided
        if "name" not in deployment_config:
            deployment_config["name"] = self._get_deployment_name()

        if "ray_actor_options" not in deployment_config:
            deployment_config["ray_actor_options"] = {}

        # Set placement group options
        placement_options = self._set_deployment_placement_options()
        if placement_options:
            deployment_config["ray_actor_options"].update(placement_options)

        # Set environment variables
        if ENABLE_WORKER_PROCESS_SETUP_HOOK:
            deployment_config["ray_actor_options"]["env_vars"] = {
                "RAY_SERVE_ENABLE_WORKER_PROCESS_SETUP_HOOK": "1"
            }

        if name_prefix:
            deployment_config["name"] = name_prefix + deployment_config["name"]

        return deployment_config


def _is_yaml_file(filename: str) -> bool:
    """Check if the file is a YAML file."""
    return filename.endswith((".yaml", ".yml"))


def _parse_path_args(path: str) -> List[LLMConfig]:
    """Parse path arguments into LLMConfig objects."""
    # TODO: Implement path parsing
    return []


def parse_args(
    args: Union[str, LLMConfig, Any, Sequence[Union[LLMConfig, str, Any]]],
) -> List[LLMConfig]:
    """Parse arguments into LLMConfig objects."""
    # TODO: Implement argument parsing
    return []


class LLMServingArgs(BaseModel):
    llm_configs: List[Union[str, LLMConfig]] = Field(
        description="The LLM configs to serve."
    )

    def parse_args(self) -> "LLMServingArgs":
        """Parse the arguments."""
        # TODO: Implement argument parsing
        return self


class ModelData(BaseModel):
    model_config = ConfigDict(protected_namespaces=tuple())

    id: str
    object: str
    owned_by: str
    permission: List[str]
    rayllm_metadata: Dict[str, Any]

    @property
    def model_type(self) -> str:
        return self.rayllm_metadata.get("model_type", "unknown")


class Model(BaseModel):
    data: List[ModelData]
    object: str = "list"

    @classmethod
    def list(cls) -> "Model":
        # TODO: Implement model listing
        return cls(data=[], object="list")


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
        metadata: The metadata for internal usage.
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
    metadata: Optional[Dict[str, Any]] = None

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
            metadata=responses[-1].metadata,
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
        """Unpack the response into individual responses."""
        return (self,)


class BatchedLLMRawResponse(LLMRawResponse):
    # Same as LLMRawResponse, but persists the individual responses
    # that were batched together to produce this response.

    _individual_responses: Optional[List[LLMRawResponse]] = PrivateAttr(None)

    @classmethod
    def merge_stream(cls, *responses: LLMRawResponse) -> LLMRawResponse:
        """Merge a stream of responses into a single response."""
        # TODO: Implement batched response merging
        return super().merge_stream(*responses)

    def unpack(self) -> Tuple[LLMRawResponse]:
        """Unpack the response into individual responses."""
        # TODO: Implement response unpacking
        return (self,)


def merge_dicts(base: Dict, overwrite: Dict) -> Dict:
    """Merge two dictionaries, with overwrite taking precedence."""
    result = base.copy()
    result.update(overwrite)
    return result


class SamplingParams(BaseModelExtended):
    """Parameters for controlling text generation sampling.

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

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Dump the model to a dictionary, excluding ignored fields."""
        # TODO: Implement model dumping with ignored fields
        return super().model_dump(**kwargs)

    @field_validator("stop", mode="before")
    @classmethod
    def validate_stopping_sequences(cls, values):
        if values is not None and len(values) > MAX_NUM_STOPPING_SEQUENCES:
            raise TooManyStoppingSequences(
                f"Too many stopping sequences. Maximum allowed is {MAX_NUM_STOPPING_SEQUENCES}, got {len(values)}."
            )
        return values

    @field_validator("stop_tokens", mode="before")
    @classmethod
    def validate_stop_tokens(cls, values):
        if values is not None and len(values) > MAX_NUM_STOPPING_SEQUENCES:
            raise TooManyStoppingSequences(
                f"Too many stop tokens. Maximum allowed is {MAX_NUM_STOPPING_SEQUENCES}, got {len(values)}."
            )
        return values

    @classmethod
    def _get_model_validate_kwargs(cls: Type[ModelT], prompt: Prompt) -> Dict[str, Any]:
        """Get the model validation kwargs from a prompt."""
        # TODO: Implement model validation kwargs extraction
        return {}

    @classmethod
    def from_prompt(cls: Type[ModelT], prompt: Prompt) -> ModelT:
        # Extract parameters object from prompt
        # TODO: Implement prompt parameter extraction
        return cls()


class GenerationRequest(BaseModelExtended):
    prompt: Union[str, List[int], List[str]]
    prompt_token_ids: Optional[List[int]] = None
    request_id: Union[str, List[str]]
    sampling_params: Optional[Union[SamplingParams, List[SamplingParams]]] = None
    stream: bool = False
    metadata: Optional[Dict[str, Any]] = None
