import pydantic
import os
import ray

from enum import Enum
from ray.llm._internal.serve.configs.error_handling import TooManyStoppingSequences

from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    Tuple,
    Sequence,
    Set,
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

from ray.llm._internal.utils import try_import


from ray.llm._internal.serve.observability.logging import get_logger
import ray.util.accelerators.accelerators as accelerators
from ray.serve._private.config import DeploymentConfig

from ray.llm._internal.serve.configs.constants import (
    DEFAULT_MULTIPLEX_DOWNLOAD_TIMEOUT_S,
    DEFAULT_MULTIPLEX_DOWNLOAD_TRIES,
    MAX_NUM_STOPPING_SEQUENCES,
    ENABLE_WORKER_PROCESS_SETUP_HOOK,
)
from ray.llm._internal.serve.configs.prompt_formats import (
    Prompt,
    HuggingFacePromptFormat,
)
from ray.llm._internal.serve.configs.openai_api_models_patch import (
    ErrorResponse,
    ResponseFormatType,
)
from ray.llm._internal.serve.configs.base import BaseModelExtended

transformers = try_import("transformers")


GPUType = Enum("GPUType", vars(accelerators))
ModelT = TypeVar("ModelT", bound=BaseModel)


logger = get_logger(__name__)


class ExtraFiles(BaseModelExtended):
    bucket_uri: str
    destination_path: str


class CloudMirrorConfig(BaseModelExtended):
    """Unified mirror config for cloud storage (S3 or GCS).

    Args:
        bucket_uri: URI of the bucket (s3:// or gs://)
        extra_files: Additional files to download
    """

    bucket_uri: Optional[str] = None
    extra_files: List[ExtraFiles] = Field(default_factory=list)

    @field_validator("bucket_uri")
    @classmethod
    def check_uri_format(cls, value):
        if value is None:
            return value

        if not (value.startswith("s3://") or value.startswith("gs://")):
            raise ValueError(
                f'Got invalid value "{value}" for bucket_uri. '
                'Expected a URI that starts with "s3://" or "gs://".'
            )
        return value

    @property
    def storage_type(self) -> str:
        """Returns the storage type ('s3' or 'gcs') based on the URI prefix."""
        if self.bucket_uri is None:
            return None
        elif self.bucket_uri.startswith("s3://"):
            return "s3"
        elif self.bucket_uri.startswith("gs://"):
            return "gcs"
        return None


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

    VLLM = "VLLM"


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
        description="The ID that should be used by end users to access this model.",
    )
    model_source: Optional[Union[str, CloudMirrorConfig]] = Field(
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
        default=LLMEngine.VLLM.value,
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

    _supports_vision: bool = PrivateAttr(False)
    _prompt_format: HuggingFacePromptFormat = PrivateAttr(
        default_factory=HuggingFacePromptFormat
    )

    def _infer_supports_vision(self, model_id_or_path: str) -> None:
        """Called in llm node initializer together with other transformers calls. It
        loads the model config from huggingface and sets the supports_vision
        attribute based on whether the config has `vision_config`. All LVM models has
        `vision_config` setup.
        """
        hf_config = transformers.PretrainedConfig.from_pretrained(model_id_or_path)
        self._supports_vision = hasattr(hf_config, "vision_config")

    def apply_checkpoint_info(
        self, model_id_or_path: str, trust_remote_code: bool = False
    ) -> None:
        """Apply the checkpoint info to the model config."""
        self._infer_supports_vision(model_id_or_path)
        self._prompt_format.set_processor(
            model_id_or_path,
            trust_remote_code=trust_remote_code,
        )

    @property
    def supports_vision(self) -> bool:
        return self._supports_vision

    @property
    def prompt_format(self) -> HuggingFacePromptFormat:
        return self._prompt_format

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

    def multiplex_config(self) -> ServeMultiplexConfig:
        multiplex_config = None
        if self.lora_config:
            multiplex_config = ServeMultiplexConfig(
                max_num_models_per_replica=self.lora_config.max_num_adapters_per_replica,
                download_timeout_s=self.lora_config.download_timeout_s,
                max_download_tries=self.lora_config.max_download_tries,
            )
        return multiplex_config

    def get_engine_config(self):
        """Returns the engine config for the given LLM config.

        LLMConfig not only has engine config but also deployment config, etc.
        """
        if self.llm_engine == LLMEngine.VLLM:
            from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
                VLLMEngineConfig,
            )

            return VLLMEngineConfig.from_llm_config(self)
        else:
            # Note (genesu): This should never happen because we validate the engine
            # in the config.
            raise ValueError(f"Unsupported engine: {self.llm_engine}")

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

        # TODO (Kourosh): There is some test code leakage happening here that should be removed.
        try:
            # resources.mock_resource is a special key we used in tests to skip placement
            # group on the gpu nodes.
            if "mock_resource" in ray_actor_options.get("resources", {}):
                bundles = []
            else:
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

    def _get_deployment_name(self, name_prefix: str) -> str:
        unsanitized_deployment_name = name_prefix + self.model_id
        return unsanitized_deployment_name.replace("/", "--").replace(".", "_")

    def get_serve_options(
        self,
        *,
        name_prefix: str,
    ) -> Dict[str, Any]:
        """Get the Serve options for the given LLM config.

        This method is used to generate the Serve options for the given LLM config.


        Examples:
            .. testcode::
                :skipif: True

                from ray import serve
                from ray.serve.llm.configs import LLMConfig, ModelLoadingConfig
                from ray.serve.llm.deployments import VLLMDeployment


                llm_config = LLMConfig(
                    model_loading_config=ModelLoadingConfig(model_id="test_model"),
                    accelerator_type="L4",
                    runtime_env={"env_vars": {"FOO": "bar"}},
                )
                serve_options = llm_config.get_serve_options(name_prefix="Test:")
                vllm_app = VLLMDeployment.options(**serve_options).bind(llm_config)
                serve.run(vllm_app)

        Keyword Args:
            name_prefix: The prefix to use for the deployment name.

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
        deployment_config["name"] = self._get_deployment_name(name_prefix)
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


TModel = TypeVar("TModel", bound="Model")


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


class Model(BaseModel):
    data: List[ModelData]
    object: str = "list"

    @classmethod
    def list(cls) -> TModel:
        pass


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


class LoraMirrorConfig(BaseModelExtended):
    lora_model_id: str
    bucket_uri: str
    max_total_tokens: Optional[int]
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
    max_total_tokens: Optional[int]
    local_path: str

    # this is a per process id assigned to the model
    lora_assigned_int_id: int


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

        unique_val = sorted(set(values))

        if len(unique_val) > MAX_NUM_STOPPING_SEQUENCES:
            TooManyStoppingSequences(
                len(unique_val), MAX_NUM_STOPPING_SEQUENCES
            ).raise_exception()

        return unique_val

    @classmethod
    def from_prompt(cls: Type[ModelT], prompt: Prompt) -> ModelT:
        # Extract parameters object from prompt
        generate_kwargs = prompt.parameters or {}
        if not isinstance(generate_kwargs, dict):
            generate_kwargs = generate_kwargs.model_dump(exclude_unset=True)

        generate_kwargs["stop"] = set(generate_kwargs.get("stop", []))
        generate_kwargs["stop_tokens"] = set(generate_kwargs.get("stop_tokens", []))

        return cls.model_validate(generate_kwargs)


class GenerationRequest(BaseModelExtended):
    prompt: Union[str, List[int], List[str]]
    request_id: Union[str, List[str]]
    sampling_params: Optional[Union[SamplingParams, List[SamplingParams]]] = None
