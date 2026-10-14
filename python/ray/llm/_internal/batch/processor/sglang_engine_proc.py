"""The SGLang engine processor."""

import hashlib
import logging
from typing import Any, Dict, List, Optional

from pydantic import Field, StrictInt, model_validator, root_validator

import ray
from ray.data.block import UserDefinedFunction
from ray.llm._internal.batch.constants import SGLangTaskType, TypeSGLangTaskType
from ray.llm._internal.batch.observability.usage_telemetry.usage import (
    BatchModelTelemetry,
    TelemetryAgent,
    get_or_create_telemetry_agent,
)
from ray.llm._internal.batch.processor.base import (
    OfflineProcessorConfig,
    Processor,
    ProcessorBuilder,
)
from ray.llm._internal.batch.processor.utils import (
    build_cpu_stage_map_kwargs,
    get_value_or_fallback,
)
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    TokenizerStageConfig,
    resolve_stage_config,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended
from ray.llm._internal.common.observability.telemetry_utils import DEFAULT_GPU_TYPE

logger = logging.getLogger(__name__)


DEFAULT_MODEL_ARCHITECTURE = "UNKNOWN_MODEL_ARCHITECTURE"

# Stages that don't apply to the score task: there is no chat to template and
# nothing to detokenize.
_SCORE_DISABLED_STAGES = ("chat_template_stage", "detokenize_stage")


class SGLangScoreOptions(BaseModelExtended):
    """Options for the SGLang score task."""

    label_token_ids: Optional[List[StrictInt]] = Field(
        default=None,
        description="Token IDs whose probabilities form each item's score. "
        "Required for generative (CausalLM) models.",
    )
    apply_softmax: bool = Field(
        default=False,
        description="Whether to softmax-normalize the scores over label_token_ids.",
    )
    item_first: bool = Field(
        default=False,
        description="Whether to put each item before the query instead of after.",
    )
    embed_override_token: Optional[str] = Field(
        default=None,
        description="Marker text whose positions are replaced by the "
        "'query_embed_overrides'/'item_embed_overrides' vectors. Must tokenize "
        "to a single token.",
    )
    embed_override_token_id: Optional[StrictInt] = Field(
        default=None,
        description="Token ID of the embedding marker. Alternative to "
        "embed_override_token.",
    )

    @model_validator(mode="after")
    def _validate(self) -> "SGLangScoreOptions":
        if self.label_token_ids is not None and not self.label_token_ids:
            raise ValueError("'label_token_ids' must not be empty.")
        if (
            self.embed_override_token is not None
            and self.embed_override_token_id is not None
        ):
            raise ValueError(
                "Set only one of 'embed_override_token' and 'embed_override_token_id'."
            )
        return self


def _validate_score_config(
    config: "SGLangEngineProcessorConfig", tokenize: bool
) -> None:
    """Check score settings that would otherwise fail later on the workers.

    Args:
        config: The processor config.
        tokenize: Whether the score tokenize stage is enabled.
    """
    options = config.score_options
    misplaced = sorted(set(config.engine_kwargs) & set(SGLangScoreOptions.model_fields))
    if misplaced:
        raise ValueError(f"Move {misplaced} from engine_kwargs to score_options.")
    if options.item_first and config.engine_kwargs.get("enable_mis"):
        raise ValueError("'item_first' can't be used with 'enable_mis'.")
    if options.item_first and tokenize:
        raise ValueError(
            "'item_first' isn't supported with the score tokenize stage. Set "
            "tokenize_stage=False and pass token IDs."
        )


def _validate_label_token_ids(options: SGLangScoreOptions, hf_config: Any) -> None:
    """Check `label_token_ids` against the model config.

    The engine runs with skip_tokenizer_init, so it doesn't range-check them.

    Args:
        options: The score options.
        hf_config: The model's Hugging Face config, or None if unavailable.
    """
    architectures = getattr(hf_config, "architectures", None) or []
    if options.label_token_ids is None:
        if any(arch.endswith("ForCausalLM") for arch in architectures):
            raise ValueError(
                f"score_options.label_token_ids is required for {architectures[0]}."
            )
        return
    vocab_size = getattr(hf_config, "vocab_size", None)
    if isinstance(vocab_size, int) and vocab_size > 0:
        out_of_range = [t for t in options.label_token_ids if not 0 <= t < vocab_size]
        if out_of_range:
            raise ValueError(
                f"'label_token_ids' {out_of_range} are outside the model "
                f"vocabulary (size {vocab_size})."
            )


def _resolve_embed_override_token_id(
    config: "SGLangEngineProcessorConfig", tokenizer_source: str
) -> Optional[int]:
    """Return the embedding marker token ID, resolving the marker text if needed.

    Args:
        config: The processor config.
        tokenizer_source: The model source to load the tokenizer from.

    Returns:
        The marker token ID, or None if embedding overrides aren't configured.
    """
    from ray.llm._internal.batch.stages.sglang_score_tokenize_stage import (
        _MIS_DELIMITER_TOKEN_ID,
        load_tokenizer,
        resolve_embed_override_token,
    )

    options = config.score_options
    token_id = options.embed_override_token_id
    if options.embed_override_token is not None:
        tokenizer = load_tokenizer(
            tokenizer_source, config.engine_kwargs.get("trust_remote_code", False)
        )
        token_id = resolve_embed_override_token(tokenizer, options.embed_override_token)
    if token_id == _MIS_DELIMITER_TOKEN_ID and config.engine_kwargs.get("enable_mis"):
        raise ValueError(
            f"The embedding marker resolves to token ID {token_id}, which "
            "multi-item scoring uses as its delimiter. Use a different marker."
        )
    return token_id


class SGLangEngineProcessorConfig(OfflineProcessorConfig):
    """The configuration for the SGLang engine processor."""

    # SGLang stage configurations.
    engine_kwargs: Dict[str, Any] = Field(
        default_factory=dict,
        description="The kwargs to pass to the SGLang engine. See "
        "https://docs.sglang.ai/backend/server_arguments.html "
        "for more details.",
    )
    task_type: TypeSGLangTaskType = Field(
        default=SGLangTaskType.GENERATE,
        description="The task type to use: 'generate' (default) or 'score'.",
    )
    score_options: SGLangScoreOptions = Field(
        default_factory=SGLangScoreOptions,
        description="Options for task_type='score'.",
    )

    @root_validator(pre=True)
    def validate_task_type(cls, values):
        task_type = values.get("task_type", SGLangTaskType.GENERATE)
        if task_type not in SGLangTaskType.values():
            raise ValueError(f"Invalid task type: {task_type}")

        engine_kwargs = values.get("engine_kwargs", {})
        engine_kwargs_task = engine_kwargs.get("task", "")
        if engine_kwargs_task != task_type:
            logger.warning(
                "The task set in engine kwargs (%s) is different from the "
                "stage (%s). Overriding the task in engine kwargs to %s.",
                engine_kwargs_task,
                task_type,
                task_type,
            )
            engine_kwargs["task"] = task_type
        values["engine_kwargs"] = engine_kwargs
        return values


def build_sglang_engine_processor(
    config: SGLangEngineProcessorConfig,
    chat_template_kwargs: Optional[Dict[str, Any]] = None,
    preprocess: Optional[UserDefinedFunction] = None,
    postprocess: Optional[UserDefinedFunction] = None,
    preprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    postprocess_map_kwargs: Optional[Dict[str, Any]] = None,
    telemetry_agent: Optional[TelemetryAgent] = None,
) -> Processor:
    """Construct a Processor and configure stages.

    Args:
        config: The configuration for the processor.
        chat_template_kwargs: The optional kwargs to pass to apply_chat_template.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
        preprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            preprocess stage (e.g., num_cpus, memory, concurrency).
        postprocess_map_kwargs: Optional kwargs to pass to Dataset.map() for the
            postprocess stage (e.g., num_cpus, memory, concurrency).
        telemetry_agent: An optional telemetry agent for collecting usage telemetry.

    Returns:
        The constructed processor.
    """
    # Defer SGLang and Transformers imports until this processor is constructed.
    import transformers

    from ray.llm._internal.batch.stages import (
        ChatTemplateStage,
        DetokenizeStage,
        SGLangEngineStage,
        SGLangScoreTokenizeStage,
        TokenizeStage,
    )
    from ray.llm._internal.common.utils.download_utils import (
        NodeModelDownloadable,
        download_model_files,
    )

    ray.init(runtime_env=config.runtime_env, ignore_reinit_error=True)

    stages = []

    is_score = config.task_type == SGLangTaskType.SCORE
    if is_score:
        config = config.model_copy(
            update={stage: False for stage in _SCORE_DISABLED_STAGES}
        )
        _validate_score_config(
            config,
            resolve_stage_config(config.tokenize_stage, TokenizerStageConfig).enabled,
        )

    # Prepare processor defaults for merging into stage configs
    trust_remote_code = config.engine_kwargs.get("trust_remote_code", False)
    processor_defaults = {
        "batch_size": config.batch_size,
        "concurrency": config.concurrency,
        "runtime_env": config.runtime_env,
        "model_source": config.model_source,
    }

    # Resolve and build ChatTemplateStage if enabled
    chat_template_stage_cfg = resolve_stage_config(
        config.chat_template_stage,
        ChatTemplateStageConfig,
        processor_defaults,
    )
    if chat_template_stage_cfg.enabled:
        stages.append(
            ChatTemplateStage(
                fn_constructor_kwargs=dict(
                    model=chat_template_stage_cfg.model_source,
                    chat_template=chat_template_stage_cfg.chat_template,
                    chat_template_kwargs=get_value_or_fallback(
                        chat_template_stage_cfg.chat_template_kwargs,
                        chat_template_kwargs,
                    ),
                    trust_remote_code=trust_remote_code,
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(chat_template_stage_cfg),
            )
        )

    # Resolve and build TokenizeStage if enabled
    tokenize_stage_cfg = resolve_stage_config(
        config.tokenize_stage,
        TokenizerStageConfig,
        processor_defaults,
    )
    score_options = None
    if is_score:
        embed_override_token_id = _resolve_embed_override_token_id(
            config, tokenize_stage_cfg.model_source or config.model_source
        )
        score_options = config.score_options.model_dump(
            exclude={"embed_override_token"}
        )
        score_options["embed_override_token_id"] = embed_override_token_id

    if tokenize_stage_cfg.enabled:
        stage_cls = SGLangScoreTokenizeStage if is_score else TokenizeStage
        tokenize_kwargs = dict(
            model=tokenize_stage_cfg.model_source,
            trust_remote_code=trust_remote_code,
        )
        if is_score and config.score_options.embed_override_token is not None:
            tokenize_kwargs.update(
                embed_override_token=config.score_options.embed_override_token,
                embed_override_token_id=score_options["embed_override_token_id"],
            )
        stages.append(
            stage_cls(
                fn_constructor_kwargs=tokenize_kwargs,
                map_batches_kwargs=build_cpu_stage_map_kwargs(tokenize_stage_cfg),
            )
        )

    # Core stage -- the SGLang engine.
    engine_stage_kwargs = dict(
        model=config.model_source,
        engine_kwargs=config.engine_kwargs,
        task_type=config.task_type,
        max_pending_requests=config.max_pending_requests,
    )
    if is_score:
        engine_stage_kwargs["score_options"] = score_options
    stages.append(
        SGLangEngineStage(
            fn_constructor_kwargs=engine_stage_kwargs,
            map_batches_kwargs=dict(
                zero_copy_batch=True,
                # The number of running replicas. This is a deprecated field, but
                # we need to set `max_tasks_in_flight_per_actor` through `compute`,
                # which initiates enough many overlapping UDF calls per actor, to
                # saturate `max_concurrency`.
                compute=ray.data.ActorPoolStrategy(
                    **config.get_concurrency(autoscaling_enabled=True),
                    max_tasks_in_flight_per_actor=config.max_tasks_in_flight_per_actor,
                ),
                # The number of running batches "per actor" in Ray Core level.
                # This is used to make sure we overlap batches to avoid the tail
                # latency of each batch.
                max_concurrency=config.max_concurrent_batches,
                accelerator_type=config.accelerator_type,
                runtime_env=config.runtime_env,
            ),
        )
    )

    # Resolve and build DetokenizeStage if enabled
    detokenize_stage_cfg = resolve_stage_config(
        config.detokenize_stage,
        DetokenizeStageConfig,
        processor_defaults,
    )
    if detokenize_stage_cfg.enabled:
        stages.append(
            DetokenizeStage(
                fn_constructor_kwargs=dict(
                    model=detokenize_stage_cfg.model_source,
                    trust_remote_code=trust_remote_code,
                ),
                map_batches_kwargs=build_cpu_stage_map_kwargs(detokenize_stage_cfg),
            )
        )

    # Download model files for telemetry before engine init.
    # Use EXCLUDE_SAFETENSORS for trust_remote_code models so custom .py config
    # files are available locally.
    try:
        download_mode = (
            NodeModelDownloadable.EXCLUDE_SAFETENSORS
            if trust_remote_code
            else NodeModelDownloadable.TOKENIZER_ONLY
        )
        model_path_or_id = download_model_files(
            model_id=config.model_source,
            mirror_config=None,
            download_model=download_mode,
            download_extra_files=False,
        )

        hf_config = transformers.AutoConfig.from_pretrained(
            model_path_or_id,
            trust_remote_code=trust_remote_code,
        )
    except Exception as e:
        # Failed to retrieve HuggingFace config for telemetry purposes.
        # This is non-fatal: we fall back to DEFAULT_MODEL_ARCHITECTURE for telemetry.
        # The actual model loading happens later in SGLang, which may support models
        # that aren't available via HuggingFace's AutoConfig.
        logger.warning(
            "Failed to retrieve HuggingFace config for %s: %s",
            config.model_source,
            e,
        )
        hf_config = None

    architectures = getattr(hf_config, "architectures", [])
    architecture = architectures[0] if architectures else DEFAULT_MODEL_ARCHITECTURE

    if is_score:
        _validate_label_token_ids(config.score_options, hf_config)

    telemetry_agent = get_or_create_telemetry_agent()
    telemetry_agent.push_telemetry_report(
        BatchModelTelemetry(
            model_id_hash=hashlib.sha256(
                config.model_source.encode("utf-8")
            ).hexdigest(),
            processor_config_name=type(config).__name__,
            model_architecture=architecture,
            batch_size=config.batch_size,
            accelerator_type=config.accelerator_type or DEFAULT_GPU_TYPE,
            concurrency=config.concurrency,
            task_type=config.task_type,
            tensor_parallel_size=config.engine_kwargs.get("tp_size", 1),
            data_parallel_size=config.engine_kwargs.get("dp_size", 1),
        )
    )

    processor = Processor(
        config,
        stages,
        preprocess=preprocess,
        postprocess=postprocess,
        preprocess_map_kwargs=preprocess_map_kwargs,
        postprocess_map_kwargs=postprocess_map_kwargs,
    )
    return processor


ProcessorBuilder.register(SGLangEngineProcessorConfig, build_sglang_engine_processor)
