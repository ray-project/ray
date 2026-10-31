"""The score task of the SGLang processor."""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from pydantic import Field, StrictInt, model_validator

from ray.llm._internal.batch.constants import SGLANG_MIS_DELIMITER_TOKEN_ID
from ray.llm._internal.batch.processor.sglang_task import _SGLangTask
from ray.llm._internal.batch.processor.utils import build_cpu_stage_map_kwargs
from ray.llm._internal.batch.stages.base import StatefulStage
from ray.llm._internal.batch.stages.configs import (
    TokenizerStageConfig,
    resolve_stage_config,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended

if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )

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
    score_extraction_token: Optional[str] = Field(
        default=None,
        description="Token text at whose every occurrence in an item the score "
        "is read (setwise scoring). Each item then gets one score list per "
        "occurrence. Must tokenize to a single token.",
    )
    score_extraction_token_id: Optional[StrictInt] = Field(
        default=None,
        description="Token ID of the score extraction token. Alternative to "
        "score_extraction_token.",
    )

    @model_validator(mode="after")
    def _validate(self) -> "SGLangScoreOptions":
        if self.label_token_ids is not None and not self.label_token_ids:
            raise ValueError("'label_token_ids' must not be empty.")
        for name in ("embed_override_token", "score_extraction_token"):
            if (
                getattr(self, name) is not None
                and getattr(self, f"{name}_id") is not None
            ):
                raise ValueError(f"Set only one of '{name}' and '{name}_id'.")
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
    if (
        options.score_extraction_token is not None
        or options.score_extraction_token_id is not None
    ):
        _validate_score_extraction(options, config.engine_kwargs)


def _validate_score_extraction(
    options: SGLangScoreOptions, engine_kwargs: Dict[str, Any]
) -> None:
    """Check the engine settings score extraction needs.

    SGLang checks these too, but only once a row reaches a worker.

    Args:
        options: The score options.
        engine_kwargs: The SGLang engine kwargs.
    """
    if options.item_first:
        raise ValueError("'item_first' can't be used with score extraction.")
    if engine_kwargs.get("allow_auto_truncate"):
        raise ValueError(
            "'allow_auto_truncate' can't be used with score extraction: "
            "truncation would drop extraction positions."
        )
    # SGLang sets both itself when enable_mis is on.
    if not engine_kwargs.get("enable_mis") and (
        engine_kwargs.get("disable_radix_cache") is not True
        or engine_kwargs.get("chunked_prefill_size") != -1
    ):
        raise ValueError(
            "Score extraction without 'enable_mis' requires "
            "engine_kwargs disable_radix_cache=True and chunked_prefill_size=-1, "
            "because the extraction positions index the full prompt."
        )


def _validate_score_token_ids(options: SGLangScoreOptions, hf_config: Any) -> None:
    """Check `label_token_ids` and `score_extraction_token_id` against the model.

    The engine runs with skip_tokenizer_init, so it doesn't range-check them.

    Args:
        options: The score options.
        hf_config: The model's Hugging Face config, or None if unavailable.
    """
    architectures = getattr(hf_config, "architectures", None) or []
    if options.label_token_ids is None and any(
        arch.endswith("ForCausalLM") for arch in architectures
    ):
        raise ValueError(
            f"score_options.label_token_ids is required for {architectures[0]}."
        )
    vocab_size = getattr(hf_config, "vocab_size", None)
    if not isinstance(vocab_size, int) or vocab_size <= 0:
        return
    for name, token_ids in (
        ("label_token_ids", options.label_token_ids or []),
        ("score_extraction_token_id", [options.score_extraction_token_id]),
    ):
        out_of_range = [
            t for t in token_ids if t is not None and not 0 <= t < vocab_size
        ]
        if out_of_range:
            raise ValueError(
                f"'{name}' {out_of_range} are outside the model "
                f"vocabulary (size {vocab_size})."
            )


def _resolve_marker_token_ids(
    config: "SGLangEngineProcessorConfig", tokenizer_source: str
) -> Tuple[Optional[int], Optional[int]]:
    """Return the embedding marker and score extraction token IDs.

    Marker text is resolved with the tokenizer, loaded only if needed.

    Args:
        config: The processor config.
        tokenizer_source: The model source to load the tokenizer from.

    Returns:
        The embedding marker and score extraction token IDs, each None if
        not configured.
    """
    from ray.llm._internal.batch.stages.sglang_score_tokenize_stage import (
        resolve_marker_token,
    )
    from ray.llm._internal.batch.utils import load_tokenizer

    options = config.score_options
    tokenizer = None
    token_ids = []
    for name in ("embed_override_token", "score_extraction_token"):
        token = getattr(options, name)
        token_id = getattr(options, f"{name}_id")
        if token is not None:
            if tokenizer is None:
                tokenizer = load_tokenizer(
                    tokenizer_source,
                    config.engine_kwargs.get("trust_remote_code", False),
                )
            token_id = resolve_marker_token(tokenizer, token, name)
        if token_id == SGLANG_MIS_DELIMITER_TOKEN_ID and config.engine_kwargs.get(
            "enable_mis"
        ):
            raise ValueError(
                f"'{name}' resolves to token ID {token_id}, which multi-item "
                "scoring uses as its delimiter. Use a different token."
            )
        token_ids.append(token_id)

    embed_override_token_id, score_extraction_token_id = token_ids
    if (
        embed_override_token_id is not None
        and embed_override_token_id == score_extraction_token_id
    ):
        raise ValueError(
            "The embedding marker and the score extraction token are both "
            f"token ID {embed_override_token_id}. Use different tokens."
        )
    return embed_override_token_id, score_extraction_token_id


class _SGLangScoreTask(_SGLangTask):
    """Builds the score task: score tokenize stage, then `Engine.async_score`."""

    def prepare_config(
        self, config: "SGLangEngineProcessorConfig"
    ) -> "SGLangEngineProcessorConfig":
        config = config.model_copy(
            update={stage: False for stage in _SCORE_DISABLED_STAGES}
        )
        _validate_score_config(
            config,
            resolve_stage_config(config.tokenize_stage, TokenizerStageConfig).enabled,
        )
        return config

    def engine_task_options(
        self, config: "SGLangEngineProcessorConfig", tokenizer_source: str
    ) -> Dict[str, Any]:
        # Marker text is resolved to token IDs here, so the engine only sees IDs.
        (
            embed_override_token_id,
            score_extraction_token_id,
        ) = _resolve_marker_token_ids(config, tokenizer_source)
        score_options = config.score_options.model_dump(
            exclude={
                "embed_override_token",
                "score_extraction_token",
                "score_extraction_token_id",
            }
        )
        score_options["embed_override_token_id"] = embed_override_token_id
        # Passed only when set, so plain scoring works on SGLang versions
        # without the argument.
        if score_extraction_token_id is not None:
            score_options["score_extraction_token_id"] = score_extraction_token_id
        return score_options

    def build_tokenize_stage(
        self,
        config: "SGLangEngineProcessorConfig",
        stage_cfg: TokenizerStageConfig,
        task_options: Optional[Dict[str, Any]],
    ) -> StatefulStage:
        from ray.llm._internal.batch.stages import SGLangScoreTokenizeStage

        kwargs = dict(
            model=stage_cfg.model_source,
            trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
        )
        # Only markers given as text are passed, because the stage checks each
        # row's text against the marker's token ID.
        for name in ("embed_override_token", "score_extraction_token"):
            token = getattr(config.score_options, name)
            if token is not None:
                kwargs[name] = token
                kwargs[f"{name}_id"] = task_options[f"{name}_id"]
        return SGLangScoreTokenizeStage(
            fn_constructor_kwargs=kwargs,
            map_batches_kwargs=build_cpu_stage_map_kwargs(stage_cfg),
        )

    def validate_model(
        self, config: "SGLangEngineProcessorConfig", hf_config: Any
    ) -> None:
        _validate_score_token_ids(config.score_options, hf_config)
