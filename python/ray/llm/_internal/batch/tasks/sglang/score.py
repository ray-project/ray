"""The score task for SGLang."""

import inspect
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import numpy as np
from pydantic import BaseModel, Field, StrictInt, model_validator

from ray.llm._internal.batch.constants import SGLANG_MIS_DELIMITER_TOKEN_ID
from ray.llm._internal.batch.processor.utils import build_cpu_stage_map_kwargs
from ray.llm._internal.batch.stages.base import StatefulStage
from ray.llm._internal.batch.stages.common import (
    is_missing,
    maybe_convert_ndarray_to_list,
    to_token_id_list,
)
from ray.llm._internal.batch.stages.configs import TokenizerStageConfig
from ray.llm._internal.batch.tasks.sglang.base import _SGLangTask
from ray.llm._internal.common.base_pydantic import BaseModelExtended

if TYPE_CHECKING:
    import torch

    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )

logger = logging.getLogger(__name__)

# Options passed to `Engine.async_score` as is.
_ASYNC_SCORE_OPTIONS = (
    "label_token_ids",
    "apply_softmax",
    "item_first",
    "embed_override_token_id",
)

# Keyword arguments of `Engine.async_score` that the score task always passes.
_REQUIRED_ASYNC_SCORE_KWARGS = (
    "query",
    "items",
    "label_token_ids",
    "apply_softmax",
    "item_first",
    "embed_override_token_id",
    "query_embed_overrides",
    "item_embed_overrides",
)


class SGLangScoreOptions(BaseModelExtended):
    """The `task_kwargs` of the score task."""

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
    embed_override_token_id: Optional[StrictInt] = Field(
        default=None,
        description="Token ID of the marker whose positions are replaced by the "
        "'query_embed_overrides'/'item_embed_overrides' vectors.",
    )
    score_extraction_token_id: Optional[StrictInt] = Field(
        default=None,
        description="Token ID at whose every occurrence in an item the score "
        "is read (setwise scoring). Each item then gets one score list per "
        "occurrence.",
    )

    @model_validator(mode="after")
    def _validate(self) -> "SGLangScoreOptions":
        if self.label_token_ids is not None and not self.label_token_ids:
            raise ValueError("'label_token_ids' must not be empty.")
        return self


class SGLangScoreRequest(BaseModel):
    """A score request to the SGLang engine: one query and its items."""

    query_token_ids: List[int]
    # One token ID list per item.
    item_token_ids: List[List[int]]
    # A list of [1, hidden_dim] tensors, one per marker in the query.
    query_embed_overrides: Optional[List[Any]] = None
    # One entry per item: a list of [1, hidden_dim] tensors, or None.
    item_embed_overrides: Optional[List[Optional[List[Any]]]] = None

    class Config:
        arbitrary_types_allowed = True


class _SGLangScoreTask(_SGLangTask):
    """Scores a query against its items with `Engine.async_score`."""

    required_input_keys = {
        "query_token_ids": "The query token IDs (List[int]).",
        "item_token_ids": "The token IDs of each item (List[List[int]]).",
    }
    optional_input_keys = {
        "query_embed_overrides": "Embedding overrides for the query "
        "markers, shaped [num_markers, hidden_dim].",
        "item_embed_overrides": "Embedding overrides for each item's "
        "markers (one entry or None per item).",
    }
    # There is no chat to template and nothing to detokenize.
    unsupported_stages = ("chat_template_stage", "detokenize_stage")
    # The score task always passes token IDs to the engine.
    forced_engine_kwargs = {"skip_tokenizer_init": True}
    options_model = SGLangScoreOptions

    @classmethod
    def prepare_config(
        cls, config: "SGLangEngineProcessorConfig"
    ) -> "SGLangEngineProcessorConfig":
        misplaced = sorted(
            set(config.engine_kwargs) & set(SGLangScoreOptions.model_fields)
        )
        if misplaced:
            raise ValueError(f"Move {misplaced} from engine_kwargs to task_kwargs.")
        config = super().prepare_config(config)
        _validate_score_config(config)
        return config

    @classmethod
    def build_tokenize_stage(
        cls,
        config: "SGLangEngineProcessorConfig",
        stage_cfg: TokenizerStageConfig,
        add_special_tokens: bool = True,
    ) -> StatefulStage:
        from ray.llm._internal.batch.stages import SGLangScoreTokenizeStage

        kwargs = dict(
            model=stage_cfg.model_source,
            trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
            add_special_tokens=add_special_tokens,
            enable_mis=bool(config.engine_kwargs.get("enable_mis")),
            item_first=config.task_kwargs["item_first"],
            score_extraction_token_id=config.task_kwargs["score_extraction_token_id"],
        )
        return SGLangScoreTokenizeStage(
            fn_constructor_kwargs=kwargs,
            map_batches_kwargs=build_cpu_stage_map_kwargs(stage_cfg),
        )

    @classmethod
    def validate_model(cls, config: "SGLangEngineProcessorConfig", hf_config: Any):
        if hf_config is None:
            logger.warning(
                "Couldn't load the model config, so the score task_kwargs "
                "token IDs aren't checked against the model."
            )
            return
        _validate_score_token_ids(config.task_kwargs, hf_config)

    def __init__(self, engine_kwargs: Dict[str, Any], **task_kwargs: Any):
        super().__init__(engine_kwargs, **task_kwargs)
        options = SGLangScoreOptions(**task_kwargs)
        self.score_kwargs = {key: getattr(options, key) for key in _ASYNC_SCORE_OPTIONS}
        # Passed only when set, so plain scoring works on SGLang versions
        # without the argument.
        if options.score_extraction_token_id is not None:
            self.score_kwargs[
                "score_extraction_token_id"
            ] = options.score_extraction_token_id

        import sglang

        # Checked before the engine is created, so no weights are loaded first.
        _check_sglang_supports_score(sglang.Engine, self.score_kwargs)

    def prepare_request(
        self, row: Dict[str, Any], params: Optional[Dict[str, Any]]
    ) -> SGLangScoreRequest:
        query_token_ids = to_token_id_list(row["query_token_ids"], "query_token_ids")
        item_token_ids = [
            to_token_id_list(item, "item_token_ids")
            for item in maybe_convert_ndarray_to_list(row["item_token_ids"])
        ]

        # Overrides are popped so the large vectors aren't carried to the output.
        query_embed_overrides = _convert_to_embed_override_tensors(
            row.pop("query_embed_overrides", None)
        )
        item_embed_overrides = None
        raw_item_embeds = row.pop("item_embed_overrides", None)
        if not is_missing(raw_item_embeds):
            converted = [
                _convert_to_embed_override_tensors(entry) for entry in raw_item_embeds
            ]
            # A row without overrides still has the column (all None) because
            # Ray Data columns are schema-uniform.
            if any(entry is not None for entry in converted):
                item_embed_overrides = converted

        return SGLangScoreRequest(
            query_token_ids=query_token_ids,
            item_token_ids=item_token_ids,
            query_embed_overrides=query_embed_overrides,
            item_embed_overrides=item_embed_overrides,
        )

    async def run(self, engine: Any, request: SGLangScoreRequest) -> Dict[str, Any]:
        # `async_score`, because the synchronous `score` runs its own event
        # loop and would deadlock inside this one.
        result = await engine.async_score(
            query=request.query_token_ids,
            items=request.item_token_ids,
            query_embed_overrides=request.query_embed_overrides,
            item_embed_overrides=request.item_embed_overrides,
            **self.score_kwargs,
        )

        # With score extraction, each item's entry is one score list per
        # extraction token instead of a single score list.
        if self.score_kwargs.get("score_extraction_token_id") is None:
            item_scores = [list(entry) for entry in result.scores]
        else:
            item_scores = [[list(row) for row in entry] for entry in result.scores]
        return {
            "scores": item_scores,
            "num_input_tokens": getattr(result, "prompt_tokens", 0) or 0,
        }


def _validate_score_config(config: "SGLangEngineProcessorConfig") -> None:
    """Check score settings that would otherwise fail on the workers.

    Args:
        config: The processor config, with validated `task_kwargs`.
    """
    options = config.task_kwargs
    engine_kwargs = config.engine_kwargs
    enable_mis = engine_kwargs.get("enable_mis")
    if options["item_first"] and enable_mis:
        raise ValueError("'item_first' can't be used with 'enable_mis'.")
    embed_id = options["embed_override_token_id"]
    extraction_id = options["score_extraction_token_id"]
    for name, token_id in (
        ("embed_override_token_id", embed_id),
        ("score_extraction_token_id", extraction_id),
    ):
        if enable_mis and token_id == SGLANG_MIS_DELIMITER_TOKEN_ID:
            raise ValueError(
                f"'{name}' {token_id} is the delimiter multi-item scoring uses. "
                "Use a different token."
            )
    if embed_id is not None and embed_id == extraction_id:
        raise ValueError(
            "'embed_override_token_id' and 'score_extraction_token_id' must " "differ."
        )
    if extraction_id is None:
        return

    # SGLang checks these too, but only once a row reaches a worker.
    if options["item_first"]:
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


def _validate_score_token_ids(options: Dict[str, Any], hf_config: Any) -> None:
    """Check the score token IDs against the model.

    The engine runs with skip_tokenizer_init, so it doesn't range-check them.

    Args:
        options: The prepared `task_kwargs`.
        hf_config: The model's Hugging Face config, or None if unavailable.
    """
    architectures = getattr(hf_config, "architectures", None) or []
    if options["label_token_ids"] is None and any(
        arch.endswith("ForCausalLM") for arch in architectures
    ):
        raise ValueError(
            f"task_kwargs['label_token_ids'] is required for {architectures[0]}."
        )
    vocab_size = getattr(hf_config, "vocab_size", None)
    if not isinstance(vocab_size, int) or vocab_size <= 0:
        return
    for name, token_ids in (
        ("label_token_ids", options["label_token_ids"] or []),
        ("embed_override_token_id", [options["embed_override_token_id"]]),
        ("score_extraction_token_id", [options["score_extraction_token_id"]]),
    ):
        out_of_range = [
            t for t in token_ids if t is not None and not 0 <= t < vocab_size
        ]
        if out_of_range:
            raise ValueError(
                f"'{name}' {out_of_range} are outside the model "
                f"vocabulary (size {vocab_size})."
            )


def _check_sglang_supports_score(
    engine_cls: Any, score_kwargs: Iterable[str] = ()
) -> None:
    """Raise if the installed SGLang's `Engine.async_score` is missing or too old.

    Args:
        engine_cls: The `sglang.Engine` class.
        score_kwargs: The `async_score` arguments the run passes besides the
            always-required ones, such as `score_extraction_token_id`.
    """
    async_score = getattr(engine_cls, "async_score", None)
    if async_score is None:
        raise ImportError(
            "task_type='score' requires `sglang.Engine.async_score`. "
            "Please upgrade SGLang."
        )
    try:
        params = inspect.signature(async_score).parameters
    except (TypeError, ValueError):
        return
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return
    required = dict.fromkeys([*_REQUIRED_ASYNC_SCORE_KWARGS, *score_kwargs])
    missing = [name for name in required if name not in params]
    if missing:
        raise ImportError(
            f"task_type='score' requires `sglang.Engine.async_score` to accept "
            f"{missing}. Please upgrade SGLang."
        )


def _convert_to_embed_override_tensors(
    embeds: Any,
) -> Optional[List["torch.Tensor"]]:
    """Convert embedding overrides into the per-position tensors SGLang expects.

    Args:
        embeds: The overrides for one sequence: `None`, or an array, list, or
            tensor shaped `[num_overrides, hidden_dim]` or `[hidden_dim]`.

    Returns:
        A list of `[1, hidden_dim]` tensors, or `None` if there are no overrides.
    """
    if is_missing(embeds):
        return None

    tensor = _to_stacked_tensor(embeds)
    if tensor is None or tensor.numel() == 0:
        return None

    if tensor.ndim == 1:
        tensor = tensor.unsqueeze(0)
    elif tensor.ndim != 2:
        raise ValueError(
            "Embedding overrides must be shaped [num_overrides, hidden_dim], "
            f"got {tuple(tensor.shape)}."
        )
    return [row.unsqueeze(0) for row in tensor]


def _to_stacked_tensor(embeds: Any) -> Optional["torch.Tensor"]:
    """Stack embedding overrides into one tensor.

    Torch tensors are stacked with `torch.stack` because numpy has no bfloat16
    dtype; everything else goes through `np.asarray`.
    """
    import torch

    if isinstance(embeds, torch.Tensor):
        return embeds

    if isinstance(embeds, (list, tuple, np.ndarray)):
        entries = list(embeds)
        if entries and all(isinstance(entry, torch.Tensor) for entry in entries):
            return _stack_tensor_entries(entries)

    array = np.asarray(embeds)
    if array.size == 0:
        return None
    if array.dtype == object:
        # Ray Data stores a nested list column as an object ndarray of ndarrays.
        return _stack_tensor_entries(
            [torch.as_tensor(np.asarray(entry)) for entry in array]
        )
    return torch.as_tensor(array)


def _stack_tensor_entries(entries: List["torch.Tensor"]) -> "torch.Tensor":
    """`torch.stack` that raises `ValueError` on mismatched shapes."""
    import torch

    try:
        return torch.stack(entries)
    except RuntimeError as e:
        shapes = [tuple(entry.shape) for entry in entries]
        raise ValueError(
            f"Embedding overrides must all have the same shape, got {shapes}."
        ) from e
