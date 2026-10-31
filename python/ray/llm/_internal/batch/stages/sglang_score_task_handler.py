"""The score task of the SGLang engine stage."""

import inspect
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import numpy as np

from ray.llm._internal.batch.stages.common import (
    maybe_convert_ndarray_to_list,
    to_token_id_list,
)
from ray.llm._internal.batch.stages.sglang_task_handler import (
    _SGLangRequest,
    _SGLangTaskHandler,
)

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# Keyword arguments of `Engine.async_score` that the score task passes.
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


def _check_sglang_supports_score(
    engine_cls: Any, score_kwargs: Iterable[str] = ()
) -> None:
    """Raise if the installed SGLang's `Engine.async_score` is missing or too old.

    Runs before the engine is constructed, so no weights are loaded first.

    Args:
        engine_cls: The `sglang.Engine` class.
        score_kwargs: Optional `async_score` arguments the run also passes,
            such as `score_extraction_token_id`.
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


def _extracted_item_scores(
    scores: List[Any], item_token_ids: List[List[int]], extraction_token_id: int
) -> List[List[List[float]]]:
    """Return one score list per extraction token in each item.

    Raises if an item's score count differs from its extraction tokens, since
    callers match scores to tokens by position.
    """
    item_scores = []
    for index, (matrix, item) in enumerate(zip(scores, item_token_ids)):
        rows = [list(row) for row in matrix]
        expected = item.count(extraction_token_id)
        if len(rows) != expected:
            raise RuntimeError(
                f"[SGLang] Expected {expected} score lists for item {index}, one "
                f"per score extraction token, got {len(rows)}. Please report "
                "this issue to the Ray team."
            )
        item_scores.append(rows)
    return item_scores


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
    if embeds is None:
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


class SGLangScoreRequest(_SGLangRequest):
    """A score request to the SGLang engine: one query and its items."""

    query_token_ids: List[int]
    # One token ID list per item.
    item_token_ids: List[List[int]]
    # A list of [1, hidden_dim] tensors, one per marker in the query.
    query_embed_overrides: Optional[List[Any]] = None
    # One entry per item: a list of [1, hidden_dim] tensors, or None.
    item_embed_overrides: Optional[List[Optional[List[Any]]]] = None

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class _SGLangScoreTaskHandler(_SGLangTaskHandler):
    """Scores a query against its items with `Engine.async_score`.

    Args:
        **score_options: Per-run arguments for `Engine.async_score`, such as
            `label_token_ids`.
    """

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

    def __init__(self, **score_options: Any):
        self.score_options = score_options

    def configure_engine_kwargs(self, engine_kwargs: Dict[str, Any]) -> None:
        if not engine_kwargs["skip_tokenizer_init"]:
            logger.warning(
                "Ignoring skip_tokenizer_init=False: the score task always "
                "passes token IDs to the engine."
            )
            engine_kwargs["skip_tokenizer_init"] = True

    def check_engine(self, engine_cls: Any) -> None:
        _check_sglang_supports_score(engine_cls, self.score_options)

    def prepare_request(
        self, row: Dict[str, Any], request_id: int, idx_in_batch: int
    ) -> SGLangScoreRequest:
        query_token_ids = to_token_id_list(row["query_token_ids"], "query_token_ids")
        items = maybe_convert_ndarray_to_list(row["item_token_ids"])
        if not isinstance(items, list) or not items:
            raise ValueError("'item_token_ids' must be a non-empty list of lists.")
        item_token_ids = [to_token_id_list(item, "item_token_ids") for item in items]

        # Overrides are popped so the large vectors aren't carried to the output.
        query_embed_overrides = _convert_to_embed_override_tensors(
            row.pop("query_embed_overrides", None)
        )
        item_embed_overrides = None
        raw_item_embeds = row.pop("item_embed_overrides", None)
        if raw_item_embeds is not None:
            converted = [
                _convert_to_embed_override_tensors(entry) for entry in raw_item_embeds
            ]
            if len(converted) != len(item_token_ids):
                raise ValueError(
                    f"'item_embed_overrides' has {len(converted)} entries for "
                    f"{len(item_token_ids)} items."
                )
            # A row without overrides still has the column (all None) because
            # Ray Data columns are schema-uniform.
            if any(entry is not None for entry in converted):
                item_embed_overrides = converted

        if query_embed_overrides is not None or item_embed_overrides is not None:
            if self.score_options.get("embed_override_token_id") is None:
                raise ValueError(
                    "Embedding overrides require 'embed_override_token' or "
                    "'embed_override_token_id' in score_options."
                )
            if self.score_options.get("item_first"):
                raise ValueError("'item_first' can't be used with embedding overrides.")

        return SGLangScoreRequest(
            request_id=request_id,
            idx_in_batch=idx_in_batch,
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
            **self.score_options,
        )

        scores = getattr(result, "scores", None)
        if scores is None or len(scores) != len(request.item_token_ids):
            raise RuntimeError(
                f"[SGLang] Expected {len(request.item_token_ids)} score lists, got "
                f"{None if scores is None else len(scores)}. Please report this "
                "issue to the Ray team."
            )
        extraction_token_id = self.score_options.get("score_extraction_token_id")
        if extraction_token_id is None:
            item_scores = [list(entry) for entry in scores]
        else:
            item_scores = _extracted_item_scores(
                scores, request.item_token_ids, extraction_token_id
            )
        return {
            "scores": item_scores,
            "num_input_tokens": getattr(result, "prompt_tokens", 0) or 0,
        }
