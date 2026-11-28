"""The score task for SGLang."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

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

# First SGLang release whose `Engine.async_score` accepts `score_extraction_token_id`.
_MIN_SGLANG_VERSION = "0.5.21"


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

    model_config = ConfigDict(arbitrary_types_allowed=True)

    query_token_ids: List[int]
    # One token ID list per item.
    item_token_ids: List[List[int]]
    # A list of [1, hidden_dim] tensors, one per marker in the query.
    query_embed_overrides: Optional[List[Any]] = None
    # One entry per item: a list of [1, hidden_dim] tensors, or None.
    item_embed_overrides: Optional[List[Optional[List[Any]]]] = None


class _SGLangScoreTask(_SGLangTask):
    """Scores a query against its items with `Engine.async_score`."""

    required_input_keys = {
        "query_token_ids": "The query token IDs (List[int]).",
        "item_token_ids": "The token IDs of each item (List[List[int]]).",
    }
    optional_input_keys = {
        "query_embed_overrides": "Embedding overrides for the query, "
        "shaped [num_overrides, hidden_dim].",
        "item_embed_overrides": "Embedding overrides for each item "
        "(one entry or None per item).",
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
    ) -> StatefulStage:
        from ray.llm._internal.batch.stages import SGLangScoreTokenizeStage

        kwargs = dict(
            model=stage_cfg.model_source,
            trust_remote_code=config.engine_kwargs.get("trust_remote_code", False),
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
        self.score_kwargs = SGLangScoreOptions(**task_kwargs).model_dump()

        import sglang
        from packaging.version import Version

        # Checked before the engine is created, so no weights are loaded first.
        if Version(sglang.__version__) < Version(_MIN_SGLANG_VERSION):
            raise ImportError(
                f"task_type='score' requires sglang>={_MIN_SGLANG_VERSION}, "
                f"but {sglang.__version__} is installed."
            )

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
        # The synchronous `score` drives its own event loop, which can't run
        # inside this already-running one.
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
            "num_input_tokens": result.prompt_tokens,
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


def _validate_score_token_ids(options: Dict[str, Any], hf_config: Any) -> None:
    """Check the score token IDs against the model.

    The engine runs with skip_tokenizer_init, so it doesn't range-check them.

    Args:
        options: The prepared `task_kwargs`.
        hf_config: The model's Hugging Face config, or None if unavailable.
    """
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


def _convert_to_embed_override_tensors(
    embeds: Any,
) -> Optional[List["torch.Tensor"]]:
    """Convert embedding overrides into the per-position tensors SGLang expects.

    Args:
        embeds: The overrides for one sequence: `None`, or an array or list
            shaped `[num_overrides, hidden_dim]` or `[hidden_dim]`.

    Returns:
        A list of `[1, hidden_dim]` tensors, or `None` if there are no overrides.
    """
    if is_missing(embeds):
        return None

    array = np.asarray(embeds)
    if array.dtype == object:
        # Ray Data stores a nested list column as an object ndarray of ndarrays.
        try:
            array = np.stack([np.asarray(entry) for entry in array])
        except ValueError as e:
            shapes = [np.shape(entry) for entry in embeds]
            raise ValueError(
                f"Embedding overrides must all have the same shape, got {shapes}."
            ) from e
    if array.size == 0:
        return None

    if array.ndim == 1:
        array = array[np.newaxis]
    elif array.ndim != 2:
        raise ValueError(
            "Embedding overrides must be shaped [num_overrides, hidden_dim], "
            f"got {array.shape}."
        )

    import torch

    tensor = torch.as_tensor(array)
    return [row.unsqueeze(0) for row in tensor]
