"""The stage that runs SGLang engine."""

import asyncio
import inspect
import logging
import time
import uuid
from contextlib import nullcontext
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, root_validator

from ray.llm._internal.batch.constants import SGLangTaskType, TypeSGLangTaskType
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.common import (
    convert_to_embed_override_tensors,
    maybe_convert_ndarray_to_list,
    to_token_id_list,
)

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


def _check_sglang_supports_score(engine_cls: Any) -> None:
    """Raise if the installed SGLang's `Engine.async_score` is missing or too old.

    Runs before the engine is constructed, so no weights are loaded first.
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
    missing = [name for name in _REQUIRED_ASYNC_SCORE_KWARGS if name not in params]
    if missing:
        raise ImportError(
            f"task_type='score' requires `sglang.Engine.async_score` to accept "
            f"{missing}. Please upgrade SGLang."
        )


class SGLangEngineRequest(BaseModel):
    """A request to the SGLang engine."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int
    # The input prompt.
    prompt: Optional[str] = None
    # Alternative to text. Specify the input as token IDs instead of text.
    prompt_token_ids: Optional[List[int]] = None
    # The sampling parameters (more details can be seen in https://docs.sglang.ai/backend/sampling_params.html).
    params: Optional[Dict[str, Any]] = None

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class SGLangScoreRequest(BaseModel):
    """A score request to the SGLang engine: one query and its items."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int
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


class SGLangOutputData(BaseModel):
    """The output of the SGLang engine."""

    prompt: Optional[str] = None
    prompt_token_ids: Optional[List[int]] = None
    num_input_tokens: int

    # Generate fields.
    generated_tokens: Optional[List[int]] = None
    generated_text: Optional[str] = None
    num_generated_tokens: int

    # Metrics fields.
    metrics: Optional[Dict[str, Any]] = None

    @classmethod
    def from_sglang_engine_output(cls, output: Dict[str, Any]) -> "SGLangOutputData":
        """Create a SGLangOutputData from a SGLang engine output."""

        # Set by `_generate_async`.
        assert "prompt" in output
        assert "prompt_token_ids" in output

        # Returned in the native output of the SGLang engine.
        assert "meta_info" in output
        assert "prompt_tokens" in output["meta_info"]
        assert "completion_tokens" in output["meta_info"]

        data = cls(
            prompt=output["prompt"],
            prompt_token_ids=output["prompt_token_ids"],
            num_input_tokens=output["meta_info"]["prompt_tokens"],
            generated_tokens=output["output_ids"] if "output_ids" in output else None,
            generated_text=output["text"] if "text" in output else None,
            num_generated_tokens=output["meta_info"]["completion_tokens"],
        )

        return data

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class SGLangEngineWrapper:
    """Wrapper around the SGLang engine to handle async requests.

    Args:
        idx_in_batch_column: The column name for the index of the row in the batch.
        max_pending_requests: The maximum number of pending requests in the queue.
        score_options: Per-run arguments for `Engine.async_score` (score task only).
        **kwargs: The keyword arguments for the engine.
    """

    def __init__(
        self,
        idx_in_batch_column: str,
        max_pending_requests: int = -1,
        score_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.request_id = 0
        self.idx_in_batch_column = idx_in_batch_column
        self.task_type = kwargs.pop("task", SGLangTaskType.GENERATE)
        self.model = kwargs.pop("model", None)
        assert self.model is not None
        # We need to rename the `model` to `model_path` for SGLang.
        kwargs["model_path"] = self.model
        self.score_options = dict(score_options or {})

        # Set the skip_tokenizer_init to True by default for SGLang engine
        # because we will not use the tokenizer/detokenizer in SGLang engine
        # by default.
        self.skip_tokenizer_init = kwargs.pop("skip_tokenizer_init", True)
        if self.task_type == SGLangTaskType.SCORE and not self.skip_tokenizer_init:
            logger.warning(
                "Ignoring skip_tokenizer_init=False: the score task always "
                "passes token IDs to the engine."
            )
            self.skip_tokenizer_init = True
        kwargs["skip_tokenizer_init"] = self.skip_tokenizer_init

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

        if self.task_type == SGLangTaskType.SCORE:
            _check_sglang_supports_score(sglang.Engine)

        # Initialize the SGLang engine
        self.engine = sglang.Engine(**kwargs)

        # The performance gets really bad if there are too many requests in the pending queue.
        # We work around it with semaphore to limit the number of concurrent requests in the engine.
        self.max_pending_requests = max_pending_requests
        if self.max_pending_requests > 0:
            self.semaphore = asyncio.Semaphore(self.max_pending_requests)
        else:
            # Use contextlib.nullcontext which works for both sync and async contexts.
            self.semaphore = nullcontext()

    async def _prepare_llm_request(self, row: Dict[str, Any]) -> SGLangEngineRequest:
        """Prepare the inputs for LLM inference.

        Args:
            row: The row.

        Returns:
            A single SGLangEngineRequest.
        """
        prompt = row.pop("prompt")

        if "tokenized_prompt" in row:
            tokenized_prompt = row.pop("tokenized_prompt").tolist()
        else:
            tokenized_prompt = None

        # Prepare sampling parameters.
        if self.task_type == SGLangTaskType.GENERATE:
            params = maybe_convert_ndarray_to_list(row.pop("sampling_params"))
        else:
            raise ValueError(f"Unsupported task type: {self.task_type}")

        if tokenized_prompt is not None and not self.skip_tokenizer_init:
            raise ValueError(
                "To use a token-in-token-out mode of SGLang Engine, please set engine_kwargs['skip_tokenizer_init'] to True."
            )

        request = SGLangEngineRequest(
            request_id=self.request_id,
            idx_in_batch=row[self.idx_in_batch_column],
            prompt=prompt,
            prompt_token_ids=tokenized_prompt,
            params=params,
        )
        self.request_id += 1
        return request

    def _prepare_score_request(self, row: Dict[str, Any]) -> SGLangScoreRequest:
        """Build a score request from a row.

        Args:
            row: The row, with `query_token_ids` and `item_token_ids`.

        Returns:
            A single SGLangScoreRequest.
        """
        query_token_ids = to_token_id_list(row["query_token_ids"], "query_token_ids")
        items = maybe_convert_ndarray_to_list(row["item_token_ids"])
        if not isinstance(items, list) or not items:
            raise ValueError("'item_token_ids' must be a non-empty list of lists.")
        item_token_ids = [to_token_id_list(item, "item_token_ids") for item in items]

        # Overrides are popped so the large vectors aren't carried to the output.
        query_embed_overrides = convert_to_embed_override_tensors(
            row.pop("query_embed_overrides", None)
        )
        item_embed_overrides = None
        raw_item_embeds = row.pop("item_embed_overrides", None)
        if raw_item_embeds is not None:
            converted = [
                convert_to_embed_override_tensors(entry) for entry in raw_item_embeds
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

        request = SGLangScoreRequest(
            request_id=self.request_id,
            idx_in_batch=row[self.idx_in_batch_column],
            query_token_ids=query_token_ids,
            item_token_ids=item_token_ids,
            query_embed_overrides=query_embed_overrides,
            item_embed_overrides=item_embed_overrides,
        )
        self.request_id += 1
        return request

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[SGLangEngineRequest, Dict[str, Any], float]:
        """Process a single request.

        Args:
            row: The input row.

        Returns:
            A tuple of index in batch, request output and bypassed custom fields, and time taken.
        """
        if self.task_type == SGLangTaskType.SCORE:
            return await self._score_async(row)

        request = await self._prepare_llm_request(row)
        t = time.perf_counter()

        async with self.semaphore:
            output = await self._generate_async(request)

        time_taken = time.perf_counter() - t

        output_data = SGLangOutputData.from_sglang_engine_output(output)
        return request, output_data.model_dump(), time_taken

    async def _generate_async(self, request: SGLangEngineRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        # Send the request to the LLM engine.
        stream = await self.engine.async_generate(
            prompt=request.prompt,
            input_ids=request.prompt_token_ids,
            sampling_params=request.params,
            stream=True,
        )

        # Consume the stream until the request is finished.
        async for output in stream:
            if output["meta_info"]["finish_reason"] is not None:
                output["prompt"] = request.prompt
                output["prompt_token_ids"] = request.prompt_token_ids
                return output

        raise RuntimeError(
            "[SGLang] The request is not finished. This should not happen. Please report this issue to the Ray team."
        )

    async def _score_async(
        self, row: Dict[str, Any]
    ) -> Tuple[SGLangScoreRequest, Dict[str, Any], float]:
        """Score one query against its items.

        Uses `async_score` because the synchronous `score` runs its own event
        loop and would deadlock inside this one.

        Args:
            row: The input row.

        Returns:
            A tuple of the request, the score output, and time taken.
        """
        request = self._prepare_score_request(row)
        t = time.perf_counter()
        async with self.semaphore:
            result = await self.engine.async_score(
                query=request.query_token_ids,
                items=request.item_token_ids,
                query_embed_overrides=request.query_embed_overrides,
                item_embed_overrides=request.item_embed_overrides,
                **self.score_options,
            )
        time_taken = time.perf_counter() - t

        scores = getattr(result, "scores", None)
        if scores is None or len(scores) != len(request.item_token_ids):
            raise RuntimeError(
                f"[SGLang] Expected {len(request.item_token_ids)} score lists, got "
                f"{None if scores is None else len(scores)}. Please report this "
                "issue to the Ray team."
            )
        output = {
            "scores": [list(item_scores) for item_scores in scores],
            "num_input_tokens": getattr(result, "prompt_tokens", 0) or 0,
        }
        return request, output, time_taken

    def shutdown(self):
        """Shutdown the SGLang engine."""
        if hasattr(self.engine, "shutdown"):
            logger.info("Shutting down SGLang engine")
            self.engine.shutdown()


class SGLangEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        model: str,
        engine_kwargs: Dict[str, Any],
        task_type: TypeSGLangTaskType = SGLangTaskType.GENERATE,
        max_pending_requests: Optional[int] = None,
        score_options: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the SGLangEngineStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The path to the model to use for the SGLang engine.
            engine_kwargs: The kwargs to pass to the SGLang engine.
            task_type: The task to use for the SGLang engine ("generate" or "score").
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to a default value based on engine settings.
            score_options: Per-run arguments for `Engine.async_score`, such as
                `label_token_ids`. Only used by the score task.
        """
        super().__init__(data_column, expected_input_keys)
        self.model = model

        # Setup SGLang engine kwargs.
        self.task_type = task_type
        self.engine_kwargs = self.normalize_engine_kwargs(engine_kwargs)

        # Set up the max pending requests.
        # Disable the semaphore if max_pending_requests is not set.
        self.max_pending_requests = max_pending_requests or -1
        if self.max_pending_requests > 0:
            logger.info("Max pending requests is set to %d", self.max_pending_requests)

        # Create an LLM engine.
        self.llm = SGLangEngineWrapper(
            model=self.model,
            idx_in_batch_column=self.IDX_IN_BATCH_COLUMN,
            max_pending_requests=self.max_pending_requests,
            score_options=score_options,
            **self.engine_kwargs,
        )

    def normalize_engine_kwargs(
        self,
        engine_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Normalize the engine kwargs.

        Args:
            engine_kwargs: The kwargs to normalize.

        Returns:
            The normalized kwargs.
        """
        # Copy to avoid mutating fn_constructor_kwargs. Ray Data generates UDF
        # instance keys before __init__, so in-place changes cause KeyError.
        engine_kwargs = engine_kwargs.copy()

        # Remove model from engine kwargs if set.
        model = engine_kwargs.pop("model", None)
        if model is not None and model != self.model:
            logger.warning(
                "The model set in engine kwargs (%s) is different from the "
                "stage (%s). Please remove 'model' from engine kwargs.",
                model,
                self.model,
            )
        return engine_kwargs

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Run the SGLang engine.

        Args:
            batch: A list of rows to run the SGLang engine on.

        Yields:
            Dict[str, Any]: The response of the SGLang engine.
        """
        batch_uuid = uuid.uuid4()
        batch_start_time = time.perf_counter()

        tasks = [asyncio.create_task(self.llm.generate_async(row)) for row in batch]

        for resp in asyncio.as_completed(tasks):
            request, output, time_taken_llm = await resp

            result = {
                **output,
                "request_id": request.request_id,
                self.IDX_IN_BATCH_COLUMN: request.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken_llm,
            }
            if self.task_type != SGLangTaskType.SCORE:
                result["params"] = str(request.params)
            yield result

        batch_time_taken = time.perf_counter() - batch_start_time
        logger.info(
            "[SGLang] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            batch_time_taken,
        )

    def __del__(self):
        if hasattr(self, "llm"):
            self.llm.shutdown()


class SGLangEngineStage(StatefulStage):
    """
    A stage that runs SGLang engine.
    """

    fn: Type[StatefulStageUDF] = SGLangEngineStageUDF

    @root_validator(pre=True)
    def post_init(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """Post-initialize the stage. Specifically,
        this function determines the num_gpus and Ray remote args
        for the .map_batches() call in this stage.

        Args:
            values: The raw stage values.
        Returns:
            The updated values.
        """
        map_batches_kwargs = values["map_batches_kwargs"]
        accelerator_type = map_batches_kwargs.get("accelerator_type", "")
        fn_constructor_kwargs = values["fn_constructor_kwargs"]
        engine_kwargs = fn_constructor_kwargs.get("engine_kwargs", {})

        ray_remote_args = {}
        if accelerator_type:
            ray_remote_args["accelerator_type"] = accelerator_type

        # Set up num_gpus required
        tp_size = engine_kwargs.get("tp_size", 1)
        dp_size = engine_kwargs.get("dp_size", 1)
        num_gpus = tp_size * dp_size

        ray_remote_args["num_gpus"] = num_gpus
        map_batches_kwargs.update(ray_remote_args)
        return values

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        task_type = self.fn_constructor_kwargs.get("task_type", SGLangTaskType.GENERATE)
        if task_type == SGLangTaskType.SCORE:
            return {
                "query_token_ids": "The query token IDs (List[int]).",
                "item_token_ids": "The token IDs of each item (List[List[int]]).",
            }
        ret = {"prompt": "The text prompt (str)."}
        if task_type == SGLangTaskType.GENERATE:
            ret[
                "sampling_params"
            ] = "The sampling parameters. See https://docs.sglang.ai/backend/sampling_params.html for details."
        return ret

    def get_optional_input_keys(self) -> Dict[str, str]:
        """The optional input keys of the stage and their descriptions."""
        task_type = self.fn_constructor_kwargs.get("task_type", SGLangTaskType.GENERATE)
        if task_type == SGLangTaskType.SCORE:
            return {
                "query_embed_overrides": "Embedding overrides for the query "
                "markers, shaped [num_markers, hidden_dim].",
                "item_embed_overrides": "Embedding overrides for each item's "
                "markers (one entry or None per item).",
            }
        return {}
