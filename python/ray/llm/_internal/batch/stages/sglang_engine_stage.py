"""The stage that runs SGLang engine."""

import asyncio
import logging
import time
import uuid
from contextlib import nullcontext
from enum import Enum
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, root_validator

from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.common import maybe_convert_ndarray_to_list

logger = logging.getLogger(__name__)


class SGLangTaskType(str, Enum):
    """The type of task to run on the SGLang engine."""

    """Generate text."""
    GENERATE = "generate"


class SGLangEngineRequest(BaseModel):
    """A request to the SGLang engine."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int
    # The input prompt.
    prompt: Optional[str]
    # Alternative to text. Specify the input as token IDs instead of text.
    prompt_token_ids: Optional[List[int]]
    # The sampling parameters (more details can be seen in https://docs.sglang.ai/backend/sampling_params.html).
    params: Optional[Dict[str, Any]]

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class SGLangOutputData(BaseModel):
    """The output of the SGLang engine."""

    prompt: Optional[str]
    prompt_token_ids: Optional[List[int]]
    num_input_tokens: int

    # Generate fields.
    generated_tokens: Optional[List[int]]
    generated_text: Optional[str]
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
        *args: The positional arguments for the engine.
        max_pending_requests: The maximum number of pending requests in the queue.
        **kwargs: The keyword arguments for the engine.
    """

    def __init__(
        self,
        idx_in_batch_column: str,
        max_pending_requests: int = -1,
        **kwargs,
    ):
        self.request_id = 0
        self.idx_in_batch_column = idx_in_batch_column
        self.task_type = kwargs.pop("task", SGLangTaskType.GENERATE)
        self.model = kwargs.pop("model", None)
        assert self.model is not None
        # We need to rename the `model` to `model_path` for SGLang.
        kwargs["model_path"] = self.model

        # Set the skip_tokenizer_init to True by default for SGLang engine
        # because we will not use the tokenizer/detokenizer in SGLang engine
        # by default.
        self.skip_tokenizer_init = kwargs.pop("skip_tokenizer_init", True)
        kwargs["skip_tokenizer_init"] = self.skip_tokenizer_init

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

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

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[SGLangEngineRequest, Dict[str, Any], float]:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            A tuple of index in batch, request output and bypassed custom fields, and time taken.
        """
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
        task_type: SGLangTaskType = SGLangTaskType.GENERATE,
        max_pending_requests: Optional[int] = None,
    ):
        """
        Initialize the SGLangEngineStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The path to the model to use for the SGLang engine.
            engine_kwargs: The kwargs to pass to the SGLang engine.
            task_type: The task to use for the SGLang engine (e.g., "generate", "embed", "reward").
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to a default value based on engine settings.
        """
        super().__init__(data_column, expected_input_keys)
        self.model = model

        # Setup SGLang engine kwargs.
        self.task_type = task_type
        self.engine_kwargs = self.normalize_engine_kwargs(task_type, engine_kwargs)

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
            **self.engine_kwargs,
        )

    def normalize_engine_kwargs(
        self,
        task_type: SGLangTaskType,
        engine_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Normalize the engine kwargs.

        Args:
            task_type: The task to use for the SGLang engine (e.g., "generate", etc).
            engine_kwargs: The kwargs to normalize.

        Returns:
            The normalized kwargs.
        """
        # Remove model from engine kwargs if set.
        model = engine_kwargs.pop("model", None)
        if model is not None and model != self.model:
            logger.warning(
                "The model set in engine kwargs (%s) is different from the "
                "stage (%s). Please remove 'model' from engine kwargs.",
                model,
                self.model,
            )

        # Override the task if it is different from the stage.
        task = SGLangTaskType(engine_kwargs.get("task", task_type))
        if task != task_type:
            logger.warning(
                "The task set in engine kwargs (%s) is different from the "
                "stage (%s). Overriding the task in engine kwargs to %s.",
                task,
                task_type,
                task_type,
            )
        engine_kwargs["task"] = task_type
        return engine_kwargs

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Run the SGLang engine.

        Args:
            batch: A list of rows to run the SGLang engine on.

        Returns:
            The response of the SGLang engine.
        """
        batch_uuid = uuid.uuid4()
        batch_start_time = time.perf_counter()

        tasks = [asyncio.create_task(self.llm.generate_async(row)) for row in batch]

        for resp in asyncio.as_completed(tasks):
            request, output, time_taken_llm = await resp

            yield {
                **output,
                "request_id": request.request_id,
                self.IDX_IN_BATCH_COLUMN: request.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken_llm,
                "params": str(request.params),
            }

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
    def post_init(cls, values):
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
        ret = {"prompt": "The text prompt (str)."}
        task_type = self.fn_constructor_kwargs.get("task_type", SGLangTaskType.GENERATE)
        if task_type == SGLangTaskType.GENERATE:
            ret[
                "sampling_params"
            ] = "The sampling parameters. See https://docs.sglang.ai/backend/sampling_params.html for details."
        return ret

    def get_optional_input_keys(self) -> Dict[str, str]:
        """The optional input keys of the stage and their descriptions."""
        return {}
