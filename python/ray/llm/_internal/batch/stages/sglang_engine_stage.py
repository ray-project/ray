"""The stage that runs SGLang engine."""

import asyncio
import logging
import time
import uuid
from enum import Enum
from pydantic import BaseModel, root_validator
from typing import Any, Dict, AsyncIterator, Optional, List, Tuple, Type

from ray.llm._internal.utils import try_import
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)

sgl = try_import("sglang")

logger = logging.getLogger(__name__)


class SGLangTaskType(str, Enum):
    """The type of task to run on the SGLang engine."""

    """Generate text."""
    GENERATE = "generate"

    # TODO: Add support for the following tasks.
    """Generate embeddings."""
    EMBED = "embed"

    """Classify the quality of pairwise generations."""
    REWARD = "reward"


class SGLangEngineRequest(BaseModel):
    """A request to the SGLang engine."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int
    # The input prompt.
    text: Optional[str]
    # Alternative to text. Specify the input as token IDs instead of text.
    input_ids: Optional[List[int]]
    # The sampling parameters (more details can be seen in https://docs.sglang.ai/backend/sampling_params.html).
    sampling_params: Optional[Dict[str, Any]]

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
        self.model_path = kwargs.get("model_path", None)
        assert self.model_path is not None

        if sgl is None:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install ray[llm]` to install required dependencies."
            )

        # Initialize the SGLang engine
        self.engine = sgl.Engine(**kwargs)

        # The performance gets really bad if there are too many requests in the pending queue.
        # We work around it with semaphore to limit the number of concurrent requests in the engine.
        self.max_pending_requests = max_pending_requests
        if self.max_pending_requests > 0:
            self.semaphore = asyncio.Semaphore(self.max_pending_requests)
        else:
            self.semaphore = asyncio.NullContext()

    async def _prepare_llm_request(self, row: Dict[str, Any]) -> SGLangEngineRequest:
        """Prepare the inputs for LLM inference.

        Args:
            row: The row.

        Returns:
            A single SGLangEngineRequest.
        """
        text = row.pop("text")
        if "input_ids" in row:
            input_ids = row.pop("input_ids").tolist()
        else:
            input_ids = None

        # Prepare sampling parameters.
        if self.task_type == SGLangTaskType.GENERATE:
            sampling_params = row.pop("sampling_params")
        else:
            raise ValueError(f"Unsupported task type: {self.task_type}")

        request = SGLangEngineRequest(
            request_id=self.request_id,
            idx_in_batch=row[self.idx_in_batch_column],
            text=text,
            input_ids=input_ids,
            sampling_params=sampling_params,
        )
        self.request_id += 1
        return request

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[SGLangEngineRequest, Dict[str, Any]]:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            A tuple of index in batch, request output and bypassed custom fields.
        """
        request = await self._prepare_llm_request(row)

        async with self.semaphore:
            output = await self._generate_async(request)

        output_data = SGLangOutputData.from_sglang_engine_output(output)
        return request, output_data.model_dump()

    async def _generate_async(self, request: SGLangEngineRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        # Send the request to the LLM engine.
        output = await self.engine.async_generate(
            prompt=request.text,
            sampling_params=request.sampling_params,
            input_ids=request.input_ids,
        )
        output["prompt"] = request.text
        output["prompt_token_ids"] = request.input_ids
        return output

    def shutdown(self):
        """Shutdown the SGLang engine."""
        if hasattr(self.engine, "shutdown"):
            logger.info("Shutting down SGLang engine")
            self.engine.shutdown()


class SGLangEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model_path: str,
        engine_kwargs: Dict[str, Any],
        task_type: SGLangTaskType = SGLangTaskType.GENERATE,
        max_pending_requests: Optional[int] = None,
    ):
        """
        Initialize the SGLangEngineStageUDF.

        Args:
            data_column: The data column name.
            model_path: The path to the model to use for the SGLang engine.
            engine_kwargs: The kwargs to pass to the SGLang engine.
            task_type: The task to use for the SGLang engine (e.g., "generate", "embed", "reward").
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to a default value based on engine settings.
        """
        super().__init__(data_column)
        self.model_path = model_path

        # Setup SGLang engine kwargs.
        self.task_type = task_type
        self.engine_kwargs = self.normalize_engine_kwargs(task_type, engine_kwargs)

        # Set up the max pending requests.
        # Use a reasonable default value if not specified
        self.max_pending_requests = max_pending_requests or 128
        if self.max_pending_requests > 0:
            logger.info("Max pending requests is set to %d", self.max_pending_requests)

        # Create an LLM engine.
        self.llm = SGLangEngineWrapper(
            model_path=self.model_path,
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
        model_path = engine_kwargs.pop("model_path", None)
        if model_path is not None and model_path != self.model_path:
            logger.warning(
                "The model path set in engine kwargs (%s) is different from the "
                "stage (%s). Please remove 'model_path' from engine kwargs.",
                model_path,
                self.model_path,
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
        t = time.perf_counter()

        tasks = [asyncio.create_task(self.llm.generate_async(row)) for row in batch]

        time_taken = -1.0
        for resp in asyncio.as_completed(tasks):
            request, output = await resp
            time_taken = time.perf_counter() - t

            yield {
                **output,
                "request_id": request.request_id,
                self.IDX_IN_BATCH_COLUMN: request.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken,
                "sampling_params": str(request.sampling_params),
            }

        logger.info(
            "[SGLang] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            time_taken,
        )

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""

        ret = ["text"]
        if self.task_type == SGLangTaskType.GENERATE:
            ret.append("sampling_params")
        return ret

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

        # Setup num_gpus required per SGLang engine.
        tp_size = engine_kwargs.get("tp", 1)
        dp_size = engine_kwargs.get("dp", 1)
        num_gpus = tp_size * dp_size

        map_batches_kwargs["num_gpus"] = num_gpus
        map_batches_kwargs.update(ray_remote_args)
        return values
