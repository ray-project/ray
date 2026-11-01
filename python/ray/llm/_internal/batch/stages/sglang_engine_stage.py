"""The stage that runs SGLang engine."""

import asyncio
import logging
import time
import uuid
from contextlib import nullcontext
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

from pydantic import root_validator

from ray.llm._internal.batch.constants import SGLangTaskType, TypeSGLangTaskType
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.sglang_generate_task_handler import (
    _SGLangGenerateTaskHandler,
)
from ray.llm._internal.batch.stages.sglang_score_task_handler import (
    _SGLangScoreTaskHandler,
)
from ray.llm._internal.batch.stages.sglang_task_handler import (
    _SGLangRequest,
    _SGLangTaskHandler,
)

logger = logging.getLogger(__name__)


# The handler class for each task type.
_TASK_HANDLERS: Dict[str, Type[_SGLangTaskHandler]] = {
    SGLangTaskType.GENERATE: _SGLangGenerateTaskHandler,
    SGLangTaskType.SCORE: _SGLangScoreTaskHandler,
}


def _get_task_handler_cls(task_type: str) -> Type[_SGLangTaskHandler]:
    """Return the handler class for `task_type`, or raise if it's unsupported."""
    handler_cls = _TASK_HANDLERS.get(task_type)
    if handler_cls is None:
        raise ValueError(f"Unsupported task type: {task_type}")
    return handler_cls


class SGLangEngineWrapper:
    """Wrapper around the SGLang engine to handle async requests.

    Args:
        idx_in_batch_column: The column name for the index of the row in the batch.
        max_pending_requests: The maximum number of pending requests in the queue.
        task_options: Keyword arguments for the task handler's constructor.
        **kwargs: The keyword arguments for the engine.
    """

    def __init__(
        self,
        idx_in_batch_column: str,
        max_pending_requests: int = -1,
        task_options: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.request_id = 0
        self.idx_in_batch_column = idx_in_batch_column
        self.task_type = kwargs.pop("task", SGLangTaskType.GENERATE)
        self.task_handler = _get_task_handler_cls(self.task_type)(
            **(task_options or {})
        )
        self.model = kwargs.pop("model", None)
        assert self.model is not None
        # We need to rename the `model` to `model_path` for SGLang.
        kwargs["model_path"] = self.model

        # Set the skip_tokenizer_init to True by default for SGLang engine
        # because we will not use the tokenizer/detokenizer in SGLang engine
        # by default.
        kwargs["skip_tokenizer_init"] = kwargs.pop("skip_tokenizer_init", True)
        self.task_handler.configure_engine_kwargs(kwargs)

        try:
            import sglang
        except ImportError as e:
            raise ImportError(
                "SGLang is not installed or failed to import. Please run "
                "`pip install sglang[all]` to install required dependencies."
            ) from e

        self.task_handler.check_engine(sglang.Engine)

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

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[_SGLangRequest, Dict[str, Any], float]:
        """Process a single request.

        Args:
            row: The input row.

        Returns:
            A tuple of the request, its output columns, and the time taken.
        """
        request = self.task_handler.prepare_request(
            row, self.request_id, row[self.idx_in_batch_column]
        )
        self.request_id += 1

        t = time.perf_counter()
        async with self.semaphore:
            output = await self.task_handler.run(self.engine, request)
        return request, output, time.perf_counter() - t

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
        task_options: Optional[Dict[str, Any]] = None,
    ):
        """Initialize the SGLangEngineStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The path to the model to use for the SGLang engine.
            engine_kwargs: The kwargs to pass to the SGLang engine.
            task_type: The task to use for the SGLang engine ("generate" or "score").
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to a default value based on engine settings.
            task_options: Keyword arguments for the task handler's constructor.
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
            task_options=task_options,
            **self.engine_kwargs,
        )

    def normalize_engine_kwargs(
        self,
        engine_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Normalize the engine kwargs.

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

            yield {
                **output,
                "request_id": request.request_id,
                self.IDX_IN_BATCH_COLUMN: request.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken_llm,
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
    """A stage that runs SGLang engine."""

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
        return dict(self._task_handler_cls().required_input_keys)

    def get_optional_input_keys(self) -> Dict[str, str]:
        """The optional input keys of the stage and their descriptions."""
        return dict(self._task_handler_cls().optional_input_keys)

    def _task_handler_cls(self) -> Type[_SGLangTaskHandler]:
        """The handler class for this stage's task type."""
        return _get_task_handler_cls(
            self.fn_constructor_kwargs.get("task_type", SGLangTaskType.GENERATE)
        )
