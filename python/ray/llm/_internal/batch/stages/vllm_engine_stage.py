"""The stage that runs vLLM engine."""
import asyncio
import dataclasses
import logging
import math
import time
import uuid
from enum import Enum
from functools import partial
from pydantic import BaseModel, Field, root_validator
from typing import Any, Dict, AsyncIterator, Optional, List, Tuple, Type

import numpy as np

import ray
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.utils import try_import
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

vllm = try_import("vllm")


logger = logging.getLogger(__name__)


class vLLMTaskType(str, Enum):
    """The type of task to run on the vLLM engine."""

    """Generate text."""
    GENERATE = "generate"

    """Generate embeddings."""
    EMBED = "embed"


class vLLMEngineRequest(BaseModel):
    """A request to the vLLM engine."""

    # The request ID for the LLM engine (unique per replica).
    request_id: int
    # The index of the request in the batch.
    idx_in_batch: int
    # The full prompt string (with chat template applied if any).
    prompt: str
    # The images inputs for the multimodal model. Use Any to avoid importing PIL.
    images: List[Any]
    # The tokenized prompt IDs. If None, then the string prompt will be
    # tokenized by the LLM engine. This is not recommended for performance reasons.
    prompt_token_ids: Optional[List[int]]
    # The sampling or pooling parameters. Use Any to avoid importing vLLM.
    params: Any
    # LoRA request.
    lora_request: Optional[Any] = None

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class vLLMOutputData(BaseModel):
    """The output of the vLLM engine."""

    prompt: str
    prompt_token_ids: Optional[List[int]]
    num_input_tokens: int

    # Generate fields.
    generated_tokens: List[int] = Field(default_factory=list)
    generated_text: str = Field(default="")
    num_generated_tokens: int = Field(default=0)

    # Embed fields. The type should be torch.Tensor, but we use Any to avoid
    # importing torch because of an error in sphinx-build with an unknown reason.
    embeddings: Optional[Any] = None

    # Metrics fields.
    metrics: Optional[Dict[str, Any]] = None

    @classmethod
    def from_vllm_engine_output(cls, output: Any) -> "vLLMOutputData":
        """Create a vLLMOutputData from a vLLM engine output."""

        prompt_token_ids = output.prompt_token_ids
        if isinstance(prompt_token_ids, np.ndarray):
            prompt_token_ids = prompt_token_ids.tolist()

        data = cls(
            prompt=output.prompt,
            prompt_token_ids=prompt_token_ids,
            num_input_tokens=len(prompt_token_ids),
        )

        if isinstance(output, vllm.outputs.RequestOutput):
            metrics = {}
            if output.metrics is not None:
                metrics = dict(dataclasses.asdict(output.metrics))
                data.metrics = metrics
            data.generated_tokens = output.outputs[0].token_ids
            data.generated_text = output.outputs[0].text
            data.num_generated_tokens = len(output.outputs[0].token_ids)
        elif isinstance(output, vllm.outputs.PoolingRequestOutput):
            data.embeddings = output.outputs.data.cpu()
        else:
            raise ValueError(f"Unknown output type: {type(output)}")

        return data

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


class vLLMEngineWrapper:
    """Wrapper around the vLLM engine to handle async requests.

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
        self.task_type = kwargs.get("task", vLLMTaskType.GENERATE)
        self.model = kwargs.get("model", None)
        assert self.model is not None

        # LoRA related.
        self.lora_lock = asyncio.Lock()
        self.lora_name_to_request = {}

        # Convert the task type back to a string to pass to the engine.
        kwargs["task"] = self.task_type.value

        if vllm is None:
            raise ImportError(
                "vLLM is not installed or failed to import. Please run "
                "`pip install ray[llm]` to install required dependencies."
            )

        # Construct PoolerConfig if override_pooler_config is specified.
        if self.task_type == vLLMTaskType.EMBED and "override_pooler_config" in kwargs:
            kwargs["override_pooler_config"] = vllm.config.PoolerConfig(
                **kwargs["override_pooler_config"]
            )

        # Initialize the vLLM engine.
        engine_args = vllm.AsyncEngineArgs(
            **kwargs,
            disable_log_requests=True,
        )
        self.engine = vllm.AsyncLLMEngine.from_engine_args(engine_args)

        # Determine the generate function based on vLLM v0 or v1.
        self.vllm_use_v1 = vllm.envs.VLLM_USE_V1
        if self.vllm_use_v1:
            self._generate_async = self.generate_async_v1
        else:
            self._generate_async = self.generate_async_v0

        # The performance gets really bad if there are too many requests in the pending queue.
        # We work around it with semaphore to limit the number of concurrent requests in the engine.
        self.max_pending_requests = max_pending_requests
        if self.max_pending_requests > 0:
            self.semaphore = asyncio.Semaphore(self.max_pending_requests)
        else:
            self.semaphore = asyncio.NullContext()

    def _maybe_convert_ndarray_to_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Convert all ndarray to list in the params. This is because Ray Data
        by default converts all lists to ndarrays when passing data around, but
        vLLM expects lists.

        Args:
            params: The parameters to convert.

        Returns:
            The converted parameters.
        """
        if isinstance(params, dict):
            return {
                k: self._maybe_convert_ndarray_to_list(v) for k, v in params.items()
            }
        elif isinstance(params, list):
            return [self._maybe_convert_ndarray_to_list(v) for v in params]
        elif isinstance(params, np.ndarray):
            return params.tolist()
        return params

    async def _prepare_llm_request(self, row: Dict[str, Any]) -> vLLMEngineRequest:
        """Prepare the inputs for LLM inference.

        Args:
            row: The row.

        Returns:
            A single vLLMEngineRequest.
        """
        prompt = row.pop("prompt")

        if "tokenized_prompt" in row:
            tokenized_prompt = row.pop("tokenized_prompt").tolist()
        else:
            tokenized_prompt = None

        if "image" in row:
            image = row.pop("image")
        else:
            image = []

        # If the model name is given and is different from the model
        # set in the config, then this is a LoRA.
        lora_request = None
        if "model" in row and row["model"] != self.model:
            if self.vllm_use_v1:
                raise ValueError("LoRA is only supported with vLLM v0")

            lora_name = row["model"]
            if lora_name not in self.lora_name_to_request:
                async with self.lora_lock:
                    if lora_name not in self.lora_name_to_request:
                        # Load a new LoRA adapter if it is not loaded yet.
                        lora_request = vllm.lora.request.LoRARequest(
                            lora_name=lora_name,
                            # LoRA ID starts from 1.
                            lora_int_id=len(self.lora_name_to_request) + 1,
                            lora_path=lora_name,
                        )
                        self.lora_name_to_request[lora_name] = lora_request
            lora_request = self.lora_name_to_request[lora_name]

        # Prepare sampling parameters.
        if self.task_type == vLLMTaskType.GENERATE:
            sampling_params = row.pop("sampling_params")
            if "guided_decoding" in sampling_params:
                if self.vllm_use_v1:
                    raise ValueError("Guided decoding is only supported with vLLM v0")

                guided_decoding = vllm.sampling_params.GuidedDecodingParams(
                    **self._maybe_convert_ndarray_to_list(
                        sampling_params.pop("guided_decoding")
                    )
                )
            else:
                guided_decoding = None
            params = vllm.SamplingParams(
                **sampling_params,
                guided_decoding=guided_decoding,
            )
        elif self.task_type == vLLMTaskType.EMBED:
            params = vllm.PoolingParams()
        else:
            raise ValueError(f"Unsupported task type: {self.task_type}")

        request = vLLMEngineRequest(
            request_id=self.request_id,
            idx_in_batch=row[self.idx_in_batch_column],
            prompt=prompt,
            prompt_token_ids=tokenized_prompt,
            images=image,
            params=params,
            lora_request=lora_request,
        )
        self.request_id += 1
        return request

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[vLLMEngineRequest, Dict[str, Any]]:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            A tuple of index in batch, request output and bypassed custom fields.
        """
        request = await self._prepare_llm_request(row)

        async with self.semaphore:
            output = await self._generate_async(request)

        output_data = vLLMOutputData.from_vllm_engine_output(output)
        return request, output_data.model_dump()

    async def generate_async_v0(self, request: vLLMEngineRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        if request.images:
            # FIXME: The latest vLLM does not support multi-modal inputs
            # with tokenized prompt.
            assert request.prompt
            llm_prompt = vllm.inputs.data.TextPrompt(
                prompt=request.prompt, multi_modal_data={"image": request.images}
            )
        else:
            if request.prompt_token_ids is not None:
                llm_prompt = vllm.inputs.data.TokensPrompt(
                    prompt_token_ids=request.prompt_token_ids
                )
            else:
                assert request.prompt
                llm_prompt = vllm.inputs.data.TextPrompt(prompt=request.prompt)

        # Send the request to the LLM engine.
        stream = await self.engine.add_request(
            request_id=str(request.request_id),
            prompt=llm_prompt,
            params=request.params,
            lora_request=request.lora_request,
        )
        # Consume the stream until the request is finished.
        async for request_output in stream:
            if request_output.finished:
                # Bypass the original full prompt.
                request_output.prompt = request.prompt
                return request_output

        raise RuntimeError(
            "[vLLM] The request is not finished. This should not happen. "
            "Please report this issue to the Ray team."
        )

    async def generate_async_v1(self, request: vLLMEngineRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        # NOTE: vLLM v1 tighly couples tokenizer and detokenizer to the engine.
        # We should investigate whether decoupling them could lead to better
        # performance. Given that v1 tokenizer and detokenizer are already
        # in a separate process, the benefit of decoupling them in the Processor
        # may be limited.
        assert request.prompt
        multi_modal_data = {"image": request.images} if request.images else None
        llm_prompt = vllm.inputs.data.TextPrompt(
            prompt=request.prompt, multi_modal_data=multi_modal_data
        )

        # Send the request to the LLM engine.
        stream = self.engine.generate(
            request_id=str(request.request_id),
            prompt=llm_prompt,
            sampling_params=request.params,
        )

        # Consume the stream until the request is finished.
        async for request_output in stream:
            if request_output.finished:
                # Bypass the original full prompt.
                request_output.prompt = request.prompt
                return request_output

        raise RuntimeError(
            "[vLLM] The request is not finished. This should not happen. "
            "Please report this issue to the Ray team."
        )

    def shutdown(self):
        """Shutdown the vLLM v1 engine. This kills child processes forked
        by the vLLM engine. If not called, the child processes will be
        orphaned and will not be killed when the parent process exits,
        and they won't be able to be tracked by Ray anymore.
        """
        if hasattr(self.engine, "shutdown"):
            logger.info("Shutting down vLLM engine")
            self.engine.shutdown()


class vLLMEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        model: str,
        engine_kwargs: Dict[str, Any],
        task_type: vLLMTaskType = vLLMTaskType.GENERATE,
        max_pending_requests: Optional[int] = None,
    ):
        """
        Initialize the vLLMEngineStageUDF.

        Args:
            data_column: The data column name.
            model: The model to use for the vLLM engine.
            engine_kwargs: The kwargs to pass to the vLLM engine.
            task_type: The task to use for the vLLM engine (e.g., "generate", "embed", etc).
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to 1.1 * max_num_seqs * pipeline_parallel_size.
        """
        super().__init__(data_column)
        self.model = model

        # Setup vLLM engine kwargs.
        self.task_type = task_type
        self.engine_kwargs = self.normalize_engine_kwargs(task_type, engine_kwargs)

        # Set up the max pending requests.
        pp_size = self.engine_kwargs.get("pipeline_parallel_size", 1)
        self.max_pending_requests = max_pending_requests or math.ceil(
            self.engine_kwargs.get("max_num_seqs", 128) * pp_size * 1.1
        )
        if self.max_pending_requests > 0:
            logger.info("Max pending requests is set to %d", self.max_pending_requests)

        # Create an LLM engine.
        self.llm = vLLMEngineWrapper(
            model=self.model,
            idx_in_batch_column=self.IDX_IN_BATCH_COLUMN,
            disable_log_stats=False,
            max_pending_requests=self.max_pending_requests,
            **self.engine_kwargs,
        )

    def normalize_engine_kwargs(
        self,
        task_type: vLLMTaskType,
        engine_kwargs: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Normalize the engine kwargs.

        Args:
            task_type: The task to use for the vLLM engine (e.g., "generate", "embed", etc).
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
        task = vLLMTaskType(engine_kwargs.get("task", task_type))
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
        """Run the vLLM engine.

        Args:
            batch: A list of rows to run the vLLM engine on.

        Returns:
            The response of the vLLM engine.
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
                "params": str(request.params),
            }

        # TODO: Add metrics to the UDf wrapper so that we don't need
        # timer in UDFs anymore.
        logger.info(
            "[vLLM] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            time_taken,
        )

    @property
    def expected_input_keys(self) -> List[str]:
        """The expected input keys."""

        ret = ["prompt"]
        if self.task_type == vLLMTaskType.GENERATE:
            ret.append("sampling_params")
        return ret

    def __del__(self):
        if hasattr(self, "llm"):
            self.llm.shutdown()


def _ray_scheduling_strategy_fn(num_gpus_per_instance: int, accelerator_type: str):
    """
    Create a Ray scheduling strategy for vLLM engine.

    Args:
        num_gpus_per_instance: The number of GPUs per instance.
        accelerator_type: The accelerator type.

    Returns:
        The Ray scheduling strategy.
    """

    def _get_bundle() -> Dict[str, float]:
        bundle: Dict[str, float] = {"GPU": 1, "CPU": 1}
        if accelerator_type:
            bundle[f"accelerator_type:{accelerator_type}"] = 0.001
        return bundle

    pg = ray.util.placement_group(
        [_get_bundle()] * num_gpus_per_instance,
        strategy="STRICT_PACK",
    )
    return dict(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            pg, placement_group_capture_child_tasks=True
        )
    )


class vLLMEngineStage(StatefulStage):
    """
    A stage that runs vLLM engine.
    """

    fn: Type[StatefulStageUDF] = vLLMEngineStageUDF

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

        # Setup num_gpus required per vLLM engine.
        tp_size = engine_kwargs.get("tensor_parallel_size", 1)
        pp_size = engine_kwargs.get("pipeline_parallel_size", 1)
        num_gpus = tp_size * pp_size

        # Use the MP backend by default.
        engine_kwargs.setdefault("distributed_executor_backend", "mp")
        executor_backend = engine_kwargs.get("distributed_executor_backend")

        # When Ray is used in the vLLM engine, we set num_gpus to 0 so that
        # Ray Data won't reserve GPUs in advance. Instead, we specify scheduling
        # strategy in .map_batches() arguments and let vLLM Ray executor to
        # create placement groups for each TP/PP worker.
        if executor_backend == "ray" and num_gpus > 1:
            # Note that we have to use partial() to pass a function
            # instead of an object.
            map_batches_kwargs["ray_remote_args_fn"] = partial(
                _ray_scheduling_strategy_fn,
                num_gpus,
                accelerator_type,
            )
            num_gpus = 0

        map_batches_kwargs["num_gpus"] = num_gpus
        map_batches_kwargs.update(ray_remote_args)
        return values
