"""The stage that runs vLLM engine."""
import asyncio
import atexit
import dataclasses
import importlib
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from pydantic import root_validator
from typing import Any, Dict, AsyncIterator, Optional, List, Tuple

import ray
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)


class vLLMEngineWrapper:
    """Wrapper around the vLLM engine to handle async requests.

    Args:
        *args: The positional arguments for the engine.
        max_pending_requests: The maximum number of pending requests in the queue.
        runtime_env: The runtime environment to use for the vLLM engine.
        **kwargs: The keyword arguments for the engine.
    """

    @dataclass(frozen=True)
    class LLMRequest:
        """A request to the LLM wrapper."""

        # The request ID for the LLM engine (unique per replica).
        request_id: int
        # The full prompt string (with chat template applied if any).
        prompt: str
        # The images inputs for the multimodal model.
        images: List["Image.Image"]
        # The tokenized prompt IDs. If None, then the string prompt will be
        # tokenized by the LLM engine. This is not recommended for performance reasons.
        prompt_token_ids: Optional[List[int]]
        # The sampling or pooling parameters. Use Any to avoid importing vLLM.
        params: Any

    def __init__(
        self,
        *args,
        max_pending_requests: int = -1,
        runtime_env: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        self.request_id = 0

        # Setup os.environ before importing vLLM to make sure the environment
        # variables are effective
        if runtime_env is not None and "env_vars" in runtime_env:
            os.environ.update(runtime_env["env_vars"])

        # Lazy import vLLM here.
        self.vllm = importlib.import_module("vllm")

        # Initialize the vLLM engine.
        engine_args = self.vllm.AsyncEngineArgs(
            *args,
            **kwargs,
            disable_log_requests=True,
        )
        self.engine = self.vllm.AsyncLLMEngine.from_engine_args(engine_args)

        # Determine the generate function based on vLLM v0 or v1.
        if self.vllm.envs.VLLM_USE_V1:
            self._generate_async = self.generate_async_v1
        else:
            self._generate_async = self.generate_async_v0

        # vLLM performance gets really bad if there are too many requests in the pending queue.
        # We work around it by introducing another queue that gates how many requests we are
        # sending to vLLM at once.
        # This is not a queue of requests. Instead, this queue holds "slots". Each time
        # we add a new request, we take one slot. When a request finishes, we add a new
        # slot.
        self.max_pending_requests = max_pending_requests
        self.free_queue: asyncio.Queue[bool] = asyncio.Queue()
        if self.max_pending_requests > 0:
            for _ in range(self.max_pending_requests):
                self.free_queue.put_nowait(True)

    def _prepare_llm_request(self, row: Dict[str, Any]) -> LLMRequest:
        """Prepare the inputs for LLM inference.

        Args:
            row: The row.

        Returns:
            A single LLMRequest.
        """
        if "prompt" not in row:
            raise ValueError(
                "Required 'prompt' not found in batch. This may be "
                "due to an unknown internal error if your workload needs "
                "tokenization. If your workload does not need tokenization, "
                "please make sure 'prompt' exists in the dataset."
            )
        prompt = row.pop("prompt")

        if "tokenized_prompt" in row:
            tokenized_prompt = row.pop("tokenized_prompt")
        else:
            tokenized_prompt = None

        if "image" in row:
            image = row.pop("image")
        else:
            image = []

        # If sampling_params is provided in the batch, override the default.
        if "sampling_params" in row:
            params = self.vllm.SamplingParams(**row.pop("sampling_params"))
        elif "pooling_params" in row:
            params = self.vllm.PoolingParams(**row.pop("pooling_params"))
        else:
            raise ValueError(
                "Either sampling_params or pooling_params must be provided"
            )

        request = self.LLMRequest(
            request_id=self.request_id,
            prompt=prompt,
            prompt_token_ids=tokenized_prompt,
            images=image,
            params=params,
        )
        self.request_id += 1
        return request

    def _parse_llm_output(self, output: Any) -> Dict[str, Any]:
        """Parse the LLM output.

        Args:
            output: The LLM output.

        Returns:
            The parsed output.
        """
        # Parse the common fields.
        output_data = {
            "prompt": output.prompt,
            "prompt_token_ids": output.prompt_token_ids,
            "num_input_tokens": len(output.prompt_token_ids),
        }

        if isinstance(output, self.vllm.outputs.RequestOutput):
            metrics = {}
            if output.metrics is not None:
                metrics = {k: v for k, v in dataclasses.asdict(output.metrics).items()}
            output_data.update(
                {
                    "generated_tokens": output.outputs[0].token_ids,
                    "generated_text": output.outputs[0].text,
                    "num_generated_tokens": len(output.outputs[0].token_ids),
                    **metrics,
                }
            )
        elif isinstance(output, self.vllm.outputs.PoolingRequestOutput):
            output_data.update(
                {
                    "embeddings": output.outputs.data.cpu(),
                }
            )
        else:
            raise ValueError(f"Unknown output type: {type(output)}")

        return output_data

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[LLMRequest, Dict[str, Any]]:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            A tuple of index in batch, request output and bypassed custom fields.
        """
        request = self._prepare_llm_request(row)

        # If free queue is used, guard the request here until a slot is available.
        if self.max_pending_requests > 0:
            await self.free_queue.get()

        output = await self._generate_async(request)

        # If free queue is used, release the slot.
        if self.max_pending_requests > 0:
            self.free_queue.put_nowait(True)

        return request, self._parse_llm_output(output)

    async def generate_async_v0(self, request: LLMRequest) -> Any:
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
            llm_prompt = self.vllm.inputs.data.TextPrompt(
                prompt=request.prompt, multi_modal_data={"image": request.images}
            )
        else:
            if request.prompt_token_ids is not None:
                llm_prompt = self.vllm.inputs.data.TokensPrompt(
                    prompt_token_ids=request.prompt_token_ids
                )
            else:
                assert request.prompt
                llm_prompt = self.vllm.inputs.data.TextPrompt(prompt=request.prompt)

        # Send the request to the LLM engine.
        stream = await self.engine.add_request(
            request_id=str(request.request_id),
            prompt=llm_prompt,
            params=request.params,
        )
        # Consume the stream until the request is finished.
        async for request_output in stream:
            if request_output.finished:
                # Bypass the original full prompt.
                request_output.prompt = request.prompt
                return request_output
        raise RuntimeError("Should not reach here")

    async def generate_async_v1(self, request: LLMRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        # NOTE: vLLM v1 tighly couples tokenizer and detokenizer to the engine.
        assert request.prompt
        multi_modal_data = {"image": request.images} if request.images else None
        llm_prompt = self.vllm.inputs.data.TextPrompt(
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

        raise RuntimeError("Should not reach here")

    def shutdown(self):
        """Shutdown the vLLM v1 engine. This kills child processes forked
        by the vLLM engine. If not called, the child processes will be
        orphaned and will not be killed when the parent process exits.
        """
        if hasattr(self.engine, "shutdown"):
            logger.info("Shutting down vLLM engine")
            self.engine.shutdown()


class vLLMEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        input_column: str,
        output_column: str,
        carry_over: bool,
        model: str,
        engine_kwargs: Dict[str, Any],
        task_type: str = "generate",
        runtime_env: Optional[Dict[str, Any]] = None,
        max_pending_requests: Optional[int] = None,
    ):
        """
        Initialize the HttpRequestUDF.

        Args:
            input_column: The input column name.
            output_column: The output column name.
            carry_over: Whether to carry over the input column to the output column.
            engine_kwargs: The kwargs to pass to the vLLM engine.
            task_type: The task to use for the vLLM engine (e.g., "generate", "embed", etc).
            runtime_env: The runtime environment to use for the vLLM engine.
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to 1.1 * max_num_seqs * pipeline_parallel_size.
        """
        super().__init__(input_column, output_column, carry_over)
        self.model = model

        # Setup runtime env.
        self.runtime_env = runtime_env or {}

        # Setup vLLM engine kwargs.
        self.engine_kwargs = self.normalize_engine_kwargs(task_type, engine_kwargs)

        # Set up the max pending requests.
        pp_size = self.engine_kwargs.get("pipeline_parallel_size", 1)
        self.max_pending_requests = max_pending_requests or math.ceil(
            self.engine_kwargs["max_num_seqs"] * pp_size * 1.1
        )
        if self.max_pending_requests > 0:
            logger.info("Max pending requests is set to %d", self.max_pending_requests)

        # Create an LLM engine.
        self.llm = vLLMEngineWrapper(
            model=self.model,
            disable_log_stats=False,
            max_pending_requests=self.max_pending_requests,
            runtime_env=self.runtime_env,
            **self.engine_kwargs,
        )

    def normalize_engine_kwargs(
        self,
        task_type: str,
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
        task = engine_kwargs.get("task", "generate")
        if task != task_type:
            logger.warning(
                "The task set in engine kwargs (%s) is different from the "
                "stage (%s). Overriding the task in engine kwargs to %s.",
                task,
                task_type,
                task_type,
            )
            engine_kwargs["task"] = task_type

        # Override vLLM default configs. Note that this is only effective
        # when the config is not set by users.
        engine_kwargs.setdefault("gpu_memory_utilization", 0.95)
        engine_kwargs.setdefault("use_v2_block_manager", True)
        engine_kwargs.setdefault("enable_prefix_caching", False)
        engine_kwargs.setdefault("enforce_eager", False)
        engine_kwargs.setdefault("pipeline_parallel_size", 1)
        engine_kwargs.setdefault("max_num_seqs", 256)
        engine_kwargs.setdefault("tensor_parallel_size", 1)
        engine_kwargs.setdefault("max_logprobs", 0)

        # FlashInfer does not support bfloat16 activations, so we enforce float16
        # dtype in this case.
        env_vars = self.runtime_env.get("env_vars", {})
        if (attn_backend := env_vars.get("VLLM_ATTENTION_BACKEND", None)) is not None:
            if (
                attn_backend == "FLASHINFER"
                and engine_kwargs.get("kv_cache_dtype", "auto") == "fp8"
            ):
                engine_kwargs["dtype"] = "float16"

        return engine_kwargs

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """
        Run the vLLM engine.

        Args:
            batch: A list of rows to run the vLLM engine on.

        Returns:
            The response of the vLLM engine.
        """
        batch_uuid = uuid.uuid4()
        t = time.perf_counter()

        tasks = [asyncio.create_task(self.llm.generate_async(row)) for row in batch]

        idx = 0
        time_taken = -1.0
        for resp in asyncio.as_completed(tasks):
            request, output = await resp
            time_taken = time.perf_counter() - t

            yield {
                **output,
                "request_id": request.request_id,
                "batch_uuid": batch_uuid.hex,
                "index_in_batch": idx,
                "time_taken_llm": time_taken,
                "params": str(request.params),
            }
            idx += 1

        logger.info(
            "[vLLM] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            time_taken,
        )

    @property
    def expected_input_keys(self) -> Dict[str, StatefulStageUDF.InputKeyType]:
        """The expected input keys."""
        return {
            "prompt": StatefulStageUDF.InputKeyType.REQUIRED,
            "tokenized_prompt": StatefulStageUDF.InputKeyType.OPTIONAL,
            "images": StatefulStageUDF.InputKeyType.OPTIONAL,
            "sampling_params": StatefulStageUDF.InputKeyType.OPTIONAL,
            "pooling_params": StatefulStageUDF.InputKeyType.OPTIONAL,
        }

    def __del__(self):
        self.llm.shutdown()


class vLLMEngineStage(StatefulStage):
    """
    A stage that runs vLLM engine.
    """

    fn: StatefulStageUDF = vLLMEngineStageUDF
    fn_constructor_kwargs: Dict[str, Any]
    map_batches_kwargs: Dict[str, Any] = dict(
        concurrency=1,
    )

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
        runtime_env = fn_constructor_kwargs.get("runtime_env", {})
        engine_kwargs = fn_constructor_kwargs.get("engine_kwargs", {})

        ray_remote_args = {"runtime_env": runtime_env}
        if accelerator_type:
            ray_remote_args["accelerator_type"] = accelerator_type

        # Setup num_gpus required per vLLM engine.
        tp_size = engine_kwargs.get("tensor_parallel_size", 1)
        pp_size = engine_kwargs.get("pipeline_parallel_size", 1)
        num_gpus = tp_size * pp_size

        # For TP only case, we use multi-processing engines, so we only
        # need to set the correct num_gpus, which lets Ray allocate the
        # number of GPUs, and the engine will spawn the number of processes
        # to use them.
        # For PP and PP+TP, vLLM uses Ray and placement groups, so we don't
        # need to set num_gpus. Instead, vLLM Ray executor will use the
        # scheduling strategy function provided here to create placement
        # groups for each replica.
        if pp_size > 1:

            def _scheduling_strategy_fn(
                num_gpus_per_instance: int, accelerator_type: str
            ):
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

            ray_remote_args.update(
                _scheduling_strategy_fn(
                    num_gpus,
                    accelerator_type,
                )
            )
            num_gpus = 0

        map_batches_kwargs["num_gpus"] = num_gpus
        map_batches_kwargs.update(ray_remote_args)
        return values
