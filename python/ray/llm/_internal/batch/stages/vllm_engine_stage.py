"""The stage that runs vLLM engine."""

import asyncio
import dataclasses
import logging
import math
import time
import uuid
from enum import Enum
from functools import partial
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple, Type

import numpy as np
from pydantic import BaseModel, Field, root_validator

import ray
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.common import maybe_convert_ndarray_to_list
from ray.llm._internal.common.utils.cloud_utils import is_remote_path
from ray.llm._internal.common.utils.download_utils import (
    NodeModelDownloadable,
    download_model_files,
)
from ray.llm._internal.common.utils.lora_utils import download_lora_adapter
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

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

        import vllm

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
        dynamic_lora_loading_path: The S3 path to the dynamic LoRA adapter.
        **kwargs: The keyword arguments for the engine.
    """

    def __init__(
        self,
        idx_in_batch_column: str,
        max_pending_requests: int = -1,
        dynamic_lora_loading_path: Optional[str] = None,
        **kwargs,
    ):
        self.request_id = 0
        self.idx_in_batch_column = idx_in_batch_column
        self.task_type = kwargs.get("task", vLLMTaskType.GENERATE)

        # Use model_source in kwargs["model"] because "model" is actually
        # the model source in vLLM.
        self.model = kwargs.pop("model", None)
        self.model_source = kwargs.pop("model_source", None)
        assert self.model is not None and self.model_source is not None
        kwargs["model"] = self.model_source

        # LoRA related.
        self.dynamic_lora_loading_path = dynamic_lora_loading_path
        self.lora_lock = asyncio.Lock()
        self.lora_name_to_request = {}

        # Convert the task type back to a string to pass to the engine.
        kwargs["task"] = self.task_type.value

        try:
            import vllm
        except ImportError as e:
            raise ImportError(
                "vLLM is not installed or failed to import. Please run "
                "`pip install ray[llm]` to install required dependencies."
            ) from e

        # Construct PoolerConfig if override_pooler_config is specified.
        if self.task_type == vLLMTaskType.EMBED and "override_pooler_config" in kwargs:
            kwargs["override_pooler_config"] = vllm.config.PoolerConfig(
                **kwargs["override_pooler_config"]
            )

        # Initialize the vLLM engine.
        engine_args = vllm.AsyncEngineArgs(
            **kwargs,
        )
        # create_engine_config will set default values including `max_num_seqs`.
        self._vllm_config = engine_args.create_engine_config()
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

    async def _maybe_get_lora_request(
        self,
        row: Dict[str, Any],
    ) -> Optional[Any]:
        """Get the LoRA request for the given row.
        Specifically, if the model name is given and is different from the model
        set in the config, then this request has LoRA.

        Args:
            row: The row.

        Returns:
            The LoRA request (vllm.lora.request.LoRARequest),
            or None if there is no LoRA. We use Any in type hint to
            pass doc build in the environment without vLLM.
        """
        import vllm

        lora_request = None
        if "model" in row and row["model"] != self.model:

            lora_name = row["model"]
            if lora_name not in self.lora_name_to_request:
                if is_remote_path(lora_name):
                    raise ValueError(
                        "LoRA name cannot be a remote path (s3:// or gs://). "
                        "Please specify dynamic_lora_loading_path in the processor config."
                    )

                async with self.lora_lock:
                    if lora_name not in self.lora_name_to_request:
                        # Load a new LoRA adapter if it is not loaded yet.
                        lora_path = download_lora_adapter(
                            lora_name,
                            remote_path=self.dynamic_lora_loading_path,
                        )
                        logger.info(
                            "Downloaded LoRA adapter for %s to %s", lora_name, lora_path
                        )
                        lora_request = vllm.lora.request.LoRARequest(
                            lora_name=lora_name,
                            # LoRA ID starts from 1.
                            lora_int_id=len(self.lora_name_to_request) + 1,
                            lora_path=lora_path,
                        )
                        self.lora_name_to_request[lora_name] = lora_request
            lora_request = self.lora_name_to_request[lora_name]
        return lora_request

    async def _prepare_llm_request(self, row: Dict[str, Any]) -> vLLMEngineRequest:
        """Prepare the inputs for LLM inference.

        Args:
            row: The row.

        Returns:
            A single vLLMEngineRequest.
        """
        prompt = row.pop("prompt")

        if "tokenized_prompt" in row:
            tokenized_prompt = maybe_convert_ndarray_to_list(
                row.pop("tokenized_prompt")
            )
        else:
            tokenized_prompt = None

        if "image" in row:
            image = row.pop("image")
        else:
            image = []

        lora_request = await self._maybe_get_lora_request(row)

        # Prepare sampling parameters.
        import vllm

        if self.task_type == vLLMTaskType.GENERATE:
            sampling_params = row.pop("sampling_params")
            if "guided_decoding" in sampling_params:
                guided_decoding = vllm.sampling_params.GuidedDecodingParams(
                    **maybe_convert_ndarray_to_list(
                        sampling_params.pop("guided_decoding")
                    )
                )
            else:
                guided_decoding = None
            params = vllm.SamplingParams(
                **maybe_convert_ndarray_to_list(sampling_params),
                guided_decoding=guided_decoding,
            )
        elif self.task_type == vLLMTaskType.EMBED:
            params = vllm.PoolingParams(task=self.task_type.value)
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

        import vllm

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
            "[vLLM] The request is not finished. This should not happen. Please report this issue to the Ray team."
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
        import vllm

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
            "[vLLM] The request is not finished. This should not happen. Please report this issue to the Ray team."
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

    def get_scheduler_config(self):
        return self._vllm_config.scheduler_config


class vLLMEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        batch_size: int,
        max_concurrent_batches: int,
        model: str,
        engine_kwargs: Dict[str, Any],
        task_type: vLLMTaskType = vLLMTaskType.GENERATE,
        max_pending_requests: Optional[int] = None,
        dynamic_lora_loading_path: Optional[str] = None,
    ):
        """
        Initialize the vLLMEngineStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            model: The model to use for the vLLM engine.
            engine_kwargs: The kwargs to pass to the vLLM engine.
            task_type: The task to use for the vLLM engine (e.g., "generate", "embed", etc).
            max_pending_requests: The maximum number of pending requests. If None,
                it will be set to 1.1 * max_num_seqs * pipeline_parallel_size.
            dynamic_lora_loading_path: The path to the dynamic LoRA adapter. It is expected
                to hold subfolders each for a different lora checkpoint.
        """
        super().__init__(data_column, expected_input_keys)
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

        # Download the model if needed.
        model_source = download_model_files(
            model_id=self.model,
            mirror_config=None,
            download_model=NodeModelDownloadable.MODEL_AND_TOKENIZER,
            download_extra_files=False,
        )

        # Create an LLM engine.
        self.llm = vLLMEngineWrapper(
            model=self.model,
            model_source=model_source,
            idx_in_batch_column=self.IDX_IN_BATCH_COLUMN,
            disable_log_requests=True,
            max_pending_requests=self.max_pending_requests,
            dynamic_lora_loading_path=dynamic_lora_loading_path,
            **self.engine_kwargs,
        )

        max_num_seqs = self.llm.get_scheduler_config().max_num_seqs
        if batch_size * max_concurrent_batches < max_num_seqs:
            logger.warning(
                f"The product of batch_size ({batch_size}) and "
                f"max_concurrent_batches ({max_concurrent_batches}) is too small "
                "to saturate vLLM engine. This may lead to suboptimal "
                "throughput. Please increase max_concurrent_batches to at least "
                f"{math.ceil(max_num_seqs / batch_size)}."
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

        # Log engine stats after each batch is done conditioned on the flag
        # passed to the engine.
        if not self.engine_kwargs.get("disable_log_stats", False):
            await self.llm.engine.do_log_stats()

    def __del__(self):
        if hasattr(self, "llm"):
            # Kill the engine processes.
            self.llm.shutdown()


def _ray_scheduling_strategy_fn(
    num_bundles_per_replica: int,
    accelerator_type: Optional[str] = None,
    resources_per_bundle: Optional[Dict[str, float]] = None,
):
    """Create a Ray scheduling strategy for the engine.

    Args:
        num_bundles_per_replica: The number of device bundles per
            engine replica.
        accelerator_type: The accelerator type. If None, the
            accelerator_type label will not be set.
        resources_per_bundle: The custom resources per bundle.
            If None, we default to 1xGPU + 1xCPU bundle.

    Returns:
        The Ray scheduling strategy.
    """

    def _get_bundle() -> Dict[str, float]:
        bundle = {}
        # Custom resources
        if resources_per_bundle:
            bundle = resources_per_bundle
        else:
            # GPU bundles
            bundle = {"GPU": 1, "CPU": 1}

        # Accelerator type
        if accelerator_type:
            bundle[f"accelerator_type:{accelerator_type}"] = 0.001
        return bundle

    pg = ray.util.placement_group(
        [_get_bundle()] * num_bundles_per_replica,
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

        # Setup num_workers required per vLLM engine.
        tp_size = engine_kwargs.get("tensor_parallel_size", 1)
        pp_size = engine_kwargs.get("pipeline_parallel_size", 1)
        num_bundles_per_replica = tp_size * pp_size

        # Use the MP backend by default.
        engine_kwargs.setdefault("distributed_executor_backend", "mp")
        executor_backend = engine_kwargs.get("distributed_executor_backend")

        # When Ray is used in the vLLM engine, we set num_devices to 0 so that
        # Ray Data won't reserve GPUs in advance. Instead, we specify scheduling
        # strategy in .map_batches() arguments and let vLLM Ray executor to
        # create placement groups for each TP/PP worker.
        resources_per_bundle = map_batches_kwargs.pop("resources", None)
        if executor_backend == "ray" and num_bundles_per_replica > 1:
            # Note that we have to use partial() to pass a function
            # instead of an object.
            map_batches_kwargs["ray_remote_args_fn"] = partial(
                _ray_scheduling_strategy_fn,
                num_bundles_per_replica,
                accelerator_type,
                resources_per_bundle,
            )
            ray_remote_args["num_gpus"] = 0
        else:
            if not resources_per_bundle:
                # Default to GPUs per bundle if custom resources are not specified.
                ray_remote_args["num_gpus"] = num_bundles_per_replica
            else:
                ray_remote_args["resources"] = {
                    resource_key: resource_count * num_bundles_per_replica
                    for resource_key, resource_count in resources_per_bundle.items()
                }

        map_batches_kwargs.update(ray_remote_args)
        return values

    def get_required_input_keys(self) -> Dict[str, str]:
        """The required input keys of the stage and their descriptions."""
        ret = {"prompt": "The text prompt (str)."}
        task_type = self.fn_constructor_kwargs.get("task_type", vLLMTaskType.GENERATE)
        if task_type == vLLMTaskType.GENERATE:
            ret["sampling_params"] = (
                "The sampling parameters. See "
                "https://docs.vllm.ai/en/latest/api/inference_params.html#sampling-parameters "
                "for details."
            )
        return ret

    def get_optional_input_keys(self) -> Dict[str, str]:
        """The optional input keys of the stage and their descriptions."""
        return {
            "tokenized_prompt": "The tokenized prompt. If provided, the prompt will not be tokenized by the vLLM engine.",
            "images": "The images to generate text from. If provided, the prompt will be a multimodal prompt.",
            "model": "The model to use for this request. If the model is different from the "
            "model set in the stage, then this is a LoRA request.",
        }
