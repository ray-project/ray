"""The stage that runs vLLM engine."""

import asyncio
import copy
import dataclasses
import logging
import math
import time
import uuid
from collections import Counter
from enum import Enum
from functools import partial
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional, Tuple, Type

import numpy as np
import torch
from pydantic import BaseModel, Field, root_validator

if TYPE_CHECKING:
    from vllm.multimodal import MultiModalDataDict
else:
    MultiModalDataDict = Any

import ray
from ray.llm._internal.batch.stages.base import (
    StatefulStage,
    StatefulStageUDF,
)
from ray.llm._internal.batch.stages.common import maybe_convert_ndarray_to_list
from ray.llm._internal.common.utils.cloud_utils import is_remote_path
from ray.llm._internal.common.utils.download_utils import (
    STREAMING_LOAD_FORMATS,
    NodeModelDownloadable,
    download_model_files,
)
from ray.llm._internal.common.utils.lora_utils import download_lora_adapter
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

logger = logging.getLogger(__name__)

# vLLM fatal errors that should always be re-raised, never swallowed.
# EngineDeadError indicates the vLLM engine process has crashed and is
# unrecoverable - all subsequent requests would fail anyway.
_VLLM_FATAL_ERRORS: Tuple[Type[Exception], ...] = ()
try:
    from vllm.v1.engine.exceptions import EngineDeadError

    _VLLM_FATAL_ERRORS = (EngineDeadError,)
except ImportError:
    # vLLM not installed or older version without this exception
    pass

# Length of prompt snippet to surface in case of recoverable error
_MAX_PROMPT_LENGTH_IN_ERROR = 500


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
    # DEPRECATED: The images inputs for the multimodal model. Use Any to avoid importing PIL.
    images: List[Any]
    # The multimodal data for the multimodal model.
    multimodal_data: Optional[MultiModalDataDict]
    # The kwargs for the multimodal processor.
    mm_processor_kwargs: Optional[Dict[str, Any]]
    # The uuids for the multimodal data.
    multimodal_uuids: Optional[Dict[str, Any]]
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


def _convert_logprob_dict(logprob_dict: Dict[int, Any]) -> Dict[int, Dict[str, Any]]:
    """Convert a dict of token_id -> Logprob to token_id -> dict.

    Handles conversion of vLLM's Logprob objects (currently dataclass) to
    serializable dicts. This supports both dataclass (current vLLM format)
    and Pydantic models (for future compatibility).

    Args:
        logprob_dict: Dict mapping token_id to Logprob instance.

    Returns:
        Dict mapping token_id to serializable dict with logprob fields.
    """
    result = {}
    for token_id, logprob in logprob_dict.items():
        # Handle Pydantic models (model_dump method)
        if hasattr(logprob, "model_dump"):
            result[token_id] = logprob.model_dump()
        # Handle dataclasses (current vLLM format)
        elif dataclasses.is_dataclass(logprob):
            result[token_id] = dataclasses.asdict(logprob)
        # Already a dict
        elif isinstance(logprob, dict):
            result[token_id] = logprob
        else:
            raise TypeError(
                f"Unsupported logprob type: {type(logprob)}. "
                "Expected dataclass, Pydantic model, or dict."
            )
    return result


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

    # Logprobs fields.
    # logprobs: List[Dict[int, Dict[str, Any]]] where each dict maps token_id to
    # logprob info (logprob, rank, decoded_token) for each generated token.
    logprobs: Optional[List[Dict[int, Dict[str, Any]]]] = None
    # prompt_logprobs: List[Optional[Dict[int, Dict[str, Any]]]] where each dict
    # (or None) maps token_id to logprob info for each prompt token.
    prompt_logprobs: Optional[List[Optional[Dict[int, Dict[str, Any]]]]] = None

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

            # Extract logprobs
            if output.outputs[0].logprobs is not None:
                data.logprobs = [
                    _convert_logprob_dict(logprob_dict)
                    for logprob_dict in output.outputs[0].logprobs
                ]

            # Extract prompt_logprobs
            if output.prompt_logprobs is not None:
                data.prompt_logprobs = [
                    _convert_logprob_dict(logprob_dict)
                    if logprob_dict is not None
                    else None
                    for logprob_dict in output.prompt_logprobs
                ]
        elif isinstance(output, vllm.outputs.PoolingRequestOutput):
            data.embeddings = output.outputs.data.cpu()
            if (
                isinstance(data.embeddings, torch.Tensor)
                and data.embeddings.dtype == torch.bfloat16
            ):
                data.embeddings = data.embeddings.to(torch.float32)
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

        # Extract image data from preprocessing output
        # Note: Field name is 'image' (singular) not 'images' (plural).
        if "image" in row:
            image = row.pop("image")
        else:
            image = []

        multimodal_data = row.pop("multimodal_data", None)
        # TODO (jeffreywang): As we decouple the multimodal processor from the vLLM engine,
        # these kwargs are not needed in the vLLM engine stage.
        mm_processor_kwargs = row.pop("mm_processor_kwargs", None)
        multimodal_uuids = row.pop("multimodal_uuids", None)

        lora_request = await self._maybe_get_lora_request(row)

        # Prepare sampling parameters.
        import vllm

        if self.task_type == vLLMTaskType.GENERATE:
            sampling_params = row.pop("sampling_params")
            if "guided_decoding" in sampling_params:
                structured_outputs = vllm.sampling_params.StructuredOutputsParams(
                    **maybe_convert_ndarray_to_list(
                        sampling_params.pop("guided_decoding")
                    )
                )
            else:
                structured_outputs = None
            params = vllm.SamplingParams(
                **maybe_convert_ndarray_to_list(sampling_params),
                structured_outputs=structured_outputs,
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
            multimodal_data=multimodal_data,
            mm_processor_kwargs=mm_processor_kwargs,
            multimodal_uuids=multimodal_uuids,
            params=params,
            lora_request=lora_request,
        )
        self.request_id += 1
        return request

    async def generate_async(
        self, row: Dict[str, Any]
    ) -> Tuple[vLLMEngineRequest, Dict[str, Any], float]:
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

        output_data = vLLMOutputData.from_vllm_engine_output(output)
        return request, output_data.model_dump(), time_taken

    async def _generate_async(self, request: vLLMEngineRequest) -> Any:
        """Process a single request.

        Args:
            request: The request.

        Returns:
            The output of the request.
        """

        import vllm

        # TODO (jeffreywang): Consolidate to multimodal_data only
        if request.images:
            multi_modal_data = (
                {**request.multimodal_data, "image": request.images}
                if request.multimodal_data
                else {"image": request.images}
            )
        else:
            multi_modal_data = request.multimodal_data

        if request.prompt_token_ids is not None:
            llm_prompt = vllm.inputs.data.TokensPrompt(
                prompt_token_ids=request.prompt_token_ids,
                multi_modal_data=multi_modal_data,
                mm_processor_kwargs=request.mm_processor_kwargs,
                multi_modal_uuids=request.multimodal_uuids,
            )
        else:
            assert request.prompt
            llm_prompt = vllm.inputs.data.TextPrompt(
                prompt=request.prompt,
                multi_modal_data=multi_modal_data,
                mm_processor_kwargs=request.mm_processor_kwargs,
                multi_modal_uuids=request.multimodal_uuids,
            )

        # Send the request to the LLM engine.
        # vLLM 0.12.0 uses encode() for pooling/embedding tasks, generate() for text generation
        if self.task_type == vLLMTaskType.EMBED:
            stream = self.engine.encode(
                request_id=str(request.request_id),
                prompt=llm_prompt,
                pooling_params=request.params,
            )
        else:
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
        should_continue_on_error: bool = False,
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
            should_continue_on_error: If True, continue processing when inference fails for
                a row instead of raising. Failed rows will have '__inference_error__'
                set to the error message.
        """
        super().__init__(data_column, expected_input_keys)
        self.model = model
        self.should_continue_on_error = should_continue_on_error

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

        exclude_safetensors = (
            self.engine_kwargs.get("load_format") in STREAMING_LOAD_FORMATS
        )
        if exclude_safetensors:
            logger.info("Excluding safetensors files when downloading the model.")
            download_model = NodeModelDownloadable.EXCLUDE_SAFETENSORS
        else:
            logger.info("Downloading model and tokenizer.")
            download_model = NodeModelDownloadable.MODEL_AND_TOKENIZER

        # Download the model if needed.
        model_source = download_model_files(
            model_id=self.model,
            mirror_config=None,
            download_model=download_model,
            download_extra_files=False,
        )

        # If we are using streaming load formats, we need to pass in self.model which is a remote cloud storage path.
        source = model_source if not exclude_safetensors else self.model
        self.llm = vLLMEngineWrapper(
            model=self.model,
            model_source=source,
            idx_in_batch_column=self.IDX_IN_BATCH_COLUMN,
            enable_log_requests=False,
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

    async def _generate_with_error_handling(
        self,
        row: Dict[str, Any],
        batch_uuid: uuid.UUID,
    ) -> Dict[str, Any]:
        """Generate output for a single row, catching errors if should_continue_on_error is set.

        Args:
            row: The input row.
            batch_uuid: The batch UUID for logging.

        Returns:
            The output dict, with __inference_error__ set if an error occurred.
        """
        idx_in_batch = row[self.IDX_IN_BATCH_COLUMN]
        try:
            request, output, time_taken_llm = await self.llm.generate_async(row)
            return {
                **output,
                "request_id": request.request_id,
                self.IDX_IN_BATCH_COLUMN: request.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken_llm,
                "params": str(request.params),
                "__inference_error__": None,
            }
        except _VLLM_FATAL_ERRORS:
            # Fatal engine errors (e.g., EngineDeadError) must always propagate.
            # The engine is dead and all subsequent requests would fail.
            raise
        except Exception as e:
            if not self.should_continue_on_error:
                raise
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.warning(
                "[vLLM] Inference failed for row %d in batch %s: %s",
                idx_in_batch,
                batch_uuid.hex,
                error_msg,
            )
            # Include snippet of failed prompt
            prompt = row.get("prompt", "")
            if len(prompt) > _MAX_PROMPT_LENGTH_IN_ERROR:
                prompt = prompt[:_MAX_PROMPT_LENGTH_IN_ERROR] + "...[truncated]"
            return {
                self.IDX_IN_BATCH_COLUMN: idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "__inference_error__": error_msg,
                "prompt": prompt,
            }

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Run the vLLM engine.

        Args:
            batch: A list of rows to run the vLLM engine on.

        Returns:
            The response of the vLLM engine.
        """
        batch_uuid = uuid.uuid4()
        batch_start_time = time.perf_counter()

        tasks = [
            asyncio.create_task(self._generate_with_error_handling(row, batch_uuid))
            for row in batch
        ]

        for resp in asyncio.as_completed(tasks):
            yield await resp

        batch_time_taken = time.perf_counter() - batch_start_time
        # TODO: Add metrics to the UDf wrapper so that we don't need
        # timer in UDFs anymore.
        logger.info(
            "[vLLM] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            batch_time_taken,
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
    placement_group_config: Optional[Dict[str, Any]] = None,
):
    """Create a Ray scheduling strategy for the engine.

    Args:
        num_bundles_per_replica: The number of device bundles per
            engine replica.
        accelerator_type: The accelerator type. If None, the
            accelerator_type label will not be set.
        placement_group_config: The custom placement group configuration.
            If None, we use the default placement group configuration.

    Returns:
        The Ray scheduling strategy.
    """

    def _get_bundle() -> Dict[str, float]:
        # GPU bundles
        bundle = {"GPU": 1, "CPU": 1}

        # Accelerator type
        if accelerator_type:
            bundle[f"accelerator_type:{accelerator_type}"] = 0.001
        return bundle

    if placement_group_config:
        placement_group_config = copy.deepcopy(placement_group_config)

        if accelerator_type:
            for bundle in placement_group_config["bundles"]:
                bundle[f"accelerator_type:{accelerator_type}"] = 0.001

        pg = ray.util.placement_group(**placement_group_config)
    else:
        pg = ray.util.placement_group(
            [_get_bundle()] * num_bundles_per_replica,
            strategy="PACK",
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
        placement_group_config = fn_constructor_kwargs.pop(
            "placement_group_config", None
        )
        if executor_backend == "ray":
            # Note that we have to use partial() to pass a function
            # instead of an object.
            map_batches_kwargs["ray_remote_args_fn"] = partial(
                _ray_scheduling_strategy_fn,
                num_bundles_per_replica,
                accelerator_type,
                placement_group_config,
            )
            ray_remote_args["num_gpus"] = 0
        else:
            if not placement_group_config:
                # Default to GPUs per bundle if placement group is not specified.
                ray_remote_args["num_gpus"] = num_bundles_per_replica
            else:
                bundles = placement_group_config["bundles"]
                resource_counter = Counter()
                for bundle in bundles:
                    resource_counter.update(bundle)

                total_cpus = resource_counter.pop("CPU", 0)
                total_gpus = resource_counter.pop("GPU", 0)

                # Ray Data expects CPU/GPU to be specified via num_cpus/num_gpus,
                # not inside the resources dict.
                if total_cpus:
                    ray_remote_args["num_cpus"] = total_cpus
                if total_gpus:
                    ray_remote_args["num_gpus"] = total_gpus

                # Keep only non-CPU/GPU custom resources, if any.
                if resource_counter:
                    ray_remote_args["resources"] = dict(resource_counter)

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
            "image": "The image(s) for multimodal input. Accepts a single image or list of images.",
            "model": "The model to use for this request. If the model is different from the "
            "model set in the stage, then this is a LoRA request.",
            "multimodal_data": "The multimodal data to pass to the model, if the model supports it.",
            "mm_processor_kwargs": "The kwargs for the engine's multimodal processor.",
            "multimodal_uuids": "User-specified UUIDs for multimodal items, mapped by modality.",
        }
