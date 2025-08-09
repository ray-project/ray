"""The stage that runs shared vLLM engine via Ray Serve deployment."""

import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List

from ray.serve import get_app_handle

from ray.llm._internal.batch.stages.base import StatefulStage, StatefulStageUDF
from ray.llm._internal.batch.stages.vllm_engine_stage import (
    vLLMEngineRequest,
    vLLMOutputData,
    vLLMTaskType,
)
from ray.llm._internal.serve.configs.openai_api_models import (
    CompletionRequest,
    EmbeddingRequest,
    ErrorResponse,
)
from ray.llm._internal.batch.stages.common import maybe_convert_ndarray_to_list

import uuid
import time

logger = logging.getLogger(__name__)


class vLLMSharedEngineStageUDF(StatefulStageUDF):
    def __init__(
        self,
        data_column: str,
        expected_input_keys: List[str],
        serve_deployment_name: str,
        task_type: vLLMTaskType = vLLMTaskType.GENERATE,
        model_id: str = None,
    ):
        """
        Initialize the vLLMSharedEngineStageUDF.

        Args:
            data_column: The data column name.
            expected_input_keys: The expected input keys of the stage.
            serve_deployment_name: Name of the Ray Serve deployment of the shared vLLM engine.
            task_type: The task to use for the vLLM engine (e.g., "generate", "embed", etc).
            model_id: The model ID to use in Serve requests.
        """
        super().__init__(data_column, expected_input_keys)
        self.serve_deployment_name = serve_deployment_name
        self.task_type = task_type
        self.model_id = model_id
        self._serve_handle = None
        self.request_id = 0

    async def _get_serve_handle(self):
        """Get the Ray Serve handle for the deployment with streaming enabled."""
        if self._serve_handle is None:
            try:
                base_handle = get_app_handle(self.serve_deployment_name)
                # Enable streaming mode so that the deployment can legally return async generators.
                self._serve_handle = base_handle.options(stream=True)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to get serve handle for {self.serve_deployment_name}"
                )
        return self._serve_handle

    async def _process_request(self, request: vLLMEngineRequest) -> vLLMOutputData:
        """Process a single request through the Serve deployment."""
        serve_handle = await self._get_serve_handle()
        prompt = request.prompt
        prompt_token_ids = request.prompt_token_ids
        num_input_tokens = len(prompt_token_ids) if prompt_token_ids else 0

        # Convert vLLM request to appropriate Serve request format
        if self.task_type == vLLMTaskType.GENERATE:
            serve_request = CompletionRequest(
                model=self.model_id,
                prompt=request.prompt,
                stream=False,
            )

            response_gen = serve_handle.completions.remote(serve_request)
            response = await anext(response_gen)

            if isinstance(response, ErrorResponse):
                raise RuntimeError(f"Serve deployment error: {response.message}")

            # Convert CompletionResponse to vLLMOutputData
            output_data = vLLMOutputData(
                prompt=prompt,
                prompt_token_ids=prompt_token_ids,
                num_input_tokens=num_input_tokens,
                generated_text=response.choices[0].text,
            )
        elif self.task_type == vLLMTaskType.EMBED:
            serve_request = EmbeddingRequest(
                model=self.model_id,
                input=request.prompt,
            )

            response_gen = serve_handle.embeddings.remote(serve_request)
            response = await anext(response_gen)

            if isinstance(response, ErrorResponse):
                raise RuntimeError(f"Serve deployment error: {response.message}")

            # Convert EmbeddingResponse to vLLMOutputData
            output_data = vLLMOutputData(
                prompt=prompt,
                prompt_token_ids=prompt_token_ids,
                num_input_tokens=num_input_tokens,
                embeddings=response.data[0].embedding,
            )

        else:
            raise ValueError(f"Unsupported task type: {self.task_type}")

        return output_data

    def _prepare_llm_request(self, row: Dict[str, Any]) -> vLLMEngineRequest:
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

        sampling_params = row.get("sampling_params", {})
        request = vLLMEngineRequest(
            request_id=self.request_id,
            idx_in_batch=row[self.IDX_IN_BATCH_COLUMN],
            prompt=prompt,
            prompt_token_ids=tokenized_prompt,
            images=image,
            params=sampling_params,
            lora_request=None,  # TODO: Add LoRA support
        )
        self.request_id += 1
        return request

    async def udf(self, batch: List[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Run the shared vLLM engine through serve deployment.

        Args:
            batch: A list of rows to run the vLLM engine on.

        Returns:
            The response of the vLLM engine.
        """
        requests = [self._prepare_llm_request(row) for row in batch]

        batch_uuid = uuid.uuid4()
        t = time.perf_counter()

        tasks = []
        for i, req in enumerate(requests):

            async def process_with_index(request, idx):
                result = await self._process_request(request)
                return idx, request, result

            task = asyncio.create_task(process_with_index(req, i))
            tasks.append(task)

        time_taken = -1.0
        for resp in asyncio.as_completed(tasks):
            req_idx, req, result = await resp
            time_taken = time.perf_counter() - t

            yield {
                **result.model_dump(),
                self.IDX_IN_BATCH_COLUMN: req.idx_in_batch,
                "batch_uuid": batch_uuid.hex,
                "time_taken_llm": time_taken,
            }

        logger.info(
            "[vLLM] Elapsed time for batch %s with size %d: %s",
            batch_uuid.hex,
            len(batch),
            time_taken,
        )


class vLLMSharedEngineStage(StatefulStage):
    """A stage that runs a shared vLLM engine."""

    fn: type[StatefulStageUDF] = vLLMSharedEngineStageUDF

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
