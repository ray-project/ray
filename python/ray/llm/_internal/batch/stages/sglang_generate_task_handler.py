"""The generate task of the SGLang engine stage."""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from ray.llm._internal.batch.stages.common import maybe_convert_ndarray_to_list
from ray.llm._internal.batch.stages.sglang_task_handler import (
    _SGLangRequest,
    _SGLangTaskHandler,
)


class SGLangEngineRequest(_SGLangRequest):
    """A request to the SGLang engine."""

    # The input prompt.
    prompt: Optional[str] = None
    # Alternative to text. Specify the input as token IDs instead of text.
    prompt_token_ids: Optional[List[int]] = None
    # The sampling parameters (more details can be seen in https://docs.sglang.ai/backend/sampling_params.html).
    params: Optional[Dict[str, Any]] = None

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


class _SGLangGenerateTaskHandler(_SGLangTaskHandler):
    """Generates text with `Engine.async_generate`."""

    required_input_keys = {
        "prompt": "The text prompt (str).",
        "sampling_params": "The sampling parameters. See "
        "https://docs.sglang.ai/backend/sampling_params.html for details.",
    }

    def __init__(self):
        self.skip_tokenizer_init = True

    def configure_engine_kwargs(self, engine_kwargs: Dict[str, Any]) -> None:
        self.skip_tokenizer_init = engine_kwargs["skip_tokenizer_init"]

    def prepare_request(
        self, row: Dict[str, Any], request_id: int, idx_in_batch: int
    ) -> SGLangEngineRequest:
        prompt = row.pop("prompt")

        if "tokenized_prompt" in row:
            tokenized_prompt = row.pop("tokenized_prompt").tolist()
        else:
            tokenized_prompt = None

        params = maybe_convert_ndarray_to_list(row.pop("sampling_params"))

        if tokenized_prompt is not None and not self.skip_tokenizer_init:
            raise ValueError(
                "To use a token-in-token-out mode of SGLang Engine, please set engine_kwargs['skip_tokenizer_init'] to True."
            )

        return SGLangEngineRequest(
            request_id=request_id,
            idx_in_batch=idx_in_batch,
            prompt=prompt,
            prompt_token_ids=tokenized_prompt,
            params=params,
        )

    async def run(self, engine: Any, request: SGLangEngineRequest) -> Dict[str, Any]:
        output = await self._generate_async(engine, request)
        return {
            **SGLangOutputData.from_sglang_engine_output(output).model_dump(),
            "params": str(request.params),
        }

    async def _generate_async(
        self, engine: Any, request: SGLangEngineRequest
    ) -> Dict[str, Any]:
        """Send the request to the engine and return its final output."""
        stream = await engine.async_generate(
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
