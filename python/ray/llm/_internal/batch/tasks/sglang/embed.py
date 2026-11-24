"""The embed task for SGLang."""

from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np
from pydantic import BaseModel, ConfigDict

from ray.llm._internal.batch.tasks.sglang.base import _SGLangTask

if TYPE_CHECKING:
    from ray.llm._internal.batch.processor.sglang_engine_proc import (
        SGLangEngineProcessorConfig,
    )


class SGLangEmbedParams(BaseModel):
    """The `Engine.async_encode` arguments a row can set. SGLang validates them."""

    model_config = ConfigDict(extra="forbid")

    # The output dimensions, for Matryoshka models.
    dimensions: Optional[int] = None
    # The LoRA adapter to use.
    lora_path: Optional[str] = None


class SGLangEmbedRequest(BaseModel):
    """An embed request to the SGLang engine."""

    prompt: str
    params: Dict[str, Any]


class _SGLangEmbedTask(_SGLangTask):
    """Embeds text prompts with `Engine.async_encode`."""

    required_input_keys = {"prompt": "The text prompt (str)."}
    optional_input_keys = {
        "embed_params": "The `Engine.async_encode` arguments: `dimensions` "
        "and `lora_path`.",
    }
    params_column = "embed_params"
    # SGLang tokenizes the text prompt, and an embedding has no tokens to
    # detokenize.
    unsupported_stages = ("tokenize_stage", "detokenize_stage")
    forced_engine_kwargs = {"is_embedding": True, "skip_tokenizer_init": False}

    @classmethod
    def prepare_config(
        cls, config: "SGLangEngineProcessorConfig"
    ) -> "SGLangEngineProcessorConfig":
        # Embed rows carry a prompt, so the chat template stage is off unless
        # it's set explicitly.
        if "chat_template_stage" not in config.model_fields_set:
            config = config.model_copy(update={"chat_template_stage": False})
        return super().prepare_config(config)

    def prepare_request(
        self, row: Dict[str, Any], params: Optional[Dict[str, Any]]
    ) -> SGLangEmbedRequest:
        # Arrow stores ints as numpy scalars, and fills fields a row doesn't
        # set with None.
        params = {
            key: value.item() if isinstance(value, np.generic) else value
            for key, value in (params or {}).items()
        }
        return SGLangEmbedRequest(
            prompt=row.pop("prompt"),
            params=SGLangEmbedParams(**params).model_dump(exclude_none=True),
        )

    async def run(self, engine: Any, request: SGLangEmbedRequest) -> Dict[str, Any]:
        output = await engine.async_encode(prompt=request.prompt, **request.params)
        return {
            "prompt": request.prompt,
            "embeddings": np.asarray(output["embedding"], dtype=np.float32),
            "num_input_tokens": output["meta_info"]["prompt_tokens"],
            "num_generated_tokens": 0,
            "params": str(request.params),
        }
