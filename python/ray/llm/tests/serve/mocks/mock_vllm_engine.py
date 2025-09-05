import asyncio
import json
import random
from random import randint
from typing import AsyncGenerator, Dict, Union

from ray.llm._internal.common.utils.cloud_utils import LoraMirrorConfig
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    CompletionRequest,
    CompletionResponse,
    EmbeddingRequest,
    EmbeddingResponse,
    ErrorResponse,
    ScoreRequest,
    ScoreResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_engine import LLMEngine
from ray.llm._internal.serve.utils.lora_serve_utils import LoraModelLoader


class MockVLLMEngine(LLMEngine):
    """Mock vLLM Engine that generates fake text responses.

    - In case of LoRA it generates a prefix with the model name in the text part of the response.
    """

    def __init__(self, llm_config: LLMConfig):
        """Create a mock vLLM Engine.

        Args:
            llm_config: The llm configuration for this engine
        """
        self.llm_config = llm_config
        self.started = False
        self._current_lora_model: Dict[str, DiskMultiplexConfig] = {}

    async def start(self):
        """Start the mock engine."""
        self.started = True

    async def resolve_lora(self, lora_model: DiskMultiplexConfig):
        """Resolve/load a LoRA model."""
        self._current_lora_model[lora_model.model_id] = lora_model

    async def check_health(self) -> None:
        """Check the health of the mock engine."""
        if not self.started:
            raise RuntimeError("Engine not started")

    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the mock engine."""
        if not self.started:
            raise RuntimeError("Engine not started")

    async def start_profile(self) -> None:
        """Start profiling of the mock engine."""
        if not self.started:
            raise RuntimeError("Engine not started")

    async def stop_profile(self) -> None:
        """Stop profiling of the mock engine."""
        if not self.started:
            raise RuntimeError("Engine not started")

    async def chat(
        self, request: ChatCompletionRequest
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse, ErrorResponse], None]:
        """Mock chat completion."""
        if not self.started:
            raise RuntimeError("Engine not started")

        # Extract prompt text from messages
        prompt_text = ""
        if request.messages:
            for message in request.messages:
                if hasattr(message, "content") and message.content:
                    prompt_text += str(message.content) + " "

        max_tokens = getattr(request, "max_tokens", None) or randint(1, 10)

        # Generate streaming response
        async for response in self._generate_chat_response(
            request=request, prompt_text=prompt_text.strip(), max_tokens=max_tokens
        ):
            yield response

    async def completions(
        self, request: CompletionRequest
    ) -> AsyncGenerator[Union[str, CompletionResponse, ErrorResponse], None]:
        """Mock text completion."""
        if not self.started:
            raise RuntimeError("Engine not started")

        prompt_text = str(request.prompt) if request.prompt else ""
        max_tokens = getattr(request, "max_tokens", None) or randint(5, 20)

        # Generate streaming response
        async for response in self._generate_completion_response(
            request=request, prompt_text=prompt_text, max_tokens=max_tokens
        ):
            yield response

    async def embeddings(
        self, request: EmbeddingRequest
    ) -> AsyncGenerator[Union[str, EmbeddingResponse, ErrorResponse], None]:
        """Mock embeddings generation."""
        if not self.started:
            raise RuntimeError("Engine not started")

        # Generate a mock embedding response
        embedding_data = []
        inputs = request.input if isinstance(request.input, list) else [request.input]

        for i, text in enumerate(inputs):
            # Generate random embedding vector
            dimensions = getattr(request, "dimensions", None) or 1536
            embedding = [random.uniform(-1, 1) for _ in range(dimensions)]

            embedding_data.append(
                {"object": "embedding", "embedding": embedding, "index": i}
            )

        response = EmbeddingResponse(
            object="list",
            data=embedding_data,
            model=getattr(request, "model", "mock-model"),
            usage={
                "prompt_tokens": len(str(request.input).split()),
                "total_tokens": len(str(request.input).split()),
            },
        )
        yield response

    async def score(
        self, request: ScoreRequest
    ) -> AsyncGenerator[Union[str, ScoreResponse, ErrorResponse], None]:
        """Mock score generation for text pairs."""
        if not self.started:
            raise RuntimeError("Engine not started")

        # Extract text_1 and text_2 from the request
        text_1 = getattr(request, "text_1", "")
        text_2 = getattr(request, "text_2", "")

        # Convert to lists if they aren't already
        text_1_list = text_1 if isinstance(text_1, list) else [text_1]
        text_2_list = text_2 if isinstance(text_2, list) else [text_2]

        # Generate mock scores for each pair
        score_data = []
        for i, (t1, t2) in enumerate(zip(text_1_list, text_2_list)):
            # Generate a random score (can be any float value)
            score = random.uniform(-10.0, 10.0)

            score_data.append({"object": "score", "score": score, "index": i})

        # Create the response
        response = ScoreResponse(
            object="list",
            data=score_data,
            model=getattr(request, "model", "mock-model"),
            usage={
                "prompt_tokens": len(str(text_1).split()) + len(str(text_2).split()),
                "total_tokens": len(str(text_1).split()) + len(str(text_2).split()),
            },
        )
        yield response

    async def _generate_chat_response(
        self, request: ChatCompletionRequest, prompt_text: str, max_tokens: int
    ) -> AsyncGenerator[Union[str, ChatCompletionResponse], None]:
        """Generate mock chat completion response."""

        request_id = request.request_id or f"chatcmpl-{random.randint(1000, 9999)}"
        lora_prefix = (
            ""
            if request.model not in self._current_lora_model
            else f"[lora_model] {request.model}: "
        )
        if request.stream:
            # Streaming response - return SSE formatted strings
            created_time = int(asyncio.get_event_loop().time())
            model_name = getattr(request, "model", "mock-model")

            for i in range(max_tokens):
                if i == 0:
                    token = f"{lora_prefix}test_{i} "
                else:
                    token = f"test_{i} "
                if i == max_tokens - 1:
                    # no space for the last token
                    token = f"test_{i}"

                # Create streaming chunk
                choice = {
                    "index": 0,
                    "delta": {
                        "content": token,
                        "role": "assistant" if i == 0 else None,
                    },
                    "finish_reason": "stop" if i == max_tokens - 1 else None,
                }

                chunk_data = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created_time,
                    "model": model_name,
                    "choices": [choice],
                }

                # Format as SSE
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(0.01)  # Simulate processing time

            # Send final [DONE] message
            yield "data: [DONE]\n\n"
        else:
            # Non-streaming response - return response object
            generated_text = " ".join([f"test_{i}" for i in range(max_tokens)])
            generated_text = f"{lora_prefix}{generated_text}"

            choice = {
                "index": 0,
                "message": {"role": "assistant", "content": generated_text},
                "finish_reason": "stop",
            }

            response = ChatCompletionResponse(
                id=request_id,
                object="chat.completion",
                created=int(asyncio.get_event_loop().time()),
                model=getattr(request, "model", "mock-model"),
                choices=[choice],
                usage={
                    "prompt_tokens": len(prompt_text.split()),
                    "completion_tokens": max_tokens,
                    "total_tokens": len(prompt_text.split()) + max_tokens,
                },
            )

            yield response

    async def _generate_completion_response(
        self, request: CompletionRequest, prompt_text: str, max_tokens: int
    ) -> AsyncGenerator[Union[str, CompletionResponse], None]:
        """Generate mock completion response."""

        request_id = request.request_id or f"cmpl-{random.randint(1000, 9999)}"
        lora_prefix = (
            ""
            if request.model not in self._current_lora_model
            else f"[lora_model] {request.model}: "
        )
        if request.stream:
            # Streaming response - return SSE formatted strings
            created_time = int(asyncio.get_event_loop().time())
            model_name = getattr(request, "model", "mock-model")

            for i in range(max_tokens):
                if i == 0:
                    token = f"{lora_prefix}test_{i} "
                else:
                    token = f"test_{i} "
                if i == max_tokens - 1:
                    # no space for the last token
                    token = f"test_{i}"

                choice = {
                    "index": 0,
                    "text": token,
                    "finish_reason": "stop" if i == max_tokens - 1 else None,
                }

                chunk_data = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created_time,
                    "model": model_name,
                    "choices": [choice],
                }

                # Format as SSE
                yield f"data: {json.dumps(chunk_data)}\n\n"
                await asyncio.sleep(0.01)

            # Send final [DONE] message
            yield "data: [DONE]\n\n"
        else:
            # Non-streaming response - return response object
            generated_text = " ".join([f"test_{i}" for i in range(max_tokens)])
            generated_text = f"{lora_prefix}{generated_text}"

            choice = {"index": 0, "text": generated_text, "finish_reason": "stop"}

            response = CompletionResponse(
                id=request_id,
                object="text_completion",
                created=int(asyncio.get_event_loop().time()),
                model=getattr(request, "model", "mock-model"),
                choices=[choice],
                usage={
                    "prompt_tokens": len(prompt_text.split()),
                    "completion_tokens": max_tokens,
                    "total_tokens": len(prompt_text.split()) + max_tokens,
                },
            )

            yield response


class FakeLoraModelLoader(LoraModelLoader):
    """Fake LoRA model loader for testing that bypasses S3 entirely."""

    async def load_model_from_config(
        self, lora_model_id: str, llm_config
    ) -> DiskMultiplexConfig:
        """Load a fake LoRA model without any S3 access."""
        return DiskMultiplexConfig(
            model_id=lora_model_id,
            max_total_tokens=llm_config.max_request_context_length,
            local_path="/fake/local/path",
            lora_assigned_int_id=random.randint(1, 100),
        )

    async def load_model(
        self, lora_model_id: str, lora_mirror_config: LoraMirrorConfig
    ) -> DiskMultiplexConfig:
        """Load a fake LoRA model."""
        return DiskMultiplexConfig(
            model_id=lora_model_id,
            max_total_tokens=lora_mirror_config.max_total_tokens,
            local_path="/fake/local/path",
            lora_assigned_int_id=random.randint(1, 100),
        )
