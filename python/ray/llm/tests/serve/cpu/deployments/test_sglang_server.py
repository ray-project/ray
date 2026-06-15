import os
import sys
import asyncio
import json
import types
from unittest.mock import MagicMock, AsyncMock
from importlib.machinery import ModuleSpec
from typing import Any, List, Optional, Union
import pydantic

# Add the local ray python directory to sys.path dynamically
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../")))

# Mock native compiled modules and filelock before imports
sys.modules["ray._raylet"] = MagicMock()
sys.modules["ray._version"] = MagicMock(commit="mock", version="mock")
sys.modules["filelock"] = MagicMock()

# Implement sys.meta_path hook to mock all generated protobuf modules and sglang submodules
class PydanticStub(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)
    prompt: Any = None
    model: str = ""
    stream: bool = False
    messages: List[Any] = []
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    stop: Optional[Union[str, List[str]]] = None

class SglangProtocolModule(types.ModuleType):
    def __init__(self, name):
        super().__init__(name)
        self.__path__ = []
        
        stub_names = [
            "ChatCompletionRequest",
            "ChatCompletionResponse",
            "ChatCompletionStreamResponse",
            "CompletionRequest",
            "CompletionResponse",
            "CompletionStreamResponse",
            "DetokenizeRequest",
            "DetokenizeResponse",
            "EmbeddingRequest",
            "EmbeddingResponse",
            "ScoringRequest",
            "ScoringResponse",
            "TokenizeRequest",
            "TokenizeResponse",
        ]
        for stub_name in stub_names:
            stub_cls = type(stub_name, (PydanticStub,), {})
            setattr(self, stub_name, stub_cls)

class MockModule(types.ModuleType):
    def __init__(self, name):
        super().__init__(name)
        self.__path__ = []

    def __getattr__(self, name):
        return MagicMock()

class MockLoader:
    def create_module(self, spec):
        if spec.name == "sglang.srt.entrypoints.openai.protocol":
            return SglangProtocolModule(spec.name)
        return MockModule(spec.name)

    def exec_module(self, module):
        pass

class MockFinder:
    def find_spec(self, fullname, path, target=None):
        if (fullname.startswith("ray.core.generated") or 
            fullname.startswith("ray.serve.generated") or 
            fullname.startswith("sglang")):
            return ModuleSpec(fullname, MockLoader())
        return None

sys.meta_path.insert(0, MockFinder())

# Import ray first and ensure _raylet is bound
import ray
ray._raylet = sys.modules["ray._raylet"]

import pytest
from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import CompletionRequest


@pytest.mark.anyio
async def test_concurrent_streaming_completions():
    # Setup mock engine
    mock_engine_instance = MagicMock()

    # Mock async_generate for streaming to test interleaved streaming
    async def mock_async_generate(**kwargs):
        prompt = kwargs["prompt"]
        assert kwargs["stream"] is True
        
        if prompt == "prompt1":
            async def gen1():
                yield {"text": "1", "meta_info": {"finish_reason": None}}
                await asyncio.sleep(0.05)
                yield {"text": "12", "meta_info": {"finish_reason": "stop"}}
            return gen1()
        elif prompt == "prompt2":
            async def gen2():
                # Sleep to ensure gen1 starts first
                await asyncio.sleep(0.02)
                yield {"text": "A", "meta_info": {"finish_reason": None}}
                await asyncio.sleep(0.05)
                yield {"text": "AB", "meta_info": {"finish_reason": "stop"}}
            return gen2()
        else:
            raise ValueError(f"Unknown prompt: {prompt}")

    mock_engine_instance.async_generate = AsyncMock(side_effect=mock_async_generate)

    # Initialize SGLangServer
    llm_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={"tp_size": 1},
    )
    server = SGLangServer(llm_config)
    server.engine = mock_engine_instance

    # Create completion request with multiple prompts and stream=True
    request = CompletionRequest(
        model="test-model",
        prompt=["prompt1", "prompt2"],
        stream=True,
    )

    # Call completions
    chunks = []
    async for chunk in server.completions(request):
        chunks.append(chunk)

    # Verify the interleaving behavior of the SSE chunks
    parsed_chunks = []
    for chunk in chunks:
        assert chunk.startswith("data: ")
        assert chunk.endswith("\n\n")
        data_str = chunk[len("data: "):-2]
        parsed_chunks.append(json.loads(data_str))

    # We expect 4 completion chunks (2 for prompt1, 2 for prompt2)
    assert len(parsed_chunks) == 4

    # Let's check the indices of the choices in the chunks to confirm they are interleaved
    indices = [c["choices"][0]["index"] for c in parsed_chunks]
    texts = [c["choices"][0]["text"] for c in parsed_chunks]

    # Because prompt1 yields immediately, then prompt2 yields, then prompt1 yields, then prompt2 yields,
    # the indices should be interleaved: [0, 1, 0, 1]
    assert indices == [0, 1, 0, 1]
    
    # Delta text should be yielded (not cumulative)
    assert texts == ["1", "A", "2", "B"]


@pytest.mark.anyio
async def test_concurrent_streaming_completions_exception_handling():
    # Setup mock engine
    mock_engine_instance = MagicMock()

    # We want one prompt to succeed and one to raise an exception
    async def mock_async_generate(**kwargs):
        prompt = kwargs["prompt"]
        if prompt == "prompt1":
            async def gen1():
                yield {"text": "1", "meta_info": {}}
                await asyncio.sleep(0.1)
                yield {"text": "12", "meta_info": {}}
            return gen1()
        elif prompt == "prompt2":
            async def gen2():
                await asyncio.sleep(0.02)
                raise RuntimeError("Engine failure")
                # Yield to satisfy async generator syntax
                if False:
                    yield {}
            return gen2()

    mock_engine_instance.async_generate = AsyncMock(side_effect=mock_async_generate)

    llm_config = LLMConfig(
        model_loading_config={"model_id": "test-model"},
        engine_kwargs={"tp_size": 1},
    )
    server = SGLangServer(llm_config)
    server.engine = mock_engine_instance

    request = CompletionRequest(
        model="test-model",
        prompt=["prompt1", "prompt2"],
        stream=True,
    )

    try:
        async for _ in server.completions(request):
            pass
        raise AssertionError("Expected RuntimeError was not raised")
    except RuntimeError as e:
        assert str(e) == "Engine failure"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
