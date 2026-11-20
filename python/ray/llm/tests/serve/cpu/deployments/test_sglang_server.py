# ruff: noqa: E402
import asyncio
import json
import os
import sys
import types
from importlib.machinery import ModuleSpec
from typing import Any, List, Optional, Union
from unittest.mock import AsyncMock, MagicMock

import pydantic

# Add the local ray python directory to sys.path dynamically
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../../"))
)

# 1. Save original state before modifying sys.meta_path and sys.modules
_original_meta_path = list(sys.meta_path)
_original_modules = {}
for name in ["ray._raylet", "ray._version", "filelock", "ray", "sglang"]:
    if name in sys.modules:
        _original_modules[name] = sys.modules[name]

# 2. Setup import-time mock modules
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
        if (
            fullname.startswith("ray.core.generated")
            or fullname.startswith("ray.serve.generated")
            or fullname.startswith("sglang")
        ):
            return ModuleSpec(fullname, MockLoader())
        return None


# 3. Add MockFinder to meta_path
sys.meta_path.insert(0, MockFinder())

# 4. Import the mocked modules at import-time
import ray

ray._raylet = sys.modules["ray._raylet"]

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.openai_api_models import CompletionRequest
from ray.llm._internal.serve.engines.sglang.sglang_engine import SGLangServer


# 5. Define module-scoped autouse fixture to clean up after this module finishes
@pytest.fixture(scope="module", autouse=True)
def cleanup_mocks():
    yield
    # Restore sys.meta_path
    sys.meta_path = _original_meta_path

    # Clean up any imported modules that were imported under the MockFinder to prevent pollution
    for name in list(sys.modules.keys()):
        if name.startswith("sglang") or name.startswith("ray.llm") or name == "ray":
            if name not in _original_modules:
                del sys.modules[name]

    # Restore original modules
    for name, original_mod in _original_modules.items():
        sys.modules[name] = original_mod


@pytest.mark.anyio
async def test_concurrent_streaming_completions():
    # Setup mock engine
    mock_engine_instance = MagicMock()

    # Synchronize generators deterministically without relying on timing
    event1 = asyncio.Event()
    event2 = asyncio.Event()
    event3 = asyncio.Event()

    # Mock async_generate for streaming to test interleaved streaming
    async def mock_async_generate(**kwargs):
        prompt = kwargs["prompt"]
        assert kwargs["stream"] is True

        if prompt == "prompt1":

            async def gen1():
                yield {"text": "1", "meta_info": {"finish_reason": None}}
                event1.set()
                await event2.wait()
                yield {"text": "12", "meta_info": {"finish_reason": "stop"}}
                event3.set()

            return gen1()
        elif prompt == "prompt2":

            async def gen2():
                await event1.wait()
                yield {"text": "A", "meta_info": {"finish_reason": None}}
                event2.set()
                await event3.wait()
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
        data_str = chunk[len("data: ") : -2]
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


@pytest.mark.anyio
async def test_concurrent_streaming_cancelled_producer_no_sentinel():
    """Verify that a cancelled producer does not enqueue the None sentinel.

    When ``asyncio.CancelledError`` (a ``BaseException``) bypasses
    ``except Exception``, the producer must NOT unconditionally enqueue
    ``None`` in the ``finally`` block.  The ``completed`` flag ensures the
    sentinel is only sent after the generator finished normally or after
    the error was forwarded to the queue.

    This test simulates early consumer disconnection, which triggers task
    cancellation.  A successfully cancelled producer should leave no
    dangling tasks and should not enqueue ``None``.
    """
    mock_engine_instance = MagicMock()

    cancel_reached = asyncio.Event()

    async def mock_async_generate(**kwargs):
        prompt = kwargs["prompt"]
        if prompt == "prompt1":

            async def gen1():
                yield {"text": "fast", "meta_info": {"finish_reason": "stop"}}

            return gen1()
        elif prompt == "prompt2":

            async def gen2():
                # Block long enough for the consumer to break out and
                # cancel this task.
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    cancel_reached.set()
                    raise
                yield {"text": "never", "meta_info": {"finish_reason": "stop"}}

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

    # Consume only the first chunk (from prompt1) then break to simulate
    # client disconnection.  The generator's finally block should cancel
    # the still-running prompt2 producer.
    chunks = []
    async for chunk in server.completions(request):
        chunks.append(chunk)
        break

    assert len(chunks) == 1

    # Allow a brief moment for task cancellation to propagate.
    await asyncio.sleep(0.05)

    # Confirm prompt2 was actually cancelled (not silently completed).
    assert cancel_reached.is_set()


@pytest.mark.anyio
async def test_concurrent_streaming_early_disconnect():
    """Consumer closing the generator early must cancel remaining producers."""
    mock_engine_instance = MagicMock()

    async def mock_async_generate(**kwargs):
        async def slow_gen():
            for i in range(100):
                yield {"text": str(i), "meta_info": {"finish_reason": None}}
                await asyncio.sleep(0.01)

        return slow_gen()

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

    # Consume only a few chunks then break to simulate client disconnection.
    chunks = []
    async for chunk in server.completions(request):
        chunks.append(chunk)
        if len(chunks) >= 3:
            break

    assert len(chunks) == 3

    # Allow a brief moment for task cancellation to propagate.
    await asyncio.sleep(0.05)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
