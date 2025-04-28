import asyncio
import json
from types import SimpleNamespace
from typing import List, Optional
from unittest.mock import Mock
import sys
import pytest

from ray.llm._internal.serve.configs.server_models import FinishReason
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import (
    BatchLLMRawResponses,
    VLLMEngine,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMGenerationRequest,
    VLLMSamplingParams,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMRawResponse,
)
from ray.llm._internal.serve.configs.constants import MODEL_RESPONSE_BATCH_TIMEOUT_MS


class FakeVLLMEngine:
    def __init__(self, mock: Mock, output=None):
        self.engine = mock

        self._output = output or []
        self.num_generated = 0

    async def generate(self, *args, **kwargs):
        # Record the call
        self.engine.generate(*args, **kwargs)

        for x in self._output:
            await asyncio.sleep(0.01)
            self.num_generated += 1
            yield x

    async def abort(self, request_id: str):
        # Record the call
        self.engine.abort(request_id)

    def _abort(self, request_id: str, **kwargs):
        # Record the call
        self.engine.abort(request_id)


def get_fake_responses(*tokens: List[str]):
    total = ""
    output = []

    for token in tokens:
        total += token
        # For some reason vLLM appears to return the full text on each iteration
        # We should fix this in vllm
        output.append(
            SimpleNamespace(
                outputs=[
                    SimpleNamespace(
                        text=total,
                        finish_reason="stop",  # for some reason, vllm returns a finish reason on all tokens. We should fix this too.
                        token_ids=[0],
                        logprobs=[],
                    )
                ],
                prompt_token_ids=[0],
                metrics=SimpleNamespace(time_in_queue=0.01),
            )
        )

    return output


def get_fake_engine_and_request(llm_config: LLMConfig, expected_out: List[str]):
    vllm_engine = VLLMEngine(llm_config)
    # We normally set the model config when calling VLLMEngine.start()
    vllm_engine.model_config = Mock()
    vllm_engine.model_config.max_model_len = 1

    engine_mock = Mock()
    vllm_engine.engine = FakeVLLMEngine(engine_mock, get_fake_responses(*expected_out))

    req = VLLMGenerationRequest(
        prompt="prompt",
        request_id="req_id",
        sampling_params=VLLMSamplingParams(),
        disk_multiplex_config=None,
    )
    return vllm_engine, req, engine_mock


class TestVLLMEngine:
    """Test the VLLMEngine."""

    @pytest.mark.asyncio
    async def test_vllm_engine(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm."]
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        cur_idx = 0
        # Generate without the 100ms batching
        async for x in vllm_engine._generate(req):
            if cur_idx < len(expected_out):
                assert x.generated_text == expected_out[cur_idx]
                cur_idx += 1
                assert x.generation_time == pytest.approx(
                    0.01, abs=0.01
                ), "We are sleeping for this long before returning tokens in the fake"
                assert (
                    x.num_input_tokens == 1
                ), "We are setting the num input tokens to len 1 in the fake output"
            else:
                assert x.finish_reason == FinishReason.STOP

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling

        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")

    @pytest.mark.asyncio
    async def test_vllm_engine_error_in_caller(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm."]
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        with pytest.raises(RuntimeError):
            async for _x in vllm_engine.generate(req, stream=True):
                raise RuntimeError()

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling
        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")

    @pytest.mark.asyncio
    async def test_vllm_engine_caller_cancellation(self, llm_config):
        expected_out = ["hi ", "i ", "am ", "vllm.", "and more"] * 10  # many tokens
        vllm_engine, req, engine_mock = get_fake_engine_and_request(
            llm_config, expected_out
        )

        async def run():
            async for x in vllm_engine.generate(req, stream=True):
                print(x)

        task = asyncio.create_task(run())
        await asyncio.sleep(0.02)  # wait for some tokens to be returned

        # Cancel the task
        task.cancel()

        await asyncio.sleep(0.02)  # wait for asyncio task scheduling
        # Abort should be called
        engine_mock.abort.assert_called_once_with("req_id")
        assert (
            vllm_engine.engine.num_generated <= 4
        ), "We should have generated not more than 4 tokens"

    @pytest.mark.parametrize("enable_json_mode", [True, False])
    def test_parse_sampling_params_json_mode(
        self, llm_config: LLMConfig, enable_json_mode: bool
    ):
        # Make a deep copy to avoid modifying the session-scoped fixture
        llm_config = llm_config.model_copy(deep=True)
        vllm_engine = VLLMEngine(llm_config)

        # Mock model_config to avoid None errors
        vllm_engine.model_config = Mock()
        vllm_engine.model_config.max_model_len = 1000

        # Create sampling params with response format
        sampling_params = VLLMSamplingParams(
            response_format={
                "type": "json_object",
                "schema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
            }
        )

        # Parse the sampling params
        parsed_params = vllm_engine._parse_sampling_params(sampling_params)

        # For both cases we should now have guided decoding since we are using oss vllm.
        # When json_mode is disabled, guided_decoding should be used instead
        assert hasattr(parsed_params, "guided_decoding")
        # Parse the JSON string from guided_decoding into a dict
        guided_json = json.loads(parsed_params.guided_decoding.json)
        assert guided_json == sampling_params.response_format.json_schema
        assert getattr(parsed_params, "response_format", None) is None


TEXT_VALUE = "foo"
FINAL_TEXT_VALUE = "bar"


async def fake_generator():
    """Returns 100 responses with no delay"""
    for _i in range(100):
        yield LLMRawResponse(num_generated_tokens=1, generated_text=TEXT_VALUE)


async def fake_generator_slow(num_batches: int):
    """Returns 100 responses with small delay.

    Delay is set such that the responses are batched into roughly num_batches
    batches.
    """

    for _i in range(100):
        await asyncio.sleep(MODEL_RESPONSE_BATCH_TIMEOUT_MS / 1000 / num_batches)
        yield LLMRawResponse(num_generated_tokens=1, generated_text=TEXT_VALUE)


async def fake_generator_slow_last_return_immediate():
    """Returns 11 responses with small delay, aside from the last one which is immediate"""
    for _i in range(10):
        await asyncio.sleep(MODEL_RESPONSE_BATCH_TIMEOUT_MS / 1000)
        yield LLMRawResponse(num_generated_tokens=1, generated_text=TEXT_VALUE)
    yield LLMRawResponse(num_generated_tokens=1, generated_text=FINAL_TEXT_VALUE)


class TestBatching:
    @pytest.mark.asyncio
    async def test_batch(self):
        count = 0
        batcher = BatchLLMRawResponses(fake_generator())
        async for x in batcher.stream():
            count += 1
            assert x.num_generated_tokens == 100
            assert x.generated_text == TEXT_VALUE * 100

        # Should only have been called once
        assert count == 1
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_timing(self):
        count = 0
        batcher = BatchLLMRawResponses(fake_generator_slow(num_batches=10))
        async for _x in batcher.stream():
            count += 1

        assert 9 <= count <= 12, (
            "Count should have been called between 9 and 12 times, "
            "because each iteration takes 1/10th of an interval to yield."
        )
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_last_return_is_immediate(self):
        """Test that we don't wait the entire interval for
        the last response if it returns quickly."""
        count = 0
        token_count = 0
        batcher = BatchLLMRawResponses(fake_generator_slow_last_return_immediate())
        last_response = None
        async for _x in batcher.stream():
            count += 1
            token_count += _x.num_generated_tokens
            last_response = _x

        assert (
            last_response.generated_text == TEXT_VALUE + FINAL_TEXT_VALUE
        ), "the last generated response should be batched with previous one"
        assert token_count == 11, "token_count should be exactly 11"
        assert (
            count == 10
        ), "Count should have been called exactly 10 times (as many as we generated - 1)"
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    async def test_batch_no_interval(self):
        """Check that the class creates only one batch if there's no interval."""

        batcher = BatchLLMRawResponses(
            fake_generator_slow(num_batches=10), interval_ms=None
        )

        count = 0
        async for _x in batcher.stream():
            count += 1

        assert count == 1
        assert batcher.queue.empty()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("interval_ms", [100, None])
    async def test_exception_propagation(self, interval_ms: Optional[float]):
        """Test that exceptions are propagated correctly to parent."""

        async def generator_should_raise():
            for _i in range(100):
                await asyncio.sleep(0.01)
                yield LLMRawResponse(num_generated_tokens=1, generated_text=TEXT_VALUE)
                raise ValueError()

        count = 0
        batched = BatchLLMRawResponses(
            generator_should_raise(), interval_ms=interval_ms
        )

        async def parent():
            nonlocal count
            nonlocal batched
            async for _x in batched.stream():
                count += 1

        task = asyncio.create_task(parent())
        await asyncio.sleep(0.2)

        with pytest.raises(ValueError):
            task.result()
        assert count == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("interval_ms", [100, None])
    @pytest.mark.parametrize("to_cancel", ["parent", "inner", "stream"])
    async def test_cancellation(self, interval_ms: Optional[float], to_cancel: str):
        """There are 3 ways cancellation can happen:
        1. The parent is cancelled
        2. The generator is cancelled
        3. The stream task is directly cancelled.

        Make sure all associated tasks are cancelled in each instance.
        """

        async def generator_should_raise():
            with pytest.raises(asyncio.CancelledError):
                for _i in range(100):
                    await asyncio.sleep(0.01)
                    yield LLMRawResponse(
                        num_generated_tokens=1, generated_text=TEXT_VALUE
                    )
                    if to_cancel == "inner":
                        raise asyncio.CancelledError()

        batched = BatchLLMRawResponses(
            generator_should_raise(), interval_ms=interval_ms
        )

        async def parent():
            nonlocal batched
            async for _x in batched.stream():
                pass

        task = asyncio.create_task(parent())
        await asyncio.sleep(0.2)

        cancel_task = {
            "parent": task,
            "stream": batched.read_task,
        }.get(to_cancel)

        if cancel_task:
            assert not task.done()
            assert not batched.read_task.done()
            cancel_task.cancel()

        await asyncio.sleep(0.3)
        assert batched.read_task.done(), "Read task should be completed"
        assert task.done(), "All tasks should be done"

        # Inner task is checked automatically with pytest.raises


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
