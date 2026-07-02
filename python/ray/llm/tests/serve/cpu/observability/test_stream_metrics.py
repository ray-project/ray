"""Unit tests for instrumented_stream TTFT/TPOT metrics."""

import asyncio
from unittest import mock

import pytest

from ray.llm._internal.serve.observability.metrics.stream_metrics import (
    instrumented_stream,
    llm_generation_time_ms,
    llm_tpot_ms,
    llm_ttft_ms,
)


async def _fake_stream(items, delay_s=0.01):
    for item in items:
        await asyncio.sleep(delay_s)
        yield item


@pytest.mark.asyncio
async def test_ttft_recorded_on_first_token():
    with mock.patch.object(llm_ttft_ms, "observe") as mock_ttft:
        items = []
        async for item in instrumented_stream(_fake_stream(["a", "b", "c"]), "chat"):
            items.append(item)

        assert items == ["a", "b", "c"]
        assert mock_ttft.call_count == 1
        ttft_ms = mock_ttft.call_args[0][0]
        assert ttft_ms > 0


@pytest.mark.asyncio
async def test_tpot_recorded_between_tokens():
    with mock.patch.object(llm_tpot_ms, "observe") as mock_tpot:
        items = []
        async for item in instrumented_stream(
            _fake_stream(["a", "b", "c", "d"]), "completions"
        ):
            items.append(item)

        assert len(items) == 4
        assert mock_tpot.call_count == 3
        for call in mock_tpot.call_args_list:
            tpot_ms = call[0][0]
            assert tpot_ms > 0


@pytest.mark.asyncio
async def test_generation_time_recorded():
    with mock.patch.object(llm_generation_time_ms, "observe") as mock_gen:
        async for _ in instrumented_stream(_fake_stream(["a", "b"]), "chat"):
            pass

        assert mock_gen.call_count == 1
        gen_ms = mock_gen.call_args[0][0]
        assert gen_ms > 0


@pytest.mark.asyncio
async def test_no_metrics_on_empty_stream():
    with mock.patch.object(llm_ttft_ms, "observe") as mock_ttft, mock.patch.object(
        llm_tpot_ms, "observe"
    ) as mock_tpot, mock.patch.object(
        llm_generation_time_ms, "observe"
    ) as mock_gen:
        async for _ in instrumented_stream(_fake_stream([]), "chat"):
            pass

        mock_ttft.assert_not_called()
        mock_tpot.assert_not_called()
        mock_gen.assert_not_called()


@pytest.mark.asyncio
async def test_single_token_records_ttft_only():
    with mock.patch.object(llm_ttft_ms, "observe") as mock_ttft, mock.patch.object(
        llm_tpot_ms, "observe"
    ) as mock_tpot:
        async for _ in instrumented_stream(_fake_stream(["only"]), "chat"):
            pass

        assert mock_ttft.call_count == 1
        mock_tpot.assert_not_called()


@pytest.mark.asyncio
async def test_api_type_tag_passed():
    with mock.patch.object(llm_ttft_ms, "observe") as mock_ttft:
        async for _ in instrumented_stream(_fake_stream(["a"]), "completions"):
            pass

        tags = mock_ttft.call_args[0][1]
        assert tags == {"api_type": "completions"}
