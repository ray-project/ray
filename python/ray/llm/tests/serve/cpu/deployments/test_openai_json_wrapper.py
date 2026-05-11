import sys

import pytest

from ray.llm._internal.serve.core.ingress.ingress import _openai_json_wrapper


async def _async_gen_from_list(items):
    for item in items:
        yield item


@pytest.mark.asyncio
class TestOpenAIJsonWrapper:
    async def test_no_duplicate_done_when_upstream_sends_done(self):
        """When the upstream generator already yields 'data: [DONE]', the
        wrapper must not append a second one."""
        upstream = ['data: {"id": 1}\n\n', "data: [DONE]\n\n"]
        chunks = [c async for c in _openai_json_wrapper(_async_gen_from_list(upstream))]
        assert chunks == upstream
        assert chunks.count("data: [DONE]\n\n") == 1

    async def test_done_appended_when_upstream_does_not_send_done(self):
        """When the upstream generator does not yield 'data: [DONE]', the
        wrapper must append it."""
        upstream = ['data: {"id": 1}\n\n']
        chunks = [c async for c in _openai_json_wrapper(_async_gen_from_list(upstream))]
        assert chunks == ['data: {"id": 1}\n\n', "data: [DONE]\n\n"]

    async def test_done_appended_for_empty_stream(self):
        """An empty upstream stream should still produce a [DONE] sentinel."""
        chunks = [c async for c in _openai_json_wrapper(_async_gen_from_list([]))]
        assert chunks == ["data: [DONE]\n\n"]

    async def test_no_duplicate_done_when_upstream_sends_done_batched(self):
        """When the upstream generator yields a batch containing 'data: [DONE]',
        the wrapper must not append a second one."""
        upstream = [['data: {"id": 1}\n\n', "data: [DONE]\n\n"]]
        chunks = [c async for c in _openai_json_wrapper(_async_gen_from_list(upstream))]
        assert chunks == ['data: {"id": 1}\n\ndata: [DONE]\n\n']


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
