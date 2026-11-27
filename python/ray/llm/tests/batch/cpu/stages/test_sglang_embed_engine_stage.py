"""Embed task tests for the SGLang engine stage. SGLang is mocked."""

import sys
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pydantic
import pytest

from ray.llm._internal.batch.stages.sglang_engine_stage import (
    SGLangEngineWrapper,
    SGLangTaskType,
)


@pytest.fixture
def embed_engine():
    """Patch `sglang.Engine` with a mock whose `async_encode` returns an embedding."""
    engine = MagicMock()
    engine.async_encode = AsyncMock(
        return_value={"embedding": [0.5, -1.0, 2.0], "meta_info": {"prompt_tokens": 4}}
    )
    engine_cls = MagicMock(return_value=engine)
    with patch.dict(sys.modules, {"sglang": MagicMock(Engine=engine_cls)}):
        engine.engine_cls = engine_cls
        yield engine


def _embed_wrapper():
    return SGLangEngineWrapper(
        model="test-model",
        idx_in_batch_column="__idx_in_batch",
        task=SGLangTaskType.EMBED,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "embed_params,expected_kwargs",
    [
        (None, {}),
        # Arrow stores ints as numpy scalars and fills unset fields with None.
        ({"dimensions": np.int64(2), "lora_path": None}, {"dimensions": 2}),
    ],
)
async def test_embed_wrapper(embed_engine, embed_params, expected_kwargs):
    row = {"__idx_in_batch": 3, "prompt": "hello"}
    if embed_params is not None:
        row["embed_params"] = embed_params

    _, idx_in_batch, output, _ = await _embed_wrapper().generate_async(row)

    # The stage forces these even without the processor.
    engine_kwargs = embed_engine.engine_cls.call_args.kwargs
    assert engine_kwargs["is_embedding"] is True
    assert engine_kwargs["skip_tokenizer_init"] is False

    embed_engine.async_encode.assert_awaited_once_with(
        prompt="hello", **expected_kwargs
    )
    if expected_kwargs:
        assert type(embed_engine.async_encode.call_args.kwargs["dimensions"]) is int
    assert idx_in_batch == 3
    assert output["embeddings"].dtype == np.float32
    np.testing.assert_array_equal(output["embeddings"], [0.5, -1.0, 2.0])
    assert output["num_input_tokens"] == 4
    assert output["num_generated_tokens"] == 0
    assert output["params"] == str(expected_kwargs)


@pytest.mark.asyncio
async def test_embed_wrapper_rejects_unknown_params(embed_engine):
    row = {"__idx_in_batch": 0, "prompt": "hello", "embed_params": {"prompt": "x"}}

    with pytest.raises(pydantic.ValidationError, match="prompt"):
        await _embed_wrapper().generate_async(row)
    embed_engine.async_encode.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
