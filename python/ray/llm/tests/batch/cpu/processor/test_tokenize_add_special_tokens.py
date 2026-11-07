"""Tests for how processors set add_special_tokens on the tokenize stage.

Chat templates such as Llama 3's render the BOS token into the prompt text.
Tokenizing that text again with add_special_tokens=True prepends a second BOS,
so the tokenize stage must not add special tokens after the chat template stage.

The stage modules are imported at module level so that they bind the real
download_model_files before any test here patches it.
"""
import sys
from unittest.mock import patch

import pytest

from ray.llm._internal.batch.processor.sglang_engine_proc import (
    SGLangEngineProcessorConfig,
    build_sglang_engine_processor,
)
from ray.llm._internal.batch.processor.vllm_engine_proc import (
    build_vllm_engine_processor,
    vLLMEngineProcessorConfig,
)
from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    TokenizerStageConfig,
)
from ray.llm._internal.batch.stages.tokenize_stage import TokenizeUDF


@pytest.mark.parametrize(
    "add_special_tokens, chat_template_enabled, expected",
    [
        (None, True, False),
        (None, False, True),
        (True, True, True),
        (False, False, False),
    ],
    ids=[
        "default_chat_template",
        "default_raw_prompt",
        "explicit_true",
        "explicit_false",
    ],
)
def test_resolve_tokenize_add_special_tokens(
    add_special_tokens, chat_template_enabled, expected
):
    from ray.llm._internal.batch.processor.utils import (
        resolve_tokenize_add_special_tokens,
    )

    tokenize_cfg = TokenizerStageConfig(add_special_tokens=add_special_tokens)
    chat_template_cfg = ChatTemplateStageConfig(enabled=chat_template_enabled)
    assert (
        resolve_tokenize_add_special_tokens(tokenize_cfg, chat_template_cfg) is expected
    )


# Call the builders directly rather than through ProcessorBuilder, whose
# registry other tests in this directory clear.
BUILDERS = [
    (vLLMEngineProcessorConfig, build_vllm_engine_processor),
    (SGLangEngineProcessorConfig, build_sglang_engine_processor),
]
BUILDER_IDS = ["vllm", "sglang"]


@pytest.fixture
def no_model_download():
    """Build processors without downloading model files."""
    with patch(
        "ray.llm._internal.common.utils.download_utils.download_model_files",
        return_value="/nonexistent-model-path",
    ), patch("transformers.AutoConfig.from_pretrained", side_effect=OSError):
        yield


@pytest.mark.parametrize("config_cls, build_fn", BUILDERS, ids=BUILDER_IDS)
@pytest.mark.parametrize(
    "stage_kwargs, expected",
    [
        # Default: the chat template already renders BOS, so don't add another.
        ({}, False),
        # Raw prompts without a chat template get the tokenizer's special tokens.
        ({"chat_template_stage": False}, True),
        # An explicit setting always wins.
        ({"tokenize_stage": {"add_special_tokens": True}}, True),
        (
            {
                "chat_template_stage": False,
                "tokenize_stage": {"add_special_tokens": False},
            },
            False,
        ),
    ],
    ids=[
        "default",
        "no_chat_template",
        "explicit_true",
        "explicit_false_no_chat_template",
    ],
)
def test_processor_tokenize_stage_add_special_tokens(
    no_model_download, config_cls, build_fn, stage_kwargs, expected
):
    config = config_cls(model_source="test-model", **stage_kwargs)
    processor = build_fn(config)

    stage = processor.get_stage_by_name("TokenizeStage")
    assert stage.fn_constructor_kwargs["add_special_tokens"] is expected


@pytest.mark.parametrize("config_cls, build_fn", BUILDERS, ids=BUILDER_IDS)
def test_processor_tokenize_stage_add_special_tokens_typed_config(
    no_model_download, config_cls, build_fn
):
    config = config_cls(
        model_source="test-model",
        tokenize_stage=TokenizerStageConfig(add_special_tokens=True),
    )
    processor = build_fn(config)

    stage = processor.get_stage_by_name("TokenizeStage")
    assert stage.fn_constructor_kwargs["add_special_tokens"] is True


BOS_ID = 0


@pytest.fixture
def bos_template_model(tmp_path):
    """A tiny local tokenizer that adds BOS and whose chat template renders it.

    This mirrors Llama 3's setup without downloading anything.
    """
    from tokenizers import Tokenizer, models, pre_tokenizers, processors
    from transformers import PreTrainedTokenizerFast

    vocab = {
        "<s>": BOS_ID,
        "</s>": 1,
        "<unk>": 2,
        "user": 3,
        "assistant": 4,
        "Hello": 5,
    }
    backend = Tokenizer(models.WordLevel(vocab=vocab, unk_token="<unk>"))
    backend.pre_tokenizer = pre_tokenizers.WhitespaceSplit()
    # Like Llama 3, the tokenizer prepends BOS when add_special_tokens=True.
    backend.post_processor = processors.TemplateProcessing(
        single="<s> $A", special_tokens=[("<s>", BOS_ID)]
    )
    tokenizer = PreTrainedTokenizerFast(
        tokenizer_object=backend,
        bos_token="<s>",
        eos_token="</s>",
        unk_token="<unk>",
    )
    tokenizer.chat_template = (
        "{{ bos_token }}"
        "{% for m in messages %}{{ m['role'] }} {{ m['content'] }} {% endfor %}"
        "{% if add_generation_prompt %}assistant {% endif %}"
    )
    tokenizer.save_pretrained(str(tmp_path))
    return str(tmp_path)


async def _run_udf(udf, rows):
    outputs = []
    async for out in udf({"__data": rows}):
        outputs.extend(out["__data"])
    return outputs


@pytest.mark.asyncio
@pytest.mark.parametrize("config_cls, build_fn", BUILDERS, ids=BUILDER_IDS)
async def test_default_stages_send_a_single_bos(
    bos_template_model, config_cls, build_fn
):
    """The default chat template + tokenize stages produce exactly one BOS."""
    from transformers import AutoTokenizer

    with patch("transformers.AutoConfig.from_pretrained", side_effect=OSError):
        processor = build_fn(config_cls(model_source=bos_template_model))

    chat_template_udf = ChatTemplateUDF(
        data_column="__data",
        expected_input_keys=["messages"],
        **processor.get_stage_by_name("ChatTemplateStage").fn_constructor_kwargs,
    )
    tokenize_udf = TokenizeUDF(
        data_column="__data",
        expected_input_keys=["prompt"],
        **processor.get_stage_by_name("TokenizeStage").fn_constructor_kwargs,
    )

    rows = [{"messages": [{"role": "user", "content": "Hello"}]}]
    rows = await _run_udf(chat_template_udf, rows)
    assert rows[0]["prompt"] == "<s>user Hello assistant "
    rows = await _run_udf(tokenize_udf, rows)

    token_ids = list(rows[0]["tokenized_prompt"])
    assert token_ids.count(BOS_ID) == 1
    # Same ids as tokenizing the rendered chat without adding special tokens,
    # which is what apply_chat_template(tokenize=True) does.
    tokenizer = AutoTokenizer.from_pretrained(bos_template_model)
    assert (
        token_ids == tokenizer(rows[0]["prompt"], add_special_tokens=False)["input_ids"]
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
