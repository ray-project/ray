import shutil
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.batch.stages.tokenize_stage import DetokenizeUDF, TokenizeUDF
from ray.llm._internal.common.utils.download_utils import get_model_entrypoint


@pytest.fixture
def mock_tokenizer_setup():
    with patch(
        "ray.llm._internal.batch.stages.tokenize_stage.get_cached_tokenizer"
    ) as mock_get_tokenizer, patch("transformers.AutoTokenizer") as mock_auto_tokenizer:
        mock_tokenizer = MagicMock()
        mock_tokenizer.side_effect = lambda texts, **kwargs: {
            "input_ids": [[1, 2, 3] for _ in texts]
        }
        mock_get_tokenizer.return_value = mock_tokenizer
        mock_auto_tokenizer.from_pretrained.return_value = mock_tokenizer
        yield mock_tokenizer


@pytest.mark.asyncio
async def test_tokenize_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.return_value = [
        {"input_ids": [1, 2, 3]},
        {"input_ids": [4, 5, 6]},
    ]

    udf = TokenizeUDF(
        data_column="__data", model="test-model", expected_input_keys=["prompt"]
    )
    batch = {"__data": [{"prompt": "Hello"}, {"prompt": "World"}]}

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 2
    assert all(result["tokenized_prompt"] == [1, 2, 3] for result in results)
    assert all(
        original["prompt"] == result["prompt"]
        for original, result in zip(batch, results)
    )


@pytest.mark.asyncio
async def test_detokenize_udf_basic(mock_tokenizer_setup):
    mock_tokenizer = mock_tokenizer_setup
    mock_tokenizer.batch_decode.return_value = ["Hello", "World"]

    udf = DetokenizeUDF(
        data_column="__data",
        model="test-model",
        expected_input_keys=["generated_tokens"],
    )
    batch = {
        "__data": [
            {"generated_tokens": [1, 2, 3]},
            {"generated_tokens": [4, 5, 6]},
        ]
    }

    results = []
    async for result in udf(batch):
        results.extend(result["__data"])

    assert len(results) == 2
    assert results[0]["generated_text"] == "Hello"
    assert results[1]["generated_text"] == "World"
    mock_tokenizer.batch_decode.assert_called_once_with(
        [[1, 2, 3], [4, 5, 6]], skip_special_tokens=True
    )


def _save_bos_tokenizer(path):
    """Save a word-level tokenizer that adds BOS (id 1), with a chat template
    that also starts with BOS, as Llama 3 templates do."""
    from tokenizers import Tokenizer, models, pre_tokenizers, processors
    from transformers import PreTrainedTokenizerFast

    vocab = {"[UNK]": 0, "<bos>": 1, "user": 2, "assistant": 3, "hi": 4}
    tokenizer = Tokenizer(models.WordLevel(vocab, unk_token="[UNK]"))
    tokenizer.pre_tokenizer = pre_tokenizers.WhitespaceSplit()
    tokenizer.post_processor = processors.TemplateProcessing(
        single="<bos> $A", special_tokens=[("<bos>", 1)]
    )
    hf_tokenizer = PreTrainedTokenizerFast(
        tokenizer_object=tokenizer, bos_token="<bos>", unk_token="[UNK]"
    )
    hf_tokenizer.chat_template = (
        "{{ bos_token }} {% for m in messages %}{{ m['role'] }} "
        "{{ m['content'] }} {% endfor %}assistant"
    )
    hf_tokenizer.save_pretrained(path)
    (path / "config.json").write_text('{"model_type": "llama"}')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "add_special_tokens, expected_ids",
    [(True, [1, 1, 2, 4, 3]), (False, [1, 2, 4, 3])],
)
async def test_tokenize_udf_chat_template_prompt(
    tmp_path, add_special_tokens, expected_ids
):
    """A prompt rendered by the chat template already starts with BOS, so the
    tokenizer adds a second one unless add_special_tokens is False."""
    from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF

    _save_bos_tokenizer(tmp_path)
    chat_template_udf = ChatTemplateUDF(
        data_column="__data", expected_input_keys=["messages"], model=str(tmp_path)
    )
    tokenize_udf = TokenizeUDF(
        data_column="__data",
        expected_input_keys=["prompt"],
        model=str(tmp_path),
        add_special_tokens=add_special_tokens,
    )

    batch = {"__data": [{"messages": [{"role": "user", "content": "hi"}]}]}
    rows = []
    async for result in chat_template_udf(batch):
        rows.extend(result["__data"])
    assert rows[0]["prompt"].startswith("<bos>")

    results = []
    async for result in tokenize_udf({"__data": rows}):
        results.extend(result["__data"])
    assert list(results[0]["tokenized_prompt"]) == expected_ids


@pytest.mark.parametrize(
    "udf_cls, expected_input_keys",
    [
        (TokenizeUDF, ["prompt"]),
        (DetokenizeUDF, ["generated_tokens"]),
    ],
)
def test_trust_remote_code(model_internlm2_1_8b, udf_cls, expected_input_keys):
    model_entry = get_model_entrypoint(model_internlm2_1_8b)
    if model_entry != model_internlm2_1_8b:
        shutil.rmtree(model_entry, ignore_errors=True)

    udf = udf_cls(
        data_column="__data",
        model=model_internlm2_1_8b,
        expected_input_keys=expected_input_keys,
        trust_remote_code=True,
    )
    assert udf.tokenizer is not None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
