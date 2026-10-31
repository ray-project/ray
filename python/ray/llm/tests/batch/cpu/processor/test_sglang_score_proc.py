"""Score task tests for the SGLang processor. SGLang isn't needed."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from ray.data.llm import SGLangEngineProcessorConfig
from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder

_SCORE_MODEL = "unsloth/Llama-3.2-1B-Instruct"


@pytest.fixture
def hf_config():
    """Patch the Hugging Face config the builder loads."""
    config = SimpleNamespace(architectures=["LlamaForCausalLM"], vocab_size=1000)
    with patch("transformers.AutoConfig.from_pretrained", return_value=config):
        yield config


@pytest.fixture
def marker_tokenizer():
    """Patch the tokenizer used to resolve the embedding marker to ID 77."""

    def tokenize(text, add_special_tokens=False):
        ids = []
        for part in text.split("<emb>"):
            ids.extend(ord(c) for c in part)
            ids.append(77)
        return {"input_ids": ids[:-1]}

    tokenizer = MagicMock(side_effect=tokenize, unk_token_id=0)
    with patch(
        "ray.llm._internal.batch.utils.load_tokenizer",
        return_value=tokenizer,
    ):
        yield tokenizer


def _score_config(score_options=None, **kwargs):
    return SGLangEngineProcessorConfig(
        model_source=_SCORE_MODEL,
        task_type=SGLangTaskType.SCORE,
        score_options={"label_token_ids": [10, 20]}
        if score_options is None
        else score_options,
        **kwargs,
    )


@pytest.mark.parametrize(
    "kwargs,expected_stages",
    [
        ({}, ["SGLangScoreTokenizeStage", "SGLangEngineStage"]),
        ({"tokenize_stage": False}, ["SGLangEngineStage"]),
        (
            {"chat_template_stage": True, "detokenize_stage": True},
            ["SGLangScoreTokenizeStage", "SGLangEngineStage"],
        ),
    ],
)
def test_score_stages(hf_config, kwargs, expected_stages):
    processor = ProcessorBuilder.build(_score_config(**kwargs))
    assert processor.list_stage_names() == expected_stages


def test_score_stage_kwargs(hf_config):
    processor = ProcessorBuilder.build(
        _score_config(
            score_options={"label_token_ids": [10, 20], "apply_softmax": True},
            engine_kwargs={"enable_mis": True},
            tokenize_stage={"batch_size": 13, "model_source": "other-tokenizer"},
        )
    )

    tokenize = processor.get_stage_by_name("SGLangScoreTokenizeStage")
    assert tokenize.map_batches_kwargs["batch_size"] == 13
    assert tokenize.fn_constructor_kwargs["model"] == "other-tokenizer"

    engine = processor.get_stage_by_name("SGLangEngineStage")
    assert engine.fn_constructor_kwargs["task_options"] == {
        "label_token_ids": [10, 20],
        "apply_softmax": True,
        "item_first": False,
        "embed_override_token_id": None,
    }
    assert engine.fn_constructor_kwargs["engine_kwargs"]["enable_mis"] is True


def test_score_config_round_trip(hf_config):
    config = _score_config(tokenize_stage=False)
    rebuilt = SGLangEngineProcessorConfig(**config.model_dump())
    assert (
        ProcessorBuilder.build(rebuilt).list_stage_names()
        == ProcessorBuilder.build(config).list_stage_names()
    )


def test_score_embed_override_token_resolved_on_driver(hf_config, marker_tokenizer):
    processor = ProcessorBuilder.build(
        _score_config(
            score_options={"label_token_ids": [10], "embed_override_token": "<emb>"}
        )
    )

    tokenize = processor.get_stage_by_name("SGLangScoreTokenizeStage")
    assert tokenize.fn_constructor_kwargs["embed_override_token"] == "<emb>"
    assert tokenize.fn_constructor_kwargs["embed_override_token_id"] == 77
    engine = processor.get_stage_by_name("SGLangEngineStage")
    assert engine.fn_constructor_kwargs["task_options"]["embed_override_token_id"] == 77
    assert "embed_override_token" not in engine.fn_constructor_kwargs["task_options"]


def test_score_extraction_token_resolved_on_driver(hf_config, marker_tokenizer):
    processor = ProcessorBuilder.build(
        _score_config(
            score_options={"label_token_ids": [10], "score_extraction_token": "<emb>"},
            engine_kwargs={"enable_mis": True},
        )
    )

    tokenize = processor.get_stage_by_name("SGLangScoreTokenizeStage")
    assert tokenize.fn_constructor_kwargs["score_extraction_token"] == "<emb>"
    assert tokenize.fn_constructor_kwargs["score_extraction_token_id"] == 77
    assert "embed_override_token" not in tokenize.fn_constructor_kwargs
    score_options = processor.get_stage_by_name(
        "SGLangEngineStage"
    ).fn_constructor_kwargs["task_options"]
    assert score_options["score_extraction_token_id"] == 77
    assert "score_extraction_token" not in score_options
    marker_tokenizer.assert_called()


@pytest.mark.parametrize(
    "score_options,kwargs,match",
    [
        ({"label_token_ids": []}, {}, "must not be empty"),
        ({"label_token_ids": [True]}, {}, "label_token_ids"),
        ({"label_token_ids": [1], "typo": 1}, {}, "typo"),
        (
            {"embed_override_token": "<emb>", "embed_override_token_id": 3},
            {},
            "only one of",
        ),
        (
            {"score_extraction_token": "<x>", "score_extraction_token_id": 3},
            {},
            "only one of",
        ),
    ],
)
def test_score_options_validation(score_options, kwargs, match):
    with pytest.raises(ValueError, match=match):
        _score_config(score_options=score_options, **kwargs)


@pytest.mark.parametrize(
    "score_options,kwargs,match",
    [
        ({}, {}, "label_token_ids is required for LlamaForCausalLM"),
        ({"label_token_ids": [10, 1000]}, {}, r"\[1000\] are outside"),
        (
            {"label_token_ids": [10], "item_first": True},
            {"engine_kwargs": {"enable_mis": True}, "tokenize_stage": False},
            "enable_mis",
        ),
        ({"label_token_ids": [10], "item_first": True}, {}, "tokenize stage"),
        (
            {"label_token_ids": [10]},
            {"engine_kwargs": {"apply_softmax": True}},
            r"Move \['apply_softmax'\]",
        ),
        (
            {"label_token_ids": [10], "embed_override_token_id": 9999},
            {"engine_kwargs": {"enable_mis": True}},
            "delimiter",
        ),
        (
            {"label_token_ids": [10], "score_extraction_token_id": 1000},
            {"engine_kwargs": {"enable_mis": True}},
            r"'score_extraction_token_id' \[1000\] are outside",
        ),
        (
            {"label_token_ids": [10], "score_extraction_token_id": 5},
            {},
            "disable_radix_cache=True and chunked_prefill_size=-1",
        ),
        (
            {"label_token_ids": [10], "score_extraction_token_id": 5},
            {"engine_kwargs": {"enable_mis": True, "allow_auto_truncate": True}},
            "allow_auto_truncate",
        ),
        (
            {
                "label_token_ids": [10],
                "score_extraction_token_id": 5,
                "item_first": True,
            },
            {
                "engine_kwargs": {
                    "disable_radix_cache": True,
                    "chunked_prefill_size": -1,
                },
                "tokenize_stage": False,
            },
            "'item_first' can't be used with score extraction",
        ),
        (
            {
                "label_token_ids": [10],
                "embed_override_token_id": 5,
                "score_extraction_token_id": 5,
            },
            {"engine_kwargs": {"enable_mis": True}},
            "both token ID 5",
        ),
    ],
)
def test_score_build_validation(hf_config, score_options, kwargs, match):
    with pytest.raises(ValueError, match=match):
        ProcessorBuilder.build(_score_config(score_options=score_options, **kwargs))


@pytest.mark.parametrize(
    "hf_config_value,score_options",
    [
        (SimpleNamespace(architectures=["BertForSequenceClassification"]), {}),
        (None, {}),
        (
            SimpleNamespace(architectures=["LlamaForCausalLM"], vocab_size=1000),
            {"label_token_ids": [10], "item_first": True},
        ),
    ],
    ids=["classifier_without_labels", "unknown_config", "item_first_token_ids"],
)
def test_score_build_accepts(hf_config_value, score_options):
    with patch("transformers.AutoConfig.from_pretrained", return_value=hf_config_value):
        processor = ProcessorBuilder.build(
            _score_config(score_options=score_options, tokenize_stage=False)
        )
    assert processor.list_stage_names() == ["SGLangEngineStage"]


def test_generate_stages_unchanged(hf_config):
    processor = ProcessorBuilder.build(
        SGLangEngineProcessorConfig(model_source=_SCORE_MODEL)
    )
    assert processor.list_stage_names() == [
        "ChatTemplateStage",
        "TokenizeStage",
        "SGLangEngineStage",
        "DetokenizeStage",
    ]
    engine = processor.get_stage_by_name("SGLangEngineStage")
    assert "task_options" not in engine.fn_constructor_kwargs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
