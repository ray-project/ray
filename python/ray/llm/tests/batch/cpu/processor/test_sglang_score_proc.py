"""Score task tests for the SGLang processor. SGLang isn't needed."""

import sys
from types import SimpleNamespace
from unittest.mock import patch

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


def _score_config(task_kwargs=None, **kwargs):
    return SGLangEngineProcessorConfig(
        model_source=_SCORE_MODEL,
        task_type=SGLangTaskType.SCORE,
        task_kwargs={"label_token_ids": [10, 20]}
        if task_kwargs is None
        else task_kwargs,
        **kwargs,
    )


@pytest.mark.parametrize(
    "kwargs,expected_stages",
    [
        ({}, ["SGLangScoreTokenizeStage", "SGLangEngineStage"]),
        ({"tokenize_stage": False}, ["SGLangEngineStage"]),
        (
            {"chat_template_stage": False, "detokenize_stage": False},
            ["SGLangScoreTokenizeStage", "SGLangEngineStage"],
        ),
    ],
)
def test_score_stages(hf_config, kwargs, expected_stages):
    processor = ProcessorBuilder.build(_score_config(**kwargs))
    assert processor.list_stage_names() == expected_stages


@pytest.mark.parametrize("stage", ["chat_template_stage", "detokenize_stage"])
def test_score_rejects_unsupported_stage(hf_config, stage):
    with pytest.raises(ValueError, match=f"doesn't support {stage}"):
        ProcessorBuilder.build(_score_config(**{stage: True}))


def test_score_stage_kwargs(hf_config):
    processor = ProcessorBuilder.build(
        _score_config(
            task_kwargs={"label_token_ids": [10, 20], "apply_softmax": True},
            engine_kwargs={"enable_mis": True, "skip_tokenizer_init": False},
            tokenize_stage={"batch_size": 13, "model_source": "other-tokenizer"},
        )
    )

    tokenize = processor.get_stage_by_name("SGLangScoreTokenizeStage")
    assert tokenize.map_batches_kwargs["batch_size"] == 13
    assert tokenize.fn_constructor_kwargs["model"] == "other-tokenizer"
    assert tokenize.fn_constructor_kwargs["enable_mis"] is True
    assert tokenize.fn_constructor_kwargs["item_first"] is False
    assert tokenize.fn_constructor_kwargs["score_extraction_token_id"] is None

    engine = processor.get_stage_by_name("SGLangEngineStage")
    assert engine.fn_constructor_kwargs["task_kwargs"] == {
        "label_token_ids": [10, 20],
        "apply_softmax": True,
        "item_first": False,
        "embed_override_token_id": None,
        "score_extraction_token_id": None,
    }
    engine_kwargs = engine.fn_constructor_kwargs["engine_kwargs"]
    assert engine_kwargs["enable_mis"] is True
    assert engine_kwargs["skip_tokenizer_init"] is True


def test_score_config_round_trip(hf_config):
    config = _score_config(tokenize_stage=False)
    rebuilt = SGLangEngineProcessorConfig(**config.model_dump(exclude_unset=True))
    assert (
        ProcessorBuilder.build(rebuilt).list_stage_names()
        == ProcessorBuilder.build(config).list_stage_names()
    )


@pytest.mark.parametrize("add_special_tokens", [True, False])
def test_score_build_tokenize_stage_add_special_tokens(hf_config, add_special_tokens):
    from ray.llm._internal.batch.stages.configs import TokenizerStageConfig
    from ray.llm._internal.batch.tasks.sglang.score import _SGLangScoreTask

    config = _SGLangScoreTask.prepare_config(_score_config())
    stage = _SGLangScoreTask.build_tokenize_stage(
        config,
        TokenizerStageConfig(model_source=_SCORE_MODEL),
        add_special_tokens=add_special_tokens,
    )
    assert stage.fn_constructor_kwargs["add_special_tokens"] is add_special_tokens


@pytest.mark.parametrize(
    "task_kwargs,kwargs,match",
    [
        ({"label_token_ids": []}, {}, "must not be empty"),
        ({"label_token_ids": [True]}, {}, "label_token_ids"),
        ({"label_token_ids": [1], "typo": 1}, {}, "typo"),
    ],
)
def test_task_kwargs_validation(hf_config, task_kwargs, kwargs, match):
    with pytest.raises(ValueError, match=match):
        ProcessorBuilder.build(_score_config(task_kwargs=task_kwargs, **kwargs))


@pytest.mark.parametrize(
    "task_kwargs,kwargs,match",
    [
        ({}, {}, r"label_token_ids'\] is required for LlamaForCausalLM"),
        ({"label_token_ids": [10, 1000]}, {}, r"\[1000\] are outside"),
        (
            {"label_token_ids": [10], "embed_override_token_id": 1000},
            {},
            r"'embed_override_token_id' \[1000\] are outside",
        ),
        (
            {"label_token_ids": [10], "item_first": True},
            {"engine_kwargs": {"enable_mis": True}, "tokenize_stage": False},
            "enable_mis",
        ),
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
            "must differ",
        ),
    ],
)
def test_score_build_validation(hf_config, task_kwargs, kwargs, match):
    with pytest.raises(ValueError, match=match):
        ProcessorBuilder.build(_score_config(task_kwargs=task_kwargs, **kwargs))


@pytest.mark.parametrize(
    "hf_config_value,task_kwargs",
    [
        (SimpleNamespace(architectures=["BertForSequenceClassification"]), {}),
        (None, {}),
        (
            SimpleNamespace(architectures=["LlamaForCausalLM"], vocab_size=1000),
            {"label_token_ids": [10], "item_first": True},
        ),
    ],
    ids=["classifier_without_labels", "unknown_config", "item_first"],
)
def test_score_build_accepts(hf_config_value, task_kwargs, caplog):
    with patch("transformers.AutoConfig.from_pretrained", return_value=hf_config_value):
        processor = ProcessorBuilder.build(_score_config(task_kwargs=task_kwargs))
    assert processor.list_stage_names() == [
        "SGLangScoreTokenizeStage",
        "SGLangEngineStage",
    ]
    skipped = "aren't checked against the model" in caplog.text
    assert skipped is (hf_config_value is None)


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
    assert "task_kwargs" not in engine.fn_constructor_kwargs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
