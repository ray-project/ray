"""Score task tests for the SGLang processor. SGLang isn't needed."""

import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.data.llm import SGLangEngineProcessorConfig
from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.tasks.sglang import score as score_task

_SCORE_MODEL = "Qwen/Qwen3-0.6B"


@pytest.fixture
def hf_config():
    """Patch the Hugging Face config the builder loads."""
    config = SimpleNamespace(architectures=["Qwen3ForCausalLM"], vocab_size=1000)
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
    ],
)
def test_score_stages(hf_config, kwargs, expected_stages):
    processor = ProcessorBuilder.build(_score_config(**kwargs))
    assert processor.list_stage_names() == expected_stages


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
            SimpleNamespace(architectures=["Qwen3ForCausalLM"], vocab_size=1000),
            {"label_token_ids": [10], "item_first": True},
        ),
    ],
    ids=["classifier_without_labels", "unknown_config", "item_first"],
)
def test_score_build_accepts(hf_config_value, task_kwargs):
    # Patch the logger rather than use caplog: the "ray" logger doesn't
    # propagate to the root logger, so caplog misses it on older pytest.
    with patch(
        "transformers.AutoConfig.from_pretrained", return_value=hf_config_value
    ), patch.object(score_task.logger, "warning") as warning:
        processor = ProcessorBuilder.build(_score_config(task_kwargs=task_kwargs))
    assert processor.list_stage_names() == [
        "SGLangScoreTokenizeStage",
        "SGLangEngineStage",
    ]
    skipped = any(
        "aren't checked against the model" in call.args[0]
        for call in warning.call_args_list
    )
    assert skipped is (hf_config_value is None)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
