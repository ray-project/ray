"""This test suite does not need sglang to be installed."""

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data.llm import SGLangEngineProcessorConfig
from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder
from ray.llm._internal.batch.processor.sglang_engine_proc import (
    build_sglang_engine_processor,
)


def test_sglang_engine_processor(gpu_type, model_llama_3_2_216M):
    config = SGLangEngineProcessorConfig(
        model_source=model_llama_3_2_216M,
        engine_kwargs=dict(
            context_length=8192,
            tp_size=2,
            dp_size=2,
            disable_cuda_graph=True,
            dtype="half",  # Older GPUs (e.g. T4) don't support bfloat16
        ),
        runtime_env=dict(
            env_vars=dict(
                RANDOM_ENV_VAR="12345",
            ),
        ),
        accelerator_type=gpu_type,
        concurrency=4,
        batch_size=64,
        max_concurrent_batches=4,
        max_pending_requests=111,
        chat_template_stage=True,
        tokenize_stage=True,
        detokenize_stage=True,
    )
    processor = ProcessorBuilder.build(config)
    assert processor.list_stage_names() == [
        "ChatTemplateStage",
        "TokenizeStage",
        "SGLangEngineStage",
        "DetokenizeStage",
    ]

    stage = processor.get_stage_by_name("SGLangEngineStage")
    assert stage.fn_constructor_kwargs == {
        "model": model_llama_3_2_216M,
        "engine_kwargs": {
            "context_length": 8192,
            "tp_size": 2,
            "dp_size": 2,
            "disable_cuda_graph": True,
            "dtype": "half",
            "task": SGLangTaskType.GENERATE,
        },
        "task_type": SGLangTaskType.GENERATE,
        "max_pending_requests": 111,
    }

    runtime_env = stage.map_batches_kwargs.pop("runtime_env")
    assert "env_vars" in runtime_env
    assert runtime_env["env_vars"]["RANDOM_ENV_VAR"] == "12345"
    compute = stage.map_batches_kwargs.pop("compute")
    assert isinstance(compute, ray.data._internal.compute.ActorPoolStrategy)
    assert stage.map_batches_kwargs == {
        "zero_copy_batch": True,
        "max_concurrency": 4,
        "accelerator_type": gpu_type,
        "num_gpus": 4,  # Based on tp_size=2, dp_size=2 in engine_kwargs
    }


class TestSGLangEngineProcessorConfig:
    def test_build_processor_autoconfig_failure_with_trust_remote_code(self):
        config = SGLangEngineProcessorConfig(
            model_source="nonexistent-org/nonexistent-model",
            engine_kwargs={"trust_remote_code": True},
        )

        processor = build_sglang_engine_processor(config)
        assert processor is not None

    def test_build_processor_import_error_with_trust_remote_code(self):
        config = SGLangEngineProcessorConfig(
            model_source="org/model-with-custom-code",
            engine_kwargs={"trust_remote_code": True},
        )

        with (
            patch(
                "ray.llm._internal.common.utils.download_utils.download_model_files",
                return_value="/tmp/fake_model_dir",
            ),
            patch(
                "transformers.AutoConfig.from_pretrained",
                side_effect=ModuleNotFoundError("custom modeling module missing"),
            ),
        ):
            processor = build_sglang_engine_processor(config)

        assert processor is not None

    def test_build_processor_download_error_with_trust_remote_code(self):
        config = SGLangEngineProcessorConfig(
            model_source="org/model-with-custom-code",
            engine_kwargs={"trust_remote_code": True},
        )

        with patch(
            "ray.llm._internal.common.utils.download_utils.download_model_files",
            side_effect=RuntimeError("download failed"),
        ):
            processor = build_sglang_engine_processor(config)

        assert processor is not None


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
        "ray.llm._internal.batch.stages.sglang_score_tokenize_stage.load_tokenizer",
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
    assert engine.fn_constructor_kwargs["score_options"] == {
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
    assert (
        engine.fn_constructor_kwargs["score_options"]["embed_override_token_id"] == 77
    )
    assert "embed_override_token" not in engine.fn_constructor_kwargs["score_options"]


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
    assert "score_options" not in engine.fn_constructor_kwargs


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
