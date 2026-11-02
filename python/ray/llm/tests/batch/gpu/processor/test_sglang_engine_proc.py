"""This test suite does not need sglang to be installed."""

import sys
from types import SimpleNamespace
from unittest.mock import patch

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


class TestEmbedTaskStages:
    """Embedding returns vectors, so the detokenize stage is dropped. Prompts
    go to SGLang as text unless the tokenize stage is requested explicitly."""

    @staticmethod
    def _patch_hf_config():
        return patch(
            "transformers.AutoConfig.from_pretrained",
            return_value=SimpleNamespace(architectures=["Qwen3ForCausalLM"]),
        )

    def _build(self, **kwargs):
        config = SGLangEngineProcessorConfig(
            model_source="Qwen/Qwen3-Embedding-0.6B",
            task_type=SGLangTaskType.EMBED,
            **kwargs,
        )
        with self._patch_hf_config():
            return ProcessorBuilder.build(config)

    def test_embed_defaults_to_text_input(self):
        processor = self._build()

        assert processor.list_stage_names() == ["SGLangEngineStage"]
        stage = processor.get_stage_by_name("SGLangEngineStage")
        assert stage.fn_constructor_kwargs["task_type"] == SGLangTaskType.EMBED
        engine_kwargs = stage.fn_constructor_kwargs["engine_kwargs"]
        assert engine_kwargs["task"] == SGLangTaskType.EMBED
        assert engine_kwargs["skip_tokenizer_init"] is False

    def test_embed_explicit_chat_template_stage_is_kept(self):
        processor = self._build(chat_template_stage=True)
        assert processor.list_stage_names() == [
            "ChatTemplateStage",
            "SGLangEngineStage",
        ]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"tokenize_stage": True},
            {"tokenize_stage": True, "engine_kwargs": {"skip_tokenizer_init": True}},
        ],
    )
    def test_embed_explicit_tokenize_stage_sends_token_ids(self, kwargs):
        processor = self._build(**kwargs)

        assert processor.list_stage_names() == ["TokenizeStage", "SGLangEngineStage"]
        stage = processor.get_stage_by_name("SGLangEngineStage")
        engine_kwargs = stage.fn_constructor_kwargs["engine_kwargs"]
        # Left unset or True; the engine stage defaults it to True.
        assert engine_kwargs.get("skip_tokenizer_init", True) is True

    @pytest.mark.parametrize(
        "kwargs,match",
        [
            ({"detokenize_stage": True}, "detokenize_stage cannot be used"),
            ({"detokenize_stage": {"enabled": True}}, "detokenize_stage cannot"),
            (
                {
                    "tokenize_stage": True,
                    "engine_kwargs": {"skip_tokenizer_init": False},
                },
                "requires engine_kwargs\\['skip_tokenizer_init'\\]=True",
            ),
            (
                {"engine_kwargs": {"skip_tokenizer_init": True}},
                "needs token IDs from the tokenize stage",
            ),
            (
                {
                    "tokenize_stage": False,
                    "engine_kwargs": {"skip_tokenizer_init": True},
                },
                "needs token IDs from the tokenize stage",
            ),
        ],
    )
    def test_embed_rejects_conflicting_settings(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            self._build(**kwargs)

    def test_embed_accepts_explicitly_disabled_stages(self):
        processor = self._build(tokenize_stage=False, detokenize_stage=False)
        assert processor.list_stage_names() == ["SGLangEngineStage"]

    def test_embed_does_not_mutate_caller_config(self):
        config = SGLangEngineProcessorConfig(
            model_source="Qwen/Qwen3-Embedding-0.6B",
            task_type=SGLangTaskType.EMBED,
        )
        with self._patch_hf_config():
            ProcessorBuilder.build(config)
        assert "skip_tokenizer_init" not in config.engine_kwargs
        assert config.chat_template_stage is True
        assert config.tokenize_stage is True
        assert config.detokenize_stage is True


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
