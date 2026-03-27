"""This test suite does not need sglang to be installed."""

import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray.data.llm import SGLangEngineProcessorConfig
from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder


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
        apply_chat_template=True,
        tokenize=True,
        detokenize=True,
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
    @pytest.mark.parametrize(
        "experimental_config",
        [
            {"max_tasks_in_flight_per_actor": 10},
            {},
        ],
    )
    def test_experimental_max_tasks_in_flight_per_actor_usage(
        self, experimental_config
    ):
        """Tests that max_tasks_in_flight_per_actor is set properly in the ActorPoolStrategy."""

        from ray.llm._internal.batch.processor.base import DEFAULT_MAX_TASKS_IN_FLIGHT
        from ray.llm._internal.batch.processor.sglang_engine_proc import (
            SGLangEngineProcessorConfig,
            build_sglang_engine_processor,
        )

        with patch("ray.data.ActorPoolStrategy") as mock_actor_pool:
            mock_actor_pool.return_value = MagicMock()

            config = SGLangEngineProcessorConfig(
                model_source="unsloth/Llama-3.2-1B-Instruct",
                experimental=experimental_config,
            )
            build_sglang_engine_processor(config)

            mock_actor_pool.assert_called()
            call_kwargs = mock_actor_pool.call_args[1]
            if experimental_config:
                assert (
                    call_kwargs["max_tasks_in_flight_per_actor"]
                    == experimental_config["max_tasks_in_flight_per_actor"]
                )
            else:
                assert (
                    call_kwargs["max_tasks_in_flight_per_actor"]
                    == DEFAULT_MAX_TASKS_IN_FLIGHT
                )


class TestSGLangTelemetryTrustRemoteCode:
    """Tests for SGLang telemetry initialization with trust_remote_code support.

    Covers the fix for https://github.com/ray-project/ray/issues/62075:
    when trust_remote_code=True is set in engine_kwargs, telemetry should
    still initialise correctly rather than raising an exception.
    """

    _MODULE = "ray.llm._internal.batch.processor.sglang_engine_proc"

    def _build_config(self, trust_remote_code: bool = False):
        return SGLangEngineProcessorConfig(
            model_source="unsloth/Llama-3.2-1B-Instruct",
            engine_kwargs={"trust_remote_code": trust_remote_code},
        )

    def test_download_model_files_called_with_exclude_safetensors_when_trust_remote_code(
        self,
    ):
        """download_model_files should use EXCLUDE_SAFETENSORS mode when
        trust_remote_code=True so that custom architecture .py files are
        available locally before AutoConfig.from_pretrained is called."""
        from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable

        with (
            patch(f"{self._MODULE}.download_model_files") as mock_download,
            patch(f"{self._MODULE}.transformers") as mock_transformers,
            patch(f"{self._MODULE}.get_or_create_telemetry_agent"),
        ):
            mock_download.return_value = "/tmp/fake_model"
            mock_hf_config = MagicMock()
            mock_hf_config.architectures = ["MiniMaxM2ForCausalLM"]
            mock_transformers.AutoConfig.from_pretrained.return_value = mock_hf_config

            config = self._build_config(trust_remote_code=True)
            from ray.llm._internal.batch.processor.sglang_engine_proc import (
                build_sglang_engine_processor,
            )

            build_sglang_engine_processor(config)

            mock_download.assert_called_once()
            call_kwargs = mock_download.call_args[1]
            assert (
                call_kwargs["download_model"]
                == NodeModelDownloadable.EXCLUDE_SAFETENSORS
            )

    def test_download_model_files_called_with_tokenizer_only_when_no_trust_remote_code(
        self,
    ):
        """download_model_files should use TOKENIZER_ONLY mode when
        trust_remote_code=False (the default)."""
        from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable

        with (
            patch(f"{self._MODULE}.download_model_files") as mock_download,
            patch(f"{self._MODULE}.transformers") as mock_transformers,
            patch(f"{self._MODULE}.get_or_create_telemetry_agent"),
        ):
            mock_download.return_value = "unsloth/Llama-3.2-1B-Instruct"
            mock_hf_config = MagicMock()
            mock_hf_config.architectures = ["LlamaForCausalLM"]
            mock_transformers.AutoConfig.from_pretrained.return_value = mock_hf_config

            config = self._build_config(trust_remote_code=False)
            from ray.llm._internal.batch.processor.sglang_engine_proc import (
                build_sglang_engine_processor,
            )

            build_sglang_engine_processor(config)

            mock_download.assert_called_once()
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs["download_model"] == NodeModelDownloadable.TOKENIZER_ONLY

    def test_trust_remote_code_passed_to_autoconfig(self):
        """trust_remote_code from engine_kwargs must be forwarded to
        AutoConfig.from_pretrained so that custom tokeniser/config code runs."""
        with (
            patch(f"{self._MODULE}.download_model_files") as mock_download,
            patch(f"{self._MODULE}.transformers") as mock_transformers,
            patch(f"{self._MODULE}.get_or_create_telemetry_agent"),
        ):
            fake_path = "/tmp/fake_trust_model"
            mock_download.return_value = fake_path
            mock_hf_config = MagicMock()
            mock_hf_config.architectures = ["CustomArch"]
            mock_transformers.AutoConfig.from_pretrained.return_value = mock_hf_config

            config = self._build_config(trust_remote_code=True)
            from ray.llm._internal.batch.processor.sglang_engine_proc import (
                build_sglang_engine_processor,
            )

            build_sglang_engine_processor(config)

            mock_transformers.AutoConfig.from_pretrained.assert_called_once_with(
                fake_path,
                trust_remote_code=True,
            )

    def test_telemetry_falls_back_to_default_architecture_on_autoconfig_error(
        self,
    ):
        """If AutoConfig.from_pretrained raises (e.g. unsupported model), the
        processor must still be built successfully and telemetry must record
        DEFAULT_MODEL_ARCHITECTURE instead of propagating the exception."""
        from ray.llm._internal.batch.processor.sglang_engine_proc import (
            DEFAULT_MODEL_ARCHITECTURE,
            build_sglang_engine_processor,
        )

        with (
            patch(f"{self._MODULE}.download_model_files") as mock_download,
            patch(f"{self._MODULE}.transformers") as mock_transformers,
            patch(f"{self._MODULE}.get_or_create_telemetry_agent") as mock_telemetry,
        ):
            mock_download.return_value = "/tmp/unsupported_model"
            mock_transformers.AutoConfig.from_pretrained.side_effect = OSError(
                "model config not found"
            )
            mock_agent = MagicMock()
            mock_telemetry.return_value = mock_agent

            config = self._build_config(trust_remote_code=True)
            # Should not raise.
            build_sglang_engine_processor(config)

            mock_agent.push_telemetry_report.assert_called_once()
            report = mock_agent.push_telemetry_report.call_args[0][0]
            assert report.model_architecture == DEFAULT_MODEL_ARCHITECTURE


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
