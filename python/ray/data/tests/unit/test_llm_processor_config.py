import subprocess
import sys
import textwrap


def test_vllm_engine_processor_cpu_only_config():
    script = textwrap.dedent(
        """
        import sys
        import types
        from types import SimpleNamespace

        fake_transformers = types.ModuleType("transformers")

        class FakeAutoConfig:
            @staticmethod
            def from_pretrained(*args, **kwargs):
                return SimpleNamespace(architectures=["OPTForCausalLM"])

        fake_transformers.AutoConfig = FakeAutoConfig
        sys.modules["transformers"] = fake_transformers

        import ray

        ray.init = lambda *args, **kwargs: None

        import ray.llm._internal.batch.processor.vllm_engine_proc as vllm_engine_proc
        from ray.data.llm import vLLMEngineProcessorConfig
        from ray.llm._internal.batch.constants import vLLMTaskType
        from ray.llm._internal.batch.processor import ProcessorBuilder
        from ray.llm._internal.batch.stages.configs import (
            ChatTemplateStageConfig,
            TokenizerStageConfig,
        )

        class DummyTelemetryAgent:
            def push_telemetry_report(self, telemetry):
                pass

        vllm_engine_proc.download_model_files = lambda **kwargs: "/tmp/fake-model"
        vllm_engine_proc.get_or_create_telemetry_agent = lambda: DummyTelemetryAgent()

        config = vLLMEngineProcessorConfig(
            model_source="/tmp/fake-model",
            engine_kwargs={
                "distributed_executor_backend": "mp",
                "enforce_eager": True,
                "max_model_len": 2048,
            },
            concurrency=1,
            batch_size=32,
            chat_template_stage=ChatTemplateStageConfig(enabled=True),
            tokenize_stage=TokenizerStageConfig(enabled=True),
            placement_group_config={"bundles": [{"CPU": 4, "GPU": 0}]},
        )
        processor = ProcessorBuilder.build(config)
        stage = processor.get_stage_by_name("vLLMEngineStage")

        assert stage.fn_constructor_kwargs["engine_kwargs"] == {
            "distributed_executor_backend": "mp",
            "enforce_eager": True,
            "max_model_len": 2048,
            "task_type": vLLMTaskType.GENERATE,
        }

        stage.map_batches_kwargs.pop("runtime_env")
        stage.map_batches_kwargs.pop("compute")

        assert "ray_remote_args_fn" not in stage.map_batches_kwargs
        assert stage.map_batches_kwargs == {
            "zero_copy_batch": True,
            "max_concurrency": 8,
            "accelerator_type": None,
            "num_cpus": 4.0,
        }
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
