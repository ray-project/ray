from typing import Any, Dict, Tuple
from unittest.mock import MagicMock, patch

import pytest
from vllm.entrypoints.openai.api_server import build_async_engine_client
from vllm.platforms.interface import DeviceCapability

from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import VLLMEngine
from ray.serve.llm import LLMConfig, ModelLoadingConfig


@pytest.mark.asyncio
async def test_vllm_engine_start_with_custom_resource_bundle(
    # defined in conftest.py
    model_smolvlm_256m,
):
    """vLLM engine starts with custom resource bundle."""
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id="smolvlm-256m",
            model_source=model_smolvlm_256m,
        ),
        engine_kwargs=dict(
            gpu_memory_utilization=0.4,
            use_tqdm_on_load=False,
            enforce_eager=True,
        ),
        resources_per_bundle=dict(GPU=0.49),
        runtime_env=dict(
            env_vars={
                "VLLM_RAY_PER_WORKER_GPUS": "0.49",
                "VLLM_DISABLE_COMPILE_CACHE": "1",
            },
        ),
    )

    engine = VLLMEngine(llm_config)
    await engine.start()
    await engine.check_health()
    engine.shutdown()


def deep_compare(dict1: Any, dict2: Any) -> bool:
    if type(dict1) is not type(dict2):
        return False
    if isinstance(dict1, dict):
        if dict1.keys() != dict2.keys():
            return False
        return all(deep_compare(dict1[k], dict2[k]) for k in dict1)
    elif isinstance(dict1, list):
        return set(dict1) == set(dict2)
    else:
        return dict1 == dict2


class TestVllmConfigCongruence:
    """Verify Ray Serve LLM and vllm serve CLI config are identical."""

    TEST_MODEL = "meta-llama/Llama-3.1-8B-Instruct"
    TEST_MAX_MODEL_LEN = 10500
    TEST_TENSOR_PARALLEL_SIZE = 1
    TEST_GPU_MEMORY_UTILIZATION = 0.95

    GPU_CONFIGS = [
        ("L4", DeviceCapability(major=8, minor=9)),  # Ada Lovelace architecture
        ("H100", DeviceCapability(major=9, minor=0)),  # Hopper architecture
        ("B200", DeviceCapability(major=10, minor=0)),  # Blackwell architecture
    ]

    EXPECTED_DIFF_FIELDS = {
        "instance_id",
    }

    async def _get_ray_serve_llm_vllm_config(self) -> Tuple[Any, str]:
        """Get VllmConfig by hooking into Ray Serve LLM's AsyncLLM instantiation."""
        captured_configs = []

        def mock_async_llm_class(vllm_config=None, **kwargs):
            captured_configs.append(("AsyncLLM", vllm_config))
            mock_obj = MagicMock()
            mock_obj._dummy_engine = True
            return mock_obj

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=self.TEST_MODEL,
                model_source=self.TEST_MODEL,
            ),
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 1,
                    "max_replicas": 1,
                },
                "max_ongoing_requests": 8192,
            },
            runtime_env={
                "env_vars": {
                    "VLLM_USE_V1": "1",
                },
            },
            engine_kwargs={
                "enable_chunked_prefill": True,
                "max_model_len": self.TEST_MAX_MODEL_LEN,
                "tensor_parallel_size": self.TEST_TENSOR_PARALLEL_SIZE,
                "gpu_memory_utilization": self.TEST_GPU_MEMORY_UTILIZATION,
            },
        )

        with patch(
            "vllm.v1.engine.async_llm.AsyncLLM", side_effect=mock_async_llm_class
        ):
            try:
                engine = VLLMEngine(llm_config)
                await engine.start()
            except Exception:
                # Expected since we're mocking the constructor
                pass

        if not captured_configs:
            raise RuntimeError("Failed to capture VllmConfig from Ray Serve LLM path")

        engine_type, vllm_config = captured_configs[-1]
        return vllm_config, engine_type

    async def _get_vllm_standalone_config(self) -> Tuple[Any, str]:
        """Get VllmConfig by hooking into vllm serve CLI's AsyncLLM instantiation."""
        captured_configs = []

        def mock_from_vllm_config(vllm_config=None, **kwargs):
            captured_configs.append(("AsyncLLM.from_vllm_config", vllm_config))
            mock_engine = MagicMock()

            async def dummy_reset():
                pass

            mock_engine.reset_mm_cache = MagicMock(return_value=dummy_reset())
            mock_engine.shutdown = MagicMock()
            return mock_engine

        # Create CLI args using vLLM's argument parser
        from vllm.entrypoints.openai.cli_args import make_arg_parser
        from vllm.utils import FlexibleArgumentParser

        parser = make_arg_parser(FlexibleArgumentParser())
        cli_args = parser.parse_args(
            [
                "--model",
                self.TEST_MODEL,
                "--enable-chunked-prefill",
                "--max-model-len",
                str(self.TEST_MAX_MODEL_LEN),
                "--tensor-parallel-size",
                str(self.TEST_TENSOR_PARALLEL_SIZE),
                "--gpu-memory-utilization",
                str(self.TEST_GPU_MEMORY_UTILIZATION),
                "--distributed-executor-backend",
                "ray",
                "--disable-log-requests",
            ]
        )

        with patch(
            "vllm.v1.engine.async_llm.AsyncLLM.from_vllm_config",
            side_effect=mock_from_vllm_config,
        ):
            try:
                async with build_async_engine_client(cli_args):
                    pass
            except Exception:
                # Expected since we're mocking the constructor
                pass

        for engine_type, config in captured_configs:
            if engine_type == "AsyncLLM.from_vllm_config":
                return config, engine_type

        raise RuntimeError("No valid VllmConfig found in captured configurations")

    @pytest.mark.parametrize("gpu_type,capability", GPU_CONFIGS)
    @pytest.mark.asyncio
    async def test_vllm_config_ray_serve_vs_cli_comparison(
        self, gpu_type: str, capability: DeviceCapability
    ):
        """Test VllmConfig consistency between Ray Serve LLM and vllm serve CLI.

        This test verifies that Ray Serve LLM and vllm serve CLI generate identical
        VllmConfig objects for the same model parameters across different GPU architectures.

        1. Ray Serve LLM: VLLMEngine.start() -> AsyncLLM(vllm_config=...)
        2. vllm serve CLI: build_async_engine_client() -> AsyncLLM.from_vllm_config(vllm_config=...)

        Args:
            gpu_type: GPU model name (L4, H100, B200)
            capability: DeviceCapability object with compute capability version
        """
        with patch(
            "vllm.platforms.cuda.NvmlCudaPlatform.get_device_capability",
            return_value=capability,
        ):
            ray_vllm_config, _ = await self._get_ray_serve_llm_vllm_config()
            cli_vllm_config, _ = await self._get_vllm_standalone_config()

            ray_config_dict = {
                k: v
                for k, v in vars(ray_vllm_config).items()
                if k not in self.EXPECTED_DIFF_FIELDS
            }
            cli_config_dict = {
                k: v
                for k, v in vars(cli_vllm_config).items()
                if k not in self.EXPECTED_DIFF_FIELDS
            }

            self._normalize_parallel_config(ray_config_dict)
            self._normalize_parallel_config(cli_config_dict)

            if not deep_compare(ray_config_dict, cli_config_dict):
                differences = self._get_config_differences(
                    ray_config_dict, cli_config_dict
                )
                diff_msg = "\n".join(differences)
                pytest.fail(
                    f"VllmConfig objects differ for {gpu_type} GPUs "
                    f"(compute capability {capability.major}.{capability.minor}):\n{diff_msg}"
                )

    def _normalize_parallel_config(self, config_dict: Dict[str, Any]) -> None:
        """Placement groups may differ, that's okay."""
        if "parallel_config" in config_dict:
            pc_dict = vars(config_dict["parallel_config"]).copy()
            pc_dict.pop("placement_group", None)
            config_dict["parallel_config"] = pc_dict

    def _get_config_differences(
        self, dict1: Dict[str, Any], dict2: Dict[str, Any]
    ) -> list[str]:
        differences = []
        for key in dict1.keys() | dict2.keys():
            if not deep_compare(dict1.get(key), dict2.get(key)):
                differences.append(
                    f"{key}: Ray={dict1.get(key)} vs CLI={dict2.get(key)}"
                )
        return differences


if __name__ == "__main__":
    pytest.main(["-vs", __file__])
