from typing import Any, Dict, Optional

import pytest

from ray.llm._internal.serve.config_generator.generator import (
    get_serve_config,
)
from ray.llm._internal.serve.config_generator.utils.gpu import (
    GPUType,
    DEFAULT_MODEL_ID_TO_GPU,
    ALL_GPU_TYPES,
)
from ray.llm._internal.serve.config_generator.utils.models import (
    TextCompletionLoraModelConfig,
    TextCompletionModelConfig,
)
from ray.llm._internal.serve.config_generator.utils.text_completion import (
    populate_text_completion_model_config,
)


class TestTextCompletionModelConfig:
    @pytest.mark.parametrize("model_id", DEFAULT_MODEL_ID_TO_GPU.keys())
    @pytest.mark.parametrize("gpu_type", ALL_GPU_TYPES)
    def test_populate_default_models(
        self,
        model_id: str,
        gpu_type: GPUType,
    ):
        """
        We know a list of (model_id, gpu_type) combos that are not supported. So we skip testing them.
        """
        skipped_cases = {
            ("meta-llama/Meta-Llama-3.1-70B-Instruct", GPUType.NVIDIA_TESLA_A10G),
            ("meta-llama/Meta-Llama-3.1-70B-Instruct", GPUType.NVIDIA_L4),
            ("meta-llama/Meta-Llama-3.1-70B-Instruct", GPUType.NVIDIA_L40S),
            ("meta-llama/Meta-Llama-3.1-405B-Instruct-FP8", GPUType.NVIDIA_TESLA_A10G),
            ("meta-llama/Meta-Llama-3.1-405B-Instruct-FP8", GPUType.NVIDIA_L4),
            ("meta-llama/Meta-Llama-3.1-405B-Instruct-FP8", GPUType.NVIDIA_L40S),
            ("meta-llama/Meta-Llama-3.1-405B-Instruct-FP8", GPUType.NVIDIA_A100_40G),
            ("meta-llama/Meta-Llama-3.1-405B-Instruct-FP8", GPUType.NVIDIA_A100_80G),
            ("mistral-community/pixtral-12b", GPUType.NVIDIA_TESLA_A10G),
            ("mistral-community/pixtral-12b", GPUType.NVIDIA_L4),
            ("meta-llama/Llama-3.2-11B-Vision-Instruct", GPUType.NVIDIA_TESLA_A10G),
            ("meta-llama/Llama-3.2-11B-Vision-Instruct", GPUType.NVIDIA_L4),
            ("meta-llama/Llama-3.2-90B-Vision-Instruct", GPUType.NVIDIA_TESLA_A10G),
            ("meta-llama/Llama-3.2-90B-Vision-Instruct", GPUType.NVIDIA_L4),
            ("meta-llama/Llama-3.2-90B-Vision-Instruct", GPUType.NVIDIA_L40S),
        }
        if (model_id, gpu_type) in skipped_cases:
            return

        input_model_config = TextCompletionModelConfig(
            id=model_id,
            gpu_type=gpu_type,
            tensor_parallelism=2,
            is_json_mode_enabled=False,
        )
        model_config = populate_text_completion_model_config(input_model_config)
        self._assert_models(model_config, input_model_config)

    @pytest.mark.parametrize("gpu_type", ALL_GPU_TYPES)
    @pytest.mark.parametrize("tensor_parallelism", [1, 2, 8])
    @pytest.mark.parametrize("hf_token", [None, "hf_abc"])
    @pytest.mark.parametrize("max_num_lora_per_replica", [None, 24])
    @pytest.mark.parametrize("enable_json_mode", [False, True])
    @pytest.mark.parametrize(
        "remote_storage_uri",
        [None, "s3://my_fake/bucket/path", "gs://my_other/fake/bucket/path"],
    )
    def test_populate_custom_model(
        self,
        gpu_type: GPUType,
        tensor_parallelism: int,
        hf_token: Optional[str],
        max_num_lora_per_replica: Optional[int],
        enable_json_mode: bool,
        remote_storage_uri: Optional[str],
    ):
        """
        It tests that custom model id can be used to construct the final serve config.
        """
        model_id = "abc"
        lora_config = (
            TextCompletionLoraModelConfig(
                max_num_lora_per_replica=max_num_lora_per_replica
            )
            if max_num_lora_per_replica
            else None
        )
        input_model_config = TextCompletionModelConfig(
            id=model_id,
            gpu_type=gpu_type,
            tensor_parallelism=tensor_parallelism,
            reference_model_id="mistralai/Mistral-7B-Instruct-v0.1",
            hf_token=hf_token,
            lora_config=lora_config,
            is_json_mode_enabled=enable_json_mode,
            remote_storage_uri=remote_storage_uri,
        )
        model_config = populate_text_completion_model_config(input_model_config)
        self._assert_models(model_config, input_model_config)

        serve_config = get_serve_config(input_model_config, "./file.yaml")
        assert len(serve_config["applications"][0]["args"]["llm_configs"]) == 1

    def _assert_models(
        self,
        model_config: Dict[str, Any],
        input_model_config: TextCompletionModelConfig,
    ):
        accelerator_type = input_model_config.gpu_type.value

        assert model_config["model_loading_config"]["model_id"] == input_model_config.id

        if input_model_config.remote_storage_uri:
            assert model_config["model_loading_config"]["model_source"] == {
                "bucket_uri": input_model_config.remote_storage_uri
            }
        else:
            assert (
                model_config["model_loading_config"]["model_source"]
                == input_model_config.id
            )

        assert model_config["accelerator_type"] == accelerator_type

        assert (
            model_config["engine_kwargs"]["tensor_parallel_size"]
            == input_model_config.tensor_parallelism
        )
        assert (
            model_config.get("runtime_env", {})
            .get("env_vars", {})
            .get("HF_TOKEN", None)
            == input_model_config.hf_token
        )
