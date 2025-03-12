from typing import Optional

import pytest

from ray.llm._internal.serve.config_generator.utils.gpu import GPUType, ALL_GPU_TYPES
from ray.llm._internal.serve.config_generator.utils.input_converter import (
    convert_inputs_to_text_completion_model,
)
from ray.llm._internal.serve.config_generator.utils.models import (
    TextCompletionLoraModelConfig,
)

_MODEL_ID = "test-model-id"


class TestTextCompletionModelConverter:
    @pytest.mark.parametrize("hf_token", [None, "hf_bac"])
    @pytest.mark.parametrize("remote_storage_uri", [None, "s3://test-uri"])
    @pytest.mark.parametrize("gpu_type", ALL_GPU_TYPES)
    @pytest.mark.parametrize("tensor_parallelism", [1, 2, 8])
    @pytest.mark.parametrize(
        "lora_config",
        [None, TextCompletionLoraModelConfig(max_num_lora_per_replica=24)],
    )
    @pytest.mark.parametrize(
        "reference_model_id", [None, "meta-llama/Meta-Llama-3.1-8B-Instruct"]
    )
    def test_model(
        self,
        hf_token: Optional[str],
        remote_storage_uri: Optional[str],
        gpu_type: GPUType,
        tensor_parallelism: int,
        lora_config: Optional[TextCompletionLoraModelConfig],
        reference_model_id: Optional[str],
    ):
        model = convert_inputs_to_text_completion_model(
            model_id=_MODEL_ID,
            hf_token=hf_token,
            remote_storage_uri=remote_storage_uri,
            gpu_type=gpu_type,
            lora_config=lora_config,
            tensor_parallelism=tensor_parallelism,
            reference_model_id=reference_model_id,
        )

        assert model.id == _MODEL_ID
        assert model.hf_token == hf_token
        assert model.remote_storage_uri == remote_storage_uri
        assert model.gpu_type.value == gpu_type.value
        assert model.tensor_parallelism == tensor_parallelism
        assert model.reference_model_id == reference_model_id
