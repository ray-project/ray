"""Tests for SGLangDPServer deployment options.

Unit tests for Wide-EP gang scheduling configuration.
"""

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.configs.model_loading_config import (
    ModelLoadingConfig,
)
from ray.llm._internal.serve.serving_patterns.sglang.sglang_dp_server import (
    SGLangDPServer,
)


class TestSGLangDPServerDeploymentOptions:
    """Test SGLangDPServer.get_deployment_options() for Wide-EP."""

    def test_no_gang_scheduling_when_moe_dp_size_is_1(self):
        """moe_dp_size=1: no gang scheduling."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model",
                model_source="test-model",
            ),
            engine_kwargs={"moe_dp_size": 1},
        )
        options = SGLangDPServer.get_deployment_options(llm_config)
        assert "gang_scheduling_config" not in options

    def test_gang_scheduling_when_moe_dp_size_gt_1(self):
        """moe_dp_size>1: gang scheduling configured."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model",
                model_source="test-model",
            ),
            engine_kwargs={"moe_dp_size": 4},
        )
        options = SGLangDPServer.get_deployment_options(llm_config)
        assert "gang_scheduling_config" in options
        assert options["gang_scheduling_config"].gang_size == 4

    def test_num_replicas_multiplied(self):
        """num_replicas refers to gang count, multiplied by moe_dp_size."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model",
                model_source="test-model",
            ),
            deployment_config={"num_replicas": 2},
            engine_kwargs={"moe_dp_size": 3},
        )
        options = SGLangDPServer.get_deployment_options(llm_config)
        assert options["num_replicas"] == 6  # 2 gangs * 3

    def test_invalid_moe_dp_size(self):
        """Invalid moe_dp_size raises ValueError."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="test-model",
                model_source="test-model",
            ),
            engine_kwargs={"moe_dp_size": 0},
        )
        with pytest.raises(ValueError, match="Invalid moe_dp_size"):
            SGLangDPServer.get_deployment_options(llm_config)


if __name__ == "__main__":
    pytest.main(["-v", __file__])
