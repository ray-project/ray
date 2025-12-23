import sys
from copy import deepcopy

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.serving_patterns.data_parallel.dp_server import DPServer


class TestGetDeploymentOptions:
    @pytest.mark.parametrize(
        "data_parallel_size,num_replicas,expected_num_replicas",
        [
            # No DP: num_replicas is used as-is
            (None, 1, 1),
            (None, 2, 2),
            (None, 3, 3),
            (1, 1, 1),
            (1, 2, 2),
            (1, 3, 3),
            # DP with no num_replicas: defaults to single group
            (2, None, 2),
            (4, None, 4),
            (None, None, 1),
            # DP group replicas: num_replicas must be divisible by dp_size
            (2, 2, 2),  # 1 group of 2
            (2, 4, 4),  # 2 groups of 2
            (4, 8, 8),  # 2 groups of 4
            (8, 16, 16),  # 2 groups of 8
        ],
    )
    def test_dp_group_replicas_valid(
        self, data_parallel_size, num_replicas, expected_num_replicas
    ):
        """Test valid DP group replica configurations."""
        engine_kwargs = (
            {}
            if data_parallel_size is None
            else {"data_parallel_size": data_parallel_size}
        )
        deployment_config = (
            {} if num_replicas is None else {"num_replicas": num_replicas}
        )

        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=deepcopy(engine_kwargs),
            deployment_config=deepcopy(deployment_config),
        )
        deployment_options = DPServer.get_deployment_options(llm_config)

        actual_num_replicas = deployment_options.get("num_replicas", 1)
        assert actual_num_replicas == expected_num_replicas

    @pytest.mark.parametrize(
        "data_parallel_size,num_replicas",
        [
            (2, 3),  # 3 not divisible by 2
            (4, 2),  # 2 not divisible by 4
            (4, 6),  # 6 not divisible by 4
            (8, 12),  # 12 not divisible by 8
        ],
    )
    def test_dp_group_replicas_invalid_divisibility(
        self, data_parallel_size, num_replicas
    ):
        """Test that num_replicas must be divisible by data_parallel_size."""
        llm_config = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs={"data_parallel_size": data_parallel_size},
            deployment_config={"num_replicas": num_replicas},
        )
        with pytest.raises(ValueError, match="must be divisible by"):
            DPServer.get_deployment_options(llm_config)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
