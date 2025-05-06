import pytest
import sys

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter,
)


def test_router_with_num_router_replicas_config():
    """Test the router with num_router_replicas config."""
    # Test with no num_router_replicas config.
    llm_configs = [
        LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )
    ]
    llm_router_deployment = LLMRouter.as_deployment(llm_configs=llm_configs)
    autoscaling_config = llm_router_deployment._deployment_config.autoscaling_config
    assert autoscaling_config.min_replicas == 2
    assert autoscaling_config.initial_replicas == 2
    assert autoscaling_config.max_replicas == 2

    # Test with num_router_replicas config on multiple llm configs.
    llm_configs = [
        LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "num_router_replicas": 3,
            },
        ),
        LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs={
                "num_router_replicas": 5,
            },
        ),
    ]
    llm_router_deployment = LLMRouter.as_deployment(llm_configs=llm_configs)
    autoscaling_config = llm_router_deployment._deployment_config.autoscaling_config
    assert autoscaling_config.min_replicas == 5
    assert autoscaling_config.initial_replicas == 5
    assert autoscaling_config.max_replicas == 5


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
