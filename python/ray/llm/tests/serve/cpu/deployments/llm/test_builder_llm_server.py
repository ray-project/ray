import sys

import pytest

from ray import serve
from ray.llm._internal.serve.constants import DEFAULT_MAX_TARGET_ONGOING_REQUESTS
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.server.builder import (
    build_llm_deployment,
)


class TestBuildVllmDeployment:
    def test_build_llm_deployment(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        """Test `build_llm_deployment` can build a vLLM deployment."""

        app = build_llm_deployment(llm_config_with_mock_engine)
        assert isinstance(app, serve.Application)
        handle = serve.run(app)
        assert handle.deployment_name.startswith("LLMServer")

    def test_build_llm_deployment_with_name_prefix(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        """Test `build_llm_deployment` can build a vLLM deployment with name prefix."""

        _name_prefix_for_test = "test_name_prefix"
        app = build_llm_deployment(
            llm_config_with_mock_engine, name_prefix=_name_prefix_for_test
        )
        assert isinstance(app, serve.Application)
        handle = serve.run(app)
        assert handle.deployment_name.startswith(_name_prefix_for_test)

    def test_build_llm_deployment_name_prefix_along_with_deployment_config(
        self,
        llm_config_with_mock_engine,
        shutdown_ray_and_serve,
        disable_placement_bundles,
    ):
        """Test `build_llm_deployment` can build a vLLM deployment with name prefix and deployment config."""

        config_with_name: LLMConfig = llm_config_with_mock_engine.model_copy(deep=True)
        _deployment_name = "deployment_name_from_config"
        _name_prefix_for_test = "test_name_prefix"
        config_with_name.deployment_config["name"] = _deployment_name
        app = build_llm_deployment(config_with_name, name_prefix=_name_prefix_for_test)
        assert isinstance(app, serve.Application)
        handle = serve.run(app)
        assert handle.deployment_name == _name_prefix_for_test + _deployment_name

    def test_default_autoscaling_config_included_without_num_replicas(
        self, disable_placement_bundles
    ):
        """Test that default autoscaling_config with target_ongoing_requests is included
        when num_replicas is not specified.
        """
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
        )
        app = build_llm_deployment(llm_config)

        deployment = app._bound_deployment
        autoscaling_config = deployment._deployment_config.autoscaling_config
        assert autoscaling_config is not None
        assert (
            autoscaling_config.target_ongoing_requests
            == DEFAULT_MAX_TARGET_ONGOING_REQUESTS
        )

    def test_autoscaling_config_removed_from_defaults_when_num_replicas_specified(
        self, disable_placement_bundles
    ):
        """Test that autoscaling_config from defaults is removed when user specifies
        num_replicas, since Ray Serve does not allow both.
        """
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "num_replicas": 2,
            },
        )
        app = build_llm_deployment(llm_config)

        deployment = app._bound_deployment
        assert deployment._deployment_config.num_replicas == 2
        # autoscaling_config should be None since num_replicas is set
        assert deployment._deployment_config.autoscaling_config is None

    def test_user_target_ongoing_requests_respected(self, disable_placement_bundles):
        """Test that user-specified target_ongoing_requests is respected and not
        overridden by defaults.
        """
        user_target = 50
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "autoscaling_config": {
                    "target_ongoing_requests": user_target,
                },
            },
        )
        app = build_llm_deployment(llm_config)

        deployment = app._bound_deployment
        autoscaling_config = deployment._deployment_config.autoscaling_config
        assert autoscaling_config is not None
        assert autoscaling_config.target_ongoing_requests == user_target


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
