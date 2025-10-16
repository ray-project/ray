import sys

import pytest

from ray import serve
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.builder_llm_server import (
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
