import sys

import pytest

from ray._common.utils import import_attr
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress import builder as builder_module
from ray.llm._internal.serve.core.ingress.builder import build_openai_router_app
from ray.llm._internal.serve.core.ingress.router_application import RouterApplication
from ray.serve._private.api import call_user_app_builder_with_args_if_necessary
from ray.serve._private.build_app import build_app


@pytest.fixture(autouse=True)
def _direct_streaming(monkeypatch):
    monkeypatch.setattr(builder_module, "RAY_SERVE_ENABLE_HA_PROXY", True)
    monkeypatch.setattr(builder_module, "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", True)
    monkeypatch.setattr("ray.serve._private.build_app.RAY_SERVE_ENABLE_HA_PROXY", True)


def _llm_configs(*model_ids):
    return [
        LLMConfig(model_loading_config=ModelLoadingConfig(model_id=model_id))
        for model_id in model_ids
    ]


def test_builds_marked_router_application():
    model_applications = {
        "model-a": "llm-model-a",
        "org/model-b": "llm-model-b",
    }

    llm_configs = _llm_configs(*model_applications)
    app = build_openai_router_app(
        {"model_applications": model_applications, "llm_configs": llm_configs}
    )

    assert app._bound_deployment.func_or_class is RouterApplication
    assert app._bound_deployment.init_kwargs == {
        "model_applications": model_applications,
        "llm_configs": llm_configs,
    }
    assert app._ingress_request_router is None
    built = build_app(app, name="main", route_prefix="/")
    assert built.is_router_application


@pytest.mark.parametrize(
    "model_applications, error, match",
    [
        ({}, ValueError, "at least one"),
        ({"": "app"}, ValueError, "Model IDs"),
        ({1: "app"}, ValueError, "Model IDs"),
        ({"model": ""}, ValueError, "Application names"),
        ({"model": 1}, ValueError, "Application names"),
        ({"a": "same", "b": "same"}, ValueError, "different"),
        (["not", "a", "mapping"], TypeError, "must be a mapping"),
    ],
)
def test_validation(model_applications, error, match):
    with pytest.raises(error, match=match):
        build_openai_router_app(
            {
                "model_applications": model_applications,
                "llm_configs": _llm_configs("model-a"),
            }
        )


@pytest.mark.parametrize("router_args", [{}, {"unexpected": "value"}])
def test_requires_model_applications(router_args):
    with pytest.raises(ValueError, match="model_applications"):
        build_openai_router_app(router_args)


@pytest.mark.parametrize(
    "flag", ["RAY_SERVE_ENABLE_HA_PROXY", "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING"]
)
def test_requires_direct_streaming_haproxy(monkeypatch, flag):
    monkeypatch.setattr(builder_module, flag, False)

    with pytest.raises(ValueError, match=flag):
        build_openai_router_app(
            {
                "model_applications": {"model": "app"},
                "llm_configs": _llm_configs("model"),
            }
        )


def test_requires_matching_llm_configs():
    with pytest.raises(ValueError, match="exactly one config"):
        build_openai_router_app(
            {
                "model_applications": {"model-a": "app-a"},
                "llm_configs": _llm_configs("model-b"),
            }
        )


def test_rejects_lora_configs():
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="model-a"),
        lora_config={},
    )

    with pytest.raises(ValueError, match="does not support LoRA"):
        build_openai_router_app(
            {
                "model_applications": {"model-a": "app-a"},
                "llm_configs": [llm_config],
            }
        )


def test_builder_accepts_declarative_yaml_args():
    builder = import_attr(
        "ray.llm._internal.serve.core.ingress.builder:build_openai_router_app"
    )
    app = call_user_app_builder_with_args_if_necessary(
        builder,
        {
            "model_applications": {"model-a": "llm-model-a"},
            "llm_configs": [{"model_loading_config": {"model_id": "model-a"}}],
        },
    )

    assert build_app(app, name="main", route_prefix="/").is_router_application
    assert app._bound_deployment.init_kwargs == {
        "model_applications": {"model-a": "llm-model-a"},
        "llm_configs": _llm_configs("model-a"),
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
