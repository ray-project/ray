import sys

import pytest

from ray.llm._internal.serve.core.ingress import builder as builder_module
from ray.llm._internal.serve.core.ingress.applications import RouterApplication
from ray.llm._internal.serve.core.ingress.builder import build_openai_router_app
from ray.serve._private.build_app import build_app


@pytest.fixture(autouse=True)
def _direct_streaming(monkeypatch):
    monkeypatch.setattr(builder_module, "RAY_SERVE_ENABLE_HA_PROXY", True)
    monkeypatch.setattr(builder_module, "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", True)
    monkeypatch.setattr("ray.serve._private.build_app.RAY_SERVE_ENABLE_HA_PROXY", True)


def test_builds_marked_router_application():
    model_applications = {
        "model-a": "llm-model-a",
        "org/model-b": "llm-model-b",
    }

    app = build_openai_router_app(model_applications)

    assert app._bound_deployment.func_or_class is RouterApplication
    assert app._bound_deployment.init_kwargs == {
        "model_applications": model_applications
    }
    assert app._is_router_application
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
        build_openai_router_app(model_applications)


@pytest.mark.parametrize(
    "flag", ["RAY_SERVE_ENABLE_HA_PROXY", "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING"]
)
def test_requires_direct_streaming_haproxy(monkeypatch, flag):
    monkeypatch.setattr(builder_module, flag, False)

    with pytest.raises(ValueError, match=flag):
        build_openai_router_app({"model": "app"})


def test_public_builder():
    from ray.serve.llm import build_openai_router_app as public_builder

    app = public_builder({"model": "app"})
    assert app._bound_deployment.func_or_class is RouterApplication


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
