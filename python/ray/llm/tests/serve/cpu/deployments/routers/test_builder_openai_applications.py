import sys
from typing import List

import pytest

from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.core.ingress import builder as builder_module
from ray.llm._internal.serve.core.ingress.applications import (
    ApplicationDescriptor,
    ControlApplication,
    ModelApplication,
    RouterApplication,
)
from ray.llm._internal.serve.core.ingress.builder import (
    IngressClsConfig,
    build_openai_app,
    build_openai_applications,
)
from ray.llm._internal.serve.core.ingress.ingress import OpenAiIngress
from ray.serve._private.build_app import build_app
from ray.serve.api import RunTarget
from ray.serve.llm.request_router import KVAwareRouter


@pytest.fixture(autouse=True)
def _direct_streaming(monkeypatch):
    monkeypatch.setattr(builder_module, "RAY_SERVE_ENABLE_HA_PROXY", True)
    monkeypatch.setattr(builder_module, "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING", True)
    monkeypatch.setattr("ray.serve._private.build_app.RAY_SERVE_ENABLE_HA_PROXY", True)


def _config(model_id: str, **kwargs) -> LLMConfig:
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_id, model_source="test-source"
        ),
        **kwargs,
    )


def _build(model_ids: List[str], **kwargs) -> List[RunTarget]:
    return build_openai_applications(
        {"llm_configs": [_config(m) for m in model_ids]}, **kwargs
    )


def _model_name(model_id: str, name: str = "llm") -> str:
    return builder_module._model_application_name(name, model_id)


class TestTopology:
    def test_n_models_make_n_plus_2_targets(self):
        targets = _build(["model-a", "org/model-b"])

        assert [(t.name, t.route_prefix) for t in targets] == [
            (_model_name("model-a"), "/v1/model-a"),
            (_model_name("org/model-b"), "/v1/org--model-b"),
            ("llm-control", "/v1/control"),
            ("llm", "/"),
        ]
        assert _model_name("org/model-b").startswith("llm-model-org--model-b-")

    def test_model_applications_keep_single_model_direct_streaming(self):
        """Direct model routes still go through the model's LLMRouter."""
        model_app = _build(["model-a", "model-b"])[0].target
        single = build_openai_app({"llm_configs": [_config("model-a")]})

        assert model_app._bound_deployment.name == single._bound_deployment.name
        assert model_app._ingress_request_router._bound_deployment.name == "LLMRouter"
        assert not model_app._is_router_application

    def test_control_application(self):
        control = _build(["model-a", "org/model-b"])[2].target

        assert control._bound_deployment.func_or_class is ControlApplication
        assert control._bound_deployment.name == "ControlApplication"
        assert control._ingress_request_router is None
        assert not control._is_router_application
        cards = control._bound_deployment.init_kwargs["model_cards"]
        assert [card["id"] for card in cards.values()] == ["model-a", "org/model-b"]

    def test_router_application(self):
        targets = _build(["model-a", "org/model-b"])
        router = targets[-1].target

        assert router._bound_deployment.func_or_class is RouterApplication
        assert router._is_router_application
        assert router._ingress_request_router is None
        kwargs = router._bound_deployment.init_kwargs
        assert kwargs["model_applications"] == [
            ModelApplication(
                application_name=target.name,
                ingress_deployment_name=target.target._bound_deployment.name,
                model_id=model_id,
            )
            for model_id, target in zip(["model-a", "org/model-b"], targets)
        ]
        assert kwargs["control_application"] == ApplicationDescriptor(
            application_name=targets[2].name,
            ingress_deployment_name=targets[2].target._bound_deployment.name,
        )

    def test_targets_build(self):
        built = {
            t.name: build_app(t.target, name=t.name, route_prefix=t.route_prefix)
            for t in _build(["model-a"])
        }

        assert built["llm"].is_router_application
        assert built["llm"].ingress_request_router_deployment is None
        assert built["llm-control"].ingress_request_router_deployment is None
        model = built[_model_name("model-a")]
        assert not model.is_router_application
        assert model.ingress_request_router_deployment.name == "LLMRouter"

    @pytest.mark.parametrize(
        "route_prefix, expected",
        [
            ("/", ["/v1/model-a", "/v1/control", "/"]),
            ("/llm", ["/llm/v1/model-a", "/llm/v1/control", "/llm"]),
        ],
    )
    def test_route_prefix(self, route_prefix, expected):
        targets = _build(["model-a"], route_prefix=route_prefix)

        assert [t.route_prefix for t in targets] == expected

    def test_names_are_stable_and_order_independent(self):
        forward = {t.route_prefix: t.name for t in _build(["a", "b", "c"])}
        backward = {t.route_prefix: t.name for t in _build(["c", "b", "a"])}

        assert forward == backward
        assert [t.name for t in _build(["a"], name="group")][1:] == [
            "group-control",
            "group",
        ]
        assert _build(["a"], name="group")[0].name.startswith("group-model-a-")

    def test_names_distinguish_ids_that_sanitize_alike(self):
        names = {t.name for t in _build(["org/model", "org.model"])}
        assert len(names) == 4

    def test_router_and_control_scale_to_zero_with_all_models(self):
        def configs(min_replicas):
            return [
                _config(
                    m,
                    deployment_config={
                        "autoscaling_config": {"min_replicas": r, "max_replicas": 2}
                    },
                )
                for m, r in zip(["a", "b"], min_replicas)
            ]

        def min_replicas(llm_configs):
            targets = build_openai_applications({"llm_configs": llm_configs})
            return [
                t.target._bound_deployment._deployment_config.autoscaling_config.min_replicas
                for t in targets[-2:]
            ]

        assert min_replicas(configs([0, 0])) == [0, 0]
        assert 0 not in min_replicas(configs([0, 1]))


class TestValidation:
    @pytest.mark.parametrize(
        "flag", ["RAY_SERVE_ENABLE_HA_PROXY", "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING"]
    )
    def test_requires_flag(self, monkeypatch, flag):
        monkeypatch.setattr(builder_module, flag, False)
        with pytest.raises(ValueError, match=flag):
            _build(["model-a"])

    def test_rejects_duplicate_model_ids(self):
        with pytest.raises(ValueError, match="Duplicate models"):
            _build(["model-a", "model-a"])

    def test_rejects_route_collisions(self):
        with pytest.raises(ValueError, match="both route at /v1/org--model"):
            _build(["org/model", "org--model"])

    @pytest.mark.parametrize("model_id", ["models", "chat", "control"])
    def test_rejects_reserved_segments(self, model_id):
        with pytest.raises(ValueError, match="reserved"):
            _build([model_id])

    @pytest.mark.parametrize("model_id", ["a b", "a%2Fb", "a?b", "a:b", "a#b"])
    def test_rejects_ids_that_cannot_be_routed(self, model_id):
        with pytest.raises(ValueError, match="cannot be used in a route"):
            _build([model_id])

    def test_rejects_invalid_route_prefix(self):
        with pytest.raises(ValueError):
            _build(["model-a"], route_prefix="llm")

    def test_rejects_custom_ingress_cls(self):
        class CustomIngress(OpenAiIngress):
            pass

        with pytest.raises(ValueError, match="ingress_cls_config"):
            build_openai_applications(
                {
                    "llm_configs": [_config("model-a")],
                    "ingress_cls_config": IngressClsConfig(ingress_cls=CustomIngress),
                }
            )

    def test_rejects_ingress_deployment_config(self):
        with pytest.raises(ValueError, match="ingress_deployment_config"):
            build_openai_applications(
                {
                    "llm_configs": [_config("model-a")],
                    "ingress_deployment_config": {"num_replicas": 2},
                }
            )

    def test_rejects_kv_aware_routing(self):
        config = _config(
            "model-a",
            deployment_config={
                "request_router_config": {"request_router_class": KVAwareRouter}
            },
        )
        with pytest.raises(ValueError, match="KV-aware"):
            build_openai_applications({"llm_configs": [config]})

    def test_rejects_lora(self):
        config = _config(
            "model-a", lora_config=LoraConfig(dynamic_lora_loading_path="s3://x")
        )
        with pytest.raises(ValueError, match="LoRA"):
            build_openai_applications({"llm_configs": [config]})


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
