"""The KVRouterActor is attached iff the request router is a KVAwareRouter.

Covered two ways: ``build_openai_app`` with a Python ``LLMConfig``, and a
declarative YAML config deployed via ``serve deploy`` (the dotted-string router
class only YAML can express).
"""

import os
import subprocess
import sys

import pytest

import ray
from ray import serve
from ray._common.test_utils import wait_for_condition
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
)
from ray.serve._private.constants import SERVE_DEPLOYMENT_ACTOR_PREFIX
from ray.serve.llm.request_router import KVAwareRouter
from ray.util.state import list_actors


def get_kv_actor_configs(deployment):
    return [
        cfg
        for cfg in (deployment._deployment_config.deployment_actors or [])
        if (cfg["name"] if isinstance(cfg, dict) else cfg.name) == KV_ROUTER_ACTOR_NAME
    ]


def build_test_llm_config() -> LLMConfig:
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        deployment_config={
            "autoscaling_config": {"min_replicas": 1, "max_replicas": 1},
            "request_router_config": {"request_router_class": KVAwareRouter},
        },
    )

@pytest.fixture(autouse=True)
def enable_direct_streaming(monkeypatch):
    monkeypatch.setattr(
        "ray.llm._internal.serve.core.ingress.builder."
        "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
        True,
    )


def test_build_openai_app_attaches_kv_actor():
    """A KVAwareRouter on the LLMConfig attaches the KVRouterActor."""
    app = build_openai_app(LLMServingArgs(llm_configs=[build_test_llm_config()]))

    configs = get_kv_actor_configs(app._bound_deployment)
    assert len(configs) == 1
    actor_cfg = configs[0]
    assert (
        actor_cfg.get_actor_class().__ray_actor_class__
        is KVRouterActor.__ray_actor_class__
    )
    assert actor_cfg.actor_options["num_cpus"] == 0


@pytest.fixture(scope="module")
def serve_instance():
    if not ray.is_initialized():
        ray.init(address="auto")
    yield
    serve.shutdown()


def get_kv_actor_names(app_name: str) -> list:
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::"
    suffix = f"::{KV_ROUTER_ACTOR_NAME}"
    return [
        a["name"]
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a["name"] and a["name"].startswith(prefix) and a["name"].endswith(suffix)
    ]


def test_yaml_config_attaches_kv_actor(serve_instance):
    """Deploying a YAML config that selects KVAwareRouter creates the KVRouterActor."""
    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "llm_kv_aware_deployment.yaml"
    )
    app_name = "kv-llm"

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)
    try:
        wait_for_condition(
            lambda: len(get_kv_actor_names(app_name)) == 1, timeout=60
        )
    finally:
        serve.delete(app_name, _blocking=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
