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
    get_worker_id,
)
from ray.serve._private.common import (
    REPLICA_ID_FULL_ID_STR_PREFIX,
    ReplicaID,
)
from ray.serve._private.constants import SERVE_DEPLOYMENT_ACTOR_PREFIX, SERVE_NAMESPACE
from ray.serve.config import DeploymentActorConfig
from ray.serve.llm.request_router import KVAwareRouter
from ray.util.state import list_actors


def get_kv_actor_configs(deployment):
    return [
        cfg
        for cfg in (deployment._deployment_config.deployment_actors or [])
        if (cfg["name"] if isinstance(cfg, dict) else cfg.name) == KV_ROUTER_ACTOR_NAME
    ]


def build_test_llm_config(**engine_kwargs) -> LLMConfig:
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
        engine_kwargs=engine_kwargs,
    )


def get_kv_actor_names(app_name: str) -> list:
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::"
    suffix = f"::{KV_ROUTER_ACTOR_NAME}"
    return [
        a["name"]
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a["name"] and a["name"].startswith(prefix) and a["name"].endswith(suffix)
    ]


def discover_deployment_actor(app_name, deployment_name, actor_name):
    """Handle to a deployment-scoped actor by app/deployment/logical name."""
    prefix = f"{SERVE_DEPLOYMENT_ACTOR_PREFIX}{app_name}::{deployment_name}::"
    suffix = f"::{actor_name}"
    for entry in ray.util.list_named_actors(all_namespaces=True):
        name = entry.get("name") or ""
        if (
            entry.get("namespace") == SERVE_NAMESPACE
            and name.startswith(prefix)
            and (name.endswith(suffix))
        ):
            return ray.get_actor(name, namespace=SERVE_NAMESPACE)
    return None


def get_candidate_ids(app_name):
    handle = discover_deployment_actor(app_name, "Driver", KV_ROUTER_ACTOR_NAME)
    assert handle is not None
    return ray.get(handle.get_candidate_worker_ids.remote())


def get_live_replica_worker_ids(app_name, deployment_name="Driver"):
    """Worker ids derived directly from the deployment's alive replica actors."""
    prefix = f"{REPLICA_ID_FULL_ID_STR_PREFIX}{app_name}#{deployment_name}#"
    return {
        get_worker_id(a["name"][len(prefix) :])
        for a in list_actors(filters=[("state", "=", "ALIVE")])
        if a["name"] and a["name"].startswith(prefix)
    }


@pytest.fixture(autouse=True)
def enable_direct_streaming(monkeypatch):
    monkeypatch.setattr(
        "ray.llm._internal.serve.core.ingress.builder."
        "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
        True,
    )


@pytest.fixture(scope="module")
def serve_instance():
    if not ray.is_initialized():
        ray.init(address="auto")
    yield
    serve.shutdown()


@pytest.mark.parametrize("engine_kwargs", [{}, {"block_size": 32}])
def test_build_openai_app_attaches_kv_actor(engine_kwargs):
    """A KVAwareRouter on the LLMConfig attaches the KVRouterActor.

    The actor receives the deployment's KV block size, taken from the
    ``block_size`` engine kwarg or vLLM's own default when omitted.
    """
    from vllm.config import CacheConfig

    llm_config = build_test_llm_config(**engine_kwargs)
    app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

    configs = get_kv_actor_configs(app._bound_deployment)
    assert len(configs) == 1
    actor_cfg = configs[0]
    assert (
        actor_cfg.get_actor_class().__ray_actor_class__
        is KVRouterActor.__ray_actor_class__
    )
    assert actor_cfg.actor_options["num_cpus"] == 0
    expected_block_size = engine_kwargs.get(
        "block_size", CacheConfig.DEFAULT_BLOCK_SIZE
    )
    assert actor_cfg.init_kwargs == {"block_size": expected_block_size}


def test_yaml_config_attaches_kv_actor(serve_instance):
    """Deploying a YAML config that selects KVAwareRouter creates the KVRouterActor."""
    config_file = os.path.join(
        os.path.dirname(__file__), "test_config_files", "llm_kv_aware_deployment.yaml"
    )
    app_name = "kv-llm"

    subprocess.check_output(["serve", "deploy", config_file], stderr=subprocess.STDOUT)
    try:
        wait_for_condition(lambda: len(get_kv_actor_names(app_name)) == 1, timeout=60)
    finally:
        serve.delete(app_name, _blocking=True)


@serve.deployment(
    num_replicas=4,
    deployment_actors=[
        DeploymentActorConfig(
            name=KV_ROUTER_ACTOR_NAME,
            actor_class=KVRouterActor,
            init_kwargs={"block_size": 16},
            actor_options={"num_cpus": 0},
        ),
    ],
)
class Driver:
    """Dummy deployment with KVRouterActor deployment actor."""

    async def __call__(self) -> str:
        return "ok"


class TestReplicaTrackingIntegration:
    def test_tracks_running_replicas(self, serve_instance):
        """KVRouterActor's LongPollClient receives the running replicas."""
        app_name = "kv-replica-tracking"
        serve.run(Driver.bind(), name=app_name, route_prefix="/kv_track")
        try:
            wait_for_condition(
                lambda: len(get_candidate_ids(app_name)) == 4, timeout=10
            )
            handle = discover_deployment_actor(app_name, "Driver", KV_ROUTER_ACTOR_NAME)
            for worker_id in ray.get(handle.get_candidate_worker_ids.remote()):
                full_id = ray.get(handle.get_replica_id.remote(worker_id))
                assert ReplicaID.is_full_id_str(full_id)
                assert (
                    ray.get(handle.get_tracked_worker_id.remote(full_id)) == worker_id
                )
        finally:
            serve.delete(app_name, _blocking=True)

    @pytest.mark.parametrize("num_start_replicas,num_end_replicas", [(2, 4), (4, 2)])
    def test_membership_broadcast_on_scale(
        self, serve_instance, num_start_replicas, num_end_replicas
    ):
        """An up- or down-scale is broadcast over LongPoll to the KVRouterActor."""
        app_name = "kv-replica-scale"

        # The actor's tracked workers must match the live replicas by their
        # actual ids (a stale handle is possible while an actor is torn down).
        def tracks_live_replicas(expected):
            try:
                tracked = set(get_candidate_ids(app_name))
            except ray.exceptions.RayActorError:
                return False
            return len(tracked) == expected and tracked == get_live_replica_worker_ids(
                app_name
            )

        serve.run(
            Driver.options(num_replicas=num_start_replicas).bind(),
            name=app_name,
            route_prefix="/kv_scale",
        )
        try:
            wait_for_condition(
                lambda: tracks_live_replicas(num_start_replicas), timeout=10
            )

            serve.run(
                Driver.options(num_replicas=num_end_replicas).bind(),
                name=app_name,
                route_prefix="/kv_scale",
            )

            # The broadcast re-syncs the actor to exactly the live replicas.
            wait_for_condition(
                lambda: tracks_live_replicas(num_end_replicas), timeout=10
            )
        finally:
            serve.delete(app_name, _blocking=True)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
