"""KVRouterActor attachment and live replica-membership tracking.

Attachment is covered two ways: ``build_openai_app`` with a Python ``LLMConfig``,
and a declarative YAML config deployed via ``serve deploy`` (the dotted-string
router class only YAML can express). Membership tracking is covered by deploying
a dummy multi-replica deployment and asserting the actor's LongPoll listener
stays in sync with the live replicas across scale up/down.
"""

import os
import subprocess
import sys
from typing import List

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
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RunningReplicaInfo,
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
    handle = discover_deployment_actor(
        app_name, "ReplicaTrackingDeployment", KV_ROUTER_ACTOR_NAME
    )
    assert handle is not None
    return ray.get(handle.get_candidate_worker_ids.remote())


def get_live_replica_worker_ids(app_name, deployment_name="ReplicaTrackingDeployment"):
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


def test_build_openai_app_attaches_kv_actor():
    """A KVAwareRouter on the LLMConfig attaches the KVRouterActor."""
    app = build_openai_app(LLMServingArgs(llm_configs=[build_test_llm_config()]))

    configs = get_kv_actor_configs(app._bound_deployment)
    assert len(configs) == 1
    actor_cfg = configs[0]
    assert actor_cfg.get_actor_class().__ray_actor_class__ is KVRouterActor
    assert actor_cfg.actor_options["num_cpus"] == 0
    assert actor_cfg.init_kwargs == {"block_size": 16}


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


class _TestKVRouterActor(KVRouterActor):
    """KVRouterActor augmented with test-only introspection."""

    async def get_candidate_worker_ids(self) -> List[int]:
        """The workers currently tracked from running replicas.

        Async so it runs on the actor's event loop, serialized with
        ``_on_deployment_targets`` which mutates the same map on that loop.
        """
        return sorted(self._replica_id_by_worker)


@serve.deployment(
    num_replicas=4,
    deployment_actors=[
        DeploymentActorConfig(
            name=KV_ROUTER_ACTOR_NAME,
            actor_class=ray.remote(_TestKVRouterActor),
            actor_options={"num_cpus": 0},
            init_kwargs={"block_size": 16},
        ),
    ],
)
class ReplicaTrackingDeployment:
    """Dummy deployment with a KVRouterActor deployment actor.

    Advertises a per-replica KV-events endpoint via ``record_routing_stats`` as a
    real engine would, so the selection service tracks each replica as a worker.
    """

    async def __call__(self) -> str:
        return "ok"

    async def record_routing_stats(self) -> dict:
        rank = serve.get_replica_context().rank.local_rank
        return {
            "kv_event_metadata": {
                "endpoint": f"tcp://{ray.util.get_node_ip_address()}:{25000 + rank}",
                "max_num_batched_tokens": 8192,
                "dp_rank": 0,
            }
        }


class TestReplicaTrackingIntegration:
    def test_tracks_running_replicas(self, serve_instance):
        """KVRouterActor's LongPollClient receives the running replicas."""
        app_name = "kv-replica-tracking"
        serve.run(
            ReplicaTrackingDeployment.bind(), name=app_name, route_prefix="/kv_track"
        )
        try:
            wait_for_condition(
                lambda: len(get_candidate_ids(app_name)) == 4, timeout=30
            )
            # The tracked workers are exactly those of the live replica actors.
            assert set(get_candidate_ids(app_name)) == get_live_replica_worker_ids(
                app_name
            )
        finally:
            serve.delete(app_name, _blocking=True)

    def test_membership_broadcast_on_scale(self, serve_instance):
        """A scale up then down is broadcast over LongPoll; the actor re-syncs to
        exactly the live replica set each time.
        """
        app_name = "kv-replica-scale"

        def tracks_live_replicas(expected):
            # The tracked workers match the live replica actors by their actual
            # ids (a stale handle is possible while the deployment is updated).
            try:
                tracked = set(get_candidate_ids(app_name))
            except ray.exceptions.RayActorError:
                return False
            return len(tracked) == expected and tracked == get_live_replica_worker_ids(
                app_name
            )

        def scale(num_replicas):
            serve.run(
                ReplicaTrackingDeployment.options(num_replicas=num_replicas).bind(),
                name=app_name,
                route_prefix="/kv_scale",
            )

        scale(2)
        try:
            wait_for_condition(lambda: tracks_live_replicas(2), timeout=30)
            scale(4)  # upscale: the new replicas are picked up over LongPoll.
            wait_for_condition(lambda: tracks_live_replicas(4), timeout=30)
            scale(2)  # downscale: the departed replicas are dropped.
            wait_for_condition(lambda: tracks_live_replicas(2), timeout=30)
        finally:
            serve.delete(app_name, _blocking=True)


class _LocalKVRouterActor(_TestKVRouterActor):
    """In-process KVRouterActor with the selection service and LongPoll disabled,
    to drive ``_on_deployment_targets`` directly with synthetic snapshots.
    """

    def _create_selection_service(self) -> None:
        self._svc = None  # reconcile membership without dynamo

    def _start_replica_tracking(self) -> None:
        pass

    def _schedule(self, coro) -> None:
        coro.close()  # _svc is None, so the scheduled upsert is a no-op


def make_target_info(unique_ids):
    """A DeploymentTargetInfo whose replicas advertise a KV-events endpoint via
    routing_stats, exactly as the controller broadcasts it over LongPoll."""
    deployment_id = DeploymentID(name="d", app_name="app")
    running_replicas = [
        RunningReplicaInfo(
            replica_id=ReplicaID(unique_id=uid, deployment_id=deployment_id),
            node_id="node",
            node_ip="10.0.0.1",
            availability_zone="az",
            actor_name=f"actor-{uid}",
            max_ongoing_requests=1,
            routing_stats={
                "kv_event_metadata": {
                    "endpoint": "tcp://10.0.0.1:25000",
                    "max_num_batched_tokens": 8192,
                    "dp_rank": 0,
                }
            },
        )
        for uid in unique_ids
    ]
    return DeploymentTargetInfo(is_available=True, running_replicas=running_replicas)


class TestOnDeploymentTargets:
    async def test_reconciles_added_and_removed_workers(self):
        actor = _LocalKVRouterActor(block_size=16)
        actor._on_deployment_targets(make_target_info(["a", "b"]))
        assert set(await actor.get_candidate_worker_ids()) == {
            get_worker_id("a"),
            get_worker_id("b"),
        }
        # "a" departs and "c" joins: the tracked set follows the new snapshot.
        actor._on_deployment_targets(make_target_info(["b", "c"]))
        assert set(await actor.get_candidate_worker_ids()) == {
            get_worker_id("b"),
            get_worker_id("c"),
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
