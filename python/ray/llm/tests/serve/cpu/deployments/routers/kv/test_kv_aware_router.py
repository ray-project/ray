"""KV routing behavior: the non-KV warning path, ``select_worker`` guards, the
``KVAwareRouter.choose_replicas`` candidate mapping, and ``_on_deployment_targets``
replica reconciliation.
"""

import sys
from typing import List
from unittest import mock

import pytest

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.ingress.builder import (
    LLMServingArgs,
    build_openai_app,
)
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_router import (
    is_kv_aware,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    KVTokenTracker,
    get_worker_id,
)
from ray.serve._private.common import (
    DeploymentID,
    DeploymentTargetInfo,
    ReplicaID,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.request_router import PendingRequest
from ray.serve.llm.request_router import KVAwareRouter


def build_test_llm_config(experimental_configs=None) -> LLMConfig:
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
        experimental_configs=experimental_configs or {},
    )


def build_non_kv_llm_config(**engine_kwargs) -> LLMConfig:
    """An LLMConfig whose request router is the default (not a KVAwareRouter)."""
    return LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        deployment_config={
            "autoscaling_config": {"min_replicas": 1, "max_replicas": 1}
        },
        engine_kwargs=engine_kwargs,
    )


@pytest.fixture(autouse=True)
def enable_direct_streaming(monkeypatch):
    monkeypatch.setattr(
        "ray.llm._internal.serve.core.ingress.builder."
        "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING",
        True,
    )


def test_non_kv_router_warns_kv_events_config():
    """Without a KVAwareRouter a user-provided kv_events_config is left untouched
    (just unused), with a warning pointing at how to consume the engine's KV
    events."""
    kv_events_config = {
        "enable_kv_cache_events": True,
        "publisher": "zmq",
        "endpoint": "tcp://*:5557",
    }
    llm_config = build_non_kv_llm_config(kv_events_config=kv_events_config)

    with mock.patch(
        "ray.llm._internal.serve.routing_policies.kv_aware.utils.logger"
    ) as logger:
        build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

    assert llm_config.engine_kwargs["kv_events_config"] == kv_events_config
    logger.warning.assert_called_once()
    assert "KVAwareRouter" in logger.warning.call_args.args[0]


def test_build_openai_app_configures_kv_routing():
    """A KVAwareRouter LLMConfig enables the engine's KV-cache events at build
    time, so the tracker the LLMRouter ingress builds has events to index."""
    llm_config = build_test_llm_config()
    build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

    kv_events_config = llm_config.engine_kwargs.get("kv_events_config")
    assert kv_events_config is not None
    assert kv_events_config["enable_kv_cache_events"] is True


def test_string_router_enables_kv_routing():
    """A dotted-string request_router_class (as YAML configs use) resolves as
    KV-aware and enables the engine's KV-cache events."""
    llm_config = LLMConfig(
        model_loading_config={
            "model_id": "qwen3-0.6b",
            "model_source": "Qwen/Qwen3-0.6B",
        },
        accelerator_type=None,
        deployment_config={
            "autoscaling_config": {"min_replicas": 1, "max_replicas": 1},
            "request_router_config": {
                "request_router_class": "ray.serve.llm.request_router.KVAwareRouter"
            },
        },
    )
    assert is_kv_aware(llm_config) is True

    build_openai_app(LLMServingArgs(llm_configs=[llm_config]))
    assert llm_config.engine_kwargs.get("kv_events_config") is not None


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
                    "block_size": 16,
                    "max_num_batched_tokens": 8192,
                    "dp_rank": 0,
                }
            },
        )
        for uid in unique_ids
    ]
    return DeploymentTargetInfo(is_available=True, running_replicas=running_replicas)


class _LocalKVTokenTracker(KVTokenTracker):
    """Router-local KVTokenTracker with the selection service and LongPoll disabled,
    to drive ``_on_deployment_targets`` directly with synthetic snapshots.
    """

    async def get_candidate_worker_ids(self) -> List[int]:
        """The workers currently tracked from running replicas.

        Async so it runs on the ingress event loop, serialized with
        ``_on_deployment_targets`` which mutates the same map on that loop.
        """
        return sorted(self._replica_id_by_worker)

    def _create_selection_service(self) -> None:
        self._svc = None  # reconcile membership without dynamo

    def _start_replica_tracking(self) -> None:
        pass

    def _schedule(self, coro) -> None:
        coro.close()  # _svc is None, so the scheduled upsert is a no-op


class TestOnDeploymentTargets:
    async def test_reconciles_added_and_removed_workers(self):
        tracker = _LocalKVTokenTracker()
        tracker._on_deployment_targets(make_target_info(["a", "b"]))
        assert set(await tracker.get_candidate_worker_ids()) == {
            get_worker_id("a"),
            get_worker_id("b"),
        }
        # "a" departs and "c" joins: the tracked set follows the new snapshot.
        tracker._on_deployment_targets(make_target_info(["b", "c"]))
        assert set(await tracker.get_candidate_worker_ids()) == {
            get_worker_id("b"),
            get_worker_id("c"),
        }


class _StubReplica:
    """RunningReplica stand-in exposing only replica_id.unique_id."""

    def __init__(self, unique_id: str):
        self.replica_id = ReplicaID(
            unique_id=unique_id, deployment_id=DeploymentID(name="d", app_name="app")
        )


class _SelectWorkerStub:
    def __init__(self, worker_id: int):
        self._worker_id = worker_id
        self.token_ids = None
        self.allowed = None
        self.expected_output_tokens = None

    async def __call__(
        self, request_id, token_ids, allowed_worker_ids, expected_output_tokens=None
    ):
        self.token_ids = token_ids
        self.allowed = allowed_worker_ids
        self.expected_output_tokens = expected_output_tokens
        return {
            "worker_id": self._worker_id,
            "dp_rank": 0,
            "overlap_tokens": 1,
            "effective_prefill_tokens": len(token_ids),
        }


class _KVTokenTrackerStub:
    def __init__(self, worker_id: int):
        self.select_worker = _SelectWorkerStub(worker_id)


class _StubKVAwareRouter(KVAwareRouter):
    """KVAwareRouter with the router-local tracker injected, bypassing discovery."""

    def __init__(self, kv_token_tracker):
        self._kv_token_tracker = kv_token_tracker


def _build_kv_aware_router(worker_id: int) -> KVAwareRouter:
    return _StubKVAwareRouter(_KVTokenTrackerStub(worker_id))


@pytest.mark.asyncio
async def test_select_worker_requires_tokens():
    tracker = KVTokenTracker.__new__(KVTokenTracker)
    tracker._svc = object()

    with pytest.raises(ValueError, match="non-empty token_ids"):
        await tracker.select_worker("req-empty", [], [get_worker_id("r1")])


@pytest.mark.asyncio
async def test_select_worker_without_dynamo_raises():
    """Without ai-dynamo the tracker cannot score, so it raises a clear error
    instead of silently degrading to a non-KV-aware pick."""
    tracker = KVTokenTracker.__new__(KVTokenTracker)
    tracker._svc = None

    with pytest.raises(RuntimeError, match="ai-dynamo is not installed"):
        await tracker.select_worker("req", [1, 2, 3], [get_worker_id("r1")])


@pytest.mark.asyncio
async def test_choose_replicas_routes_to_selected_worker():
    """choose_replicas maps candidates to worker ids, asks the tracker to select,
    and returns the chosen worker's replica."""
    replicas = [_StubReplica("r1"), _StubReplica("r2")]
    worker_ids = [get_worker_id("r1"), get_worker_id("r2")]

    router = _build_kv_aware_router(worker_ids[1])
    pending = PendingRequest(
        args=[],
        kwargs={REQUEST_TOKEN_IDS_KWARG: [10, 11, 12]},
        metadata=RequestMetadata(request_id="req-1", internal_request_id="int-1"),
    )

    groups = await router.choose_replicas(replicas, pending)

    # The tracker selected r2's worker, so r2 is returned.
    assert groups == [[replicas[1]]]
    # choose_replicas forwarded the prompt token ids and the full candidate set.
    select = router._kv_token_tracker.select_worker
    assert select.token_ids == [10, 11, 12]
    assert sorted(select.allowed) == sorted(worker_ids)
    assert select.expected_output_tokens is None


@pytest.mark.asyncio
async def test_missing_token_ids_picks_random_replica():
    """Token-less requests (batch prompts, truncated bodies) route to a single
    random replica so they spread."""
    replicas = [_StubReplica("r1"), _StubReplica("r2")]
    router = _build_kv_aware_router(get_worker_id("r1"))

    picked = set()
    for _ in range(50):
        pending = PendingRequest(
            args=[],
            kwargs={},
            metadata=RequestMetadata(request_id="req", internal_request_id="int"),
        )
        groups = await router.choose_replicas(replicas, pending)
        assert len(groups) == 1 and len(groups[0]) == 1
        assert groups[0][0] in replicas
        picked.add(groups[0][0].replica_id.unique_id)

    # The picked replica varies across calls, so load spreads (not stuck on one).
    assert picked == {"r1", "r2"}
    assert router._kv_token_tracker.select_worker.token_ids is None


@pytest.mark.asyncio
async def test_tokenize_call_picks_random_replica():
    """The pre-routing /tokenize RPC is routed through choose_replicas before any
    token ids exist; it must resolve so KV routing can bootstrap, and picks a random
    replica without scoring."""
    replicas = [_StubReplica("r1"), _StubReplica("r2")]

    router = _build_kv_aware_router(get_worker_id("r2"))
    pending = PendingRequest(
        args=[],
        kwargs={},
        metadata=RequestMetadata(
            request_id="req-tokenize",
            internal_request_id="int-tokenize",
            call_method="tokenize",
        ),
    )

    groups = await router.choose_replicas(replicas, pending)

    assert len(groups) == 1 and len(groups[0]) == 1
    assert groups[0][0] in replicas
    assert router._kv_token_tracker.select_worker.token_ids is None


@pytest.mark.asyncio
async def test_empty_token_ids_picks_random_replica():
    """Empty token ids carry no KV signal, so pick a random replica instead of
    handing an empty prompt to the Dynamo selection service (which rejects it)."""
    replicas = [_StubReplica("r1"), _StubReplica("r2")]

    router = _build_kv_aware_router(get_worker_id("r2"))
    pending = PendingRequest(
        args=[],
        kwargs={REQUEST_TOKEN_IDS_KWARG: []},
        metadata=RequestMetadata(
            request_id="req-empty", internal_request_id="int-empty"
        ),
    )

    groups = await router.choose_replicas(replicas, pending)

    assert len(groups) == 1 and len(groups[0]) == 1
    assert groups[0][0] in replicas
    assert router._kv_token_tracker.select_worker.token_ids is None


@pytest.mark.asyncio
async def test_no_pending_request_picks_random_replica():
    """Serve may ask again after route metadata has been consumed; pick a random
    replica (nothing to score on)."""
    replicas = [_StubReplica("r1"), _StubReplica("r2")]

    router = _build_kv_aware_router(get_worker_id("r1"))

    groups = await router.choose_replicas(replicas, pending_request=None)

    assert len(groups) == 1 and len(groups[0]) == 1
    assert groups[0][0] in replicas
    assert router._kv_token_tracker.select_worker.token_ids is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
