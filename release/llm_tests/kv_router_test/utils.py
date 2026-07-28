"""Shared helpers for the KV-router GPU release tests.

The KVTokenTracker is a plain object built by the LLMRouter ingress replica.
These tests reach it through the LLMRouter deployment handle: ``patch_ingress``
swaps in an ``LLMRouter`` subclass (kept named ``LLMRouter`` so the deployment
name the engine resolves is unchanged) that records booked lifecycle events and
exposes the tracker's state as handle-callable methods.
"""

from contextlib import contextmanager
from dataclasses import asdict
import sys
from unittest import mock

import ray.cloudpickle
from ray import serve
from ray.llm._internal.serve.core.ingress.router import LLMRouter as _LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    _MODEL_NAME,
    _TENANT_ID,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.request_router import KVAwareRouter

MODEL_ID = "Qwen/Qwen3-0.6B"


def build_kv_config(*, request_router_class, kv_events_port_base, num_replicas=1):
    """Config for a direct-streaming KV-aware app with engine KV events enabled.

    Build it outside ``patch_ingress``: serializing the router class clears this
    module from cloudpickle's pickle-by-value registry.
    """
    llm_config = LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=MODEL_ID,
            model_source=MODEL_ID,
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=num_replicas, max_replicas=num_replicas
            ),
            # A KVAwareRouter (subclass) gates engine token tracking and the
            # KV-events plane; the ingress builds the KVTokenTracker.
            request_router_config=RequestRouterConfig(
                request_router_class=request_router_class
            ),
        ),
        engine_kwargs=dict(
            max_model_len=2048,
            gpu_memory_utilization=0.4,
        ),
        placement_group_config={"bundles": [{"GPU": 1}]},
        experimental_configs={"KV_EVENTS_PORT_BASE": kv_events_port_base},
    )
    # Emit engine KV-cache events so each ingress tracker registers the
    # replica's worker (schedulable, required to book a reservation against it).
    configure_kv_events_for_kv_routing(llm_config)
    return llm_config


def build_kv_app(llm_config):
    """The Serve app for ``llm_config``; call inside ``patch_ingress``."""
    return build_openai_app({"llm_configs": [llm_config]})


class _TestKVAwareRouter(RoundRobinRouter, KVAwareRouter):
    """A ``KVAwareRouter`` subclass that borrows ``RoundRobinRouter``'s selection.

    The KV-events-plane tests send requests directly to each replica's endpoint
    (not through KV scoring) and need to enumerate every replica, so this
    inherits RoundRobinRouter's ``choose_replicas`` (via MRO) while remaining a
    KVAwareRouter subclass so the deployment still enables the KV-events plane
    and the tracker.
    """


class LLMRouter(_LLMRouter):
    """(Test only) LLMRouter that exposes its embedded KVTokenTracker over the
    deployment handle for the KV-router release tests.

    Named ``LLMRouter`` so the deployment name stays ``LLMRouter`` (the engine
    resolves lifecycle events by that name). It records every lifecycle event
    booked through ``on_lifecycle_events`` and any error raised while applying
    it, and forwards read-only queries to the tracker and its selection service.
    """

    async def __init__(self, *args, **kwargs):
        await super().__init__(*args, **kwargs)
        self._event_log = []
        self._errors = []

    async def on_lifecycle_events(self, events):
        """Record events, then apply each hook to the tracker directly so a
        hook raising is captured in ``_errors`` rather than swallowed."""
        self._event_log.extend(events)
        for hook_name, hook_args in events:
            try:
                await getattr(self._kv_token_tracker, hook_name)(*hook_args)
            except Exception as e:  # noqa: BLE001 - recorded for assertion
                self._errors.append((hook_name, repr(e)))

    # -- lifecycle-event booking passthroughs (probe requests) --------------
    async def on_request_added(self, *args, **kwargs):
        return await self._kv_token_tracker.on_request_added(*args, **kwargs)

    async def on_prefill_complete(self, *args, **kwargs):
        return await self._kv_token_tracker.on_prefill_complete(*args, **kwargs)

    async def on_request_completed(self, *args, **kwargs):
        return await self._kv_token_tracker.on_request_completed(*args, **kwargs)

    # -- introspection ------------------------------------------------------
    def get_event_log(self):
        """(Test only) Every lifecycle event booked through this ingress."""
        return self._event_log

    def get_errors(self):
        """(Test only) (hook, repr(exc)) for each hook that raised while booking."""
        return self._errors

    def get_kv_event_worker_replicas(self):
        """(Test only) Registered Dynamo worker id -> replica full id mapping."""
        return dict(self._kv_token_tracker._replica_id_by_worker)

    def get_candidate_worker_ids(self):
        """(Test only) Workers currently tracked from running replicas."""
        return sorted(self._kv_token_tracker._replica_id_by_worker)

    def get_registered_worker_ids(self):
        """(Test only) Worker ids the selection service can currently schedule."""
        svc = self._kv_token_tracker._svc
        if svc is None:
            return []
        workers = svc.list_workers(model_name=_MODEL_NAME, tenant_id=_TENANT_ID)
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_kv_overlap_blocks(self, token_ids):
        """(Test only) Per-worker device-tier KV overlap blocks for a sequence."""
        scores = await self.get_kv_overlap_scores(token_ids)
        return {
            worker_id: score["device_blocks"] for worker_id, score in scores.items()
        }

    async def get_kv_overlap_scores(self, token_ids):
        """(Test only) Per-worker overlap across every KV storage tier."""
        svc = self._kv_token_tracker._svc
        if svc is None:
            return {}
        scores = await svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": list(token_ids),
            }
        )
        return {worker["worker_id"]: worker for worker in scores["workers"]}

    async def get_worker_active_requests(self, worker_id):
        """(Test only) In-flight requests the service tracks as active load on
        ``worker_id`` -- the count scoring factors in."""
        svc = self._kv_token_tracker._svc
        if svc is None:
            return 0
        for model in svc.loads(model_name=_MODEL_NAME, tenant_id=_TENANT_ID):
            for load in model["loads"]:
                if load["worker_id"] == worker_id:
                    return load["active_requests"]
        return 0

    async def get_worker_load(self, worker_id):
        """(Test only) Full tracked load for ``worker_id`` (active requests plus
        potential prefill tokens and decode blocks -- the token-load state
        scoring consumes), or ``None`` when the worker is untracked."""
        svc = self._kv_token_tracker._svc
        if svc is None:
            return None
        for model in svc.loads(model_name=_MODEL_NAME, tenant_id=_TENANT_ID):
            for load in model["loads"]:
                if load["worker_id"] == worker_id:
                    return load
        return None

    def get_replica_id(self):
        """(Test only) This ingress replica's full id string, to tell the
        per-replica results of a broadcast apart."""
        return serve.get_replica_context().replica_id.to_full_id_str()

    async def get_request_lifecycle(self, request_id):
        """(Test only) Snapshot of a request's local lifecycle state, or ``None``."""
        state = self._kv_token_tracker._requests.get(request_id)
        if state is None:
            return None
        snapshot = asdict(state)
        snapshot.pop("created_at", None)
        return snapshot

    async def get_lifecycle_snapshot(self, request_id, worker_id):
        """(Test only) This replica's id, its view of a request's lifecycle and
        the load it books on ``worker_id``."""
        return {
            "replica_id": self.get_replica_id(),
            "lifecycle": await self.get_request_lifecycle(request_id),
            "active_requests": await self.get_worker_active_requests(worker_id),
        }

    async def get_active_request_ids(self):
        """(Test only) Ids of the requests in the tracker's in-flight view."""
        return list(self._kv_token_tracker._requests)

    def get_block_size(self):
        """(Test only) The KV-cache block size the tracker pinned."""
        return self._kv_token_tracker.get_block_size()

    async def select_worker(self, request_id, token_ids, allowed_worker_ids):
        """(Test only) Score ``allowed_worker_ids`` for a prompt via the tracker."""
        return await self._kv_token_tracker.select_worker(
            request_id, token_ids, allowed_worker_ids
        )


@contextmanager
def patch_ingress():
    """Deploy with the introspection ``LLMRouter`` subclass as the ingress.

    This test-only module is available to the driver, not Serve replicas.
    Pickle it by value so the patched ingress can deserialize there.
    """
    module = sys.modules[__name__]
    ray.cloudpickle.register_pickle_by_value(module)
    try:
        with mock.patch(
            "ray.llm._internal.serve.core.ingress.router.LLMRouter", LLMRouter
        ):
            yield
    finally:
        ray.cloudpickle.unregister_pickle_by_value(module)
