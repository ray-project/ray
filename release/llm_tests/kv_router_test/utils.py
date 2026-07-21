"""Shared helpers for the KV-router GPU release tests.

The KVTokenTracker is a plain object built by the LLMRouter ingress replica.
These tests reach it through the LLMRouter deployment handle: ``patch_ingress``
swaps in an ``LLMRouter`` subclass (kept named ``LLMRouter`` so the deployment
name the engine resolves is unchanged) that records booked lifecycle events and
exposes the tracker's state as handle-callable methods.
"""

from contextlib import contextmanager
from dataclasses import asdict
from unittest import mock

from ray.llm._internal.serve.core.ingress.router import LLMRouter as _LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    _MODEL_NAME,
    _TENANT_ID,
)
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm.request_router import KVAwareRouter


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
        return {w["worker_id"]: w["device_blocks"] for w in scores["workers"]}

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

    async def get_request_lifecycle(self, request_id):
        """(Test only) Snapshot of a request's local lifecycle state, or ``None``."""
        state = self._kv_token_tracker._requests.get(request_id)
        if state is None:
            return None
        snapshot = asdict(state)
        snapshot.pop("created_at", None)
        return snapshot

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
    """Deploy with the introspection ``LLMRouter`` subclass as the ingress."""
    with mock.patch("ray.llm._internal.serve.core.ingress.router.LLMRouter", LLMRouter):
        yield
