"""Shared helpers for the KV-router GPU release tests.

The KVTokenTracker is a plain object built by the LLMRouter ingress replica.
These tests reach it through the LLMRouter deployment handle: ``patch_ingress``
swaps in an ``LLMRouter`` subclass that exposes the tracker's and its selection
service's state as handle-callable methods.
"""

import asyncio
from contextlib import contextmanager
import sys
from unittest import mock

import ray.cloudpickle
from ray import serve
from ray.llm._internal.serve.core.ingress.router import LLMRouter as _LLMRouter
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    SELECTION_MODEL_NAME,
    SELECTION_ROUTING_GROUP,
)
from ray.llm._internal.serve.routing_policies.kv_aware.vllm.kv_events import (
    configure_kv_events_for_kv_routing,
)
from ray.serve.config import RequestRouterConfig
from ray.serve.experimental.round_robin_router import RoundRobinRouter
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.request_router import KVAwareRouter

MODEL_ID = "Qwen/Qwen3-0.6B"


def build_kv_config(
    *,
    request_router_class,
    kv_events_port_base,
    num_replicas=1,
    request_router_kwargs=None,
):
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
                request_router_class=request_router_class,
                request_router_kwargs=request_router_kwargs or {},
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


async def discover_replica_endpoints(handle, expected_replicas):
    """Map each replica id to its direct-ingress HTTP endpoint."""
    endpoints = {}
    for _ in range(100):
        async with handle.choose_replica() as selection:
            replica = selection._replica
            if replica.backend_http_endpoint is not None:
                endpoints[
                    replica.replica_id.to_full_id_str()
                ] = replica.backend_http_endpoint
        if len(endpoints) == expected_replicas:
            return endpoints
        await asyncio.sleep(0.5)
    raise AssertionError(
        f"Expected {expected_replicas} replicas with backend endpoints, "
        f"found {len(endpoints)}."
    )


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

    It forwards queries to the tracker and its selection service. Engines'
    lifecycle events reach the selection service directly, without Python;
    ``prefill_complete`` and ``free_reservation`` stand in for them on requests
    no engine serves.
    """

    async def __init__(self, *args, **kwargs):
        await super().__init__(*args, **kwargs)
        self._token_pushes = []
        self._selections = []
        renderer = self._tokenizer._renderer.renderer
        self._chat_render = mock.AsyncMock(wraps=renderer.render_chat_async)
        renderer.render_chat_async = self._chat_render
        # Record KVAwareRouter's choices so tests can see where each session
        # turn routed. Selections a test makes through ``select_worker`` skip
        # the record.
        tracker = self._kv_token_tracker
        self._select_worker = select_worker = tracker.select_worker

        async def recorded_select_worker(request_id, *args, session_id=None, **kwargs):
            selection = await select_worker(
                request_id, *args, session_id=session_id, **kwargs
            )
            self._selections.append(
                dict(
                    request_id=request_id,
                    session_id=session_id,
                    worker_id=selection["worker_id"],
                )
            )
            return selection

        tracker.select_worker = recorded_select_worker

    def _push_prompt_tokens(self, *, token_endpoint, replica_id, request_token_ids):
        key = super()._push_prompt_tokens(
            token_endpoint=token_endpoint,
            replica_id=replica_id,
            request_token_ids=request_token_ids,
        )
        self._token_pushes.append(
            dict(
                endpoint=token_endpoint,
                sent=key is not None,
            )
        )
        return key

    async def prefill_complete(self, request_id):
        """(Test only) Mark this router's booking of ``request_id`` prefilled.
        Returns whether it held one."""
        try:
            await self._kv_token_tracker._svc.prefill_complete(request_id)
            return True
        except Exception:
            return False

    async def free_reservation(self, request_id):
        """(Test only) Free this router's booking of ``request_id``. Returns
        whether it held one."""
        try:
            await self._kv_token_tracker._svc.free_reservation(request_id)
            return True
        except Exception:
            return False

    # -- introspection ------------------------------------------------------
    def reset_token_pushes(self):
        self._token_pushes.clear()
        self._chat_render.reset_mock()

    def get_token_push_report(self):
        return dict(
            node_ip=ray.util.get_node_ip_address(),
            pushes=list(self._token_pushes),
            tokenizations=self._chat_render.await_count,
        )

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
        workers = svc.list_workers(
            model_name=SELECTION_MODEL_NAME, routing_group=SELECTION_ROUTING_GROUP
        )
        return sorted(
            w["worker_id"] for w in workers if w["lifecycle"] == "schedulable"
        )

    async def get_kv_overlap_blocks(self, token_ids, lora_name=None):
        """(Test only) Per-worker device-tier KV overlap blocks for a sequence."""
        scores = await self.get_kv_overlap_scores(token_ids, lora_name)
        return {
            worker_id: score["device_blocks"] for worker_id, score in scores.items()
        }

    async def get_kv_overlap_scores(self, token_ids, lora_name=None):
        """(Test only) Per-worker overlap across every KV storage tier.

        ``lora_name`` selects the LoRA namespace to score in; the selection
        service salts its KV hashes with it, so the base model and each adapter
        see disjoint blocks for the same tokens.
        """
        svc = self._kv_token_tracker._svc
        if svc is None:
            return {}
        scores = await svc.overlap_scores(
            {
                "model_name": SELECTION_MODEL_NAME,
                "routing_group": SELECTION_ROUTING_GROUP,
                "token_ids": list(token_ids),
                "lora_name": lora_name,
            }
        )
        return {worker["worker_id"]: worker for worker in scores["workers"]}

    async def get_worker_active_requests(self, worker_id):
        """(Test only) In-flight requests the service tracks as active load on
        ``worker_id`` -- the count scoring factors in."""
        svc = self._kv_token_tracker._svc
        if svc is None:
            return 0
        for model in svc.loads(
            model_name=SELECTION_MODEL_NAME, routing_group=SELECTION_ROUTING_GROUP
        ):
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
        for model in svc.loads(
            model_name=SELECTION_MODEL_NAME, routing_group=SELECTION_ROUTING_GROUP
        ):
            for load in model["loads"]:
                if load["worker_id"] == worker_id:
                    return load
        return None

    async def get_load_snapshot(self, worker_id):
        """(Test only) This replica's id and node, and the load it books on
        ``worker_id``."""
        load = await self.get_worker_load(worker_id) or {
            "active_requests": 0,
            "potential_prefill_tokens": 0,
        }
        return {
            "replica_id": self.get_replica_id(),
            "node_ip": self.get_node_ip(),
            "active_requests": load["active_requests"],
            "potential_prefill_tokens": load["potential_prefill_tokens"],
        }

    def get_replica_id(self):
        """(Test only) This ingress replica's full id string, to tell the
        per-replica results of a broadcast apart."""
        return serve.get_replica_context().replica_id.to_full_id_str()

    def get_actor_of(self, replica_id):
        """(Test only) This ingress replica's actor if it is ``replica_id``, to
        kill it. Broadcast it; the others return ``None``."""
        if self.get_replica_id() != replica_id:
            return None
        return ray.get_runtime_context().current_actor

    def get_node_ip(self):
        """(Test only) The node this ingress replica runs on."""
        return ray.util.get_node_ip_address()

    def get_replica_mesh_peers(self):
        """(Test only) Mesh endpoints registered with this router's selector."""
        return self._kv_token_tracker._svc.list_replica_peers()

    def get_lifecycle_sources(self):
        """(Test only) Engine lifecycle endpoints this router's selector
        subscribes to."""
        return self._kv_token_tracker._svc.list_worker_publishers()

    def get_block_size(self):
        """(Test only) The KV-cache block size the tracker pinned."""
        return self._kv_token_tracker.get_block_size()

    async def select_worker(
        self,
        request_id,
        token_ids,
        allowed_worker_ids,
        expected_output_tokens=None,
        lora_name=None,
        session_id=None,
    ):
        """(Test only) Score ``allowed_worker_ids`` for a prompt via the tracker."""
        return await self._select_worker(
            request_id,
            token_ids,
            allowed_worker_ids,
            expected_output_tokens,
            lora_name=lora_name,
            session_id=session_id,
        )

    async def select_worker_on(self, replica_id, *args, **kwargs):
        """(Test only) ``select_worker`` on the ingress replica ``replica_id``.

        Broadcast it to pin a selection to one ingress; the others return
        ``None``.
        """
        if self.get_replica_id() != replica_id:
            return None
        return await self.select_worker(*args, **kwargs)

    def get_selections(self):
        """(Test only) Every request KVAwareRouter routed through this ingress."""
        return self._selections

    async def get_session_worker(self, session_id, token_ids, allowed_worker_ids):
        """(Test only) Where this ingress would route ``session_id`` now.

        A query: it neither binds the session nor books load.
        """
        selection = await self._kv_token_tracker._svc.select(
            {
                "model_name": SELECTION_MODEL_NAME,
                "routing_group": SELECTION_ROUTING_GROUP,
                "token_ids": list(token_ids),
                "allowed_worker_ids": list(allowed_worker_ids),
                "session_id": session_id,
            }
        )
        return selection["worker_id"]


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
