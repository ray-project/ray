"""Test-only views into the two real KV trackers in a P/D ingress router."""

import asyncio
import base64
from contextlib import contextmanager
from dataclasses import asdict
import json
import os
import socket
import sys
from unittest import mock

import ray
from ray import serve
from ray.llm._internal.serve.core.ingress.pd_router import LLMPDRouter
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    KV_TOKEN_KEY_HEADER,
    KV_TRANSFER_PARAMS_HEADER,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    _MODEL_NAME,
    _TENANT_ID,
)
from ray.llm._internal.serve.serving_patterns.prefill_decode.pd_server import (
    PDDecodeServer,
    PDPrefillServer,
)
from ray.serve._private.constants import (
    RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD,
)

GATE_NAMESPACE = "pd-kv-router-test"


@ray.remote(num_cpus=0)
class PhaseGate:
    """Pause one real request at routing and lifecycle checkpoints."""

    def __init__(self):
        self._enabled = False
        self._reached = {}
        self._released = {}
        self._request_ids = {}

    def enable(self):
        self._enabled = True
        stages = (
            "prefill_booked",
            "before_decode",
            "decode_booked",
            "decode_completing",
        )
        self._reached = {stage: asyncio.Event() for stage in stages}
        self._released = {stage: asyncio.Event() for stage in stages}
        self._request_ids = {}

    async def pause(self, stage, request_id):
        if not self._enabled:
            return
        self._request_ids[stage] = request_id
        self._reached[stage].set()
        await self._released[stage].wait()

    async def reached(self, stage):
        await self._reached[stage].wait()
        return self._request_ids[stage]

    def release(self, stage):
        self._released[stage].set()

    def disable(self):
        self._enabled = False
        for event in self._released.values():
            event.set()


class PDTestRouter(LLMPDRouter):
    """Expose each stage's real Dynamo load and received lifecycle events."""

    GATE_NAME = None

    async def __init__(self, *args, **kwargs):
        self._routes = {}
        self._route_tokens = {}
        self._transfer_params = {}
        self._events = []
        self._decode_progress = {}
        await super().__init__(*args, **kwargs)
        gate = ray.get_actor(self.GATE_NAME, namespace=GATE_NAMESPACE)
        prefill_select = self.prefill_kv_token_tracker.select_worker
        decode_select = self.decode_kv_token_tracker.select_worker
        decode_progress = self.decode_kv_token_tracker.on_decode_progress
        decode_complete = self.decode_kv_token_tracker.on_request_completed

        async def select_prefill(request_id, *args, **kwargs):
            selection = await prefill_select(request_id, *args, **kwargs)
            self._routes.setdefault(request_id, {})["prefill"] = selection["worker_id"]
            await gate.pause.remote("prefill_booked", request_id)
            return selection

        async def select_decode(request_id, *args, **kwargs):
            await gate.pause.remote("before_decode", request_id)
            selection = await decode_select(request_id, *args, **kwargs)
            self._routes.setdefault(request_id, {})["decode"] = selection["worker_id"]
            self._route_tokens[request_id] = args[0]
            await gate.pause.remote("decode_booked", request_id)
            return selection

        async def record_decode_progress(request_id, output_tokens):
            await decode_progress(request_id, output_tokens)
            state = self.decode_kv_token_tracker._requests.get(request_id)
            self._decode_progress.setdefault(request_id, []).append(
                (
                    output_tokens,
                    state.output_tokens if state is not None else None,
                    request_id in self.prefill_kv_token_tracker._requests,
                )
            )

        async def complete_decode(request_id):
            await gate.pause.remote("decode_completing", request_id)
            await decode_complete(request_id)

        self.prefill_kv_token_tracker.select_worker = select_prefill
        self.decode_kv_token_tracker.select_worker = select_decode
        self.decode_kv_token_tracker.on_decode_progress = record_decode_progress
        self.decode_kv_token_tracker.on_request_completed = complete_decode

    def _tracker(self, stage):
        return (
            self.prefill_kv_token_tracker
            if stage == "prefill"
            else self.decode_kv_token_tracker
        )

    def get_replica_id(self):
        return serve.get_replica_context().replica_id.to_full_id_str()

    def get_stage_workers(self, stage):
        tracker = self._tracker(stage)
        schedulable = {
            worker["worker_id"]
            for worker in tracker._svc.list_workers(
                model_name=_MODEL_NAME, routing_group=_TENANT_ID
            )
            if worker["lifecycle"] == "schedulable"
        }
        return {
            worker_id: replica_id
            for worker_id, replica_id in tracker._replica_id_by_worker.items()
            if worker_id in schedulable
        }

    def get_stage_state(self, stage, request_id):
        tracker = self._tracker(stage)
        state = tracker._requests.get(request_id)
        loads = tracker._svc.loads(model_name=_MODEL_NAME, routing_group=_TENANT_ID)
        return {
            "replica_id": self.get_replica_id(),
            "request": asdict(state) if state is not None else None,
            "loads": {
                load["worker_id"]: load for model in loads for load in model["loads"]
            },
        }

    async def get_stage_overlap(self, stage, token_ids):
        tracker = self._tracker(stage)
        scores = await tracker._svc.overlap_scores(
            {
                "model_name": _MODEL_NAME,
                "tenant_id": _TENANT_ID,
                "token_ids": token_ids,
            }
        )
        return {worker["worker_id"]: worker for worker in scores["workers"]}

    async def select_stage(self, stage, request_id, token_ids, worker_ids, max_tokens):
        return await self._tracker(stage).select_worker(
            request_id, token_ids, worker_ids, max_tokens
        )

    async def release_stage(self, stage, request_id):
        await self._tracker(stage).release_request(request_id)

    async def route_request(self, request):
        response = await super().route_request(request)
        headers = response[RAY_SERVE_INGRESS_REQUEST_ROUTER_OPT_HEADERS_FIELD]
        request_id = headers[KV_TOKEN_KEY_HEADER]
        self._transfer_params[request_id] = json.loads(
            base64.b64decode(headers[KV_TRANSFER_PARAMS_HEADER])
        )
        return response

    async def on_lifecycle_events(self, batch, deployment_id):
        await super().on_lifecycle_events(batch, deployment_id)
        stage = (
            "prefill"
            if deployment_id == self.prefill_server.deployment_id
            else "decode"
        )
        self._events.extend((stage, name, args) for name, args in batch)

    def get_request_trace(self, request_id):
        return {
            "routes": self._routes.get(request_id, {}),
            "transfer_params": self._transfer_params.get(request_id),
            "events": [
                (stage, name, args)
                for stage, name, args in self._events
                if args[0] == request_id
            ],
            "decode_progress": self._decode_progress.get(request_id, []),
        }

    def get_route_records(self):
        return dict(self._routes)

    def get_route_token_ids(self, request_id):
        return self._route_tokens.get(request_id)


def _set_unique_vllm_port(llm_config):
    # vLLM's single-rank Ray executor derives its rendezvous port from this
    # value; give colocated test engines separate ports before they start.
    with socket.socket() as sock:
        sock.bind(("", 0))
        port = str(sock.getsockname()[1] - 100)
    os.environ["VLLM_DP_MASTER_PORT"] = port
    # Engine config is built in a separate Ray task using this runtime env.
    llm_config.runtime_env = dict(llm_config.runtime_env or {})
    llm_config.runtime_env["env_vars"] = {
        **llm_config.runtime_env.get("env_vars", {}),
        "VLLM_DP_MASTER_PORT": port,
    }
    if llm_config._engine_config is not None:
        llm_config._engine_config.runtime_env = llm_config.runtime_env


class PDTestPrefillServer(PDPrefillServer):
    async def __init__(self, *args, **kwargs):
        _set_unique_vllm_port(kwargs.get("llm_config") or args[0])
        await super().__init__(*args, **kwargs)

    def get_connector_identity(self):
        return (
            serve.get_replica_context().replica_id.to_full_id_str(),
            self._llm_config.kv_connector_backend.kv_transfer_config["engine_id"],
        )


class PDTestDecodeServer(PDDecodeServer):
    async def __init__(self, *args, **kwargs):
        self._received = {}
        _set_unique_vllm_port(kwargs.get("llm_config") or args[0])
        await super().__init__(*args, **kwargs)

    async def completions(self, request, raw_request_info=None):
        headers = raw_request_info.headers if raw_request_info else {}
        key = headers.get(KV_TOKEN_KEY_HEADER)
        metadata = headers.get(KV_TRANSFER_PARAMS_HEADER)
        if key and metadata:
            self._received[key] = json.loads(base64.b64decode(metadata))
        return await super().completions(request, raw_request_info)

    def get_received_params(self, request_id):
        return (
            serve.get_replica_context().replica_id.to_full_id_str(),
            self._received.get(request_id),
        )


@contextmanager
def patch_pd_classes(gate_name):
    """Serialize test views into Serve replicas and restore builder classes."""
    module = sys.modules[__name__]
    PDTestRouter.GATE_NAME = gate_name
    ray.cloudpickle.register_pickle_by_value(module)
    try:
        with (
            mock.patch(
                "ray.llm._internal.serve.core.ingress.builder.LLMPDRouter", PDTestRouter
            ),
            mock.patch(
                "ray.llm._internal.serve.serving_patterns.prefill_decode.builder.PDPrefillServer",
                PDTestPrefillServer,
            ),
            mock.patch(
                "ray.llm._internal.serve.serving_patterns.prefill_decode.builder.PDDecodeServer",
                PDTestDecodeServer,
            ),
        ):
            yield
    finally:
        ray.cloudpickle.unregister_pickle_by_value(module)
