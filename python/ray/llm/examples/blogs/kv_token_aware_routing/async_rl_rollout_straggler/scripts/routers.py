"""Logging wrappers for the three routers used by this benchmark."""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from ray.serve._private.request_router import PendingRequest, RunningReplica
from ray.serve.experimental.consistent_hash_router import ConsistentHashRouter
from ray.serve.llm.request_router import KVAwareRouter


class RoutingLogMixin:
    """Record placement decisions and KVAwareRouter SelectionService load."""

    LOG_DIR: Optional[str] = None

    def _log_init(self) -> None:
        if getattr(self, "_benchmark_log_ready", False):
            return
        self._benchmark_log_ready = True
        self._log_file: Any = None
        self._selection_file: Any = None
        self._log_lock = threading.Lock()
        self._selection_stop = threading.Event()
        if self.LOG_DIR is None:
            return
        os.makedirs(self.LOG_DIR, exist_ok=True)
        self._log_file = open(
            os.path.join(self.LOG_DIR, f"routing.{os.getpid()}.jsonl"), "a", buffering=1
        )
        if isinstance(self, KVAwareRouter):
            self._selection_file = open(
                os.path.join(self.LOG_DIR, f"selector_loads.{os.getpid()}.jsonl"),
                "a",
                buffering=1,
            )
            threading.Thread(target=self._sample_selection_loads, daemon=True).start()

    @staticmethod
    def _value(record: Any, field: str, default: Any = None) -> Any:
        if isinstance(record, Mapping):
            return record.get(field, default)
        return getattr(record, field, default)

    def _sample_selection_loads(self) -> None:
        interval = 0.5
        while not self._selection_stop.is_set():
            try:
                tracker = getattr(self, "_kv_token_tracker", None)
                service = getattr(tracker, "_svc", None)
                if service is not None and self._selection_file is not None:
                    models = []
                    for response in service.loads(
                        model_name="default", routing_group="default"
                    ):
                        loads = [
                            {
                                "worker_id": str(self._value(load, "worker_id", "")),
                                "potential_prefill_tokens": int(
                                    self._value(load, "potential_prefill_tokens", 0)
                                ),
                                "potential_decode_blocks": int(
                                    self._value(load, "potential_decode_blocks", 0)
                                ),
                                "active_requests": int(
                                    self._value(load, "active_requests", 0)
                                ),
                            }
                            for load in self._value(response, "loads", []) or []
                        ]
                        models.append({"loads": loads})
                    with self._log_lock:
                        self._selection_file.write(
                            json.dumps({"ts": time.time(), "models": models}) + "\n"
                        )
            except Exception:
                pass
            self._selection_stop.wait(interval)

    async def choose_replicas(
        self,
        candidate_replicas: list[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> Sequence[list[RunningReplica]]:
        self._log_init()
        started = time.perf_counter()
        result = await super().choose_replicas(  # type: ignore[misc]
            candidate_replicas, pending_request
        )
        if self._log_file is None:
            return result
        try:
            chosen = result[0][0].replica_id.unique_id if result and result[0] else None
            metadata = pending_request.metadata if pending_request is not None else None
            request_kwargs = (
                getattr(pending_request, "kwargs", {})
                if pending_request is not None
                else {}
            )
            token_ids = (
                request_kwargs.get("request_token_ids") if request_kwargs else None
            )
            row = {
                "ts": time.time(),
                "session_id": getattr(metadata, "session_id", None),
                "request_id": getattr(metadata, "request_id", None),
                "replica_id": chosen,
                "decision_us": round((time.perf_counter() - started) * 1e6, 1),
                "kv_token_count": len(token_ids) if token_ids else None,
                "kv_tracker_present": (
                    getattr(self, "_kv_token_tracker", None) is not None
                    if isinstance(self, KVAwareRouter)
                    else None
                ),
            }
            with self._log_lock:
                self._log_file.write(json.dumps(row) + "\n")
        except Exception:
            pass
        return result


class LoggingKVAwareRouter(RoutingLogMixin, KVAwareRouter):  # type: ignore[misc]
    """KVAwareRouter with benchmark telemetry."""


class LoggingConsistentHashRouter(RoutingLogMixin, ConsistentHashRouter):  # type: ignore[misc]
    """ConsistentHashRouter with benchmark telemetry."""


KV_TOKEN_AWARE_ENV = {
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT": "1.0",
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT_DECAY": "0.0",
    "DYN_ROUTER_PREFILL_LOAD_SCALE": "1.0",
    "DYN_ROUTER_DECODE_ACTIVE_REQUEST_WEIGHT": "0.0",
    "DYN_ROUTER_TEMPERATURE": "0.0",
    "DYN_ROUTER_TRACK_ACTIVE_BLOCKS": "1",
    "DYN_ROUTER_TRACK_PREFILL_TOKENS": "1",
    "DYN_ROUTER_TRACK_OUTPUT_BLOCKS": "1",
}

# This patched, cache-only scorer is for demonstration purposes and isn't an upstream
# router. It scores only KV cache overlap, ignoring prefill and decode load.
PURE_KV_CACHE_ENV = {**KV_TOKEN_AWARE_ENV, "DYN_ROUTER_CACHE_AFFINITY_ONLY": "1"}

ROUTER_VARIANTS = {
    "session-affinity": (
        LoggingConsistentHashRouter,
        {"num_virtual_nodes": 100, "num_fallback_replicas": 0},
        {},
    ),
    "pure-kv-cache": (LoggingKVAwareRouter, {}, PURE_KV_CACHE_ENV),
    "kv-token-aware": (LoggingKVAwareRouter, {}, KV_TOKEN_AWARE_ENV),
}
