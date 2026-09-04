"""Routers and auditable scoring settings for the CC benchmark."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Optional, Sequence

from ray.serve._private.request_router import PendingRequest, RunningReplica
from ray.serve.experimental.consistent_hash_router import ConsistentHashRouter
from ray.serve.llm.request_router import KVAwareRouter


class RoutingLogMixin:
    """Record the selected replica for each workload request."""

    LOG_DIR: Optional[str] = None

    def _ensure_log(self) -> None:
        if getattr(self, "_routing_log_ready", False):
            return
        self._routing_log_ready = True
        self._routing_log_failed = False
        self._routing_log_file = None
        self._routing_log_lock = threading.Lock()
        if not self.LOG_DIR:
            return
        try:
            os.makedirs(self.LOG_DIR, exist_ok=True)
            path = os.path.join(self.LOG_DIR, f"routing.{os.getpid()}.jsonl")
            self._routing_log_file = open(path, "a", buffering=1)
        except OSError:
            self._routing_log_failed = True

    async def choose_replicas(
        self,
        candidate_replicas: list[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> Sequence[list[RunningReplica]]:
        self._ensure_log()
        started = time.perf_counter()
        result = await super().choose_replicas(  # type: ignore[misc]
            candidate_replicas, pending_request
        )
        if self._routing_log_file is None or self._routing_log_failed:
            return result

        try:
            replica_id = None
            if result and result[0]:
                replica_id = result[0][0].replica_id.unique_id
            metadata = pending_request.metadata if pending_request is not None else None
            kwargs: dict[str, Any] = (
                getattr(pending_request, "kwargs", {}) if pending_request is not None else {}
            )
            token_ids = kwargs.get("request_token_ids")
            row = {
                "ts": time.time(),
                "session_id": getattr(metadata, "session_id", None),
                "request_id": getattr(metadata, "request_id", None),
                "replica_id": replica_id,
                "n_candidates": len(candidate_replicas),
                "decision_us": round((time.perf_counter() - started) * 1e6, 1),
                "kv_token_count": len(token_ids) if token_ids else None,
                "kv_tracker_present": (
                    getattr(self, "_kv_token_tracker", None) is not None
                    if isinstance(self, KVAwareRouter)
                    else None
                ),
            }
            with self._routing_log_lock:
                self._routing_log_file.write(json.dumps(row) + "\n")
        except (AttributeError, OSError, TypeError):
            self._routing_log_failed = True
        return result


class LoggingKVAwareRouter(RoutingLogMixin, KVAwareRouter):
    """KVAwareRouter with route-decision logging."""


class LoggingConsistentHashRouter(RoutingLogMixin, ConsistentHashRouter):
    """ConsistentHashRouter with route-decision logging."""


KV_BALANCED_ENV = {
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT": "0.5",
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT_DECAY": "1.0",
    "DYN_ROUTER_PREFILL_LOAD_SCALE": "1.0",
    "DYN_ROUTER_DECODE_ACTIVE_REQUEST_WEIGHT": "32",
    "DYN_ROUTER_TEMPERATURE": "0.0",
}

KV_CACHE_ENV = {
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT": "2.0",
    "DYN_ROUTER_KV_OVERLAP_SCORE_CREDIT_DECAY": "0.0",
    "DYN_ROUTER_PREFILL_LOAD_SCALE": "1.0",
    "DYN_ROUTER_DECODE_ACTIVE_REQUEST_WEIGHT": "0.0",
    "DYN_ROUTER_TEMPERATURE": "0.0",
}


ROUTER_VARIANTS = {
    "session-affinity": (
        LoggingConsistentHashRouter,
        {"num_virtual_nodes": 100, "num_fallback_replicas": 0},
        {},
    ),
    "kv-token-aware-balanced": (LoggingKVAwareRouter, {}, KV_BALANCED_ENV),
    "kv-token-aware-kv-biased": (LoggingKVAwareRouter, {}, KV_CACHE_ENV),
}
