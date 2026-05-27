import asyncio
from dataclasses import dataclass
from typing import Any, Dict, List

import pytest

from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.llm._internal.serve.routing_policies.dynamo_kv import (
    DynamoDirectStreamingLifecycleMiddleware,
    DynamoKVRequestRouter,
)


@dataclass
class FakeReplica:
    replica_id: ReplicaID
    dynamo_worker_id: int
    routing_stats: Dict[str, Any] = None


class FakeDynamoActor:
    def __init__(self):
        self.rank_calls = []
        self.start_calls = []
        self.prefill_calls = []
        self.finish_calls = []

    async def rank_and_prepare_route(
        self,
        request_body: bytes,
        body_truncated: bool,
        candidate_worker_ids: List[int],
        internal_request_id: str,
    ):
        self.rank_calls.append(
            {
                "request_body": request_body,
                "body_truncated": body_truncated,
                "candidate_worker_ids": candidate_worker_ids,
                "internal_request_id": internal_request_id,
            }
        )
        return [
            {
                "worker_id": 20,
                "dp_rank": 0,
                "overlap_blocks": 7,
                "score": 1.0,
                "dynamo_route_token": "route-token-20",
            },
            {
                "worker_id": 10,
                "dp_rank": 0,
                "overlap_blocks": 2,
                "score": 5.0,
                "dynamo_route_token": "route-token-10",
            },
        ]

    async def start_direct_request(self, route_token: str):
        self.start_calls.append(route_token)

    async def mark_prefill_complete(self, route_token: str):
        self.prefill_calls.append(route_token)

    async def finish_direct_request(self, route_token: str):
        self.finish_calls.append(route_token)


def _replica_id(unique_id: str) -> ReplicaID:
    return ReplicaID(unique_id, DeploymentID(name="llm"))


def _pending_request(body: bytes, request_id: str = "req-1") -> PendingRequest:
    return PendingRequest(
        args=[],
        kwargs={"request_body": body, "body_truncated": False},
        metadata=RequestMetadata(
            request_id=request_id,
            internal_request_id=request_id,
        ),
    )


@pytest.mark.asyncio
async def test_dynamo_router_ranks_ray_candidates_and_attaches_route_token():
    actor = FakeDynamoActor()
    router = DynamoKVRequestRouter(
        deployment_id=DeploymentID(name="llm"),
        handle_source=DeploymentHandleSource.UNKNOWN,
    )
    router.initialize_state(actor=actor)

    replica_a = FakeReplica(_replica_id("replica-a"), dynamo_worker_id=10)
    replica_b = FakeReplica(_replica_id("replica-b"), dynamo_worker_id=20)
    pending_request = _pending_request(b'{"prompt": "hello"}')

    ranked = await router.choose_replicas(
        [replica_a, replica_b],
        pending_request=pending_request,
    )

    assert ranked == [[replica_b], [replica_a]]
    assert actor.rank_calls == [
        {
            "request_body": b'{"prompt": "hello"}',
            "body_truncated": False,
            "candidate_worker_ids": [10, 20],
            "internal_request_id": "req-1",
        }
    ]
    assert router.get_selection_metadata(
        pending_request, replica_b.replica_id
    ) == {"dynamo_route_token": "route-token-20"}


@pytest.mark.asyncio
async def test_dynamo_lifecycle_middleware_marks_start_prefill_and_finish(monkeypatch):
    actor = FakeDynamoActor()
    monkeypatch.setattr(
        "ray.llm._internal.serve.routing_policies.dynamo_kv.ray.get_actor",
        lambda *args, **kwargs: actor,
    )

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send(
            {
                "type": "http.response.body",
                "body": b"data: first\n\n",
                "more_body": True,
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    middleware = DynamoDirectStreamingLifecycleMiddleware(app, "actor")
    sent = []
    scope = {
        "type": "http",
        "headers": [(b"x-ray-serve-dynamo-route-token", b"route-token-20")],
    }

    async def receive():
        await asyncio.sleep(0)
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await middleware(scope, receive, send)

    assert actor.start_calls == ["route-token-20"]
    assert actor.prefill_calls == ["route-token-20"]
    assert actor.finish_calls == ["route-token-20"]
    assert [message["type"] for message in sent] == [
        "http.response.start",
        "http.response.body",
        "http.response.body",
    ]
