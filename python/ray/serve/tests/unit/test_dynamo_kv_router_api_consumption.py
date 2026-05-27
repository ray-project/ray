import asyncio
from dataclasses import dataclass

import pytest

from ray.serve._private.common import (
    DeploymentHandleSource,
    DeploymentID,
    ReplicaID,
    RequestMetadata,
)
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.request_router import RequestRouter


@dataclass
class FakeReplica:
    replica_id: ReplicaID


class FakeDynamoRouter:
    def __init__(self, ranked_workers):
        self.ranked_workers = ranked_workers
        self.rank_calls = []
        self.add_request_calls = []
        self.free_calls = []

    async def rank_workers(self, token_ids, *, allowed_worker_ids):
        self.rank_calls.append(
            {
                "token_ids": token_ids,
                "allowed_worker_ids": allowed_worker_ids,
            }
        )
        return [
            worker
            for worker in self.ranked_workers
            if worker["worker_id"] in allowed_worker_ids
        ]

    async def add_request(self, **kwargs):
        self.add_request_calls.append(kwargs)

    async def free(self, request_id):
        self.free_calls.append(request_id)


class DynamoApiConsumingRouter(RequestRouter):
    """Small Serve-style adapter proving the Dynamo Python API shape is usable."""

    def initialize_state(self, *, replica_to_worker, dynamo_router):
        self.replica_to_worker = replica_to_worker
        self.worker_to_replica = {
            worker_id: replica_id
            for replica_id, worker_id in self.replica_to_worker.items()
        }
        self.dynamo_router = dynamo_router
        self._pending_selection = {}

    async def choose_replicas(self, candidate_replicas, pending_request=None):
        allowed_worker_ids = [
            self.replica_to_worker[replica.replica_id]
            for replica in candidate_replicas
            if replica.replica_id in self.replica_to_worker
        ]
        rankings = await self.dynamo_router.rank_workers(
            pending_request.args[0],
            allowed_worker_ids=allowed_worker_ids,
        )

        replicas_by_id = {replica.replica_id: replica for replica in candidate_replicas}
        ranked_replicas = []
        for rank in rankings:
            replica_id = self.worker_to_replica.get(rank["worker_id"])
            if replica_id in replicas_by_id:
                ranked_replicas.append(replicas_by_id[replica_id])
                self._pending_selection.setdefault(
                    pending_request.metadata.internal_request_id,
                    {
                        "token_ids": pending_request.args[0],
                        "worker_id": rank["worker_id"],
                        "dp_rank": rank["dp_rank"],
                        "overlap_blocks": rank["overlap_blocks"],
                    },
                )

        return [[replica] for replica in ranked_replicas]

    async def book_request(self, request_id):
        selection = self._pending_selection[request_id]
        await self.dynamo_router.add_request(
            request_id=request_id,
            **selection,
        )

    def on_request_completed(self, replica_id, internal_request_id):
        asyncio.get_running_loop().create_task(
            self.dynamo_router.free(internal_request_id)
        )


def _replica_id(unique_id):
    return ReplicaID(unique_id, DeploymentID(name="llm"))


def _pending_request(token_ids, request_id="req-1"):
    return PendingRequest(
        args=[token_ids],
        kwargs={},
        metadata=RequestMetadata(
            request_id=request_id,
            internal_request_id=request_id,
        ),
    )


@pytest.mark.asyncio
async def test_serve_router_uses_ray_candidates_as_dynamo_allowed_workers():
    replica_a = FakeReplica(_replica_id("replica-a"))
    replica_b = FakeReplica(_replica_id("replica-b"))
    dynamo_router = FakeDynamoRouter(
        ranked_workers=[
            {
                "worker_id": 20,
                "dp_rank": 0,
                "overlap_blocks": 8,
                "potential_prefill_tokens": 16,
                "potential_decode_blocks": 1,
                "score": 5.0,
            },
            {
                "worker_id": 10,
                "dp_rank": 0,
                "overlap_blocks": 2,
                "potential_prefill_tokens": 64,
                "potential_decode_blocks": 0,
                "score": 16.0,
            },
        ]
    )
    router = DynamoApiConsumingRouter(
        deployment_id=DeploymentID(name="llm"),
        handle_source=DeploymentHandleSource.UNKNOWN,
    )
    router.initialize_state(
        replica_to_worker={
            replica_a.replica_id: 10,
            replica_b.replica_id: 20,
        },
        dynamo_router=dynamo_router,
    )

    pending_request = _pending_request([1, 2, 3, 4])
    ranked = await router.choose_replicas(
        [replica_a, replica_b],
        pending_request=pending_request,
    )

    assert ranked == [[replica_b], [replica_a]]
    assert dynamo_router.rank_calls == [
        {
            "token_ids": [1, 2, 3, 4],
            # Ray candidate replicas are the source of feasible worker membership.
            "allowed_worker_ids": [10, 20],
        }
    ]

    await router.book_request("req-1")
    assert dynamo_router.add_request_calls == [
        {
            "request_id": "req-1",
            "token_ids": [1, 2, 3, 4],
            "worker_id": 20,
            "dp_rank": 0,
            "overlap_blocks": 8,
        }
    ]

    router.on_request_completed(replica_b.replica_id, "req-1")
    await asyncio.sleep(0)
    assert dynamo_router.free_calls == ["req-1"]
