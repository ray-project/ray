"""High-signal P/D direct-routing saga tests.

These tests exercise the router-owned pair state without requiring a model or a
GPU.  The pool doubles stand in for Dynamo's atomic ``select_and_reserve``
boundary and let the assertions focus on the invariants that are easy to break:
P gets overlap-aware selection, D gets its own non-overlap pending load policy,
and transfer-safe P completion releases P before D is activated.
"""

import time

import pytest

from ray.llm._internal.serve.routing_policies.kv_aware.pd_router import (
    PDPairTracker,
    PDTicketState,
)


class _Pool:
    def __init__(self, *, selected_worker, routes):
        self._replica_id_by_worker = {worker_id: str(worker_id) for worker_id in routes}
        self._selected_worker = selected_worker
        self._routes = routes
        self.select_calls = []
        self.events = []

    async def select_worker(
        self,
        request_id,
        token_ids,
        allowed_worker_ids,
        expected_output_tokens=None,
        router_config_override=None,
    ):
        self.select_calls.append(
            {
                "request_id": request_id,
                "token_ids": token_ids,
                "allowed": allowed_worker_ids,
                "expected_output_tokens": expected_output_tokens,
                "override": router_config_override,
            }
        )
        return {"worker_id": self._selected_worker}

    def get_replica_route(self, worker_id):
        return dict(self._routes[worker_id])

    def get_block_size(self):
        return 16

    async def on_prefill_complete(self, reservation_id):
        self.events.append(("prefill_complete", reservation_id))

    async def on_request_completed(self, reservation_id):
        self.events.append(("completed", reservation_id))

    async def on_decode_progress(self, reservation_id, output_tokens):
        self.events.append(("decode_progress", reservation_id, output_tokens))


def _tracker():
    # D worker 10 represents the lower-load decode choice; P worker 21
    # represents the higher-overlap prefill choice.  Each pool is independent.
    decode = _Pool(
        selected_worker=10,
        routes={
            10: {
                "replica_id": "d-low-load",
                "full_replica_id": "SERVE_REPLICA::app#Decode:d-low-load",
                "host": "10.0.0.10",
                "port": 9010,
                "token_endpoint": "tcp://10.0.0.10:7510",
            },
            11: {"replica_id": "d-busy"},
        },
    )
    prefill = _Pool(
        selected_worker=21,
        routes={
            20: {"replica_id": "p-no-overlap"},
            21: {
                "replica_id": "p-high-overlap",
                "token_endpoint": "tcp://10.0.0.21:7521",
            },
        },
    )
    tracker = PDPairTracker.__new__(PDPairTracker)
    tracker.prefill = prefill
    tracker.decode = decode
    tracker._ticket_ttl_s = 60
    tracker._pending_decode_load_scale = 1.0
    tracker._selection_policy = "kv_aware"
    tracker._next_prefill_index = 0
    tracker._next_decode_index = 0
    tracker._tickets_by_d_reservation = {}
    tracker._d_reservation_by_request = {}
    tracker._cleanup_task = None
    return tracker, prefill, decode


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_decode_load_scale", [0.5, 1.0, 2.0])
async def test_pair_selection_uses_overlap_aware_p_and_lower_load_d(
    pending_decode_load_scale,
):
    tracker, prefill, decode = _tracker()
    tracker._pending_decode_load_scale = pending_decode_load_scale

    ticket = await tracker.reserve_pair(
        request_id="request-1",
        prefill_token_ids=[1, 2, 3, 4],
        decode_token_ids=[101, 102, 103, 104],
        expected_output_tokens=64,
    )

    assert ticket.p_route["replica_id"] == "p-high-overlap"
    assert ticket.d_route["replica_id"] == "d-low-load"
    # D is selected first and explicitly gets no prefix credit.  Its pending
    # load is controlled by the P/D-only scale, independently of P's overlap.
    assert decode.select_calls[0]["token_ids"] == [101, 102, 103, 104]
    assert decode.select_calls[0]["override"] == {
        "assume_kv_reuse": False,
        "prefill_load_scale": pending_decode_load_scale,
    }
    assert decode.select_calls[0]["expected_output_tokens"] == 64
    assert prefill.select_calls[0]["token_ids"] == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_round_robin_policy_rotates_each_pd_pool_without_changing_tickets():
    tracker, prefill, decode = _tracker()
    tracker._selection_policy = "round_robin"

    first = await tracker.reserve_pair(
        request_id="rr-1",
        prefill_token_ids=[1],
        decode_token_ids=[1],
        expected_output_tokens=None,
    )
    second = await tracker.reserve_pair(
        request_id="rr-2",
        prefill_token_ids=[2],
        decode_token_ids=[2],
        expected_output_tokens=None,
    )

    assert decode.select_calls[0]["allowed"] == [10]
    assert decode.select_calls[1]["allowed"] == [11]
    assert prefill.select_calls[0]["allowed"] == [20]
    assert prefill.select_calls[1]["allowed"] == [21]
    assert first.d_reservation_id != second.d_reservation_id
    assert first.p_reservation_id != second.p_reservation_id


@pytest.mark.asyncio
async def test_prefill_completion_frees_p_then_activates_and_releases_d():
    tracker, prefill, decode = _tracker()
    ticket = await tracker.reserve_pair(
        request_id="request-2",
        prefill_token_ids=[1],
        decode_token_ids=[1],
        expected_output_tokens=None,
    )
    tracker.claim_prefill(
        d_reservation_id=ticket.d_reservation_id,
        p_reservation_id=ticket.p_reservation_id,
        d_replica_id="d-low-load",
        p_replica_id="p-high-overlap",
    )

    await tracker.prefill_complete(ticket.d_reservation_id)

    assert ticket.state is PDTicketState.DECODE_ACTIVE
    # The configured ticket TTL protects only the unclaimed capability.  An
    # active long decode uses the normal request-tracking leak guard instead.
    assert ticket.expires_at - time.monotonic() > 3500
    assert prefill.events == [
        ("prefill_complete", ticket.p_reservation_id),
        ("completed", ticket.p_reservation_id),
    ]
    assert decode.events == [("prefill_complete", ticket.d_reservation_id)]

    await tracker.release(ticket.d_reservation_id)

    assert prefill.events == [
        ("prefill_complete", ticket.p_reservation_id),
        ("completed", ticket.p_reservation_id),
    ]
    assert decode.events[-1] == ("completed", ticket.d_reservation_id)


@pytest.mark.asyncio
async def test_expired_ticket_is_cleaned_up_without_a_decode_dispatch():
    tracker, prefill, decode = _tracker()
    ticket = await tracker.reserve_pair(
        request_id="request-3",
        prefill_token_ids=[1],
        decode_token_ids=[1],
        expected_output_tokens=None,
    )
    ticket.expires_at = time.monotonic() - 1

    await tracker.evict_expired()

    assert ticket.d_reservation_id not in tracker._tickets_by_d_reservation
    assert ("completed", ticket.p_reservation_id) in prefill.events
    assert ("completed", ticket.d_reservation_id) in decode.events


@pytest.mark.asyncio
async def test_pair_selection_rejects_incompatible_kv_geometry_before_booking():
    tracker, prefill, decode = _tracker()
    prefill.get_block_size = lambda: 8
    decode.get_block_size = lambda: 16

    with pytest.raises(RuntimeError, match="KV block-size mismatch"):
        await tracker.reserve_pair(
            request_id="request-geometry",
            prefill_token_ids=[1],
            decode_token_ids=[1],
            expected_output_tokens=None,
        )

    assert not prefill.select_calls
    assert not decode.select_calls
