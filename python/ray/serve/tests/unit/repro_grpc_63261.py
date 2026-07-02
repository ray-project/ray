#!/usr/bin/env python3
"""
Repro script for ray#63261: Router skips cache invalidation on gRPC
request failure.

Problem
-------
When a gRPC replica becomes unavailable, the ``gRPCReplicaResult``
done-callback receives the raw ``grpc.aio.Call`` object regardless of
outcome.  The router's ``_process_finished_request`` expects an
``ActorUnavailableError`` to trigger cache invalidation — without it stale
routes persist and requests are retried against dead replicas.

This script simulates the bug scenario using the controllable fake-call
test infrastructure.  It can be run standalone *without* a Ray cluster::

    python repro_grpc_63261.py

Expected output **before** the fix:

    BUG CONFIRMED: Router callback received Call (not ActorUnavailableError).
    Stale cache entries would NOT be invalidated.

Expected output **after** the fix:

    FIX VERIFIED: Router callback received ActorUnavailableError.
    Cache invalidation branch will fire correctly.
"""

import asyncio
import sys
from typing import Callable, List

import grpc

from ray import ActorID
from ray.exceptions import ActorUnavailableError
from ray.serve._private.common import RequestMetadata
from ray.serve._private.replica_result import gRPCReplicaResult


class ControllableFakeCall:
    """Fake grpc.aio.Call that lets the test trigger done-callback firing."""

    def __init__(self, *, exception: BaseException = None):
        self._loop = asyncio.get_running_loop()
        self._done = False
        self._exception = exception
        self._callbacks: List[Callable] = []

    def __getattr__(self, name):
        if name == "__aiter__":
            raise AttributeError(name)
        raise AttributeError(name)

    def add_done_callback(self, cb: Callable) -> None:
        if self._done:
            cb(self)
        else:
            self._callbacks.append(cb)

    def done(self) -> bool:
        return self._done

    def exception(self):
        if not self._done:
            raise asyncio.InvalidStateError("Call is not done.")
        return self._exception

    def cancelled(self) -> bool:
        return False

    def fire_done(self) -> None:
        self._done = True
        for cb in list(self._callbacks):
            cb(self)
        self._callbacks.clear()


def make_aio_rpc_error(code: grpc.StatusCode) -> grpc.aio.AioRpcError:
    return grpc.aio.AioRpcError(
        code=code,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="simulated replica unavailable",
    )


async def main():
    # Simulate a UNAVAILABLE gRPC call — the exact scenario that triggers the bug.
    fake_call = ControllableFakeCall(
        exception=make_aio_rpc_error(grpc.StatusCode.UNAVAILABLE)
    )

    result = gRPCReplicaResult(
        fake_call,
        metadata=RequestMetadata(
            request_id="repro-request",
            internal_request_id="repro-internal",
            is_streaming=False,
            _on_separate_loop=False,
        ),
        actor_id=ActorID(b"R" * 16),
        loop=asyncio.get_running_loop(),
    )

    # This is what the router does: register a done-callback and wait.
    received: List = []

    def router_callback(value):
        received.append(value)
        if isinstance(value, ActorUnavailableError):
            print(
                "\n✅ FIX VERIFIED: Router callback received " "ActorUnavailableError."
            )
            print("   Cache invalidation branch will fire correctly.")
        else:
            print(
                "\n🐛 BUG CONFIRMED: Router callback received "
                f"{type(value).__name__} (not ActorUnavailableError)."
            )
            print("   Stale cache entries would NOT be invalidated.")

    result.add_done_callback(router_callback)

    # Fire the done callback to simulate request completion.
    fake_call.fire_done()

    if not received:
        print("\n❌ NO CALLBACK FIRED — something is wrong.")
        return 1

    return 0 if isinstance(received[0], ActorUnavailableError) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
