import asyncio
import pickle
import sys
from types import SimpleNamespace
from typing import Union

import pytest

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray._common.test_utils import SignalActor, async_wait_for_condition
from ray._common.utils import get_or_create_event_loop
from ray.exceptions import ActorDiedError, ActorUnavailableError, TaskCancelledError
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.constants import SERVE_NAMESPACE
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.test_utils import send_signal_on_cancellation
from ray.serve._private.utils import Semaphore


class _IntMetricsManager:
    """Minimal metrics manager that tracks only the in-flight count."""

    def __init__(self):
        self._n = 0

    def get_num_ongoing_requests(self):
        return self._n

    def inc_num_ongoing_requests(self, _):
        self._n += 1

    def dec_num_ongoing_requests(self, _):
        self._n -= 1


@ray.remote(num_cpus=0)
class SlotReservationActor:
    """Ray actor wrapping the real Replica.reserve_slot / release_slot.

    Used by integration tests that need production slot-reservation logic
    running under Ray's actor concurrency model — unit tests share one event
    loop and can't observe sync/async ordering on a real ReplicaActor.
    """

    def __init__(self, max_ongoing_requests: int):
        from ray.serve._private.replica import Replica

        replica = Replica.__new__(Replica)
        replica._deployment_config = SimpleNamespace(
            max_ongoing_requests=max_ongoing_requests
        )
        replica._reserved_slots = set()
        replica._semaphore = Semaphore(lambda: max_ongoing_requests)
        replica._metrics_manager = _IntMetricsManager()
        self._replica = replica

    async def reserve_slot(self, request_metadata, slot_token: str):
        return await self._replica.reserve_slot(request_metadata, slot_token)

    def release_slot(self, slot_token: str):
        return self._replica.release_slot(slot_token)

    def get_num_ongoing_requests(self) -> int:
        return self._replica.get_num_ongoing_requests()


@ray.remote(num_cpus=0)
class BlockingReserveActor:
    """Actor whose reserve_slot blocks on a SignalActor.

    Records every release_slot token it receives so a test can verify the
    cancellation cleanup path in RunningReplica.reserve_slot.
    """

    def __init__(self, signal_actor):
        self._signal = signal_actor
        self._released_tokens = []

    async def reserve_slot(self, request_metadata, slot_token: str):
        await self._signal.wait.remote()
        return True, 1

    def release_slot(self, slot_token: str):
        self._released_tokens.append(slot_token)
        return True, 0

    def get_released_tokens(self):
        return list(self._released_tokens)


@ray.remote(num_cpus=0)
class FakeReplicaActor:
    def __init__(self):
        self._replica_queue_length_info = None

    def set_replica_queue_length_info(self, info: ReplicaQueueLengthInfo):
        self._replica_queue_length_info = info

    async def handle_request(
        self,
        request_metadata: Union[bytes, RequestMetadata],
        message: str,
        *,
        is_streaming: bool,
    ):
        if isinstance(request_metadata, bytes):
            request_metadata = pickle.loads(request_metadata)

        assert not is_streaming and not request_metadata.is_streaming
        return message

    async def handle_request_streaming(
        self,
        request_metadata: Union[bytes, RequestMetadata],
        message: str,
        *,
        is_streaming: bool,
    ):
        if isinstance(request_metadata, bytes):
            request_metadata = pickle.loads(request_metadata)

        assert is_streaming and request_metadata.is_streaming
        for i in range(5):
            yield f"{message}-{i}"

    async def handle_request_with_rejection(
        self,
        pickled_request_metadata: bytes,
        *args,
        **kwargs,
    ):
        cancelled_signal_actor = kwargs.pop("cancelled_signal_actor", None)
        if cancelled_signal_actor is not None:
            executing_signal_actor = kwargs.pop("executing_signal_actor")
            async with send_signal_on_cancellation(cancelled_signal_actor):
                await executing_signal_actor.send.remote()

            return

        # Special case: if "raise_task_cancelled_error" is in kwargs, raise TaskCancelledError
        # This simulates the scenario where the underlying Ray task gets cancelled
        if kwargs.pop("raise_task_cancelled_error", False):
            raise TaskCancelledError()

        yield pickle.dumps(self._replica_queue_length_info)
        if not self._replica_queue_length_info.accepted:
            return

        request_metadata = pickle.loads(pickled_request_metadata)
        if request_metadata.is_streaming:
            async for result in self.handle_request_streaming(
                request_metadata, *args, **kwargs
            ):
                yield result
        else:
            yield await self.handle_request(request_metadata, *args, **kwargs)


@pytest.fixture
def setup_fake_replica(ray_instance) -> RunningReplica:
    replica_id = ReplicaID(
        "fake_replica", deployment_id=DeploymentID(name="fake_deployment")
    )
    actor_name = replica_id.to_full_id_str()
    # Create actor with a name so it can be retrieved by get_actor_handle()
    _ = FakeReplicaActor.options(
        name=actor_name, namespace=SERVE_NAMESPACE, lifetime="detached"
    ).remote()
    return RunningReplicaInfo(
        replica_id=replica_id,
        node_id=None,
        node_ip=None,
        availability_zone=None,
        actor_name=actor_name,
        max_ongoing_requests=10,
        is_cross_language=False,
    )


def test_update_replica_info_refreshes_backend_http_endpoint(setup_fake_replica):
    replica = RunningReplica(setup_fake_replica)
    assert replica.backend_http_endpoint is None

    updated_info = RunningReplicaInfo(
        replica_id=setup_fake_replica.replica_id,
        node_id=setup_fake_replica.node_id,
        node_ip="127.0.0.1",
        availability_zone=setup_fake_replica.availability_zone,
        actor_name=setup_fake_replica.actor_name,
        max_ongoing_requests=setup_fake_replica.max_ongoing_requests,
        is_cross_language=setup_fake_replica.is_cross_language,
        backend_http_port=8001,
    )

    replica.update_replica_info(updated_info)
    assert replica.backend_http_endpoint == ("127.0.0.1", 8001)


def test_backend_http_endpoint_requires_host_and_port(setup_fake_replica):
    replica = RunningReplica(setup_fake_replica)

    updated_info = RunningReplicaInfo(
        replica_id=setup_fake_replica.replica_id,
        node_id=setup_fake_replica.node_id,
        node_ip=None,
        availability_zone=setup_fake_replica.availability_zone,
        actor_name=setup_fake_replica.actor_name,
        max_ongoing_requests=setup_fake_replica.max_ongoing_requests,
        is_cross_language=setup_fake_replica.is_cross_language,
        backend_http_port=8001,
    )

    replica.update_replica_info(updated_info)
    assert replica.backend_http_endpoint is None


@pytest.mark.asyncio
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request_without_rejection(setup_fake_replica, is_streaming: bool):
    replica = RunningReplica(setup_fake_replica)

    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
        ),
    )
    replica_result = replica.try_send_request(pr, with_rejection=False)
    if is_streaming:
        assert isinstance(replica_result.to_object_ref_gen(), ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.to_object_ref(), ObjectRef)
        assert isinstance(await replica_result.to_object_ref_async(), ObjectRef)
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request_with_rejection(
    setup_fake_replica, accepted: bool, is_streaming: bool
):
    actor_handle = setup_fake_replica.get_actor_handle()
    replica = RunningReplica(setup_fake_replica)
    ray.get(
        actor_handle.set_replica_queue_length_info.remote(
            ReplicaQueueLengthInfo(accepted=accepted, num_ongoing_requests=10),
        )
    )

    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            is_streaming=is_streaming,
        ),
    )
    replica_result = replica.try_send_request(pr, with_rejection=True)
    info = await replica_result.get_rejection_response()
    assert info.accepted == accepted
    assert info.num_ongoing_requests == 10
    if not accepted:
        pass
    elif is_streaming:
        assert isinstance(replica_result.to_object_ref_gen(), ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.to_object_ref(), ObjectRef)
        assert isinstance(await replica_result.to_object_ref_async(), ObjectRef)
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
async def test_send_request_with_rejection_cancellation(setup_fake_replica):
    """
    Verify that the downstream actor method call is cancelled if the call to send the
    request to the replica is cancelled.
    """
    replica = RunningReplica(setup_fake_replica)

    executing_signal_actor = SignalActor.remote()
    cancelled_signal_actor = SignalActor.remote()

    pr = PendingRequest(
        args=["Hello"],
        kwargs={
            "cancelled_signal_actor": cancelled_signal_actor,
            "executing_signal_actor": executing_signal_actor,
        },
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
        ),
    )

    # Send request should hang because the downstream actor method call blocks
    # before sending the system message.
    replica_result = replica.try_send_request(pr, with_rejection=True)
    request_task = get_or_create_event_loop().create_task(
        replica_result.get_rejection_response()
    )

    # Check that the downstream actor method call has started.
    await executing_signal_actor.wait.remote()

    _, pending = await asyncio.wait([request_task], timeout=0.001)
    assert len(pending) == 1

    # Cancel the task. This should cause the downstream actor method call to
    # be cancelled (verified via signal actor).
    request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request_task

    await cancelled_signal_actor.wait.remote()


@pytest.mark.asyncio
async def test_send_request_with_rejection_task_cancelled_error(setup_fake_replica):
    """
    Test that TaskCancelledError from the underlying Ray task gets converted to
    asyncio.CancelledError when sending request with rejection.
    """
    actor_handle = setup_fake_replica.get_actor_handle()
    replica = RunningReplica(setup_fake_replica)

    # Set up the replica to accept the request
    ray.get(
        actor_handle.set_replica_queue_length_info.remote(
            ReplicaQueueLengthInfo(accepted=True, num_ongoing_requests=5),
        )
    )

    pr = PendingRequest(
        args=["Hello"],
        kwargs={
            "raise_task_cancelled_error": True
        },  # This will trigger TaskCancelledError
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
        ),
    )

    # The TaskCancelledError should be caught and converted to asyncio.CancelledError
    replica_result = replica.try_send_request(pr, with_rejection=True)
    with pytest.raises(asyncio.CancelledError):
        await replica_result.get_rejection_response()


def _spawn_running_replica(actor_cls, replica_id_str: str, *actor_args, **actor_kwargs):
    """Spawn a named actor and wrap it in a RunningReplica.

    Returns ``(running_replica, actor_handle)``. The actor must be created
    with the canonical replica-id name so RunningReplica can resolve it
    through its normal GCS lookup.
    """
    replica_id = ReplicaID(
        replica_id_str, deployment_id=DeploymentID(name="slot_reservation_test")
    )
    actor_name = replica_id.to_full_id_str()
    actor_handle = actor_cls.options(
        name=actor_name, namespace=SERVE_NAMESPACE, lifetime="detached"
    ).remote(*actor_args, **actor_kwargs)
    info = RunningReplicaInfo(
        replica_id=replica_id,
        node_id=None,
        node_ip=None,
        availability_zone=None,
        actor_name=actor_name,
        max_ongoing_requests=10,
        is_cross_language=False,
    )
    return RunningReplica(info), actor_handle


def _dummy_request_metadata() -> RequestMetadata:
    return RequestMetadata(request_id="abc", internal_request_id="def")


@pytest.mark.asyncio
async def test_reserve_slot_cancellation_releases_slot_on_actor(ray_instance):
    """If the awaiting reserve_slot task is cancelled, the wrapper must fire a
    follow-up release_slot.remote(token) so the actor doesn't leak the slot.
    """
    signal = SignalActor.remote()
    replica, actor = _spawn_running_replica(
        BlockingReserveActor, "blocking-replica", signal
    )

    task = get_or_create_event_loop().create_task(
        replica.reserve_slot(_dummy_request_metadata())
    )

    # Let the actor enter reserve_slot and start awaiting the signal.
    _, pending = await asyncio.wait([task], timeout=0.5)
    assert len(pending) == 1

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Unblock the actor so it can process the follow-up release_slot.remote().
    await signal.send.remote()

    # The wrapper's cancellation cleanup fires release_slot.remote(token)
    # without awaiting it; wait until the actor records the call.
    async def _release_received():
        return bool(await actor.get_released_tokens.remote())

    await async_wait_for_condition(_release_received, timeout=5)

    released_tokens = await actor.get_released_tokens.remote()
    assert len(released_tokens) == 1


@pytest.mark.asyncio
async def test_reserve_slot_propagates_actor_died_error(ray_instance):
    """If the replica actor is dead, RunningReplica.reserve_slot must raise
    ActorDiedError so AsyncioRouter.choose_replica can retry against another
    replica. ActorUnavailableError is also acceptable on the brief window
    before the actor failure has propagated.
    """
    replica, actor = _spawn_running_replica(
        SlotReservationActor, "doomed-replica", max_ongoing_requests=1
    )

    # Confirm liveness via a successful reservation first.
    _, info = await replica.reserve_slot(_dummy_request_metadata())
    assert info.accepted

    ray.kill(actor)

    with pytest.raises((ActorDiedError, ActorUnavailableError)):
        await replica.reserve_slot(_dummy_request_metadata())


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
