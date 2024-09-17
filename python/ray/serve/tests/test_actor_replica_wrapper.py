import asyncio
import pickle
import sys
from typing import Tuple, Union

import pytest

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray._private.test_utils import SignalActor
from ray._private.utils import get_or_create_event_loop
from ray.actor import ActorHandle
from ray.serve._private.common import (
    DeploymentID,
    ReplicaID,
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.replica_scheduler.common import PendingRequest
from ray.serve._private.replica_scheduler.replica_wrapper import ActorReplicaWrapper
from ray.serve._private.test_utils import send_signal_on_cancellation


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
            await executing_signal_actor.send.remote()
            await send_signal_on_cancellation(cancelled_signal_actor)
            return

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
def setup_fake_replica(ray_instance) -> Tuple[ActorReplicaWrapper, ActorHandle]:
    actor_handle = FakeReplicaActor.remote()
    return (
        ActorReplicaWrapper(
            RunningReplicaInfo(
                ReplicaID(
                    "fake_replica", deployment_id=DeploymentID(name="fake_deployment")
                ),
                node_id=None,
                node_ip=None,
                availability_zone=None,
                actor_handle=actor_handle,
                max_ongoing_requests=10,
                is_cross_language=False,
            )
        ),
        actor_handle,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request(setup_fake_replica, is_streaming: bool):
    replica, _ = setup_fake_replica

    pr = PendingRequest(
        args=["Hello"],
        kwargs={"is_streaming": is_streaming},
        metadata=RequestMetadata(
            request_id="abc",
            internal_request_id="def",
            endpoint="123",
            is_streaming=is_streaming,
        ),
    )
    replica_result = replica.send_request(pr)
    if is_streaming:
        assert isinstance(replica_result.obj_ref_gen, ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.obj_ref, ObjectRef)
        assert await replica_result.get_async() == "Hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
@pytest.mark.parametrize("is_streaming", [False, True])
async def test_send_request_with_rejection(
    setup_fake_replica, accepted: bool, is_streaming: bool
):
    replica, actor_handle = setup_fake_replica
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
            endpoint="123",
            is_streaming=is_streaming,
        ),
    )
    replica_result, info = await replica.send_request_with_rejection(pr)
    assert info.accepted == accepted
    assert info.num_ongoing_requests == 10
    if not accepted:
        assert replica_result is None
    elif is_streaming:
        assert isinstance(replica_result.obj_ref_gen, ObjectRefGenerator)
        for i in range(5):
            assert await replica_result.__anext__() == f"Hello-{i}"
    else:
        assert isinstance(replica_result.obj_ref_gen, ObjectRefGenerator)
        assert await replica_result.__anext__() == "Hello"


@pytest.mark.asyncio
async def test_send_request_with_rejection_cancellation(setup_fake_replica):
    """
    Verify that the downstream actor method call is cancelled if the call to send the
    request to the replica is cancelled.
    """
    replica, actor_handle = setup_fake_replica

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
            endpoint="123",
        ),
    )

    # Send request should hang because the downstream actor method call blocks
    # before sending the system message.
    send_request_task = get_or_create_event_loop().create_task(
        replica.send_request_with_rejection(pr)
    )

    # Check that the downstream actor method call has started.
    await executing_signal_actor.wait.remote()

    _, pending = await asyncio.wait([send_request_task], timeout=0.001)
    assert len(pending) == 1

    # Cancel the task. This should cause the downstream actor method call to
    # be cancelled (verified via signal actor).
    send_request_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await send_request_task

    await cancelled_signal_actor.wait.remote()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
