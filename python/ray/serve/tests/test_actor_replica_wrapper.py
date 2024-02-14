import pickle
import sys
from typing import Tuple, Union

import pytest

import ray
from ray import ObjectRef, ObjectRefGenerator
from ray.actor import ActorHandle
from ray.serve._private.common import (
    ReplicaQueueLengthInfo,
    RequestMetadata,
    RunningReplicaInfo,
)
from ray.serve._private.replica_scheduler.common import (
    ActorReplicaWrapper,
    PendingRequest,
)


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
                deployment_name="fake_deployment",
                replica_tag="fake_replica",
                node_id=None,
                availability_zone=None,
                actor_handle=actor_handle,
                max_concurrent_queries=10,
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
            endpoint="123",
            is_streaming=is_streaming,
        ),
    )
    obj_ref_or_gen = replica.send_request(pr)
    if is_streaming:
        assert isinstance(obj_ref_or_gen, ObjectRefGenerator)
        for i in range(5):
            next_obj_ref = await obj_ref_or_gen.__anext__()
            assert await next_obj_ref == f"Hello-{i}"
    else:
        assert isinstance(obj_ref_or_gen, ObjectRef)
        assert await obj_ref_or_gen == "Hello"


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
            endpoint="123",
            is_streaming=is_streaming,
        ),
    )
    obj_ref_or_gen, info = await replica.send_request_with_rejection(pr)
    assert info.accepted == accepted
    assert info.num_ongoing_requests == 10
    if not accepted:
        assert obj_ref_or_gen is None
    elif is_streaming:
        assert isinstance(obj_ref_or_gen, ObjectRefGenerator)
        for i in range(5):
            next_obj_ref = await obj_ref_or_gen.__anext__()
            assert await next_obj_ref == f"Hello-{i}"
    else:
        assert isinstance(obj_ref_or_gen, ObjectRef)
        assert await obj_ref_or_gen == "Hello"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
