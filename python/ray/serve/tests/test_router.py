"""
Unit tests for the router class. Please don't add any test that will involve
controller or the actual replica wrapper, use mock if necessary.
"""
import asyncio

import pytest

import ray
from ray.serve._private.common import RunningReplicaInfo
from ray.serve._private.router import Query, ReplicaSet, RequestMetadata
from ray._private.test_utils import SignalActor

pytestmark = pytest.mark.asyncio


@pytest.fixture
def ray_instance():
    # Note(simon):
    # This line should be not turned on on master because it leads to very
    # spammy and not useful log in case of a failure in CI.
    # To run locally, please use this instead.
    # SERVE_DEBUG_LOG=1 pytest -v -s test_api.py
    # os.environ["SERVE_DEBUG_LOG"] = "1" <- Do not uncomment this.
    ray.init(num_cpus=16)
    yield
    ray.shutdown()


def mock_task_runner():
    @ray.remote(num_cpus=0)
    class TaskRunnerMock:
        def __init__(self):
            self.query = None
            self.queries = []

        @ray.method(num_returns=2)
        async def handle_request(self, request_metadata, *args, **kwargs):
            self.query = Query(args, kwargs, request_metadata)
            self.queries.append(self.query)
            return b"", "DONE"

        def get_recent_call(self):
            return self.query

        def get_all_calls(self):
            return self.queries

        def clear_calls(self):
            self.queries = []

        async def reconfigure(self, user_config):
            return

    return TaskRunnerMock.remote()


@pytest.fixture
def task_runner_mock_actor():
    yield mock_task_runner()


async def test_replica_set(ray_instance):
    signal = SignalActor.remote()

    @ray.remote(num_cpus=0)
    class MockWorker:
        _num_queries = 0

        @ray.method(num_returns=2)
        async def handle_request(self, request):
            self._num_queries += 1
            await signal.wait.remote()
            return b"", "DONE"

        async def num_queries(self):
            return self._num_queries

    # We will test a scenario with two replicas in the replica set.
    rs = ReplicaSet(
        "my_deployment",
        asyncio.get_event_loop(),
    )
    replicas = [
        RunningReplicaInfo(
            deployment_name="my_deployment",
            replica_tag=str(i),
            actor_handle=MockWorker.remote(),
            max_concurrent_queries=1,
        )
        for i in range(2)
    ]
    rs.update_running_replicas(replicas)

    # Send two queries. They should go through the router but blocked by signal
    # actors.
    query = Query([], {}, RequestMetadata("request-id", "endpoint"))
    first_ref = await rs.assign_replica(query)
    second_ref = await rs.assign_replica(query)

    # These should be blocked by signal actor.
    with pytest.raises(ray.exceptions.GetTimeoutError):
        ray.get([first_ref, second_ref], timeout=1)

    # Each replica should have exactly one inflight query. Let make sure the
    # queries arrived there.
    for replica in replicas:
        while await replica.actor_handle.num_queries.remote() != 1:
            await asyncio.sleep(1)

    # Let's try to send another query.
    third_ref_pending_task = asyncio.get_event_loop().create_task(
        rs.assign_replica(query)
    )
    # We should fail to assign a replica, so this coroutine should still be
    # pending after some time.
    await asyncio.sleep(0.2)
    assert not third_ref_pending_task.done()

    # Let's unblock the two replicas
    await signal.send.remote()
    assert await first_ref == "DONE"
    assert await second_ref == "DONE"

    # The third request should be unblocked and sent to first replica.
    # This meas we should be able to get the object ref.
    third_ref = await third_ref_pending_task

    # Now we got the object ref, let's get it result.
    await signal.send.remote()
    assert await third_ref == "DONE"

    # Finally, make sure that one of the replica processed the third query.
    num_queries_set = {
        (await replica.actor_handle.num_queries.remote()) for replica in replicas
    }
    assert num_queries_set == {2, 1}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
