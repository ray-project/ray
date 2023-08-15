"""
Unit tests for the router class. Please don't add any test that will involve
controller or the actual replica wrapper, use mock if necessary.
"""
import asyncio
import copy

import pytest

import ray
from ray._private.utils import get_or_create_event_loop
from ray.serve._private.common import RunningReplicaInfo, RequestProtocol
from ray.serve._private.router import Query, RoundRobinReplicaScheduler, RequestMetadata
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


async def test_replica_scheduler(ray_instance):
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
    rs = RoundRobinReplicaScheduler(get_or_create_event_loop())
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
    third_ref_pending_task = get_or_create_event_loop().create_task(
        rs.assign_replica(query)
    )
    # We should fail to assign a replica, so this coroutine should still be
    # pending after some time.
    await asyncio.sleep(0.2)
    assert not third_ref_pending_task.done()

    # Let's make sure in flight queries is 1 for each replica.
    assert len(rs.in_flight_queries[replicas[0]]) == 1
    assert len(rs.in_flight_queries[replicas[1]]) == 1

    # Let's copy a new RunningReplicaInfo object and update the router
    cur_replicas_info = list(rs.in_flight_queries.keys())
    replicas = copy.deepcopy(cur_replicas_info)
    assert id(replicas[0].actor_handle) != id(cur_replicas_info[0].actor_handle)
    assert replicas[0].replica_tag == cur_replicas_info[0].replica_tag
    assert id(replicas[1].actor_handle) != id(cur_replicas_info[1].actor_handle)
    assert replicas[1].replica_tag == cur_replicas_info[1].replica_tag
    rs.update_running_replicas(replicas)

    # Let's make sure in flight queries is 1 for each replica even if replicas update
    assert len(rs.in_flight_queries[replicas[0]]) == 1
    assert len(rs.in_flight_queries[replicas[1]]) == 1

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


def test_request_metadata():
    """Test logic on RequestMetadata

    Ensure the default values are set correctly and both is_http_request and
    is_grpc_request returns the correct value. When the request_protocol is set to HTTP,
    is_http_request should return True and is_grpc_request should return False. When the
    request_protocol is set to gRPC, is_http_request should return False and
    is_grpc_request should return True.
    """
    request_id = "request-id"
    endpoint = "endpoint"
    request_metadata = RequestMetadata(request_id=request_id, endpoint=endpoint)

    # Ensure the default values are set correctly.
    assert request_metadata.request_id == request_id
    assert request_metadata.endpoint == endpoint
    assert request_metadata.call_method == "__call__"
    assert request_metadata.route == ""
    assert request_metadata.app_name == ""
    assert request_metadata.multiplexed_model_id == ""
    assert request_metadata.is_streaming is False
    assert request_metadata.request_protocol == RequestProtocol.UNDEFINED
    assert request_metadata.is_http_request is False
    assert request_metadata.is_grpc_request is False

    # is_http_request and is_grpc_request returns the correct values when the
    # request_protocol is set to HTTP.
    request_metadata.request_protocol = RequestProtocol.HTTP
    assert request_metadata.is_http_request is True
    assert request_metadata.is_grpc_request is False

    # is_http_request and is_grpc_request returns the correct values when the
    # request_protocol is set to gRPC.
    request_metadata.request_protocol = RequestProtocol.GRPC
    assert request_metadata.is_http_request is False
    assert request_metadata.is_grpc_request is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
