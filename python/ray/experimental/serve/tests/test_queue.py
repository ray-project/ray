import pytest
import ray
from ray.experimental.serve.queues import RandomPolicyQueue
from ray.experimental.serve.queues import (RoundRobinPolicyQueue,
                                           FixedPackingPolicyQueue)


@pytest.fixture(scope="session")
def task_runner_mock_actor():
    @ray.remote
    class TaskRunnerMock:
        def __init__(self):
            self.result = None

        def _ray_serve_call(self, request_item):
            self.result = request_item

        def get_recent_call(self):
            return self.result

    actor = TaskRunnerMock.remote()
    yield actor


def test_single_prod_cons_queue(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueue()
    q.link("svc", "backend")

    result_object_id = q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend", task_runner_mock_actor)
    got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
    assert got_work.request_args == 1
    assert got_work.request_kwargs == "kwargs"

    ray.worker.global_worker.put_object(2, got_work.result_object_id)
    assert ray.get(ray.ObjectID(result_object_id)) == 2


def test_slo(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueue()
    q.link("svc", "backend")

    for i in range(10):
        slo_ms = 1000 - 100 * i
        q.enqueue_request("svc", i, "kwargs", None, request_slo_ms=slo_ms)
    for i in range(10):
        q.dequeue_request("backend", task_runner_mock_actor)
        got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
        assert got_work.request_args == (9 - i)


def test_alter_backend(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueue()

    q.set_traffic("svc", {"backend-1": 1})
    result_object_id = q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend-1", task_runner_mock_actor)
    got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
    assert got_work.request_args == 1
    ray.worker.global_worker.put_object(2, got_work.result_object_id)
    assert ray.get(ray.ObjectID(result_object_id)) == 2

    q.set_traffic("svc", {"backend-2": 1})
    result_object_id = q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend-2", task_runner_mock_actor)
    got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
    assert got_work.request_args == 1
    ray.worker.global_worker.put_object(2, got_work.result_object_id)
    assert ray.get(ray.ObjectID(result_object_id)) == 2


def test_split_traffic(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueue()

    q.set_traffic("svc", {"backend-1": 0.5, "backend-2": 0.5})
    # assume 50% split, the probability of all 20 requests goes to a
    # single queue is 0.5^20 ~ 1-6
    for _ in range(20):
        q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend-1", task_runner_mock_actor)
    result_one = ray.get(task_runner_mock_actor.get_recent_call.remote())
    q.dequeue_request("backend-2", task_runner_mock_actor)
    result_two = ray.get(task_runner_mock_actor.get_recent_call.remote())

    got_work = [result_one, result_two]
    assert [g.request_args for g in got_work] == [1, 1]


def test_split_traffic_round_robin(serve_instance, task_runner_mock_actor):
    q = RoundRobinPolicyQueue()
    q.set_traffic("svc", {"backend-1": 0.5, "backend-2": 0.5})
    # since round robin policy is stateful firing two queries consecutively
    # would transfer the queries to two different backends
    for _ in range(2):
        q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend-1", task_runner_mock_actor)
    result_one = ray.get(task_runner_mock_actor.get_recent_call.remote())
    q.dequeue_request("backend-2", task_runner_mock_actor)
    result_two = ray.get(task_runner_mock_actor.get_recent_call.remote())

    got_work = [result_one, result_two]
    assert [g.request_args for g in got_work] == [1, 1]


def test_split_traffic_fixed_packing(serve_instance, task_runner_mock_actor):
    packing_num = 4
    q = FixedPackingPolicyQueue(packing_num=packing_num)
    q.set_traffic("svc", {"backend-1": 0.5, "backend-2": 0.5})

    # fire twice the number of queries as the packing number
    for i in range(2 * packing_num):
        q.enqueue_request("svc", i, "kwargs", None)

    # both the backends will get equal number of queries
    # as it is packed round robin
    for _ in range(packing_num):
        q.dequeue_request("backend-1", task_runner_mock_actor)

    result_one = ray.get(task_runner_mock_actor.get_recent_call.remote())

    for _ in range(packing_num):
        q.dequeue_request("backend-2", task_runner_mock_actor)

    result_two = ray.get(task_runner_mock_actor.get_recent_call.remote())

    got_work = [result_one, result_two]
    assert [g.request_args
            for g in got_work] == [packing_num - 1, 2 * packing_num - 1]


def test_queue_remove_replicas(serve_instance, task_runner_mock_actor):
    q = RandomPolicyQueue()
    q.dequeue_request("backend", task_runner_mock_actor)
    q.remove_and_destory_replica("backend", task_runner_mock_actor)
    assert len(q.workers["backend"]) == 0
