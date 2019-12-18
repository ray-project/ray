import pytest
import ray
from ray.experimental.serve.queues import CentralizedQueues


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
    q = CentralizedQueues()
    q.link("svc", "backend")

    result_object_id = q.enqueue_request("svc", 1, "kwargs", None)
    q.dequeue_request("backend", task_runner_mock_actor)
    got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
    assert got_work.request_args == 1
    assert got_work.request_kwargs == "kwargs"

    ray.worker.global_worker.put_object(2, got_work.result_object_id)
    assert ray.get(ray.ObjectID(result_object_id)) == 2


def test_slo(serve_instance, task_runner_mock_actor):
    q = CentralizedQueues()
    q.link("svc", "backend")

    for i in range(10):
        slo_ms = 1000 - 100 * i
        q.enqueue_request("svc", i, "kwargs", None, request_slo_ms=slo_ms)
    for i in range(10):
        q.dequeue_request("backend", task_runner_mock_actor)
        got_work = ray.get(task_runner_mock_actor.get_recent_call.remote())
        assert got_work.request_args == (9 - i)


def test_alter_backend(serve_instance, task_runner_mock_actor):
    q = CentralizedQueues()

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
    q = CentralizedQueues()

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


def test_queue_remove_replicas(serve_instance, task_runner_mock_actor):
    q = CentralizedQueues()
    q.dequeue_request("backend", task_runner_mock_actor)
    q.remove_and_destory_replica("backend", task_runner_mock_actor)
    assert len(q.workers["backend"]) == 0
