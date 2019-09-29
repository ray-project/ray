import ray
from ray.experimental.serve.queues import CentralizedQueues


def test_single_prod_cons_queue(serve_instance):
    q = CentralizedQueues()
    q.link("svc", "backend")

    result_object_id = q.enqueue_request("svc", 1)
    work_object_id = q.dequeue_request("backend")
    got_work = ray.get(ray.ObjectID(work_object_id))
    assert got_work.request_body == 1

    ray.worker.global_worker.put_object(got_work.result_object_id, 2)
    assert ray.get(ray.ObjectID(result_object_id)) == 2


def test_alter_backend(serve_instance):
    q = CentralizedQueues()

    result_object_id = q.enqueue_request("svc", 1)
    work_object_id = q.dequeue_request("backend-1")
    q.set_traffic("svc", {"backend-1": 1})
    got_work = ray.get(ray.ObjectID(work_object_id))
    assert got_work.request_body == 1
    ray.worker.global_worker.put_object(got_work.result_object_id, 2)
    assert ray.get(ray.ObjectID(result_object_id)) == 2

    result_object_id = q.enqueue_request("svc", 1)
    work_object_id = q.dequeue_request("backend-2")
    q.set_traffic("svc", {"backend-2": 1})
    got_work = ray.get(ray.ObjectID(work_object_id))
    assert got_work.request_body == 1
    ray.worker.global_worker.put_object(got_work.result_object_id, 2)
    assert ray.get(ray.ObjectID(result_object_id)) == 2


def test_split_traffic(serve_instance):
    q = CentralizedQueues()

    q.enqueue_request("svc", 1)
    q.enqueue_request("svc", 1)
    q.set_traffic("svc", {})
    work_object_id_1 = q.dequeue_request("backend-1")
    work_object_id_2 = q.dequeue_request("backend-2")
    q.set_traffic("svc", {"backend-1": 0.5, "backend-2": 0.5})

    got_work = ray.get(
        [ray.ObjectID(work_object_id_1),
         ray.ObjectID(work_object_id_2)])
    assert [g.request_body for g in got_work] == [1, 1]


def test_probabilities(serve_instance):
    q = CentralizedQueues()

    [q.enqueue_request("svc", 1) for i in range(100)]

    work_object_id_1_s = [
        ray.ObjectID(q.dequeue_request("backend-1")) for i in range(100)
    ]
    work_object_id_2_s = [
        ray.ObjectID(q.dequeue_request("backend-2")) for i in range(100)
    ]

    q.set_traffic("svc", {"backend-1": 0.1, "backend-2": 0.9})

    backend_1_ready_object_ids, _ = ray.wait(
        work_object_id_1_s, num_returns=100, timeout=0.0)
    backend_2_ready_object_ids, _ = ray.wait(
        work_object_id_2_s, num_returns=100, timeout=0.0)
    assert len(backend_1_ready_object_ids) < len(backend_2_ready_object_ids)
