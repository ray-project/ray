import ray
from ray.experimental.serve.queues import CentralizedQueues


def test_single_prod_cons_queue(serve_instance):
    q = CentralizedQueues()
    q.link("svc", "backend")

    result_oid = q.produce("svc", 1)
    work_oid = q.consume("backend")
    got_work = ray.get(ray.ObjectID(work_oid))
    assert got_work.request_body == 1

    ray.worker.global_worker.put_object(got_work.result_oid, 2)
    assert ray.get(ray.ObjectID(result_oid)) == 2


def test_alter_backend(serve_instance):
    q = CentralizedQueues()

    result_oid = q.produce("svc", 1)
    work_oid = q.consume("backend-1")
    q.set_traffic("svc", {"backend-1": 1})
    got_work = ray.get(ray.ObjectID(work_oid))
    assert got_work.request_body == 1
    ray.worker.global_worker.put_object(got_work.result_oid, 2)
    assert ray.get(ray.ObjectID(result_oid)) == 2

    result_oid = q.produce("svc", 1)
    work_oid = q.consume("backend-2")
    q.set_traffic("svc", {"backend-2": 1})
    got_work = ray.get(ray.ObjectID(work_oid))
    assert got_work.request_body == 1
    ray.worker.global_worker.put_object(got_work.result_oid, 2)
    assert ray.get(ray.ObjectID(result_oid)) == 2


def test_split_traffic(serve_instance):
    q = CentralizedQueues()

    q.produce("svc", 1)
    q.produce("svc", 1)
    q.set_traffic("svc", {})
    work_oid_1 = q.consume("backend-1")
    work_oid_2 = q.consume("backend-2")
    q.set_traffic("svc", {"backend-1": 0.5, "backend-2": 0.5})

    got_work = ray.get([ray.ObjectID(work_oid_1), ray.ObjectID(work_oid_2)])
    assert [g.request_body for g in got_work] == [1, 1]


def test_probabilities(serve_instance):
    q = CentralizedQueues()

    [q.produce("svc", 1) for i in range(100)]

    work_oid_1_s = [ray.ObjectID(q.consume("backend-1")) for i in range(100)]
    work_oid_2_s = [ray.ObjectID(q.consume("backend-2")) for i in range(100)]

    q.set_traffic("svc", {"backend-1": 0.1, "backend-2": 0.9})

    backend_1_ready_oids, _ = ray.wait(
        work_oid_1_s, num_returns=100, timeout=0.0)
    backend_2_ready_oids, _ = ray.wait(
        work_oid_2_s, num_returns=100, timeout=0.0)
    assert len(backend_1_ready_oids) < len(backend_2_ready_oids)
