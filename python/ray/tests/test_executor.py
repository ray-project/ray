import os
import sys
import pytest
from ray.util.ray_executor import RayExecutor
import time
from concurrent.futures._base import TimeoutError
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def test_remote_function_runs_on_local_instance():
    with RayExecutor() as ex:
        result = ex.submit(lambda x: x * x, 100).result()
        assert result == 10_000


def test_remote_function_runs_multiple_tasks_on_local_instance():
    with RayExecutor() as ex:
        result0 = ex.submit(lambda x: x * x, 100).result()
        result1 = ex.submit(lambda x: x * x, 100).result()
        assert result0 == result1 == 10_000


def test_remote_function_runs_on_local_instance_with_map():
    with RayExecutor() as ex:
        futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
        for result in futures_iter:
            assert result == 10_000


def test_remote_function_map_using_max_workers():
    with RayExecutor(max_workers=3) as ex:
        futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
        for result in futures_iter:
            assert result == 10_000


def test_remote_function_runs_on_specified_instance(call_ray_start):
    with RayExecutor(address=call_ray_start) as ex:
        result = ex.submit(lambda x: x * x, 100).result()
        assert result == 10_000
        assert ex.context.address_info["address"] == call_ray_start


def test_remote_function_runs_on_specified_instance_with_map(call_ray_start):
    with RayExecutor(address=call_ray_start) as ex:
        futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
        for result in futures_iter:
            assert result == 10_000
        assert ex.context.address_info["address"] == call_ray_start


def test_map_times_out():
    with RayExecutor() as ex:
        i0 = ex.map(lambda x: time.sleep(x), [2])
        i0.__next__()
        i1 = ex.map(lambda x: time.sleep(x), [2], timeout=1)
        with pytest.raises(TimeoutError):
            i1.__next__()


def test_remote_function_runs_multiple_tasks_using_max_workers():
    with RayExecutor(max_workers=2) as ex:
        result0 = ex.submit(lambda x: x * x, 100).result()
        result1 = ex.submit(lambda x: x * x, 100).result()
        assert result0 == result1 == 10_000


def test_cannot_submit_after_shutdown():
    ex = RayExecutor()
    ex.submit(lambda: True).result()
    ex.shutdown()
    with pytest.raises(RuntimeError):
        ex.submit(lambda: True).result()


def test_cannot_map_after_shutdown():
    ex = RayExecutor()
    ex.submit(lambda: True).result()
    ex.shutdown()
    with pytest.raises(RuntimeError):
        ex.submit(lambda: True).result()


def test_pending_task_is_cancelled_after_shutdown():
    ex = RayExecutor()
    f = ex.submit(lambda: True)
    assert f._state == "PENDING"
    ex.shutdown(cancel_futures=True)
    assert f.cancelled()


def test_running_task_finishes_after_shutdown():
    ex = RayExecutor()
    f = ex.submit(lambda: True)
    assert f._state == "PENDING"
    f.set_running_or_notify_cancel()
    assert f.running()
    ex.shutdown(cancel_futures=True)
    assert f._state == "FINISHED"


def test_mixed_task_states_handled_by_shutdown():
    ex = RayExecutor()
    f0 = ex.submit(lambda: True)
    f1 = ex.submit(lambda: True)
    assert f0._state == "PENDING"
    assert f1._state == "PENDING"
    f0.set_running_or_notify_cancel()
    ex.shutdown(cancel_futures=True)
    assert f0._state == "FINISHED"
    assert f1.cancelled()


def test_with_syntax_invokes_shutdown():
    with RayExecutor() as ex:
        pass
    assert ex._shutdown_lock


# ----------------------------------------------------------------------------------------------------
# ThreadPool/ProcessPool comparison
# ----------------------------------------------------------------------------------------------------
def f_process(x):
    return len([i for i in range(x) if i % 2 == 0])


def test_conformity_with_processpool():
    with RayExecutor() as ex:
        ray_result = ex.submit(f_process, 100).result()
    with ProcessPoolExecutor() as ppe:
        ppe_result = ppe.submit(f_process, 100).result()
    assert type(ray_result) == type(ppe_result)
    assert ray_result == ppe_result


def test_conformity_with_processpool_map():
    with RayExecutor() as ex:
        ray_iter = ex.map(f_process, range(10))
        ray_result = list(ray_iter)
    with ProcessPoolExecutor() as ppe:
        ppe_iter = ppe.map(f_process, range(10))
        ppe_result = list(ppe_iter)
    assert hasattr(ray_iter, "__iter__")
    assert hasattr(ray_iter, "__next__")
    assert hasattr(ppe_iter, "__iter__")
    assert hasattr(ppe_iter, "__next__")
    assert type(ray_result) == type(ppe_result)
    assert sorted(ray_result) == sorted(ppe_result)


def test_conformity_with_threadpool():
    with RayExecutor() as ex:
        ray_result = ex.submit(lambda x: len([i for i in range(x) if i % 2 == 0]), 100)
    with ThreadPoolExecutor() as tpe:
        tpe_result = tpe.submit(lambda x: len([i for i in range(x) if i % 2 == 0]), 100)
    assert type(ray_result) == type(tpe_result)
    assert ray_result.result() == tpe_result.result()


def test_conformity_with_threadpool_map():
    with RayExecutor() as ex:
        ray_iter = ex.map(f_process, range(10))
        ray_result = list(ray_iter)
    with ThreadPoolExecutor() as tpe:
        tpe_iter = tpe.map(f_process, range(10))
        tpe_result = list(tpe_iter)
    assert hasattr(ray_iter, "__iter__")
    assert hasattr(ray_iter, "__next__")
    assert hasattr(tpe_iter, "__iter__")
    assert hasattr(tpe_iter, "__next__")
    assert type(ray_result) == type(tpe_result)
    assert sorted(ray_result) == sorted(tpe_result)


def test_conformity_with_processpool_using_max_workers():
    with RayExecutor(max_workers=2) as ex:
        ray_result = ex.submit(f_process, 100).result()
    with ProcessPoolExecutor(max_workers=2) as ppe:
        ppe_result = ppe.submit(f_process, 100).result()
    assert type(ray_result) == type(ppe_result)
    assert ray_result == ppe_result


def test_conformity_with_processpool_map_using_max_workers():
    with RayExecutor(max_workers=2) as ex:
        ray_iter = ex.map(f_process, range(10))
        ray_result = list(ray_iter)
    with ProcessPoolExecutor(max_workers=2) as ppe:
        ppe_iter = ppe.map(f_process, range(10))
        ppe_result = list(ppe_iter)
    assert hasattr(ray_iter, "__iter__")
    assert hasattr(ray_iter, "__next__")
    assert hasattr(ppe_iter, "__iter__")
    assert hasattr(ppe_iter, "__next__")
    assert type(ray_result) == type(ppe_result)
    assert sorted(ray_result) == sorted(ppe_result)


def test_conformity_with_threadpool_using_max_workers():
    with RayExecutor(max_workers=2) as ex:
        ray_result = ex.submit(lambda x: len([i for i in range(x) if i % 2 == 0]), 100)
    with ThreadPoolExecutor(max_workers=2) as tpe:
        tpe_result = tpe.submit(lambda x: len([i for i in range(x) if i % 2 == 0]), 100)
    assert type(ray_result) == type(tpe_result)
    assert ray_result.result() == tpe_result.result()


def test_conformity_with_threadpool_map_using_max_workers():
    with RayExecutor(max_workers=2) as ex:
        ray_iter = ex.map(f_process, range(10))
        ray_result = list(ray_iter)
    with ThreadPoolExecutor(max_workers=2) as tpe:
        tpe_iter = tpe.map(f_process, range(10))
        tpe_result = list(tpe_iter)
    assert hasattr(ray_iter, "__iter__")
    assert hasattr(ray_iter, "__next__")
    assert hasattr(tpe_iter, "__iter__")
    assert hasattr(tpe_iter, "__next__")
    assert type(ray_result) == type(tpe_result)
    assert sorted(ray_result) == sorted(tpe_result)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
