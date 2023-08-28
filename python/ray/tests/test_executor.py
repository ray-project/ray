import os
import sys
import pytest
from ray.util.concurrent.futures.ray_executor import RayExecutor, RoundRobinActorPool
import time
import ray
from ray.util.state import list_actors
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    TimeoutError as ConTimeoutError,
)

from ray._private.worker import RayContext

# ProcessPoolExecutor uses pickle which can only serialize top-level functions
def f_process1(x):
    return len([i for i in range(x) if i % 2 == 0])


class TestShared:

    # This class is for tests that do not need to be run with isolated ray instances

    def test_remote_function_runs_on_specified_instance(self, call_ray_start):
        with RayExecutor(address=call_ray_start) as ex:
            result = ex.submit(lambda x: x * x, 100).result()
            assert result == 10_000
            assert ex._context is not None
            assert type(ex._context) == RayContext
            assert ex._context.address_info["address"] == call_ray_start

    def test_remote_function_runs_on_specified_instance_with_map(self, call_ray_start):
        with RayExecutor(address=call_ray_start) as ex:
            futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
            for result in futures_iter:
                assert result == 10_000
            assert ex._context is not None
            assert type(ex._context) == RayContext
            assert ex._context.address_info["address"] == call_ray_start

    def test_round_robin_actor_pool_must_have_actor(self):
        with pytest.raises(ValueError):
            RoundRobinActorPool([])

    def test_round_robin_actor_pool_cycles_through_actors(self, call_ray_start):
        @ray.remote
        class ExecutorActor:
            def __init__(self) -> None:
                pass

            def actor_function(self, fn):
                return fn()
        actors = [ExecutorActor.options().remote() for _ in range(2)]
        pool = RoundRobinActorPool(actors)
        assert len(pool.pool) == 2
        assert pool.index == 0
        _ = pool.next()
        assert len(pool.pool) == 2
        assert pool.index == 1
        _ = pool.next()
        assert len(pool.pool) == 2
        assert pool.index == 0

    def test_round_robin_actor_pool_kills_actors(self, call_ray_start):
        @ray.remote
        class ExecutorActor:
            def __init__(self) -> None:
                pass

            def actor_function(self, fn):
                return fn()
        actors = [ExecutorActor.options().remote() for _ in range(2)]
        pool = RoundRobinActorPool(actors)
        assert len(pool.pool) == 2
        assert pool.index == 0
        actors = pool.pool

        def wait_actor_state(actors, state, timeout = 20):
            actor_ids = [i._ray_actor_id.hex() for i in actors]
            while timeout > 0:
                states = [actor_state for actor_state in list_actors() if actor_state.actor_id in actor_ids]
                if not all(i.state == state for i in states):
                    time.sleep(1)
                    timeout -= 1
                else:
                    break
            if timeout == 0:
                return False
            else:
                return True

        # wait for actors to live
        assert wait_actor_state(pool.pool, "ALIVE") == True
        pool.kill()
        # wait for actors to die
        assert wait_actor_state(pool.pool, "DEAD") == True





#----------------------------------------------------------------------------------------------------



class TestIsolated:

    # This class is for tests that must be run with dedicated/isolated ray
    # instances. It forces tests to be run in series and the individual test is
    # responsible for creating its own ray instances.

    @pytest.fixture(autouse=True)
    def _tear_down(self):
        yield None
        ray.shutdown()

    def test_remote_function_runs_on_local_instance(self):
        with RayExecutor() as ex:
            result = ex.submit(lambda x: x * x, 100).result()
            assert result == 10_000

    def test_reuse_existing_cluster(self):
        with RayExecutor() as ex0:
            c0 = ray.runtime_context.get_runtime_context()
            n0 = c0.get_node_id()
            with RayExecutor() as ex1:
                c1 = ray.runtime_context.get_runtime_context()
                n1 = c1.get_node_id()
                assert n0 == n1
                assert ex0._context is not None
                assert ex1._context is not None
                assert type(ex0._context) == RayContext
                assert type(ex1._context) == RayContext
                assert (
                    ex0._context.address_info["node_id"]
                    == ex1._context.address_info["node_id"]
                )

    def test_existing_instance_ignores_max_workers(self):
        _ = ray.init(num_cpus=1)
        with RayExecutor(max_workers=2):
            assert ray.available_resources()["CPU"] == 1

    def test_remote_function_runs_multiple_tasks_on_local_instance(self):
        with RayExecutor() as ex:
            result0 = ex.submit(lambda x: x * x, 100).result()
            result1 = ex.submit(lambda x: x * x, 100).result()
            assert result0 == result1 == 10_000

    def test_order_retained(self):
        def f(x, y):
            return x * y

        with RayExecutor() as ex:
            r0 = list(ex.map(f, [100, 100, 100], [1, 2, 3]))
        with RayExecutor(max_workers=2) as ex:
            r1 = list(ex.map(f, [100, 100, 100], [1, 2, 3]))
        assert r0 == r1

    def test_remote_function_runs_on_local_instance_with_map(self):
        with RayExecutor() as ex:
            futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
            for result in futures_iter:
                assert result == 10_000

    def test_map_zips_iterables(self):
        def f(x, y):
            return x * y

        with RayExecutor() as ex:
            futures_iter = ex.map(f, [100, 100, 100], [1, 2, 3])
            assert list(futures_iter) == [100, 200, 300]

    def test_remote_function_map_using_max_workers(self):
        with RayExecutor(max_workers=3) as ex:
            assert ex.actor_pool is not None
            assert len(ex.actor_pool.pool) == 3
            time_start = time.monotonic()
            _ = list(ex.map(lambda _: time.sleep(1), range(12)))
            time_end = time.monotonic()
            # we expect about (12*1) / 3 = 4 rounds
            delta = time_end - time_start
            assert delta > 3.0

    def test_results_are_not_accessible_after_shutdown(self):
        # note: this will hang indefinitely if no timeout is specified on map()
        def f(x, y):
            return x * y
        with RayExecutor() as ex:
            r1 = ex.map(f, [100, 100, 100], [1, 2, 3], timeout=2)
        assert ex._shutdown_lock
        with pytest.raises(ConTimeoutError):
            _ = list(r1)

    def test_remote_function_max_workers_same_result(self):
        with RayExecutor() as ex:
            f0 = list(ex.map(lambda x: x * x, range(12)))
        with RayExecutor(max_workers=1) as ex:
            f1 = list(ex.map(lambda x: x * x, range(12)))
        with RayExecutor(max_workers=3) as ex:
            f3 = list(ex.map(lambda x: x * x, range(12)))
        assert f0 == f1 == f3

    def test_map_times_out(self):
        def f(x):
            time.sleep(2)
            return x

        with RayExecutor() as ex:
            with pytest.raises(ConTimeoutError):
                i1 = ex.map(f, [1, 2, 3], timeout=1)
                for _ in i1:
                    pass

    def test_map_times_out_with_max_workers(self):
        def f(x):
            time.sleep(2)
            return x

        with RayExecutor(max_workers=2) as ex:
            with pytest.raises(ConTimeoutError):
                i1 = ex.map(f, [1, 2, 3], timeout=1)
                for _ in i1:
                    pass

    def test_remote_function_runs_multiple_tasks_using_max_workers(self):
        with RayExecutor(max_workers=2) as ex:
            result0 = ex.submit(lambda x: x * x, 100).result()
            result1 = ex.submit(lambda x: x * x, 100).result()
            assert result0 == result1 == 10_000

    def test_cannot_submit_after_shutdown(self):
        ex = RayExecutor()
        ex.submit(lambda: True).result()
        ex.shutdown()
        with pytest.raises(RuntimeError):
            ex.submit(lambda: True).result()

    def test_can_submit_after_shutdown(self):
        ex = RayExecutor(shutdown_ray=False)
        ex.submit(lambda: True).result()
        ex.shutdown()
        try:
            ex.submit(lambda: True).result()
        except RuntimeError:
            assert (
                False
            ), "Could not submit after calling shutdown() with shutdown_ray=False"
        ex.shutdown_ray = True
        ex.shutdown()

    def test_cannot_map_after_shutdown(self):
        ex = RayExecutor()
        ex.submit(lambda: True).result()
        ex.shutdown()
        with pytest.raises(RuntimeError):
            ex.submit(lambda: True).result()

    def test_pending_task_is_cancelled_after_shutdown(self):
        ex = RayExecutor()
        f = ex.submit(lambda: True)
        assert f._state == "PENDING"
        ex.shutdown(cancel_futures=True)
        assert f.cancelled()

    def test_running_task_finishes_after_shutdown(self):
        ex = RayExecutor()
        f = ex.submit(lambda: True)
        assert f._state == "PENDING"
        f.set_running_or_notify_cancel()
        assert f.running()
        ex.shutdown(cancel_futures=True)
        assert f._state == "FINISHED"

    def test_mixed_task_states_handled_by_shutdown(self):
        ex = RayExecutor()
        f0 = ex.submit(lambda: True)
        f1 = ex.submit(lambda: True)
        assert f0._state == "PENDING"
        assert f1._state == "PENDING"
        f0.set_running_or_notify_cancel()
        ex.shutdown(cancel_futures=True)
        assert f0._state == "FINISHED"
        assert f1.cancelled()

    def test_with_syntax_invokes_shutdown(self):
        with RayExecutor() as ex:
            pass
        assert ex._shutdown_lock

    # ----------------------------------------------------------------------------------------------------
    # ThreadPool/ProcessPool comparison
    # ----------------------------------------------------------------------------------------------------

    def test_conformity_with_processpool(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor() as ex:
            ray_future = ex.submit(f_process0, 100)
            ray_future_type = type(ray_future)
            ray_result = ray_future.result()
        with ProcessPoolExecutor() as ppe:
            ppe_future = ppe.submit(f_process1, 100)
            ppe_future_type = type(ppe_future)
            ppe_result = ppe_future.result()
        assert ray_future_type == ppe_future_type
        assert ray_result == ppe_result

    def test_conformity_with_processpool_map(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor() as ex:
            ray_iter = ex.map(f_process0, range(10))
            ray_result = list(ray_iter)
        with ProcessPoolExecutor() as ppe:
            ppe_iter = ppe.map(f_process1, range(10))
            ppe_result = list(ppe_iter)
        assert hasattr(ray_iter, "__iter__")
        assert hasattr(ray_iter, "__next__")
        assert hasattr(ppe_iter, "__iter__")
        assert hasattr(ppe_iter, "__next__")
        assert type(ray_result) == type(ppe_result)
        assert sorted(ray_result) == sorted(ppe_result)

    def test_conformity_with_threadpool(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor() as ex:
            ray_future = ex.submit(f_process0, 100)
            ray_future_type = type(ray_future)
            ray_result = ray_future.result()
        with ThreadPoolExecutor() as tpe:
            tpe_future = tpe.submit(f_process1, 100)
            tpe_future_type = type(tpe_future)
            tpe_result = tpe_future.result()
        assert ray_future_type == tpe_future_type
        assert ray_result == tpe_result

    def test_conformity_with_threadpool_map(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor() as ex:
            ray_iter = ex.map(f_process0, range(10))
            ray_result = list(ray_iter)
        with ThreadPoolExecutor() as tpe:
            tpe_iter = tpe.map(f_process1, range(10))
            tpe_result = list(tpe_iter)
        assert hasattr(ray_iter, "__iter__")
        assert hasattr(ray_iter, "__next__")
        assert hasattr(tpe_iter, "__iter__")
        assert hasattr(tpe_iter, "__next__")
        assert type(ray_result) == type(tpe_result)
        assert sorted(ray_result) == sorted(tpe_result)

    def test_conformity_with_processpool_using_max_workers(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor(max_workers=2) as ex:
            ray_result = ex.submit(f_process0, 100).result()
        with ProcessPoolExecutor(max_workers=2) as ppe:
            ppe_result = ppe.submit(f_process1, 100).result()
        assert type(ray_result) == type(ppe_result)
        assert ray_result == ppe_result

    def test_conformity_with_processpool_map_using_max_workers(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor(max_workers=2) as ex:
            ray_iter = ex.map(f_process0, range(10))
            ray_result = list(ray_iter)
        with ProcessPoolExecutor(max_workers=2) as ppe:
            ppe_iter = ppe.map(f_process1, range(10))
            ppe_result = list(ppe_iter)
        assert hasattr(ray_iter, "__iter__")
        assert hasattr(ray_iter, "__next__")
        assert hasattr(ppe_iter, "__iter__")
        assert hasattr(ppe_iter, "__next__")
        assert type(ray_result) == type(ppe_result)
        assert sorted(ray_result) == sorted(ppe_result)

    def test_conformity_with_threadpool_using_max_workers(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor(max_workers=2) as ex:
            ray_future = ex.submit(f_process0, 100)
            ray_future_type = type(ray_future)
            ray_result = ray_future.result()
        with ThreadPoolExecutor(max_workers=2) as tpe:
            tpe_future = tpe.submit(f_process1, 100)
            tpe_future_type = type(tpe_future)
            tpe_result = tpe_future.result()
        assert ray_future_type == tpe_future_type
        assert ray_result == tpe_result

    def test_conformity_with_threadpool_map_using_max_workers(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor(max_workers=2) as ex:
            ray_iter = ex.map(f_process0, range(10))
            ray_result = list(ray_iter)
        with ThreadPoolExecutor(max_workers=2) as tpe:
            tpe_iter = tpe.map(f_process1, range(10))
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
