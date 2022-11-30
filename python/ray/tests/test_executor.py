import os

from unittest.mock import patch, call

from abc import ABC, abstractmethod
import sys
import pytest
from ray.util.concurrent.futures.ray_executor import (
    RayExecutor,
    _ActorPoolBase,
    _AbstractActorPool,
    _RoundRobinActorPool,
    _RandomActorPool,
)
import time
import typing as T
from functools import partial
from multiprocessing import Process

import ray
from ray.actor import ActorHandle
from ray.util.state import list_actors
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    TimeoutError as ConTimeoutError,
)
from concurrent.futures.thread import BrokenThreadPool
from concurrent.futures import Future
from ray.exceptions import RayTaskError, RayActorError

from ray._private.worker import RayContext
import logging


# ProcessPoolExecutor uses pickle which can only serialize top-level functions
def f_process1(x):
    return len([i for i in range(x) if i % 2 == 0])


class InitializerException(Exception):
    pass


class Helpers:
    @staticmethod
    def unsafe(exc):
        raise exc

    @staticmethod
    def safe(_):
        pass

    @staticmethod
    def get_actor_states(actor_pool: _ActorPoolBase):

        return [
            actor_state["state"]
            for actor_state in list_actors()
            if actor_state.actor_id in actor_pool.get_actor_ids()
        ]

    @staticmethod
    def get_actor_state(actor):
        [actor_state] = [
            actor_state["state"]
            for actor_state in list_actors()
            if actor_state.actor_id == actor._ray_actor_id.hex()
        ]
        return actor_state

    @staticmethod
    def wait_assert(f: T.Callable[[], bool], timeout=5):
        res = False
        while timeout > 0:
            if f():
                res = True
                break
            else:
                time.sleep(1)
                timeout -= 1
        assert res

    @classmethod
    def wait_actor_state(cls, actor_pool, state, timeout=20):
        while timeout > 0:
            states = cls.get_actor_states(actor_pool)
            if not all(i == state for i in states):
                time.sleep(1)
                timeout -= 1
            else:
                break
        if timeout == 0:
            return False
        else:
            return True

    @classmethod
    def wait_actor_state_(cls, actor, expected_state, timeout=20):
        while timeout > 0:
            state = cls.get_actor_state(actor)
            if state != expected_state:
                time.sleep(1)
                timeout -= 1
            else:
                break
        if timeout == 0:
            return False
        else:
            return True


class TestIsolated:

    # This class is for tests that must be run with dedicated/isolated ray
    # instances. Individual tests are responsible for creating their own ray
    # instances.
    def setup_method(self):
        assert not ray.is_initialized()

    def teardown_method(self):
        ray.shutdown()
        while ray.is_initialized():
            time.sleep(1)
        assert not ray.is_initialized()


class TestShared:

    # This class if for tests that can share an existing ray instance. This
    # speeds up tests as the instance does not need to be created and destroyed
    # for each test.

    def setup_class(self):
        logging.warning(f"Initialising Ray instance for {self.__name__}")
        self.address = ray.init(num_cpus=2, ignore_reinit_error=True).address_info[
            "address"
        ]
        assert ray.is_initialized()

    def teardown_class(self):
        logging.warning(f"Shutting down Ray instance for {self.__name__}")
        ray.shutdown()
        assert not ray.is_initialized()


class ActorPoolTests(ABC):
    """
    This set of tests should be executed for all implementations of `_ActorPoolBase`.
    """

    @property
    @abstractmethod
    def apc(self) -> type[_AbstractActorPool]:
        ...

    @property
    @abstractmethod
    def apt(self) -> str:
        ...

    @pytest.fixture
    def actor_pool(self):
        return self.apc()

    def test_setting_max_tasks_per_actor(self, actor_pool):
        assert actor_pool.max_tasks_per_actor is None
        actor_pool.max_tasks_per_actor = 2
        assert actor_pool.max_tasks_per_actor == 2
        with pytest.raises(ValueError):
            actor_pool.max_tasks_per_actor = -2

    def test_setting_num_actors(self, actor_pool):
        assert actor_pool.num_actors == 2
        actor_pool.num_actors = 10
        assert actor_pool.num_actors == 10
        with pytest.raises(ValueError):
            actor_pool.num_actors = -2
        with pytest.raises(ValueError):
            actor_pool.num_actors = 0

    def test_setting_initializer(self, actor_pool):
        assert actor_pool.initializer is None
        actor_pool.initializer = lambda x: print(x)
        with pytest.raises(TypeError):
            actor_pool.initializer = 1
        with pytest.raises(TypeError):
            actor_pool.initializer = True

    def test_setting_initargs(self, actor_pool):
        assert actor_pool.initargs == ()
        actor_pool.initargs = (1, 2)
        assert actor_pool.initargs == (1, 2)
        with pytest.raises(TypeError):
            actor_pool.initargs = True

    def test_submit_returns_a_future(self, actor_pool):
        def f():
            return 123

        future = actor_pool.submit(f)
        assert isinstance(future, Future)

    def test_next_returns_pool_actor(self, actor_pool):
        pool_actor = actor_pool.next()
        actor = pool_actor["actor"]
        assert isinstance(actor, ActorHandle)
        task_count = pool_actor["task_count"]
        assert task_count == 0

    def test_get_actor_ids_returns_list_of_strings(self, actor_pool):
        actor_ids = actor_pool.get_actor_ids()
        assert isinstance(list, actor_ids)
        for i in actor_ids:
            assert isinstance(str, i)

    def test_actor_pool_exit_removes_from_pool(self):
        pool = self.apc(num_actors=1)
        assert len(pool.pool) == 1
        [actor] = pool.pool
        pool._exit_actor(actor)
        assert pool.pool == []
        assert Helpers.wait_actor_state_(actor["actor"], "DEAD")

    def test__kill_actor_called_for_actors(self, actor_pool):
        with patch.object(actor_pool, "_kill_actor") as mock_kill_actor:
            actor_pool.kill()
            mock_kill_actor.assert_has_calls(
                [call(i) for i in actor_pool.pool], any_order=True
            )

    def test__increment_task_count(self, actor_pool):
        mock_actor = {"task_count": 0}
        actor_pool.pool = [mock_actor]
        actor_pool._increment_task_count(mock_actor)
        assert mock_actor["task_count"] == 1

    def test_actor_pool_kills_actors(self):
        pool = self.apc(num_actors=2)
        assert len(pool.pool) == 2

        # wait for actors to live
        assert Helpers.wait_actor_state(pool, "ALIVE")
        pool.kill()
        # wait for actors to die
        assert Helpers.wait_actor_state(pool, "DEAD")

    def test_actor_pool_kills_actors_and_does_not_wait_for_tasks_to_complete(self):
        pool = self.apc(num_actors=2)

        def f():
            return 123

        future = pool.submit(f)
        pool.kill()
        assert Helpers.wait_actor_state(pool, "DEAD")
        with pytest.raises(RayActorError):
            future.result()

    def test_actor_pool_exits_actors_and_waits_for_tasks_to_complete(self):
        pool = self.apc(num_actors=2)

        def f():
            return 123

        future = pool.submit(f)
        pool_actor = pool.pool[0]
        actor = pool._exit_actor(pool_actor)
        assert Helpers.wait_actor_state_(actor, "DEAD")
        assert future.result() == 123

    def test_actor_pool_replaces_expired_actors(self):
        pool = self.apc(num_actors=1, max_tasks_per_actor=2)
        assert len(pool.pool) == 1
        actor_id0 = pool.pool[0]["actor"]._ray_actor_id.hex()
        pool._replace_actor_if_max_tasks()
        assert pool.pool[0]["actor"]._ray_actor_id.hex() == actor_id0
        pool.pool[0]["task_count"] = 2
        pool._replace_actor_if_max_tasks()
        assert pool.pool[0]["actor"]._ray_actor_id.hex() != actor_id0

    def test_actor_pool_replaces_actors_allowing_tasks_to_finish(self):
        def f():
            return 123

        pool = self.apc(num_actors=1, max_tasks_per_actor=2)
        assert len(pool.pool) == 1

        assert pool.pool[0]["task_count"] == 0
        future0 = pool.submit(f)
        assert pool.pool[0]["task_count"] == 1
        future1 = pool.submit(f)
        assert pool.pool[0]["task_count"] == 2
        actor_id01 = pool.pool[0]["actor"]._ray_actor_id.hex()
        future2 = pool.submit(f)
        actor_id02 = pool.pool[0]["actor"]._ray_actor_id.hex()
        assert pool.pool[0]["task_count"] == 1
        assert actor_id01 != actor_id02
        assert future0.result() == 123
        assert future1.result() == 123
        assert future2.result() == 123

    def test_actor_pool_replaces_actors_exits_gracefully(self):
        def f():
            return 123

        pool = self.apc(num_actors=1, max_tasks_per_actor=1)
        assert len(pool.pool) == 1

        actor_id00 = pool.pool[0]["actor"]._ray_actor_id.hex()
        future0 = pool.submit(f)
        actor_id01 = pool.pool[0]["actor"]._ray_actor_id.hex()
        future1 = pool.submit(f)
        actor_id02 = pool.pool[0]["actor"]._ray_actor_id.hex()
        assert actor_id00 == actor_id01 != actor_id02
        assert future0.result() == 123
        assert future1.result() == 123

    def test_actor_pool_replaces_actors_exits_gracefully_in_executor(self):
        def f():
            return 123

        with RayExecutor(
            address=self.address,
            max_workers=1,
            max_tasks_per_child=1,
            actor_pool_type=self.apt,
        ) as ex:
            pool = ex.actor_pool
            assert len(pool.pool) == 1

            actor_id00 = pool.pool[0]["actor"]._ray_actor_id.hex()
            future0 = pool.submit(f)
            actor_id01 = pool.pool[0]["actor"]._ray_actor_id.hex()
            future1 = pool.submit(f)
            actor_id02 = pool.pool[0]["actor"]._ray_actor_id.hex()
            assert actor_id00 == actor_id01 != actor_id02
            assert future0.result() == 123
            assert future1.result() == 123


class TestRandomActorPool(ActorPoolTests, TestShared):
    @property
    def apc(self) -> type[_AbstractActorPool]:
        return _RandomActorPool

    @property
    def apt(self) -> str:
        return "random"


class TestRoundRobinActorPool(ActorPoolTests, TestShared):
    @property
    def apc(self) -> type[_AbstractActorPool]:
        return _RoundRobinActorPool

    @property
    def apt(self) -> str:
        return "roundrobin"

    def test_actor_pool_cycles_through_actors(self):
        pool = self.apc(num_actors=2)
        assert len(pool.pool) == 2
        assert pool.index == 0
        _ = pool.next()
        assert len(pool.pool) == 2
        assert pool.index == 1
        _ = pool.next()
        assert len(pool.pool) == 2
        assert pool.index == 0


class TestExistingInstanceSetup(TestShared):
    def test_actor_pool_type(self):
        with pytest.raises(ValueError):
            RayExecutor(address=self.address, actor_pool_type=None)
        with pytest.raises(ValueError):
            RayExecutor(address=self.address, actor_pool_type="my-other-type")

    def test_remote_function_runs_on_specified_instance(self):
        with RayExecutor(address=self.address) as ex:
            result = ex.submit(lambda x: x * x, 100).result()
            assert result == 10_000
            assert ex._context is not None
            assert isinstance(RayContext, ex._context)
            assert ex._context.address_info["address"] == self.address

    def test_remote_function_runs_on_specified_instance_with_map(self):
        with RayExecutor(address=self.address) as ex:
            futures_iter = ex.map(lambda x: x * x, [100, 100, 100])
            for result in futures_iter:
                assert result == 10_000
            assert ex._context is not None
            assert isinstance(RayContext, ex._context)
            assert ex._context.address_info["address"] == self.address

    def test_context_manager_does_not_invoke_shutdown_on_existing_instance(self):
        with RayExecutor(address=self.address) as ex0:
            pass
        assert not ex0._shutdown_lock

    def test_use_cluster_from_address(self):
        with RayExecutor(address=self.address) as ex0:
            assert not ex0._initialised_ray


class TestSetupShutdown(TestIsolated):
    def test_context_manager_invokes_shutdown(self):
        with RayExecutor() as ex:
            assert not ex._shutdown_lock
            pass
        assert ex._shutdown_lock

    def test_context_manager_does_not_invoke_shutdown_on_existing_instance(self):
        with RayExecutor() as ex0:
            with RayExecutor() as ex1:
                pass
            assert not ex1._shutdown_lock
        assert ex0._shutdown_lock

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
                assert isinstance(RayContext, ex0._context)
                assert isinstance(RayContext, ex1._context)
                assert (
                    ex0._context.address_info["node_id"]
                    == ex1._context.address_info["node_id"]
                )

    def test_existing_instance_ignores_max_workers(self):
        _ = ray.init(num_cpus=1)
        with RayExecutor(max_workers=2):
            assert ray.available_resources()["CPU"] == 1

    def test_working_directory_must_be_supplied_for_initializer(self):
        with pytest.raises(ValueError):
            with RayExecutor(
                max_workers=2,
                initializer=Helpers.safe,
                initargs=(InitializerException,),
            ) as _:
                pass
        with RayExecutor(
            max_workers=2,
            initializer=Helpers.unsafe,
            initargs=(InitializerException,),
            runtime_env={"working_dir": "./python/ray/tests/."},
        ) as _:
            pass

    def test_mp_context_does_nothing(self):
        with RayExecutor(max_workers=2, mp_context="fork") as ex:
            assert ex._mp_context == "fork"

    def test_results_are_not_accessible_after_shutdown(self):
        def f(x, y):
            return x * y

        with RayExecutor() as ex:
            r1 = ex.map(f, [100, 100, 100], [1, 2, 3])
        assert ex._shutdown_lock

        # we run a custom timeout function otherwise ray take 180s to timeout
        timeout_runner = Process(target=partial(list, r1))
        timeout = 5
        timeout_runner.start()
        while timeout_runner.is_alive() and timeout > 0:
            time.sleep(1)
            timeout -= 1
        if timeout_runner.is_alive() and timeout == 0:
            timeout_runner.kill()
            assert True
        else:
            pytest.fail("Fetching results did not timeout")

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


class TestRunningTasks(TestIsolated):
    def test_remote_function_runs_on_local_instance(self):
        with RayExecutor() as ex:
            result = ex.submit(lambda x: x * x, 100).result()
            assert result == 10_000

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
            pool = getattr(ex.actor_pool, "pool")
            assert pool is not None
            assert len(pool) == 3
            time_start = time.monotonic()
            _ = list(ex.map(lambda _: time.sleep(1), range(12)))
            time_end = time.monotonic()
            # we expect about (12*1) / 3 = 4 rounds
            delta = time_end - time_start
            assert delta > 3.0

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


class TestProcessPool(TestIsolated):
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
        assert isinstance(type(ppe_result), ray_result)
        assert sorted(ray_result) == sorted(ppe_result)

    def test_conformity_with_processpool_using_max_workers(self):
        def f_process0(x):
            return len([i for i in range(x) if i % 2 == 0])

        assert f_process0.__code__.co_code == f_process1.__code__.co_code

        with RayExecutor(max_workers=2) as ex:
            ray_result = ex.submit(f_process0, 100).result()
        with ProcessPoolExecutor(max_workers=2) as ppe:
            ppe_result = ppe.submit(f_process1, 100).result()
        assert isinstance(type(ppe_result), ray_result)
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
        assert isinstance(type(ppe_result), ray_result)
        assert sorted(ray_result) == sorted(ppe_result)


class TestThreadPool(TestIsolated):
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
        assert isinstance(type(tpe_result), ray_result)
        assert sorted(ray_result) == sorted(tpe_result)

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
        assert isinstance(type(tpe_result), ray_result)
        assert sorted(ray_result) == sorted(tpe_result)

    def test_conformity_with_threadpool_initializer_initargs(self):

        # this assumes that the test is executed from the root difrectory
        assert os.path.isdir("./python/ray/tests/.")

        # ----------------------------
        with ThreadPoolExecutor(
            max_workers=2, initializer=Helpers.safe, initargs=(InitializerException,)
        ) as tpe:
            tpe_iter = tpe.map(f_process1, range(10))
            _ = list(tpe_iter)
        with ThreadPoolExecutor(
            max_workers=2, initializer=Helpers.unsafe, initargs=(InitializerException,)
        ) as tpe:
            tpe_iter = tpe.map(f_process1, range(10))
            with pytest.raises(BrokenThreadPool):
                _ = list(tpe_iter)
        # ----------------------------

        # ----------------------------
        with RayExecutor(
            max_workers=2,
            initializer=Helpers.safe,
            initargs=(InitializerException,),
            runtime_env={"working_dir": "./python/ray/tests/."},
        ) as ex:
            ray_iter = ex.map(lambda x: x, range(10))
            _ = list(ray_iter)
        with RayExecutor(
            max_workers=2,
            initializer=Helpers.unsafe,
            initargs=(InitializerException,),
            runtime_env={"working_dir": "./python/ray/tests/."},
        ) as ex:
            ray_iter = ex.map(f_process1, range(10))
            with pytest.raises(RayTaskError):
                _ = list(ray_iter)
        # ----------------------------


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
