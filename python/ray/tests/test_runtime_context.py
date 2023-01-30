import ray
import os
import signal
import time
import sys
import pytest
import warnings
import threading
from queue import Queue

from ray._private.test_utils import SignalActor
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy


@pytest.mark.skipif(sys.platform == "win32", reason="Fails on windows")
def test_was_current_actor_reconstructed(shutdown_only):
    ray.init()

    @ray.remote(max_restarts=10)
    class A(object):
        def __init__(self):
            self._was_reconstructed = (
                ray.get_runtime_context().was_current_actor_reconstructed
            )

        def get_was_reconstructed(self):
            return self._was_reconstructed

        def update_was_reconstructed(self):
            return ray.get_runtime_context().was_current_actor_reconstructed

        def get_pid(self):
            return os.getpid()

    a = A.remote()
    # `was_reconstructed` should be False when it's called in actor.
    assert ray.get(a.get_was_reconstructed.remote()) is False
    # `was_reconstructed` should be False when it's called in a remote method
    # and the actor never fails.
    assert ray.get(a.update_was_reconstructed.remote()) is False

    pid = ray.get(a.get_pid.remote())
    os.kill(pid, signal.SIGKILL)
    time.sleep(2)
    # These 2 methods should be return True because
    # this actor failed and restored.
    assert ray.get(a.get_was_reconstructed.remote()) is True
    assert ray.get(a.update_was_reconstructed.remote()) is True

    @ray.remote(max_restarts=10)
    class A(object):
        def current_job_id(self):
            return ray.get_runtime_context().job_id

        def current_actor_id(self):
            return ray.get_runtime_context().actor_id

    @ray.remote
    def f():
        assert ray.get_runtime_context().actor_id is None
        assert ray.get_runtime_context().task_id is not None
        assert ray.get_runtime_context().node_id is not None
        assert ray.get_runtime_context().job_id is not None
        context = ray.get_runtime_context().get()
        assert "actor_id" not in context
        assert context["task_id"] == ray.get_runtime_context().task_id
        assert context["node_id"] == ray.get_runtime_context().node_id
        assert context["job_id"] == ray.get_runtime_context().job_id

    a = A.remote()
    assert ray.get(a.current_job_id.remote()) is not None
    assert ray.get(a.current_actor_id.remote()) is not None
    ray.get(f.remote())


def test_get_context_dict(ray_start_regular):
    context_dict = ray.get_runtime_context().get()
    assert context_dict["node_id"] is not None
    assert context_dict["job_id"] is not None
    assert "actor_id" not in context_dict
    assert "task_id" not in context_dict

    @ray.remote
    class Actor:
        def check(self, node_id, job_id):
            context_dict = ray.get_runtime_context().get()
            assert context_dict["node_id"] == node_id
            assert context_dict["job_id"] == job_id
            assert context_dict["actor_id"] is not None
            assert context_dict["task_id"] is not None
            assert context_dict["actor_id"] != "not an ActorID"

    a = Actor.remote()
    ray.get(a.check.remote(context_dict["node_id"], context_dict["job_id"]))

    @ray.remote
    def task(node_id, job_id):
        context_dict = ray.get_runtime_context().get()
        assert context_dict["node_id"] == node_id
        assert context_dict["job_id"] == job_id
        assert context_dict["task_id"] is not None
        assert "actor_id" not in context_dict

    ray.get(task.remote(context_dict["node_id"], context_dict["job_id"]))


def test_current_actor(ray_start_regular):
    @ray.remote
    class Echo:
        def __init__(self):
            pass

        def echo(self, s):
            self_actor = ray.get_runtime_context().current_actor
            return self_actor.echo2.remote(s)

        def echo2(self, s):
            return s

    e = Echo.remote()
    obj = e.echo.remote("hello")
    assert ray.get(ray.get(obj)) == "hello"


def test_get_assigned_resources(ray_start_10_cpus):
    @ray.remote
    class Echo:
        def check(self):
            return ray.get_runtime_context().get_assigned_resources()

    e = Echo.remote()
    result = e.check.remote()
    print(ray.get(result))
    assert ray.get(result).get("CPU") is None
    ray.kill(e)

    e = Echo.options(num_cpus=4).remote()
    result = e.check.remote()
    assert ray.get(result)["CPU"] == 4.0
    ray.kill(e)

    @ray.remote
    def check():
        return ray.get_runtime_context().get_assigned_resources()

    result = check.remote()
    assert ray.get(result)["CPU"] == 1.0

    result = check.options(num_cpus=2).remote()
    assert ray.get(result)["CPU"] == 2.0


def test_actor_stats_normal_task(ray_start_regular):
    # Because it works at the core worker level, this API works for tasks.
    @ray.remote
    def func():
        return ray.get_runtime_context()._get_actor_call_stats()

    assert ray.get(func.remote())["func"] == {
        "pending": 0,
        "running": 1,
        "finished": 0,
    }


def test_actor_stats_sync_actor(ray_start_regular):
    signal = SignalActor.remote()

    @ray.remote
    class SyncActor:
        def run(self):
            return ray.get_runtime_context()._get_actor_call_stats()

        def wait_signal(self):
            ray.get(signal.wait.remote())
            return ray.get_runtime_context()._get_actor_call_stats()

    actor = SyncActor.remote()
    counts = ray.get(actor.run.remote())
    assert counts == {
        "SyncActor.run": {"pending": 0, "running": 1, "finished": 0},
        "SyncActor.__init__": {"pending": 0, "running": 0, "finished": 1},
    }

    ref = actor.wait_signal.remote()
    other_refs = [actor.run.remote() for _ in range(3)] + [
        actor.wait_signal.remote() for _ in range(5)
    ]
    ray.wait(other_refs, timeout=1)
    signal.send.remote()
    counts = ray.get(ref)
    assert counts == {
        "SyncActor.run": {
            "pending": 3,
            "running": 0,
            "finished": 1,  # from previous run
        },
        "SyncActor.wait_signal": {
            "pending": 5,
            "running": 1,
            "finished": 0,
        },
        "SyncActor.__init__": {"pending": 0, "running": 0, "finished": 1},
    }


def test_actor_stats_threaded_actor(ray_start_regular):
    signal = SignalActor.remote()

    @ray.remote
    class ThreadedActor:
        def func(self):
            ray.get(signal.wait.remote())
            return ray.get_runtime_context()._get_actor_call_stats()

    actor = ThreadedActor.options(max_concurrency=3).remote()
    refs = [actor.func.remote() for _ in range(6)]
    ready, _ = ray.wait(refs, timeout=1)
    assert len(ready) == 0
    signal.send.remote()
    results = ray.get(refs)
    assert max(result["ThreadedActor.func"]["running"] for result in results) > 1
    assert max(result["ThreadedActor.func"]["pending"] for result in results) > 1


def test_actor_stats_async_actor(ray_start_regular):
    signal = SignalActor.remote()

    @ray.remote
    class AysncActor:
        async def func(self):
            await signal.wait.remote()
            return ray.get_runtime_context()._get_actor_call_stats()

    actor = AysncActor.options(max_concurrency=3).remote()
    refs = [actor.func.remote() for _ in range(6)]
    ready, _ = ray.wait(refs, timeout=1)
    assert len(ready) == 0
    signal.send.remote()
    results = ray.get(refs)
    assert max(result["AysncActor.func"]["running"] for result in results) == 3
    assert max(result["AysncActor.func"]["pending"] for result in results) == 3


# Use default filterwarnings behavior for this test
@pytest.mark.filterwarnings("default")
def test_ids(ray_start_regular):
    rtc = ray.get_runtime_context()
    # node id
    assert isinstance(rtc.get_node_id(), str)
    with warnings.catch_warnings(record=True) as w:
        assert rtc.get_node_id() == rtc.node_id.hex()
        assert any("Use get_node_id() instead" in str(warning.message) for warning in w)

    # job id
    assert isinstance(rtc.get_job_id(), str)
    with warnings.catch_warnings(record=True) as w:
        assert rtc.get_job_id() == rtc.job_id.hex()
        assert any("Use get_job_id() instead" in str(warning.message) for warning in w)

    # placement group id
    # Driver doesn't belong to any placement group.
    assert rtc.get_placement_group_id() is None
    pg = ray.util.placement_group(
        name="bar",
        strategy="PACK",
        bundles=[
            {"CPU": 1, "GPU": 0},
        ],
    )
    ray.get(pg.ready())

    @ray.remote
    def foo_pg():
        rtc = ray.get_runtime_context()
        assert isinstance(rtc.get_placement_group_id(), str)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert rtc.get_placement_group_id() == rtc.current_placement_group_id.hex()
            assert any(
                "Use get_placement_group_id() instead" in str(warning.message)
                for warning in w
            )

    ray.get(
        foo_pg.options(
            scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
        ).remote()
    )
    ray.util.remove_placement_group(pg)

    # task id
    assert rtc.get_task_id() is None

    @ray.remote
    def foo_task():
        rtc = ray.get_runtime_context()
        assert isinstance(rtc.get_task_id(), str)
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            assert rtc.get_task_id() == rtc.task_id.hex()
            assert any(
                "Use get_task_id() instead" in str(warning.message) for warning in w
            )

    ray.get(foo_task.remote())

    # actor id
    assert rtc.get_actor_id() is None

    @ray.remote
    class FooActor:
        def foo(self):
            rtc = ray.get_runtime_context()
            assert isinstance(rtc.get_actor_id(), str)
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                assert rtc.get_actor_id() == rtc.actor_id.hex()
                assert any(
                    "Use get_actor_id() instead" in str(warning.message)
                    for warning in w
                )

    actor = FooActor.remote()
    ray.get(actor.foo.remote())


# get_runtime_context() can be called outside of Ray so it should not start
# Ray automatically.
def test_no_auto_init(shutdown_only):
    assert not ray.is_initialized()
    ray.get_runtime_context()
    assert not ray.is_initialized()


def test_errors_when_ray_not_initialized():
    with pytest.raises(AssertionError, match="Ray has not been initialized"):
        ray.get_runtime_context().get_job_id()
    with pytest.raises(AssertionError, match="Ray has not been initialized"):
        ray.get_runtime_context().get_node_id()


@ray.remote
def task_id(i):
    id = ray.get_runtime_context().get_task_id()
    return id


@ray.remote
class AsyncActor:
    async def task_id(self, i):
        id = ray.get_runtime_context().get_task_id()
        return id


@ray.remote
class ThreadedActor:
    def task_id(self):
        return ray.get_runtime_context().get_task_id()


@pytest.mark.parametrize("actor_concurrency", [1, 3, 10])
def test_threaded_actor_runtime_context(ray_start_regular, actor_concurrency):
    threaded_actor = ThreadedActor.options(max_concurrency=actor_concurrency).remote()
    # Test threaded actor
    tasks = [threaded_actor.task_id.remote() for _ in range(20)]
    submitted_task_ids = [task.task_id().hex() for task in tasks]
    actual_task_ids = ray.get(tasks)
    assert submitted_task_ids == actual_task_ids


@pytest.mark.parametrize("actor_concurrency", [1, 3, 10])
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="requires contextvars from python3.7+"
)
def test_async_actor_runtime_context(ray_start_regular, actor_concurrency):
    @ray.remote
    class NestedAsyncActor:
        async def task_id(self, i):
            id = ray.get_runtime_context().get_task_id()

            def verify_nested_task():
                nested_task = task_id.remote(i * 100)
                child_id = ray.get(nested_task)
                assert nested_task.task_id().hex() == child_id

            def verify_nested_async_actors():
                nested_actor = AsyncActor.options(
                    max_concurrency=actor_concurrency
                ).remote()
                tasks = [nested_actor.task_id.remote(j + i * 100) for j in range(20)]
                expect_task_ids = [task.task_id().hex() for task in tasks]
                actual_task_ids = ray.get(tasks)
                assert expect_task_ids == actual_task_ids

            def verify_nested_threaded_actors():
                threaded_actor = ThreadedActor.options(
                    max_concurrency=actor_concurrency
                ).remote()
                # Test threaded actor
                tasks = [threaded_actor.task_id.remote() for _ in range(20)]
                submitted_task_ids = [task.task_id().hex() for task in tasks]
                actual_task_ids = ray.get(tasks)
                assert submitted_task_ids == actual_task_ids

            verify_nested_task()
            verify_nested_async_actors()
            verify_nested_threaded_actors()

            return id

    # Asyncio actors run in another thread that's beyond knowledge of ray
    async_actor = NestedAsyncActor.options(max_concurrency=actor_concurrency).remote()
    tasks = [async_actor.task_id.options(name=f"task{i}").remote(i) for i in range(20)]
    expect_task_ids = [task.task_id().hex() for task in tasks]
    actual_task_ids = ray.get(tasks)
    assert len(expect_task_ids) == len(actual_task_ids)
    assert expect_task_ids == actual_task_ids


def test_thread_task_runtime_context(ray_start_regular):
    def threaded_task_id():
        q = Queue()

        def thd_task_id(q):
            q.put(ray.get_runtime_context().get_task_id())

        thd = threading.Thread(target=thd_task_id, args=(q,))
        thd.start()
        id = q.get()
        thd.join()
        return id

    @ray.remote
    def task_id():
        main_thread_task_id = ray.get_runtime_context().get_task_id()
        return main_thread_task_id, threaded_task_id()

    task = task_id.remote()
    main_thread_task_id, thread_task_id = ray.get(task)

    assert task.task_id().hex(), main_thread_task_id
    assert task.task_id().hex(), thread_task_id


@pytest.mark.parametrize(
    "actor_concurrency,user_thread_task_id_expect_none",
    [(1, False), (3, True), (10, True)],
)
def test_user_spawned_threads_in_threaded_actor(
    ray_start_regular, actor_concurrency, user_thread_task_id_expect_none
):
    @ray.remote
    class ThreadActor:
        def threaded_task_id(self):
            main_task_id = ray.get_runtime_context().get_task_id()
            q = Queue()

            def task_id(q):
                q.put(ray.get_runtime_context().get_task_id())

            thd = threading.Thread(target=task_id, args=(q,))
            thd.start()
            thread_task_id = q.get()
            thd.join()

            return main_task_id, thread_task_id

    a = ThreadActor.options(max_concurrency=actor_concurrency).remote()
    tasks = [a.threaded_task_id.remote() for _ in range(20)]
    # Expect new thread's tasks to have the same task id as parent threads'.
    if user_thread_task_id_expect_none:
        expect_thread_task_ids = [None for _ in tasks]
    else:
        expect_thread_task_ids = [task.task_id().hex() for task in tasks]

    expect_main_thread_task_ids = [task.task_id().hex() for task in tasks]

    result = ray.get(tasks)
    actual_task_ids = [thread_task_id for _, thread_task_id in result]
    main_thread_task_ids = [main_thread_task_id for main_thread_task_id, _ in result]

    assert expect_thread_task_ids == actual_task_ids
    assert expect_main_thread_task_ids == main_thread_task_ids


@pytest.mark.parametrize(
    "actor_concurrency,user_thread_task_id_expect_none",
    [(1, True), (3, True), (10, True)],
)
@pytest.mark.skipif(
    sys.version_info < (3, 7), reason="requires contextvars from python3.7+"
)
def test_user_spawned_threads_in_async_actors(
    ray_start_regular, actor_concurrency, user_thread_task_id_expect_none
):
    @ray.remote
    class AsyncActor:
        async def threaded_task_id(self):
            main_task_id = ray.get_runtime_context().get_task_id()
            q = Queue()

            def task_id(q):
                q.put(ray.get_runtime_context().get_task_id())

            thd = threading.Thread(target=task_id, args=(q,))
            thd.start()
            thread_task_id = q.get()
            thd.join()

            return main_task_id, thread_task_id

    async_actor = AsyncActor.options(max_concurrency=actor_concurrency).remote()
    tasks = [async_actor.threaded_task_id.remote() for _ in range(20)]
    if user_thread_task_id_expect_none:
        expect_thread_task_ids = [None for _ in tasks]
    else:
        expect_thread_task_ids = [task.task_id().hex() for task in tasks]
    expect_main_thread_task_ids = [task.task_id().hex() for task in tasks]

    result = ray.get(tasks)
    actual_task_ids = [thread_task_id for _, thread_task_id in result]
    main_thread_task_ids = [main_thread_task_id for main_thread_task_id, _ in result]

    assert expect_thread_task_ids == actual_task_ids
    assert expect_main_thread_task_ids == main_thread_task_ids


if __name__ == "__main__":
    import pytest

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
