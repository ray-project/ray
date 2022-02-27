import ray
import os
import signal
import time
import sys

from ray._private.test_utils import SignalActor


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


# get_runtime_context() can be called outside of Ray so it should not start
# Ray automatically.
def test_no_auto_init(shutdown_only):
    assert not ray.is_initialized()
    ray.get_runtime_context()
    assert not ray.is_initialized()


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
