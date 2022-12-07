import sys
import threading

import pytest

import ray


@ray.remote
class NestedActor:
    def __init__(self, expected_depth=1):
        task_depth = ray._private.worker.global_worker.task_depth
        assert task_depth == expected_depth

    def run_actor_task(self, max_depth, spawn_task, expected_depth=1):
        task_depth = ray._private.worker.global_worker.task_depth
        assert task_depth == expected_depth

        if expected_depth < max_depth:
            if spawn_task:
                ray.get(
                    nested_task.remote(
                        max_depth,
                        spawn_task,
                        expected_depth + 1,
                    )
                )
            else:
                nested_actor = NestedActor.remote(expected_depth + 1)
                ray.get(
                    nested_actor.run_actor_task.remote(
                        max_depth, spawn_task, expected_depth + 1
                    )
                )


@ray.remote
def nested_task(max_depth, spawn_task, expected_depth=1):
    task_depth = ray._private.worker.global_worker.task_depth
    assert task_depth == expected_depth

    if expected_depth < max_depth:
        if spawn_task:
            ray.get(
                nested_task.remote(
                    max_depth,
                    spawn_task,
                    expected_depth + 1,
                )
            )
        else:
            nested_actor = NestedActor.remote(expected_depth + 1)
            ray.get(
                nested_actor.run_actor_task.remote(
                    max_depth, spawn_task, expected_depth + 1
                )
            )


def test_actor_spawn_actor(shutdown_only):
    nested_actor = NestedActor.remote()
    ray.get(nested_actor.run_actor_task.remote(max_depth=4, spawn_task=False))


def test_actor_spawn_task(shutdown_only):
    nested_actor = NestedActor.remote()
    ray.get(nested_actor.run_actor_task.remote(max_depth=2, spawn_task=True))


def test_task_spawn_task(shutdown_only):
    ray.get(nested_task.remote(max_depth=5, spawn_task=True))


def test_task_spawn_actor(shutdown_only):
    ray.get(nested_task.remote(max_depth=2, spawn_task=False))


def test_task_spawn_detached_actor_spawn_actor(shutdown_only):
    @ray.remote
    def spawn_detached_actor():
        assert ray._private.worker.global_worker.task_depth == 1

        nested_actor = NestedActor.options(name="detached", lifetime="detached").remote(
            expected_depth=2
        )
        ray.get(
            nested_actor.run_actor_task.remote(
                max_depth=3, spawn_task=False, expected_depth=2
            )
        )

    ray.get(spawn_detached_actor.remote())


def test_actor_call_task_call_actor(shutdown_only):
    @ray.remote
    class TestActor:
        def __init__(self, expected_depth=1):
            task_depth = ray._private.worker.global_worker.task_depth
            assert task_depth == expected_depth

        def run(self, spawn_task=False, expected_depth=1):
            task_depth = ray._private.worker.global_worker.task_depth
            assert task_depth == expected_depth

            if spawn_task:
                ray.get(
                    call_actor.remote(
                        self, spawn_task=False, expected_depth=expected_depth + 1
                    )
                )

    @ray.remote
    def call_actor(actor, spawn_task=False, expected_depth=1):
        task_depth = ray._private.worker.global_worker.task_depth
        assert task_depth == expected_depth

        ray.get(actor.run.remote(spawn_task=spawn_task, expected_depth=task_depth + 1))

    actor = TestActor.remote()
    actor.run.remote(spawn_task=True)


def test_crashed_actor_restores_depth(shutdown_only):
    @ray.remote
    class TestActor:
        def __init__(self, expected_depth=1):
            task_depth = ray._private.worker.global_worker.task_depth
            assert task_depth == expected_depth

        def crash(self):
            assert False

        def create_restartable_actor(self):
            task_depth = ray._private.worker.global_worker.task_depth
            actor = TestActor.options(max_restarts=1, max_task_retries=0).remote(
                expected_depth=task_depth + 1
            )
            return actor

        def run(self):
            pass

    actor = TestActor.options(max_restarts=1, max_task_retries=0).remote()

    with pytest.raises(ray.exceptions.RayTaskError) as _:
        ray.get(actor.crash.remote())

    nested_actor = ray.get(actor.create_restartable_actor.remote())

    ray.get(nested_actor.run.remote())


def test_thread_create_task(shutdown_only):
    @ray.remote
    def thread_create_task():
        assert ray._private.worker.global_worker.task_depth == 1

        global has_exception
        has_exception = False

        @ray.remote
        def check_nested_depth():
            assert ray._private.worker.global_worker.task_depth == 2

        def run_check_nested_depth():
            try:
                ray.get(check_nested_depth.options(max_retries=0).remote())
            except Exception:
                global has_exception
                has_exception = True

        t1 = threading.Thread(target=run_check_nested_depth)
        t1.start()
        t1.join()

        assert not has_exception

    ray.get(thread_create_task.remote())


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
