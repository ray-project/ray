import sys

import pytest

import ray

@ray.remote
class NestedActor():
    def run_actor_task(self, max_depth, spawn_task, expected_depth = 1):
        task_depth = ray._private.worker.global_worker.task_depth
        assert task_depth == expected_depth

        if expected_depth < max_depth:
            if spawn_task:
                ray.get(nested_task.remote(max_depth, spawn_task, expected_depth + 1, ))
            else:
                nested_actor = NestedActor.remote()
                ray.get(nested_actor.run_actor_task.remote(max_depth, spawn_task, expected_depth + 1))

@ray.remote
def nested_task(max_depth, spawn_task, expected_depth = 1):
    task_depth = ray._private.worker.global_worker.task_depth
    assert task_depth == expected_depth

    if expected_depth < max_depth:
        if spawn_task:
            ray.get(nested_task.remote(max_depth, spawn_task, expected_depth + 1, ))
        else:
            nested_actor = NestedActor.remote()
            ray.get(nested_actor.run_actor_task.remote(max_depth, spawn_task, expected_depth + 1))


def test_actor_spawn_actor(shutdown_only):
    nested_actor = NestedActor.remote()
    ray.get(nested_actor.run_actor_task.remote(max_depth = 4, spawn_task = False))

def test_actor_spawn_task(shutdown_only):
    nested_actor = NestedActor.remote()
    ray.get(nested_actor.run_actor_task.remote(max_depth = 2, spawn_task = True))

def test_task_spawn_task(shutdown_only):
    ray.get(nested_task.remote(max_depth = 5, spawn_task = True))

def test_task_spawn_actor(shutdown_only):
    ray.get(nested_task.remote(max_depth = 2, spawn_task = False))

def test_task_spawn_detached_actor_spawn_actor(shutdown_only):
    @ray.remote
    def spawn_detached_actor():
        assert ray._private.worker.global_worker.task_depth == 1
        
        nested_actor = NestedActor.options(name="detached", lifetime="detached").remote()
        ray.get(nested_actor.run_actor_task.remote(max_depth = 3, spawn_task = False, expected_depth = 2))

    ray.get(spawn_detached_actor.remote())

if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
