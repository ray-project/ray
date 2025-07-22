import pytest

import ray
from ray.train.v2._internal.util import ray_get_safe


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.mark.parametrize("type", ["task", "actor_task"])
@pytest.mark.parametrize("failing", [True, False])
@pytest.mark.parametrize("task_list", [True, False])
def test_ray_get_safe(type, failing, task_list):
    num_tasks = 4

    if type == "task":

        @ray.remote
        def f():
            if failing:
                raise ValueError("failing")
            return 1

        if task_list:
            object_refs = [f.remote() for _ in range(num_tasks)]
        else:
            object_refs = f.remote()
    elif type == "actor_task":

        @ray.remote
        class Actor:
            def f(self):
                if failing:
                    raise ValueError("failing")
                return 1

        if task_list:
            actors = [Actor.remote() for _ in range(num_tasks)]
            object_refs = [actor.f.remote() for actor in actors]
        else:
            actor = Actor.remote()
            object_refs = actor.f.remote()

    if failing:
        with pytest.raises(ValueError, match="failing"):
            ray_get_safe(object_refs)
    else:
        out = ray_get_safe(object_refs)
        if task_list:
            assert out == [1] * num_tasks
        else:
            assert out == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
