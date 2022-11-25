import os
import ray
import sys
import pytest
from ray.util.executor.ray_executor import RayExecutor

# @pytest.mark.skipif(
#     sys.platform == "win32", reason="PSUtil does not work the same on windows."
# )
# @pytest.mark.parametrize(
#     "call_ray_start",
#     ["ray start --head --ray-client-server-port 25001 --port 0"],
#     indirect=True,
# )
# def test_remote_function_runs_on_local_instance(call_ray_start):
def test_remote_function_runs_on_local_instance():
    with RayExecutor() as ex:
        result = ex.submit(lambda x: len([i for i in range(x)]), 100).result()
    assert result == 100

def test_remote_function_runs_on_local_instance_with_map():
    with RayExecutor() as ex:
        futures_iter = ex.map(lambda x: len([i for i in range(x)]), [100, 100, 100])
    for result in futures_iter:
        assert result == 100

@ray.remote
class ActorTest0:
    def __init__(self, name):
        self.name = name

    def actor_function(self, arg):
        return f"{self.name}-Actor-{arg}"

def test_remote_actor_on_local_instance():
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor() as ex:
        name = ex.submit_actor_function(a.actor_function, 0)
    result = name.result()
    assert result == "A-Actor-0"

def test_remote_actor_runs_on_local_instance_with_map():
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor() as ex:
        futures_iter = ex.map_actor_function(a.actor_function, [0, 0, 0])
    for result in futures_iter:
        assert result == "A-Actor-0"

@ray.remote
class ActorTest1:
    def __init__(self, name):
        self.name = name

    def actor_function(self, arg0, arg1, arg2, extra=None):
        return f"{self.name}-Actor-{arg0}-{arg1}-{arg2}-{extra}"

def test_remote_actor_on_local_instance_multiple_args():
    a = ActorTest1.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor() as ex:
        name = ex.submit_actor_function(a.actor_function, 0, 1, 2, extra=3)
    result = name.result()
    assert result == "A-Actor-0-1-2-3"

@ray.remote
class ActorTest2:
    def __init__(self):
        self.value = 0

    def actor_function(self, i):
        self.value += i
        return self.value

def test_remote_actor_on_local_instance_keeps_state():
    a = ActorTest2.options(name="A", get_if_exists=True).remote()
    with RayExecutor() as ex:
        value1 = ex.submit_actor_function(a.actor_function, 1)
        value2 = ex.submit_actor_function(a.actor_function, 1)
    assert value1.result() == 1
    assert value2.result() == 2


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
