import os
import ray
import sys
import pytest
from ray.util.executor.ray_executor import RayExecutor

#---------------------------------------------------------------------------------------------------- 
# parameter tests
#---------------------------------------------------------------------------------------------------- 

def test_remote_function_runs_on_local_instance():
    with RayExecutor() as ex:
        result = ex.submit(lambda x: len([i for i in range(x)]), 100).result()
        assert result == 100

def test_remote_function_runs_on_local_instance_with_map():
    with RayExecutor() as ex:
        futures_iter = ex.map(lambda x: len([i for i in range(x)]), [100, 100, 100])
        for result in futures_iter:
            assert result == 100

def test_remote_function_runs_on_specified_instance(call_ray_start):
    with RayExecutor(address=call_ray_start) as ex:
        result = ex.submit(lambda x: len([i for i in range(x)]), 100).result()
        assert result == 100
        assert ex.context.address_info['address'] == call_ray_start

def test_remote_function_runs_on_specified_instance_with_map(call_ray_start):
    with RayExecutor(address=call_ray_start) as ex:
        futures_iter = ex.map(lambda x: len([i for i in range(x)]), [100, 100, 100])
        for result in futures_iter:
            assert result == 100
        assert ex.context.address_info['address'] == call_ray_start

#---------------------------------------------------------------------------------------------------- 
# basic Actor tests
#---------------------------------------------------------------------------------------------------- 


@ray.remote
class ActorTest0:
    def __init__(self, name):
        self.name = name

    def actor_function(self, arg):
        return f"{self.name}-Actor-{arg}"

@ray.remote
class ActorTest1:
    def __init__(self, name):
        self.name = name

    def actor_function(self, arg0, arg1, arg2, extra=None):
        return f"{self.name}-Actor-{arg0}-{arg1}-{arg2}-{extra}"

@ray.remote
class ActorTest2:
    def __init__(self):
        self.value = 0

    def actor_function(self, i):
        self.value += i
        return self.value


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

def test_remote_actor_on_specified_instance(call_ray_start):
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor(address=call_ray_start) as ex:
        name = ex.submit_actor_function(a.actor_function, 0)
        result = name.result()
        assert result == "A-Actor-0"
        assert ex.context.address_info['address'] == call_ray_start

def test_remote_actor_runs_on_specified_instance_with_map(call_ray_start):
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor(address=call_ray_start) as ex:
        futures_iter = ex.map_actor_function(a.actor_function, [0, 0, 0])
        for result in futures_iter:
            assert result == "A-Actor-0"
        assert ex.context.address_info['address'] == call_ray_start

def test_remote_actor_on_local_instance_multiple_args():
    a = ActorTest1.options(name="A", get_if_exists=True).remote("A")
    with RayExecutor() as ex:
        name = ex.submit_actor_function(a.actor_function, 0, 1, 2, extra=3)
        result = name.result()
        assert result == "A-Actor-0-1-2-3"

def test_remote_actor_on_local_instance_keeps_state():
    a = ActorTest2.options(name="A", get_if_exists=True).remote()
    with RayExecutor() as ex:
        value1 = ex.submit_actor_function(a.actor_function, 1)
        value2 = ex.submit_actor_function(a.actor_function, 1)
        assert value1.result() == 1
        assert value2.result() == 2


#---------------------------------------------------------------------------------------------------- 
# shutdown tests
#---------------------------------------------------------------------------------------------------- 

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

def test_cannot_submit_actor_function_after_shutdown():
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    ex = RayExecutor()
    ex.submit_actor_function(a.actor_function, 1)
    ex.shutdown()
    with pytest.raises(RuntimeError):
        ex.submit_actor_function(a.actor_function, 1)

def test_cannot_map_actor_function_after_shutdown():
    a = ActorTest0.options(name="A", get_if_exists=True).remote("A")
    ex = RayExecutor()
    ex.map_actor_function(a.actor_function, [0, 0, 0])
    ex.shutdown()
    with pytest.raises(RuntimeError):
        ex.map_actor_function(a.actor_function, [0, 0, 0])

def test_pending_task_is_cancelled_after_shutdown():
    ex = RayExecutor()
    f = ex.submit(lambda: True)
    assert f._state == 'PENDING'
    ex.shutdown(cancel_futures=True)
    assert f.cancelled()

def test_running_task_finishes_after_shutdown():
    ex = RayExecutor()
    f = ex.submit(lambda: True)
    assert f._state == 'PENDING'
    f.set_running_or_notify_cancel()
    assert f.running()
    ex.shutdown(cancel_futures=True)
    assert f._state == 'FINISHED'

def test_mixed_task_states_handled_by_shutdown():
    ex = RayExecutor()
    f0 = ex.submit(lambda: True)
    f1 = ex.submit(lambda: True)
    assert f0._state == 'PENDING'
    assert f1._state == 'PENDING'
    f0.set_running_or_notify_cancel()
    ex.shutdown(cancel_futures=True)
    assert f0._state == 'FINISHED'
    assert f1.cancelled()

def test_with_syntax_invokes_shutdown():
    with RayExecutor() as ex:
        pass
    assert ex._shutdown_lock




if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
