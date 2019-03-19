from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

import ray
import ray.exceptions
import ray.experimental.no_return
import ray.worker


def test_set_single_output(ray_start_regular):
    @ray.remote
    def f():
        return_object_ids = ray.worker.global_worker._current_task.returns()
        ray.worker.global_worker.put_object(return_object_ids[0], 123)
        return ray.experimental.no_return.NoReturn

    assert ray.get(f.remote()) == 123


def test_set_multiple_outputs(ray_start_regular):
    @ray.remote(num_return_vals=3)
    def f(set_out0, set_out1, set_out2):
        returns = []
        return_object_ids = ray.worker.global_worker._current_task.returns()
        for i, set_out in enumerate([set_out0, set_out1, set_out2]):
            if set_out:
                ray.worker.global_worker.put_object(return_object_ids[i], True)
                returns.append(ray.experimental.no_return.NoReturn)
            else:
                returns.append(False)
        return tuple(returns)

    for set_out0 in [True, False]:
        for set_out1 in [True, False]:
            for set_out2 in [True, False]:
                result_object_ids = f.remote(set_out0, set_out1, set_out2)
                assert ray.get(result_object_ids) == [
                    set_out0, set_out1, set_out2
                ]


def test_set_actor_method(ray_start_regular):
    @ray.remote
    class Actor(object):
        def __init__(self):
            pass

        def ping(self):
            return_object_ids = ray.worker.global_worker._current_task.returns(
            )
            ray.worker.global_worker.put_object(return_object_ids[0], 123)
            return ray.experimental.no_return.NoReturn

    actor = Actor.remote()
    assert ray.get(actor.ping.remote()) == 123


def test_exception(ray_start_regular):
    @ray.remote(num_return_vals=2)
    def f():
        return_object_ids = ray.worker.global_worker._current_task.returns()
        # The first return value is successfully stored in the object store
        ray.worker.global_worker.put_object(return_object_ids[0], 123)
        raise Exception("Error")
        # The exception is stored at the second return objcet ID.
        return ray.experimental.no_return.NoReturn, 456

    object_id, exception_id = f.remote()

    assert ray.get(object_id) == 123
    with pytest.raises(ray.exceptions.RayTaskError):
        ray.get(exception_id)


def test_no_set_and_no_return(ray_start_regular):
    @ray.remote
    def f():
        return ray.experimental.no_return.NoReturn

    object_id = f.remote()
    with pytest.raises(ray.exceptions.RayTaskError) as e:
        ray.get(object_id)
    assert "Attempting to return 'ray.experimental.NoReturn'" in str(e.value)
