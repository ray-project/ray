import dask
import pytest

import ray
from ray.util.dask import ray_dask_get, RayDaskCallback


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@dask.delayed
def add(x, y):
    return x + y


def test_callback_active():
    """Test that callbacks are active within context"""
    assert not RayDaskCallback.ray_active

    with RayDaskCallback():
        assert RayDaskCallback.ray_active

    assert not RayDaskCallback.ray_active


def test_presubmit_shortcircuit(ray_start_1_cpu):
    """
    Test that presubmit return short-circuits task submission, and that task's
    result is set to the presubmit return value.
    """

    class PresubmitShortcircuitCallback(RayDaskCallback):
        def _ray_presubmit(self, task, key, deps):
            return 0

        def _ray_postsubmit(self, task, key, deps, object_ref):
            pytest.fail(
                "_ray_postsubmit shouldn't be called when "
                "_ray_presubmit returns a value"
            )

    with PresubmitShortcircuitCallback():
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert result == 0


def test_pretask_posttask_shared_state(ray_start_1_cpu):
    """
    Test that pretask return value is passed to corresponding posttask
    callback.
    """

    class PretaskPosttaskCallback(RayDaskCallback):
        def _ray_pretask(self, key, object_refs):
            return key

        def _ray_posttask(self, key, result, pre_state):
            assert pre_state == key

    with PretaskPosttaskCallback():
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert result == 5


def test_postsubmit(ray_start_1_cpu):
    """
    Test that postsubmit is called after each task.
    """

    class PostsubmitCallback(RayDaskCallback):
        def __init__(self, postsubmit_actor):
            self.postsubmit_actor = postsubmit_actor

        def _ray_postsubmit(self, task, key, deps, object_ref):
            self.postsubmit_actor.postsubmit.remote(task, key, deps, object_ref)

    @ray.remote
    class PostsubmitActor:
        def __init__(self):
            self.postsubmit_counter = 0

        def postsubmit(self, task, key, deps, object_ref):
            self.postsubmit_counter += 1

        def get_postsubmit_counter(self):
            return self.postsubmit_counter

    postsubmit_actor = PostsubmitActor.remote()
    with PostsubmitCallback(postsubmit_actor):
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert ray.get(postsubmit_actor.get_postsubmit_counter.remote()) == 1
    assert result == 5


def test_postsubmit_all(ray_start_1_cpu):
    """
    Test that postsubmit_all is called once.
    """

    class PostsubmitAllCallback(RayDaskCallback):
        def __init__(self, postsubmit_all_actor):
            self.postsubmit_all_actor = postsubmit_all_actor

        def _ray_postsubmit_all(self, object_refs, dsk):
            self.postsubmit_all_actor.postsubmit_all.remote(object_refs, dsk)

    @ray.remote
    class PostsubmitAllActor:
        def __init__(self):
            self.postsubmit_all_called = False

        def postsubmit_all(self, object_refs, dsk):
            self.postsubmit_all_called = True

        def get_postsubmit_all_called(self):
            return self.postsubmit_all_called

    postsubmit_all_actor = PostsubmitAllActor.remote()
    with PostsubmitAllCallback(postsubmit_all_actor):
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert ray.get(postsubmit_all_actor.get_postsubmit_all_called.remote())
    assert result == 5


def test_finish(ray_start_1_cpu):
    """
    Test that finish callback is called once.
    """

    class FinishCallback(RayDaskCallback):
        def __init__(self, finish_actor):
            self.finish_actor = finish_actor

        def _ray_finish(self, result):
            self.finish_actor.finish.remote(result)

    @ray.remote
    class FinishActor:
        def __init__(self):
            self.finish_called = False

        def finish(self, result):
            self.finish_called = True

        def get_finish_called(self):
            return self.finish_called

    finish_actor = FinishActor.remote()
    with FinishCallback(finish_actor):
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert ray.get(finish_actor.get_finish_called.remote())
    assert result == 5


def test_multiple_callbacks(ray_start_1_cpu):
    """
    Test that multiple callbacks are supported.
    """

    class PostsubmitCallback(RayDaskCallback):
        def __init__(self, postsubmit_actor):
            self.postsubmit_actor = postsubmit_actor

        def _ray_postsubmit(self, task, key, deps, object_ref):
            self.postsubmit_actor.postsubmit.remote(task, key, deps, object_ref)

    @ray.remote
    class PostsubmitActor:
        def __init__(self):
            self.postsubmit_counter = 0

        def postsubmit(self, task, key, deps, object_ref):
            self.postsubmit_counter += 1

        def get_postsubmit_counter(self):
            return self.postsubmit_counter

    postsubmit_actor = PostsubmitActor.remote()
    cb1 = PostsubmitCallback(postsubmit_actor)
    cb2 = PostsubmitCallback(postsubmit_actor)
    cb3 = PostsubmitCallback(postsubmit_actor)
    with cb1, cb2, cb3:
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert ray.get(postsubmit_actor.get_postsubmit_counter.remote()) == 3
    assert result == 5


def test_pretask_posttask_shared_state_multi(ray_start_1_cpu):
    """
    Test that pretask return values are passed to the correct corresponding
    posttask callbacks when multiple callbacks are given.
    """

    class PretaskPosttaskCallback(RayDaskCallback):
        def __init__(self, suffix):
            self.suffix = suffix

        def _ray_pretask(self, key, object_refs):
            return key + self.suffix

        def _ray_posttask(self, key, result, pre_state):
            assert pre_state == key + self.suffix

    class PretaskOnlyCallback(RayDaskCallback):
        def _ray_pretask(self, key, object_refs):
            return "baz"

    class PosttaskOnlyCallback(RayDaskCallback):
        def _ray_posttask(self, key, result, pre_state):
            assert pre_state is None

    cb1 = PretaskPosttaskCallback("foo")
    cb2 = PretaskOnlyCallback()
    cb3 = PosttaskOnlyCallback()
    cb4 = PretaskPosttaskCallback("bar")
    with cb1, cb2, cb3, cb4:
        z = add(2, 3)
        result = z.compute(scheduler=ray_dask_get)

    assert result == 5


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
