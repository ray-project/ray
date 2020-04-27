# coding: utf-8
import pytest

import ray
import ray.cluster_utils
import ray.test_utils


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": True
    }, {
        "local_mode": False
    }],
    indirect=True)
def test_args_force_positional(ray_start_regular):
    def force_positional(*, a="hello", b="helxo", **kwargs):
        return a, b, kwargs

    class TestActor():
        def force_positional(self, a="hello", b="heo", *args, **kwargs):
            return a, b, args, kwargs

    def test_function(fn, remote_fn):
        assert fn(a=1, b=3, c=5) == ray.get(remote_fn.remote(a=1, b=3, c=5))
        assert fn(a=1) == ray.get(remote_fn.remote(a=1))
        assert fn(a=1) == ray.get(remote_fn.remote(a=1))

    remote_test_function = ray.remote(test_function)

    remote_force_positional = ray.remote(force_positional)
    test_function(force_positional, remote_force_positional)
    ray.get(
        remote_test_function.remote(force_positional, remote_force_positional))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.force_positional
    local_actor = TestActor()
    local_method = local_actor.force_positional
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


@pytest.mark.parametrize(
    "ray_start_regular", [{
        "local_mode": False
    }, {
        "local_mode": True
    }],
    indirect=True)
def test_args_intertwined(ray_start_regular):
    def args_intertwined(a, *args, x="hello", **kwargs):
        return a, args, x, kwargs

    class TestActor():
        def args_intertwined(self, a, *args, x="hello", **kwargs):
            return a, args, x, kwargs

        @classmethod
        def cls_args_intertwined(cls, a, *args, x="hello", **kwargs):
            return a, args, x, kwargs

    def test_function(fn, remote_fn):
        assert fn(
            1, 2, 3, x="hi", y="hello") == ray.get(
                remote_fn.remote(1, 2, 3, x="hi", y="hello"))
        assert fn(
            1, 2, 3, y="1hello") == ray.get(
                remote_fn.remote(1, 2, 3, y="1hello"))
        assert fn(1, y="1hello") == ray.get(remote_fn.remote(1, y="1hello"))

    remote_test_function = ray.remote(test_function)

    remote_args_intertwined = ray.remote(args_intertwined)
    test_function(args_intertwined, remote_args_intertwined)
    ray.get(
        remote_test_function.remote(args_intertwined, remote_args_intertwined))

    remote_actor_class = ray.remote(TestActor)
    remote_actor = remote_actor_class.remote()
    actor_method = remote_actor.args_intertwined
    local_actor = TestActor()
    local_method = local_actor.args_intertwined
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))

    actor_method = remote_actor.cls_args_intertwined
    local_actor = TestActor()
    local_method = local_actor.cls_args_intertwined
    test_function(local_method, actor_method)
    ray.get(remote_test_function.remote(local_method, actor_method))


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
