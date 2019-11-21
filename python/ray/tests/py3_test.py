# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
import pytest

import ray
import ray.tests.cluster_utils
import ray.tests.utils


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

def test_asyncio_actor(ray_start_regular):
    @ray.remote
    class AsyncBatcher(object):
        def __init__(self):
            self.batch = []
            # The event currently need to be created from the same thread.
            # We currently run async coroutines from a different thread.
            self.event = None

        async def add(self, x):
            if self.event is None:
                self.event = asyncio.Event()
            self.batch.append(x)
            if len(self.batch) >= 3:
                self.event.set()
            else:
                await self.event.wait()
            return sorted(self.batch)

    a = AsyncBatcher.options(is_direct_call=True, is_asyncio=True).remote()
    x1 = a.add.remote(1)
    x2 = a.add.remote(2)
    x3 = a.add.remote(3)
    r1 = ray.get(x1)
    r2 = ray.get(x2)
    r3 = ray.get(x3)
    assert r1 == [1, 2, 3]
    assert r1 == r2 == r3
