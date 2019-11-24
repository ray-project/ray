# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import asyncio
import threading
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


def test_asyncio_actor(ray_start_regular):
    @ray.remote
    class AsyncBatcher(object):
        def __init__(self):
            self.batch = []
            self.event = asyncio.Event()

        async def add(self, x):
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


def test_asyncio_actor_same_thread(ray_start_regular):
    @ray.remote
    class Actor:
        def sync_thread_id(self):
            return threading.current_thread().ident

        async def async_thread_id(self):
            return threading.current_thread().ident

    a = Actor.options(is_direct_call=True, is_asyncio=True).remote()
    sync_id, async_id = ray.get(
        [a.sync_thread_id.remote(),
         a.async_thread_id.remote()])
    assert sync_id == async_id


def test_asyncio_actor_concurrency(ray_start_regular):
    @ray.remote
    class RecordOrder:
        def __init__(self):
            self.history = []

        async def do_work(self):
            self.history.append("STARTED")
            # Force a context switch
            await asyncio.sleep(0)
            self.history.append("ENDED")

        def get_history(self):
            return self.history

    num_calls = 10

    a = RecordOrder.options(
        is_direct_call=True, max_concurrency=1, is_asyncio=True).remote()
    ray.get([a.do_work.remote() for _ in range(num_calls)])
    history = ray.get(a.get_history.remote())

    # We only care about ordered start-end-start-end sequence because
    # coroutines may be executed out of enqueued order.
    answer = []
    for _ in range(num_calls):
        for status in ["STARTED", "ENDED"]:
            answer.append(status)

    assert history == answer
