from __future__ import annotations

from ray.experimental.better_actor import ActorMixin, remote_method
import ray
import pytest


def test_define_actor_with_mixin(ray_start_2_cpus):
    class Actor(ActorMixin):
        def __init__(self, x: int) -> None:
            self.x = x

        @remote_method
        def add(self, value: int) -> int:
            return self.x + value

    actor = Actor.new_actor().remote(1)
    ret_ref = actor.methods.add.remote(10)
    assert ray.get(ret_ref) == 11


def test_define_actor_call_another_method(ray_start_2_cpus):
    class Actor(ActorMixin):
        def __init__(self, x: int) -> None:
            self.x = x

        @remote_method
        def add(self, value: int) -> int:
            return self.x + value

        @remote_method
        def sum(self, v1: int, v2: int) -> int:
            return sum([self.add(v1), self.add(v2)])

    actor = Actor.new_actor().remote(1)
    ret_ref = actor.methods.sum.remote(1, 2)
    assert ray.get(ret_ref) == 5


def test_define_actor_with_default_opts(ray_start_2_cpus):
    class Actor(ActorMixin, name="demo"):
        def __init__(self, x: int) -> None:
            self.x = x

        @remote_method
        def get_actor_name(self) -> str:
            ctx = ray.get_runtime_context()
            return ctx.get_actor_name()

    actor = Actor.new_actor().remote(1)
    ret_ref = actor.methods.get_actor_name.remote()
    assert ray.get(ret_ref) == "demo"


def test_define_actor_override_default_opts(ray_start_2_cpus):
    class Actor(ActorMixin, name="demo"):
        @remote_method
        def get_actor_name(self) -> str:
            ctx = ray.get_runtime_context()
            return ctx.get_actor_name()

    actor = Actor.new_actor().options(name="demo2").remote()
    ret_ref = actor.methods.get_actor_name.remote()
    assert ray.get(ret_ref) == "demo2"


def test_define_async_actor(ray_start_2_cpus):
    class Actor(ActorMixin):
        @remote_method
        async def notify(self, msg: str) -> str:
            return await self.add_prefix(msg)

        @remote_method
        async def add_prefix(self, msg: str) -> str:
            return "hello " + msg

    actor = Actor.new_actor().remote()
    ret_ref = actor.methods.notify.remote("ray")
    assert ray.get(ret_ref) == "hello ray"


def test_specify_concurrency_groups(ray_start_2_cpus):
    class Actor(ActorMixin, concurrency_groups={"io": 2}):
        @remote_method(concurrency_group="io")
        async def echo(self, msg: str) -> str:
            return msg

    actor = Actor.new_actor().remote()
    ret_ref = actor.methods.echo.remote("ray")
    assert ray.get(ret_ref) == "ray"


def test_directly_call_remote_method_will_fail(ray_start_2_cpus):
    class Actor(ActorMixin):
        def __init__(self, x: int) -> None:
            self.x = x

        @remote_method
        def add(self, value: int) -> int:
            return self.x + value

    actor = Actor.new_actor().remote(1)
    with pytest.raises(TypeError, match="Actor methods cannot be called directly"):
        actor.methods.add(1)
