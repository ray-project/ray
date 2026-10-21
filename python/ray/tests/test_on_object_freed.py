import sys

import pytest

import ray


@ray.remote
class NoHook:
    def callback_state(self):
        return (
            ray._private.worker.global_worker.core_worker.get_producer_freed_callback()
        )


@ray.remote
class SyncHook:
    def __on_object_freed__(self, object_id):
        pass

    def callback_state(self):
        return (
            ray._private.worker.global_worker.core_worker.get_producer_freed_callback()
        )


@ray.remote
class AsyncHook:
    async def __on_object_freed__(self, object_id):
        pass

    def callback_state(self):
        return (
            ray._private.worker.global_worker.core_worker.get_producer_freed_callback()
        )


@ray.remote(concurrency_groups={"freed": 1})
class GroupHook:
    @ray.method(concurrency_group="freed")
    def __on_object_freed__(self, object_id):
        pass

    def callback_state(self):
        return (
            ray._private.worker.global_worker.core_worker.get_producer_freed_callback()
        )


class HookBase:
    def __on_object_freed__(self, object_id):
        pass


@ray.remote
class InheritedHook(HookBase):
    def callback_state(self):
        return (
            ray._private.worker.global_worker.core_worker.get_producer_freed_callback()
        )


@pytest.mark.parametrize(
    "actor_cls, expected",
    [
        (NoHook, (False, "")),
        (SyncHook, (True, "")),
        (AsyncHook, (True, "")),
        (GroupHook, (True, "freed")),
        (InheritedHook, (True, "")),
    ],
)
def test_hook_detected_at_actor_creation(ray_start_regular_shared, actor_cls, expected):
    actor = actor_cls.remote()
    assert ray.get(actor.callback_state.remote()) == expected


def test_driver_has_no_hook(ray_start_regular_shared):
    core_worker = ray._private.worker.global_worker.core_worker
    assert core_worker.get_producer_freed_callback() == (False, "")


def test_hook_not_callable_through_handle(ray_start_regular_shared):
    actor = SyncHook.remote()
    with pytest.raises(AttributeError, match="is called by Ray"):
        actor.__on_object_freed__.remote("00")
    assert ray.get(actor.callback_state.remote()) == (True, "")


class AsyncDef:
    async def __on_object_freed__(self, object_id):
        pass


class ExtraOptionalArg:
    def __on_object_freed__(self, object_id, extra=None):
        pass


class VarArgs:
    def __on_object_freed__(self, *args):
        pass


class StaticHook:
    @staticmethod
    def __on_object_freed__(object_id):
        pass


class ClassHook:
    @classmethod
    def __on_object_freed__(cls, object_id):
        pass


@pytest.mark.parametrize(
    "cls", [HookBase, AsyncDef, ExtraOptionalArg, VarArgs, StaticHook, ClassHook]
)
def test_valid_hook_signatures_accepted(cls):
    ray.remote(cls)


class NoArg:
    def __on_object_freed__(self):
        pass


class TwoArgs:
    def __on_object_freed__(self, object_id, other):
        pass


class KeywordOnlyArg:
    def __on_object_freed__(self, *, object_id):
        pass


class StaticNoArg:
    @staticmethod
    def __on_object_freed__():
        pass


class Generator:
    def __on_object_freed__(self, object_id):
        yield object_id


class AsyncGenerator:
    async def __on_object_freed__(self, object_id):
        yield object_id


class NotAMethod:
    __on_object_freed__ = None


@pytest.mark.parametrize(
    "cls, match",
    [
        (NoArg, "callable with one argument"),
        (TwoArgs, "callable with one argument"),
        (KeywordOnlyArg, "callable with one argument"),
        (StaticNoArg, "callable with one argument"),
        (Generator, "can't be a generator"),
        (AsyncGenerator, "can't be a generator"),
        (NotAMethod, "must be a method"),
    ],
)
def test_invalid_hook_rejected(cls, match):
    with pytest.raises(TypeError, match=match):
        ray.remote(cls)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
