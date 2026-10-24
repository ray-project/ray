import asyncio
import sys
import threading
import time

import pytest

import ray
from ray._private.test_utils import wait_for_condition


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


@ray.remote
class SyncRecorder:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def schedule_and_wait(self, object_id_hex, timeout_s):
        """Schedule the hook, then wait for it from this method."""
        ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
            object_id_hex
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if object_id_hex in self.freed:
                return True
            time.sleep(0.01)
        return False

    def get_freed(self):
        return self.freed


@ray.remote(concurrency_groups={"freed": 1})
class GroupRecorder:
    def __init__(self):
        self.freed = []

    @ray.method(concurrency_group="freed")
    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def schedule_and_wait(self, object_id_hex, timeout_s):
        ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
            object_id_hex
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if object_id_hex in self.freed:
                return True
            time.sleep(0.01)
        return False


@ray.remote(max_concurrency=2)
class PoolRecorder:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def schedule_and_wait(self, object_id_hex, timeout_s):
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker._schedule_on_object_freed_hook_for_testing(object_id_hex)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if object_id_hex in self.freed:
                return True
            time.sleep(0.01)
        return False


@ray.remote(concurrency_groups={"other": 1})
class DefaultGroupRecorder:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)

    def _schedule_and_wait(self, object_id_hex, timeout_s):
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker._schedule_on_object_freed_hook_for_testing(object_id_hex)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if object_id_hex in self.freed:
                return True
            time.sleep(0.01)
        return False

    def schedule_and_wait(self, object_id_hex, timeout_s):
        return self._schedule_and_wait(object_id_hex, timeout_s)

    @ray.method(concurrency_group="other")
    def schedule_and_wait_in_other_group(self, object_id_hex, timeout_s):
        return self._schedule_and_wait(object_id_hex, timeout_s)

    def get_freed(self):
        return self.freed


@ray.remote(concurrency_groups={"other": 1})
class AsyncDefaultGroupRecorder:
    def __init__(self):
        self.freed = []

    async def __on_object_freed__(self, object_id):
        self.freed.append((object_id, threading.get_ident()))

    async def schedule(self, object_id_hex):
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker._schedule_on_object_freed_hook_for_testing(object_id_hex)

    async def default_group_thread(self):
        return threading.get_ident()

    @ray.method(concurrency_group="other")
    async def other_group_thread(self):
        return threading.get_ident()

    async def get_freed(self):
        return self.freed


@ray.remote
class AsyncRecorder:
    def __init__(self):
        self.freed = []

    async def __on_object_freed__(self, object_id):
        await asyncio.sleep(0)
        self.freed.append((object_id, threading.get_ident()))

    async def schedule_and_wait(self, object_id_hex, timeout_s):
        ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
            object_id_hex
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if any(object_id == object_id_hex for object_id, _ in self.freed):
                return True
            await asyncio.sleep(0.01)
        return False

    async def loop_thread(self):
        return threading.get_ident()

    async def get_freed(self):
        return self.freed


@ray.remote(concurrency_groups={"freed": 1})
class AsyncGroupRecorder:
    def __init__(self):
        self.freed = []

    @ray.method(concurrency_group="freed")
    async def __on_object_freed__(self, object_id):
        self.freed.append((object_id, threading.get_ident()))

    async def schedule_and_wait(self, object_id_hex, timeout_s):
        core_worker = ray._private.worker.global_worker.core_worker
        core_worker._schedule_on_object_freed_hook_for_testing(object_id_hex)
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if any(object_id == object_id_hex for object_id, _ in self.freed):
                return True
            await asyncio.sleep(0.01)
        return False

    async def loop_thread(self):
        return threading.get_ident()

    async def get_freed(self):
        return self.freed


@ray.remote
class AsyncActorWithSyncHook:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        # A sync hook on an asyncio actor runs on the event loop, like a sync method.
        asyncio.get_running_loop()
        self.freed.append((object_id, threading.get_ident()))

    async def schedule(self, object_id_hex):
        ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
            object_id_hex
        )

    async def loop_thread(self):
        return threading.get_ident()

    async def get_freed(self):
        return self.freed


@ray.remote
class RaisingHook:
    def __init__(self):
        self.freed = []

    def __on_object_freed__(self, object_id):
        self.freed.append(object_id)
        raise ValueError("hook failed")

    def schedule(self, object_id_hex):
        ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
            object_id_hex
        )

    def get_freed(self):
        return self.freed


def random_object_id():
    return ray.ObjectRef.from_random().hex()


def test_hook_runs_after_the_sync_actors_current_method(ray_start_regular_shared):
    # Without a group, the hook runs on the thread that runs the actor's methods, so it
    # waits for the method that scheduled it.
    actor = SyncRecorder.remote()
    object_id = random_object_id()
    assert not ray.get(actor.schedule_and_wait.remote(object_id, 1))
    wait_for_condition(lambda: ray.get(actor.get_freed.remote()) == [object_id])


def test_hook_runs_in_its_concurrency_group(ray_start_regular_shared):
    # In its own group, the hook runs while a default group method is still running.
    actor = GroupRecorder.remote()
    assert ray.get(actor.schedule_and_wait.remote(random_object_id(), 30))


def test_hook_runs_in_the_default_thread_pool(ray_start_regular_shared):
    # With max_concurrency=2, a second thread of the default pool runs the hook while
    # the method that scheduled it is still running.
    actor = PoolRecorder.remote()
    assert ray.get(actor.schedule_and_wait.remote(random_object_id(), 30))


def test_ungrouped_hook_runs_in_the_default_group(ray_start_regular_shared):
    actor = DefaultGroupRecorder.remote()
    # A method in another group doesn't hold up the hook.
    assert ray.get(
        actor.schedule_and_wait_in_other_group.remote(random_object_id(), 30)
    )
    # The default group has one thread, so the hook waits for the default group method
    # that scheduled it.
    object_id = random_object_id()
    assert not ray.get(actor.schedule_and_wait.remote(object_id, 1))
    wait_for_condition(lambda: object_id in ray.get(actor.get_freed.remote()))


def test_ungrouped_async_hook_runs_on_the_default_event_loop(ray_start_regular_shared):
    actor = AsyncDefaultGroupRecorder.remote()
    object_id = random_object_id()
    ray.get(actor.schedule.remote(object_id))
    wait_for_condition(lambda: len(ray.get(actor.get_freed.remote())) == 1)
    [(freed, thread)] = ray.get(actor.get_freed.remote())
    assert freed == object_id
    assert thread == ray.get(actor.default_group_thread.remote())
    assert thread != ray.get(actor.other_group_thread.remote())


def test_async_hook_runs_on_the_event_loop(ray_start_regular_shared):
    actor = AsyncRecorder.remote()
    object_id = random_object_id()
    assert ray.get(actor.schedule_and_wait.remote(object_id, 30))
    [(freed, thread)] = ray.get(actor.get_freed.remote())
    assert freed == object_id
    assert thread == ray.get(actor.loop_thread.remote())


def test_async_hook_runs_on_its_concurrency_groups_event_loop(ray_start_regular_shared):
    actor = AsyncGroupRecorder.remote()
    object_id = random_object_id()
    assert ray.get(actor.schedule_and_wait.remote(object_id, 30))
    [(freed, thread)] = ray.get(actor.get_freed.remote())
    assert freed == object_id
    # The "freed" group has its own event loop thread.
    assert thread != ray.get(actor.loop_thread.remote())


def test_sync_hook_on_async_actor_runs_on_the_event_loop(ray_start_regular_shared):
    actor = AsyncActorWithSyncHook.remote()
    object_id = random_object_id()
    ray.get(actor.schedule.remote(object_id))
    wait_for_condition(lambda: len(ray.get(actor.get_freed.remote())) == 1)
    [(freed, thread)] = ray.get(actor.get_freed.remote())
    assert freed == object_id
    assert thread == ray.get(actor.loop_thread.remote())


def test_hook_exception_keeps_the_actor_running(ray_start_regular_shared):
    actor = RaisingHook.remote()
    first, second = random_object_id(), random_object_id()
    ray.get(actor.schedule.remote(first))
    ray.get(actor.schedule.remote(second))
    wait_for_condition(lambda: ray.get(actor.get_freed.remote()) == [first, second])


def test_no_hook_runs_nothing(ray_start_regular_shared):
    @ray.remote
    class Plain:
        def schedule(self, object_id_hex):
            ray._private.worker.global_worker.core_worker._schedule_on_object_freed_hook_for_testing(
                object_id_hex
            )
            return "ok"

    actor = Plain.remote()
    assert ray.get(actor.schedule.remote(random_object_id())) == "ok"
    assert ray.get(actor.schedule.remote(random_object_id())) == "ok"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
