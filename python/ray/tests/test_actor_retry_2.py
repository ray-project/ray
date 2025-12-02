import os
import sys
from collections import defaultdict
from typing import Optional

import pytest

import ray
from ray._common.test_utils import SignalActor


class MyError(Exception):
    pass


@ray.remote
class Counter:
    def __init__(self) -> None:
        self._counts = defaultdict(int)

    def increment(self, key: Optional[str] = None) -> int:
        key = key or "default"
        c = self._counts[key]
        self._counts[key] += 1
        return c

    def get_count(self, key: Optional[str] = None) -> int:
        return self._counts[key or "default"]


@ray.remote(max_task_retries=3)
class TroubleMaker:
    def __init__(self, *, counter_key: Optional[str] = None):
        self._counter_key = counter_key

    @ray.method(max_task_retries=5, retry_exceptions=[MyError])
    def may_raise_n_times(self, counter, n):
        """
        Raises if there were n calls before this call.
        Returns the number of calls before this call, if it's > n.
        """
        c = ray.get(counter.increment.remote(self._counter_key))
        print(f"may_raise_n_times, n = {n}, count = {c}")
        if c < n:
            print(f"method raises in {c} th call, want {n} times")
            raise MyError()
        return c

    @ray.method(retry_exceptions=[MyError])
    def raise_or_exit(self, counter, actions):
        """
        Increments the counter and performs an action based on the param actions[count].
        If count >= len(actions), return the count.

        Note: This method doesn't set `max_task_retries`. Ray expects it to inherit
        max_task_retries = 3.

        @param actions: List["raise" | "exit"]
        """

        c = ray.get(counter.increment.remote(self._counter_key))
        action = "return" if c >= len(actions) else actions[c]
        print(f"raise_or_exit, action = {action}, count = {c}")

        if action == "raise":
            raise MyError()
        elif action == "exit":
            sys.exit(1)
        else:
            return c


@ray.remote(max_task_retries=3)
class AsyncTroubleMaker:
    """
    Same as TroubleMaker, just all methods are async.
    """

    def __init__(self, *, counter_key: Optional[str] = None):
        self._counter_key = counter_key

    @ray.method(max_task_retries=5, retry_exceptions=[MyError])
    async def may_raise_n_times(self, counter, n):
        c = await counter.increment.remote(self._counter_key)
        print(f"may_raise_n_times, n = {n}, count = {c}")
        if c < n:
            print(f"method raises in {c} th call, want {n} times")
            raise MyError()
        return c

    @ray.method(retry_exceptions=[MyError])
    async def raise_or_exit(self, counter, actions):
        c = await counter.increment.remote(self._counter_key)
        action = "return" if c >= len(actions) else actions[c]
        print(f"raise_or_exit, action = {action}, count = {c}")

        if action == "raise":
            raise MyError()
        elif action == "exit":
            # import signal
            # sys.exit(1) -> hang
            # ray.actor.exit_actor() -> failed, no retry
            # os.kill(os.getpid(), signal.SIGTERM) -> ignored, continued to return
            # os.kill(os.getpid(), signal.SIGKILL) -> retries
            os._exit(0)
            return -42
        else:
            return c

    @ray.method(num_returns="streaming")  # retry_exceptions=None aka False.
    async def yield_or_raise(self, counter, actions):
        while True:
            c = await counter.increment.remote(self._counter_key)
            a = actions[c]
            if isinstance(a, BaseException):
                raise a
            else:
                yield a
            if c == len(actions) - 1:
                # don't over call counter. Only call #yield and #raise times.
                return


def test_generator_method_no_retry_without_retry_exceptions(ray_start_regular_shared):
    counter = Counter.remote()
    trouble_maker = AsyncTroubleMaker.remote()

    gen = trouble_maker.yield_or_raise.remote(
        counter,
        [
            # First round: 1 then raise
            1,
            MyError(),
            # No retry, no second round
            1,
            2,
        ],
    )
    assert ray.get(next(gen)) == 1
    with pytest.raises(MyError):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    assert ray.get(counter.get_count.remote()) == 2


def test_generator_method_retry_exact_times(ray_start_regular_shared):
    counter = Counter.remote()
    trouble_maker = AsyncTroubleMaker.remote()

    # Should retry out max_task_retries=3 times
    gen = trouble_maker.yield_or_raise.options(retry_exceptions=[MyError]).remote(
        counter,
        [
            # First round
            1,
            MyError(),
            # retry 1
            1,
            MyError(),
            # retry 2
            1,
            MyError(),
            # retry 3
            1,
            2,
            3,
        ],
    )
    assert ray.get(next(gen)) == 1
    assert ray.get(next(gen)) == 2
    assert ray.get(next(gen)) == 3
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    assert ray.get(counter.get_count.remote()) == 9


def test_generator_method_does_not_over_retry(ray_start_regular_shared):
    counter = Counter.remote()
    trouble_maker = AsyncTroubleMaker.remote()

    # Should retry out max_task_retries=3 times
    gen = trouble_maker.yield_or_raise.options(retry_exceptions=[MyError]).remote(
        counter,
        [
            # First round
            1,
            MyError(),
            # retry 1
            1,
            MyError(),
            # retry 2,
            1,
            MyError(),
            # retry 3
            1,
            MyError(),
            # no retry 4!
            1,
            2,
        ],
    )
    assert ray.get(next(gen)) == 1
    with pytest.raises(MyError):
        ray.get(next(gen))
    with pytest.raises(StopIteration):
        ray.get(next(gen))
    assert ray.get(counter.get_count.remote()) == 8


@pytest.mark.parametrize(
    "actions",
    [
        ["exit", "exit"],
        ["exit", "raise"],
        ["raise", "exit"],
        ["raise", "raise"],
    ],
    ids=lambda lst: ",".join(lst),  # test case show name
)
@pytest.mark.parametrize(
    "max_retries_and_restarts", [-1, 2], ids=lambda r: f"max_retries_and_restarts({r})"
)
def test_method_raise_and_exit(
    actions, max_retries_and_restarts, ray_start_regular_shared
):
    """
    Test we can endure a mix of raises and exits. Note the number of exits we can endure
    is subject to max_restarts.

    The retry behavior should work for Async actors and Threaded actors.
    The retry behavior should work if the max_task_retries or max_restarts are -1
        (infinite retry).
    """
    # NOTE(edoakes): we test on all three types of actors in parallel to reduce the
    # time taken to run the test in CI.
    counter = Counter.remote()
    sync_actor = TroubleMaker.options(max_restarts=max_retries_and_restarts).remote(
        counter_key="sync",
    )
    async_actor = AsyncTroubleMaker.options(
        max_restarts=max_retries_and_restarts
    ).remote(
        counter_key="async",
    )
    threaded_actor = TroubleMaker.options(
        max_restarts=max_retries_and_restarts, max_concurrency=2
    ).remote(counter_key="threaded")

    assert ray.get(
        [
            actor.raise_or_exit.options(
                max_task_retries=max_retries_and_restarts
            ).remote(counter, actions)
            for actor in [sync_actor, async_actor, threaded_actor]
        ]
    ) == [2, 2, 2]

    # Should expect 3 total tries from each actor: 1 initial + 2 retries
    assert ray.get(
        [counter.get_count.remote(option) for option in ["sync", "async", "threaded"]]
    ) == [3, 3, 3]


@pytest.mark.parametrize(
    "actions_and_error",
    [
        (["raise", "raise", "raise"], MyError),
        (["exit", "raise", "raise"], MyError),
        (["raise", "exit", "raise"], MyError),
        # Last try is exit, the actor restarted.
        (["raise", "raise", "exit"], ray.exceptions.ActorUnavailableError),
        # Last try is exit, the actor is dead (exceeded max_restarts).
        (["raise", "exit", "exit"], ray.exceptions.ActorDiedError),
    ],
    ids=lambda p: ",".join(p[0]),  # test case show name
)
def test_method_raise_and_exit_no_over_retry(
    actions_and_error, ray_start_regular_shared
):
    """
    Test we do not over retry.

    The retry behavior should work for Async actors and Threaded actors.
    The retry behavior should work if the max_task_retries or max_restarts are -1
        (infinite retry).
    """
    max_restarts = 1
    max_task_retries = 2
    actions, error = actions_and_error

    # NOTE(edoakes): we test on all three types of actors in parallel to reduce the
    # time taken to run the test in CI.
    counter = Counter.remote()
    sync_actor = TroubleMaker.options(max_restarts=max_restarts).remote(
        counter_key="sync",
    )
    async_actor = AsyncTroubleMaker.options(max_restarts=max_restarts).remote(
        counter_key="async",
    )
    threaded_actor = TroubleMaker.options(
        max_restarts=max_restarts, max_concurrency=2
    ).remote(counter_key="threaded")

    for ref in [
        a.raise_or_exit.options(max_task_retries=max_task_retries).remote(
            counter, actions
        )
        for a in [sync_actor, async_actor, threaded_actor]
    ]:
        with pytest.raises(error):
            ray.get(ref)

    # 3 = 1 initial + 2 retries (with the 1 restart included)
    assert ray.get(
        [counter.get_count.remote(key) for key in ["sync", "async", "threaded"]]
    ) == [3, 3, 3]


def test_task_retries_on_exit(ray_start_regular_shared):
    """Sanity check that task retries work when the actor exits."""
    counter = Counter.remote()
    sync_actor = TroubleMaker.options(max_restarts=2).remote(counter_key="sync")
    async_actor = AsyncTroubleMaker.options(max_restarts=2).remote(counter_key="async")

    for ref in [
        a.raise_or_exit.options(max_task_retries=2).remote(
            counter, ["exit", "exit", "exit"]
        )
        for a in [sync_actor, async_actor]
    ]:
        with pytest.raises(ray.exceptions.RayActorError):
            ray.get(ref)

    # 3 = 1 initial + 2 retries (with the 2 restarts included)
    assert ray.get([counter.get_count.remote(key) for key in ["sync", "async"]]) == [
        3,
        3,
    ]


def test_retry_dependent_task_on_same_actor(ray_start_regular_shared):
    """
    1. Create an actor
    2. Submit an actor task (one).
    3. Submit another actor task (two) that depends on the output of one.
    4. Allow the first attempt of one to fail.
    5. Expect the second attempt of one to be run, and for two to be unblocked.

    The goal of this test is to make sure later actor tasks with dependencies on
    earlier ones don't result in deadlock when the earlier tasks need to be retried.
    See https://github.com/ray-project/ray/pull/54034 for more context.
    """

    @ray.remote
    class Actor:
        def __init__(self):
            self._counter = 0

        @ray.method(max_task_retries=1, retry_exceptions=[MyError])
        def one(self, signal_actor):
            ray.get(signal_actor.wait.remote())
            self._counter += 1
            # Fail on the first invocation.
            if self._counter <= 1:
                raise MyError()
            return 1

        def two(self, one_output):
            return 2

    signal_actor = SignalActor.remote()
    actor = Actor.remote()
    one_output_ref = actor.one.remote(signal_actor)
    two_output_ref = actor.two.remote(one_output_ref)
    # Unblock so the first attempt can fail and the second attempt gets submitted.
    ray.get(signal_actor.send.remote())

    assert ray.get(two_output_ref) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
