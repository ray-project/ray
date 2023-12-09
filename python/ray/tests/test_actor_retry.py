import os
import sys

import pytest

import ray


class MyError(Exception):
    pass


@ray.remote
class Counter:
    def __init__(self) -> None:
        self.count = 0

    def increment(self) -> int:
        c = self.count
        self.count += 1
        return c

    def get_count(self) -> int:
        return self.count


# TODO: also do work for async and threaded actors
@ray.remote(max_task_retries=3)
class TroubleMaker:
    @ray.method(max_task_retries=5, retry_exceptions=[MyError])
    def may_raise_n_times(self, counter, n):
        """
        Raises if there were n calls before this call.
        Returns the number of calls before this call, if it's > n.
        """
        c = ray.get(counter.increment.remote())
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

        c = ray.get(counter.increment.remote())
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

    @ray.method(max_task_retries=5, retry_exceptions=[MyError])
    async def may_raise_n_times(self, counter, n):
        c = ray.get(counter.increment.remote())
        print(f"may_raise_n_times, n = {n}, count = {c}")
        if c < n:
            print(f"method raises in {c} th call, want {n} times")
            raise MyError()
        return c

    @ray.method(retry_exceptions=[MyError])
    async def raise_or_exit(self, counter, actions):
        c = await counter.increment.remote()
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
            c = await counter.increment.remote()
            a = actions[c]
            if isinstance(a, BaseException):
                raise a
            else:
                yield a
            if c == len(actions) - 1:
                # don't over call counter. Only call #yield and #raise times.
                return


def test_method_raise_5_times(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()
    assert ray.get(trouble_maker.may_raise_n_times.remote(counter, 5)) == 5
    assert ray.get(counter.get_count.remote()) == 6


def test_method_raise_no_over_retry(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()
    with pytest.raises(MyError):
        ray.get(trouble_maker.may_raise_n_times.remote(counter, 6))
    assert ray.get(counter.get_count.remote()) == 6


def test_method_no_retry_without_retry_exceptions(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()
    with pytest.raises(MyError):
        ray.get(
            trouble_maker.may_raise_n_times.options(retry_exceptions=False).remote(
                counter, 5
            )
        )
    assert ray.get(counter.get_count.remote()) == 1


def test_generator_method_no_retry_without_retry_exceptions(shutdown_only):
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


def test_generator_method_retry_exact_times(shutdown_only):
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


def test_generator_method_does_not_over_retry(shutdown_only):
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


def test_options_takes_precedence(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()
    assert (
        ray.get(
            trouble_maker.may_raise_n_times.options(max_task_retries=10).remote(
                counter, 10
            )
        )
        == 10
    )
    assert ray.get(counter.get_count.remote()) == 11


def test_options_takes_precedence_no_over_retry(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()

    with pytest.raises(MyError):
        ray.get(
            trouble_maker.may_raise_n_times.options(max_task_retries=10).remote(
                counter, 11
            )
        )
    assert ray.get(counter.get_count.remote()) == 11


@pytest.mark.parametrize(
    "actions",
    [
        ["exit", "raise", "raise"],
        ["raise", "exit", "raise"],
        ["raise", "raise", "exit"],
        ["raise", "raise", "raise"],
    ],
    ids=lambda lst: ",".join(lst),  # test case show name
)
@pytest.mark.parametrize("actor_model", ["sync", "async", "threaded"])
@pytest.mark.parametrize("max_restarts", [-1, 4], ids=lambda r: f"max_restarts({r})")
@pytest.mark.parametrize(
    "max_task_retries", [-1, 4], ids=lambda r: f"max_task_retries({r})"
)
def test_method_raise_and_exit(
    actions, actor_model, max_restarts, max_task_retries, shutdown_only
):
    """
    Test we can endure a mix of raises and exits. Note the number of exits we can endure
    is subject to max_restarts.

    The retry behavior should work for Async actors and Threaded actors.
    The retry behavior should work if the max_task_retries or max_restarts are -1
        (infinite retry).
    """
    if actor_model == "sync":
        trouble_maker = TroubleMaker.options(max_restarts=max_restarts).remote()
    elif actor_model == "async":
        trouble_maker = AsyncTroubleMaker.options(max_restarts=max_restarts).remote()
    elif actor_model == "threaded":
        trouble_maker = TroubleMaker.options(
            max_restarts=max_restarts, max_concurrency=2
        ).remote()
    else:
        assert False, f"unrecognized actor_model: {actor_model}"

    counter = Counter.remote()
    assert (
        ray.get(
            trouble_maker.raise_or_exit.options(
                max_task_retries=max_task_retries
            ).remote(counter, actions)
        )
        == 3
    )
    # 4 = 1 initial + 3 retries (with the 1 restart included)
    assert ray.get(counter.get_count.remote()) == 4


@pytest.mark.parametrize(
    "actions_and_error",
    [
        (["raise", "raise", "raise"], MyError),
        (["exit", "raise", "raise"], MyError),
        (["raise", "exit", "raise"], MyError),
        (["raise", "raise", "exit"], ray.exceptions.RayActorError),
    ],
    ids=lambda p: ",".join(p[0]),  # test case show name
)
@pytest.mark.parametrize("actor_model", ["sync", "async", "threaded"])
def test_method_raise_and_exit_no_over_retry(
    actions_and_error, actor_model, shutdown_only
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

    if actor_model == "sync":
        trouble_maker = TroubleMaker.options(max_restarts=max_restarts).remote()
    elif actor_model == "async":
        trouble_maker = AsyncTroubleMaker.options(max_restarts=max_restarts).remote()
    elif actor_model == "threaded":
        trouble_maker = TroubleMaker.options(
            max_restarts=max_restarts, max_concurrency=2
        ).remote()
    else:
        assert False, f"unrecognized actor_model: {actor_model}"

    counter = Counter.remote()
    with pytest.raises(error):
        ray.get(
            trouble_maker.raise_or_exit.options(
                max_task_retries=max_task_retries
            ).remote(counter, actions)
        )
    # 3 = 1 initial + 2 retries (with the 1 restart included)
    assert ray.get(counter.get_count.remote()) == 3


def test_method_exit_no_over_retry_max_restarts(shutdown_only):
    """
    Even if we have enough max_task_retries, we may still raise due to max_restarts.
    """
    counter = Counter.remote()
    trouble_maker = TroubleMaker.options(max_restarts=1).remote()
    with pytest.raises(ray.exceptions.RayActorError):
        assert ray.get(
            trouble_maker.raise_or_exit.options(max_task_retries=4).remote(
                counter, ["raise", "exit", "exit"]
            )
        )
    # 2 calls: 1 initial + 1 exception-retry + 1 exit-retry (then no more)
    assert ray.get(counter.get_count.remote()) == 3


@pytest.mark.parametrize(
    "is_async", [False, True], ids=lambda a: "async" if a else "sync"
)
def test_exit_only(is_async, shutdown_only):
    """
    Sanity testing: only do exit-retry works
    """
    actor_class = AsyncTroubleMaker if is_async else TroubleMaker
    counter = Counter.remote()
    trouble_maker = actor_class.options(max_restarts=2).remote()
    with pytest.raises(ray.exceptions.RayActorError):
        ret = ray.get(
            trouble_maker.raise_or_exit.options(max_task_retries=2).remote(
                counter, ["exit", "exit", "exit"]
            )
        )
        print(f"should not print: ret = {ret}")
    # 3 = 1 initial + 2 retries (with the 2 restarts included)
    assert ray.get(counter.get_count.remote()) == 3


def test_exit_only_no_over_retry(shutdown_only):
    """
    Sanity testing: only do exit-retry works
    """
    counter = Counter.remote()
    trouble_maker = TroubleMaker.options(max_restarts=2).remote()
    ray.get(
        trouble_maker.raise_or_exit.options(max_task_retries=2).remote(
            counter, ["exit", "exit"]
        )
    ) == 2
    assert ray.get(counter.get_count.remote()) == 3


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
