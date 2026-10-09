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
    sys.exit(pytest.main(["-sv", __file__]))
