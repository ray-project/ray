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


@ray.remote(max_task_retries=3)
class TroubleMaker:
    @ray.method(max_retries=5, retry_exceptions=[MyError])
    def may_raise_n_times(self, counter, n):
        """
        raises if there had been n calls before this call.
        returns the number of calls before this call, if it's > n.
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
        Increments the counter, and performs an action based on the param actions[count].
        If count >= len(actions), return the count.

        Note: this method is not marked with `max_retries`, we expect it to inherit max_task_retries = 3.

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


def test_options_takes_precedence(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()
    assert (
        ray.get(
            trouble_maker.may_raise_n_times.options(max_retries=10).remote(counter, 10)
        )
        == 10
    )
    assert ray.get(counter.get_count.remote()) == 11


def test_options_takes_precedence_no_over_retry(shutdown_only):
    counter = Counter.remote()
    trouble_maker = TroubleMaker.remote()

    with pytest.raises(MyError):
        ray.get(
            trouble_maker.may_raise_n_times.options(max_retries=10).remote(counter, 11)
        )
    assert ray.get(counter.get_count.remote()) == 11


def test_method_raise_and_exit(shutdown_only):
    # TODO: not working right now, needs debugging. Symptom: actor retry eats 3 or 4 retries:
    # [2023-11-22 15:56:48,108 I 64575 34262189] task_manager.cc:1024: FailOrRetryPendingTask c2668a65bda616c1b5f72568bd8651cd3bfb59ae01000000, error 1, status UnexpectedSystemExit: Worker exits with an exit code 1., error info error_message: "Death cause not recorded."
    # [2023-11-22 15:56:48,109 I 64575 34262189] task_manager.cc:1024: FailOrRetryPendingTask c2668a65bda616c1b5f72568bd8651cd3bfb59ae01000000, error 1, status GrpcUnavailable: RPC Error message: Cancelling all calls; RPC Error details: , error info error_message: "Death cause not recorded."
    # [2023-11-22 15:56:48,110 I 64575 34262189] task_manager.cc:1024: FailOrRetryPendingTask c2668a65bda616c1b5f72568bd8651cd3bfb59ae01000000, error 1, status IOError: Fail all inflight tasks due to actor state change., error info error_message: "Death cause not recorded."
    # [2023-11-22 15:57:18,400 I 64575 34262189] task_manager.cc:1024: FailOrRetryPendingTask c2668a65bda616c1b5f72568bd8651cd3bfb59ae01000000, error 1, status Invalid: client cancelled stale rpc, error info error_message: "Death cause not recorded."
    # TroubleMaker doesn't have a max_restarts so we need to add in options.
    counter = Counter.remote()
    trouble_maker = TroubleMaker.options(max_restarts=1).remote()
    assert ray.get(trouble_maker.raise_or_exit.remote(counter, ["exit", "raise"])) == 2
    assert ray.get(counter.get_count.remote()) == 3


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
