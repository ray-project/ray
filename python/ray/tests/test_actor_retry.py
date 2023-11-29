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


# def test_method_raise_5_times(shutdown_only):
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.remote()
#     assert ray.get(trouble_maker.may_raise_n_times.remote(counter, 5)) == 5
#     assert ray.get(counter.get_count.remote()) == 6


# def test_method_raise_no_over_retry(shutdown_only):
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.remote()
#     with pytest.raises(MyError):
#         ray.get(trouble_maker.may_raise_n_times.remote(counter, 6))
#     assert ray.get(counter.get_count.remote()) == 6


# def test_options_takes_precedence(shutdown_only):
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.remote()
#     assert (
#         ray.get(
#             trouble_maker.may_raise_n_times.options(max_retries=10).remote(counter, 10)
#         )
#         == 10
#     )
#     assert ray.get(counter.get_count.remote()) == 11


# def test_options_takes_precedence_no_over_retry(shutdown_only):
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.remote()

#     with pytest.raises(MyError):
#         ray.get(
#             trouble_maker.may_raise_n_times.options(max_retries=10).remote(counter, 11)
#         )
#     assert ray.get(counter.get_count.remote()) == 11


# @pytest.mark.parametrize(
#     "actions",
#     [
#         ["exit", "raise", "raise"],
#         ["raise", "exit", "raise"],
#         ["raise", "raise", "exit"],
#         ["raise", "raise", "raise"],
#     ],
# )
# def test_method_raise_and_exit(actions, shutdown_only):
#     """
#     Test we can endure a mix of raises and exits. Note the number of exits we can endure
#     is subject to max_restarts.
#     """
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.options(max_restarts=1).remote()
#     assert (
#         ray.get(
#             trouble_maker.raise_or_exit.options(max_retries=4).remote(counter, actions)
#         )
#         == 3
#     )
#     assert ray.get(counter.get_count.remote()) == 4


# def test_method_exit_and_raise_no_over_retry(shutdown_only):
#     """
#     Test we can endure a mix of raises and exits. Note the number of exits we can endure
#     is subject to max_restarts.
#     """
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.options(max_restarts=1).remote()
#     with pytest.raises(MyError):
#         assert ray.get(
#             trouble_maker.raise_or_exit.options(max_retries=2).remote(
#                 counter, ["exit", "raise", "raise"]
#             )
#         )
#     assert ray.get(counter.get_count.remote()) == 3


# def test_method_exit_no_over_retry(shutdown_only):
#     """
#     Test we can endure a mix of raises and exits. Note the number of exits we can endure
#     is subject to max_restarts.

#     TODO: does not work
#     [2023-11-22 18:35:11,494 C 3253 34529770] task_manager.cc:1306:  Check failed: it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT
#     """
#     counter = Counter.remote()
#     trouble_maker = TroubleMaker.options(max_restarts=1).remote()
#     with pytest.raises(ray.exceptions.RayActorError):
#         assert ray.get(
#             trouble_maker.raise_or_exit.options(max_retries=2).remote(
#                 counter, ["exit", "exit", "exit"]
#             )
#         )
#     assert ray.get(counter.get_count.remote()) == 2


def test_method_raise_and_exit_no_over_retry(shutdown_only):
    """
    Test we can endure a mix of raises and exits. Note the number of exits we can endure
    is subject to max_restarts.

    TODO: does not work
    [2023-11-22 18:35:11,494 C 3253 34529770] task_manager.cc:1306:  Check failed: it->second.GetStatus() == rpc::TaskStatus::PENDING_NODE_ASSIGNMENT
    """
    counter = Counter.remote()
    trouble_maker = TroubleMaker.options(max_restarts=1).remote()
    with pytest.raises(ray.exceptions.RayActorError):
        assert ray.get(
            trouble_maker.raise_or_exit.options(max_retries=2).remote(
                counter, ["raise", "raise", "exit"]
            )
        )
    assert ray.get(counter.get_count.remote()) == 2



if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
