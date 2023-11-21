import os
import sys

import pytest

import ray


class MyError(Exception):
    pass


@ray.remote(max_task_retries=3)
class RaisesNTimesBeforeReturn:
    def __init__(self):
        self.count = 0

    @ray.method(max_retries=5, retry_exceptions=[MyError])
    def may_raise_n_times(self, n):
        """
        raises if there had been n calls before this call.
        returns the number of calls before this call, if it's > n.
        """
        c = self.count
        self.count += 1
        print(f"may_raise_n_times, n = {n}, count = {c}")
        if c < n:
            print(f"method raises in {c} th call, want {n} times")
            raise MyError()
        return c

    def get_count(self):
        return self.count


def test_method_raise_5_times(shutdown_only):
    could_die = RaisesNTimesBeforeReturn.remote()
    assert ray.get(could_die.may_raise_n_times.remote(5)) == 5
    assert ray.get(could_die.get_count.remote()) == 6


def test_method_raise_no_over_retry(shutdown_only):
    could_die = RaisesNTimesBeforeReturn.remote()
    with pytest.raises(MyError):
        ray.get(could_die.may_raise_n_times.remote(6))
    assert ray.get(could_die.get_count.remote()) == 6


def test_options_takes_precedence(shutdown_only):
    could_die = RaisesNTimesBeforeReturn.remote()
    assert ray.get(could_die.may_raise_n_times.options(max_retries=10).remote(10)) == 10
    assert ray.get(could_die.get_count.remote()) == 11


def test_options_takes_precedence_no_over_retry(shutdown_only):
    could_die = RaisesNTimesBeforeReturn.remote()

    with pytest.raises(MyError):
        ray.get(could_die.may_raise_n_times.options(max_retries=10).remote(11))
    assert ray.get(could_die.get_count.remote()) == 11


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
