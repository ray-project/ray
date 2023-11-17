import datetime
import os
import random
import sys
import tempfile

import numpy as np
import pytest

import ray
from ray import cloudpickle as pickle
from ray._private import ray_constants
from ray._private.test_utils import (
    client_test_enabled,
    wait_for_condition,
    wait_for_pid_to_exit,
)
from ray.actor import ActorClassInheritanceException
from ray.tests.client_test_utils import create_remote_signal_actor
from ray._private.test_utils import SignalActor

# NOTE: We have to import setproctitle after ray because we bundle setproctitle
# with ray.
import setproctitle  # noqa

try:
    import pytest_timeout
except ImportError:
    pytest_timeout = None


@ray.remote
class Counter:
    def __init__(self):
        self.count = 0

    def increment(self):
        ret = self.count
        self.count += 1
        return ret


@ray.remote(max_restarts=5)
class DieNTimesBeforeAlive:
    def __init__(self, counter, n):
        c = ray.get(counter.increment.remote())
        if c < n:
            print(f"actor dies in {c} th creation, want {n} times")
            os._exit(-1)
        self.c = c

    def incarnation(self):
        return self.c


class MyError(Exception):
    pass


@ray.remote(max_restarts=5)
class MethodDieNTimesBeforeAlive:
    @ray.method(max_retries=5)
    def may_die_n_times(self, counter, n):
        c = ray.get(counter.increment.remote())
        if c < n:
            print(f"method dies in {c} th creation, want {n} times")
            os._exit(-1)
        return c

    @ray.method(max_retries=5, retry_exceptions=[MyError])
    def may_raise_n_times(self, counter, n):
        c = ray.get(counter.increment.remote())
        if c < n:
            print(f"method raises in {c} th creation, want {n} times")
            raise MyError()
        return c

    def ping(self):
        return "pong"


# def test_actor_retry_5_times(shutdown_only):
#     c = Counter.remote()
#     died_five_times = DieNTimesBeforeAlive.remote(c, 5)
#     assert ray.get(died_five_times.incarnation.remote()) == 5


# def test_actor_dont_over_retry(shutdown_only):
#     c = Counter.remote()
#     died_five_times = DieNTimesBeforeAlive.remote(c, 6)
#     with pytest.raises(ray.exceptions.RayActorError):
#         ray.get(died_five_times.incarnation.remote())

# def test_method_die_5_times(shutdown_only):
#     c = Counter.remote()
#     could_die = MethodDieNTimesBeforeAlive.remote()
#     ray.get(could_die.may_die_n_times.remote(c, 5))
#     assert ray.get(could_die.ping.remote()) == 'pong'


# def test_method_dont_over_retry(shutdown_only):
#     c = Counter.remote()
#     could_die = MethodDieNTimesBeforeAlive.remote()
#     with pytest.raises(ray.exceptions.RayActorError):
#         ray.get(could_die.may_die_n_times.remote(c, 6))


def test_method_raise_5_times(shutdown_only):
    c = Counter.remote()
    could_die = MethodDieNTimesBeforeAlive.remote()
    ray.get(could_die.may_raise_n_times.remote(c, 5))
    assert ray.get(could_die.ping.remote()) == "pong"


def test_method_raise_dont_over_retry(shutdown_only):
    c = Counter.remote()
    could_die = MethodDieNTimesBeforeAlive.remote()
    with pytest.raises(MyError):
        ray.get(could_die.may_raise_n_times.remote(c, 6))


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
