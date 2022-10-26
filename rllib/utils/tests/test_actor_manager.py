import random
import sys
import time
import unittest

import ray
from ray.experimental.state.api import list_actors
from ray.rllib.utils.actor_manager import FaultTolerantActorManager


@ray.remote(max_restarts=-1)
class Actor:
    def __init__(self, i):
        random.seed(i)
        self.count = 0

    def call(self):
        self.count += 1
        r = random.random()

        # 10% chance of crashing.
        if r < 0.1:
            sys.exit(1)
        # Another 10% chance of throwing errors.
        elif r < 0.2:
            raise AttributeError("sorry")

        # Otherwise, return good result.
        return self.count

    def apply(self, func):
        return func(self)


def wait_for_restore():
    while True:
        states = [
            a["state"] == "ALIVE" or a["state"] == "DEAD"
            for a in list_actors(filters=[("class_name", "=", "Actor")])
        ]
        print("waiting ... ", states)
        if all(states):
            break
        # Otherwise, wait a bit.
        time.sleep(0.5)


class TestActorManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_sync_call_healthy_only(self):
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            results.extend(manager.foreach_actor(lambda w: w.call(), healthy_only=True))
            # Wait for actors to recover.
            wait_for_restore()

        # Notice that since we only fire calls against healthy actors,
        # we wouldn't aware that the actors have been recovered.
        # So once an actor is taken out of the lineup (10% chance),
        # it will not go back in, and we should have few results here.
        # Basically takes us 24 calls to kill all the actors.
        self.assertEqual(len(results), 24)

    def test_sync_call_all_actors(self):
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            results.extend(
                manager.foreach_actor(lambda w: w.call(), healthy_only=False)
            )
            # Wait for actors to recover.
            wait_for_restore()

        # This time, we fire against all actors regardless of their status.
        # Since the actors are always restored before next round of calls,
        # we should get a lot more results back.
        # There are calls that return Erros, so 33 in total.
        self.assertEqual(len(results), 33)

    def test_sync_call_fire_and_forget(self):
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results1 = []
        for _ in range(10):
            results1.extend(
                manager.foreach_actor(
                    lambda w: w.call(), healthy_only=False, timeout_seconds=0
                )
            )
            # Wait for actors to recover.
            wait_for_restore()

        # Shouldn't get any results back, since timeout is 0.
        self.assertEqual(len(results1), 0)

        results2 = manager.foreach_actor(lambda w: w.call(), healthy_only=False)

        # Some calls did happen.
        self.assertEqual(results2, [11, 2, 2, 5])

    def test_sync_call_not_ignore_error(self):
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(
            actors=actors,
            # Do not ignore application failures. Return all the Errors.
            ignore_worker_failure=False,
        )

        results = []
        for _ in range(10):
            results.extend(
                manager.foreach_actor(lambda w: w.call(), healthy_only=False)
            )
            # Wait for actors to recover.
            wait_for_restore()

        # Some calls did happen.
        self.assertTrue(any([isinstance(r, Exception) for r in results]))

    def test_async_call(self):
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            manager.foreach_actor_async(lambda w: w.call())
            _, r = manager.fetch_ready_async_reqs()
            results.extend(r)
            # Wait for actors to recover.
            wait_for_restore()

        # Some calls did happen.
        self.assertEqual(len(results), 27)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
