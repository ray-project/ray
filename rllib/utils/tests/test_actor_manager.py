import functools
import os
from pathlib import Path
import pickle
import sys
import time
import unittest

import ray
from ray.experimental.state.api import list_actors
from ray.rllib.utils.actor_manager import FaultAwareApply, FaultTolerantActorManager


def load_random_numbers():
    """Loads deterministic random numbers from data file."""
    rllib_dir = Path(__file__).parent.parent.parent
    pkl_file = os.path.join(
        rllib_dir,
        "utils",
        "tests",
        "random_numbers.pkl",
    )
    return pickle.load(open(pkl_file, "rb"))


RANDOM_NUMS = load_random_numbers()


@ray.remote(max_restarts=-1)
class Actor(FaultAwareApply):
    def __init__(self, i, maybe_crash=True):
        self.random_numbers = RANDOM_NUMS[i]
        self.count = 0
        self.maybe_crash = maybe_crash
        self.config = {
            "recreate_failed_workers": True,
        }

    def _maybe_crash(self):
        if not self.maybe_crash:
            return

        r = self.random_numbers[self.count]
        # 10% chance of crashing.
        if r < 0.1:
            sys.exit(1)
        # Another 10% chance of throwing errors.
        elif r < 0.2:
            raise AttributeError("sorry")

    def call(self):
        self.count += 1
        self._maybe_crash()
        # Otherwise, return good result.
        return self.count

    def ping(self):
        self._maybe_crash()
        return "pong"


def wait_for_restore():
    """Wait for Ray actor fault tolerence to restore all failed actors."""
    while True:
        states = [
            # Wait till all actors are either "ALIVE" (retored),
            # or "DEAD" (cancelled. these actors are from other
            # finished test cases).
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
        """Test synchronous remote calls to only healthy actors."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            results.extend(manager.foreach_actor(lambda w: w.call()).ignore_errors())
            # Wait for actors to recover.
            wait_for_restore()

        # Notice that since we only fire calls against healthy actors,
        # we wouldn't be aware that the actors have been recovered.
        # So once an actor is taken out of the lineup (10% chance),
        # it will not go back in, and we should have few results here.
        # Basically takes us 7 calls to kill all the actors.
        # Note that we can hardcode 10 here because we are using deterministic
        # sequences of random numbers.
        self.assertEqual(len(results), 7)

        manager.clear()

    def test_sync_call_all_actors(self):
        """Test synchronous remote calls to all actors, regardless of their states."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            # Make sure we have latest states of all actors.
            results.extend(
                manager.foreach_actor(lambda w: w.call(), healthy_only=False)
            )
            # Wait for actors to recover.
            wait_for_restore()

        # We fired against all actors regardless of their status.
        # So we should get 40 results back.
        self.assertEqual(len(results), 40)

        # Since the actors are always restored before next round of calls,
        # we should get more results back.
        # Some of these calls still failed, but 15 good results in total.
        # Note that we can hardcode 15 here because we are using deterministic
        # sequences of random numbers.
        self.assertEqual(len([r for r in results if r.ok]), 15)

        manager.clear()

    def test_sync_call_return_obj_refs(self):
        """Test synchronous remote calls to all actors asking for raw ObjectRefs."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = list(
            manager.foreach_actor(
                lambda w: w.call(),
                healthy_only=False,
                return_obj_refs=True,
            )
        )

        # We fired against all actors regardless of their status.
        # So we should get 40 results back.
        self.assertEqual(len(results), 4)

        for r in results:
            # Each result is an ObjectRef.
            self.assertTrue(r.ok)
            self.assertTrue(isinstance(r.get(), ray.ObjectRef))

        manager.clear()

    def test_sync_call_fire_and_forget(self):
        """Test synchronous remote calls with 0 timeout_seconds."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results1 = []
        for _ in range(10):
            manager.probe_unhealthy_actors()
            results1.extend(
                manager.foreach_actor(lambda w: w.call(), timeout_seconds=0)
            )
            # Wait for actors to recover.
            wait_for_restore()

        # Timeout is 0, so we returned immediately.
        # We may get a couple of results back if the calls are fast,
        # but that is not important.

        results2 = [
            r.get()
            for r in manager.foreach_actor(
                lambda w: w.call(), healthy_only=False
            ).ignore_errors()
        ]

        # Results from blocking calls show the # of calls happend on
        # each remote actor. 11 calls to each actor in total.
        self.assertEqual(results2, [11, 11, 11, 11])

        manager.clear()

    def test_sync_call_same_actor_multiple_times(self):
        """Test multiple synchronous remote calls to the same actor."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        # 2 synchronous call to actor 0.
        results = manager.foreach_actor(
            lambda w: w.call(),
            remote_actor_ids=[0, 0],
        )
        # Returns 1 and 2, representing the first and second calls to actor 0.
        self.assertEqual([r.get() for r in results.ignore_errors()], [1, 2])

        manager.clear()

    def test_async_call_same_actor_multiple_times(self):
        """Test multiple asynchronous remote calls to the same actor."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        # 2 asynchronous call to actor 0.
        num_of_calls = manager.foreach_actor_async(
            lambda w: w.call(),
            remote_actor_ids=[0, 0],
        )
        self.assertEqual(num_of_calls, 2)

        # Now, let's actually fetch the results.
        results = manager.fetch_ready_async_reqs(timeout_seconds=None)
        # Returns 1 and 2, representing the first and second calls to actor 0.
        self.assertEqual([r.get() for r in results.ignore_errors()], [1, 2])

        manager.clear()

    def test_sync_call_not_ignore_error(self):
        """Test synchronous remote calls that returns errors."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            manager.probe_unhealthy_actors()
            results.extend(manager.foreach_actor(lambda w: w.call()))
            # Wait for actors to recover.
            wait_for_restore()

        # Some calls did error out.
        self.assertTrue(any([not r.ok for r in results]))

        manager.clear()

    def test_async_call(self):
        """Test asynchronous remote calls work."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        results = []
        for _ in range(10):
            manager.foreach_actor_async(lambda w: w.call())
            results.extend(manager.fetch_ready_async_reqs(timeout_seconds=None))
            # Wait for actors to recover.
            wait_for_restore()

        # Note that we can hardcode the numbers here because of the deterministic
        # lists of random numbers we use.
        # 7 calls succeeded, 4 failed.
        # The number of results back is much lower than 40, because we do not probe
        # the actors with this test. As soon as an actor errors out, it will get
        # taken out of the lineup forever.
        self.assertEqual(len([r for r in results if r.ok]), 7)
        self.assertEqual(len([r for r in results if not r.ok]), 4)

        manager.clear()

    def test_async_calls_get_dropped_if_inflight_requests_over_limit(self):
        """Test asynchronous remote calls get dropped if too many in-flight calls."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(
            actors=actors,
            max_remote_requests_in_flight_per_actor=2,
        )

        # 2 asynchronous call to actor 1.
        num_of_calls = manager.foreach_actor_async(
            lambda w: w.call(),
            remote_actor_ids=[0, 0],
        )
        self.assertEqual(num_of_calls, 2)

        # Now, let's try to make another async call to actor 1.
        num_of_calls = manager.foreach_actor_async(
            lambda w: w.call(),
            healthy_only=False,
            remote_actor_ids=[0],
        )
        # We actually made 0 calls.
        self.assertEqual(num_of_calls, 0)

        manager.clear()

    def test_healthy_only_works_for_list_of_functions(self):
        """Test healthy only mode works when a list of funcs are provided."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        # Mark first and second actor as unhealthy.
        manager.set_actor_state(1, False)
        manager.set_actor_state(2, False)

        def f(id, _):
            return id

        func = [functools.partial(f, i) for i in range(4)]

        manager.foreach_actor_async(func, healthy_only=True)
        results = manager.fetch_ready_async_reqs(timeout_seconds=None)

        # Should get results back from calling actor 0 and 3.
        self.assertEqual([r.get() for r in results], [0, 3])

        manager.clear()

    def test_len_of_func_not_match_len_of_actors(self):
        """Test healthy only mode works when a list of funcs are provided."""
        actors = [Actor.remote(i) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        def f(id, _):
            return id

        func = [functools.partial(f, i) for i in range(3)]

        with self.assertRaisesRegexp(AssertionError, "same number of callables") as _:
            manager.foreach_actor_async(func, healthy_only=True),

        manager.clear()

    def test_probe_unhealthy_actors(self):
        """Test probe brings back unhealthy actors."""
        actors = [Actor.remote(i, maybe_crash=False) for i in range(4)]
        manager = FaultTolerantActorManager(actors=actors)

        # Mark first and second actor as unhealthy.
        manager.set_actor_state(1, False)
        manager.set_actor_state(2, False)

        # These actors are actually healthy.
        manager.probe_unhealthy_actors()
        # Both actors are now healthy.
        self.assertEqual(len(manager.healthy_actor_ids()), 4)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
