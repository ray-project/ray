import sys
import os
import time
import numpy as np
import unittest

import ray
from ray import tune
from ray.tune.execution.ray_trial_executor import RayTrialExecutor
from ray.tune.experiment import Trial
from ray.tune import Callback
from ray.tune.execution.trial_runner import TrialRunner
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.util import placement_group_table
from ray.cluster_utils import Cluster
from ray.rllib import _register_all


class TrialRunnerPlacementGroupTest(unittest.TestCase):
    def setUp(self):
        os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "10000"
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "auto"  # Reset default
        self.head_cpus = 8
        self.head_gpus = 4
        self.head_custom = 16

        self.cluster = Cluster(
            initialize_head=True,
            connect=True,
            head_node_args={
                "include_dashboard": False,
                "num_cpus": self.head_cpus,
                "num_gpus": self.head_gpus,
                "resources": {"custom": self.head_custom},
                "_system_config": {
                    "health_check_initial_delay_ms": 0,
                    "health_check_period_ms": 1000,
                    "health_check_failure_threshold": 10,
                },
            },
        )
        # Pytest doesn't play nicely with imports
        _register_all()

    def tearDown(self):
        ray.shutdown()
        self.cluster.shutdown()
        _register_all()  # re-register the evicted objects

    def _assertCleanup(self, trial_executor):
        # Assert proper cleanup
        resource_manager = trial_executor._resource_manager
        self.assertFalse(resource_manager._pg_to_request)
        self.assertFalse(resource_manager._acquired_pgs)
        self.assertFalse(resource_manager._staging_future_to_pg)
        self.assertFalse(resource_manager._pg_to_staging_future)
        for rr in resource_manager._request_to_staged_pgs:
            self.assertFalse(resource_manager._request_to_staged_pgs[rr])
        for rr in resource_manager._request_to_ready_pgs:
            self.assertFalse(resource_manager._request_to_ready_pgs[rr])

        num_non_removed_pgs = len(
            [p for pid, p in placement_group_table().items() if p["state"] != "REMOVED"]
        )
        self.assertEqual(num_non_removed_pgs, 0)

    def testPlacementGroupRequests(self, reuse_actors=False, scheduled=10):
        """In this test we try to start 10 trials but only have resources
        for 2. Placement groups should still be created and PENDING.

        Eventually they should be scheduled sequentially (i.e. in pairs
        of two)."""
        # Since we check per-step placement groups, set the reconcilation
        # interval to 0
        os.environ["TUNE_PLACEMENT_GROUP_RECON_INTERVAL"] = "0"

        def train(config):
            time.sleep(1)
            now = time.time()
            tune.report(end=now - config["start_time"])

        head_bundle = {"CPU": 4, "GPU": 0, "custom": 0}
        child_bundle = {"custom": 1}
        # Manually calculated number of parallel trials
        max_num_parallel = 2

        placement_group_factory = PlacementGroupFactory(
            [head_bundle, child_bundle, child_bundle]
        )

        trial_executor = RayTrialExecutor(reuse_actors=reuse_actors)
        trial_executor.setup(max_pending_trials=max_num_parallel, trainable_kwargs={})

        this = self

        class _TestCallback(Callback):
            def on_step_end(self, iteration, trials, **info):
                num_finished = len(
                    [
                        t
                        for t in trials
                        if t.status == Trial.TERMINATED or t.status == Trial.ERROR
                    ]
                )

                resource_manager = trial_executor._resource_manager

                num_staging = sum(
                    len(s) for s in resource_manager._request_to_staged_pgs.values()
                )
                num_ready = sum(
                    len(s) for s in resource_manager._request_to_ready_pgs.values()
                )
                num_in_use = len(resource_manager._acquired_pgs)
                num_cached = sum(
                    len(a)
                    for a in trial_executor._resource_request_to_cached_actors.values()
                )

                total_num_tracked = num_staging + num_ready + num_in_use + num_cached

                # All trials should be scheduled
                this.assertEqual(
                    scheduled,
                    min(scheduled, len(trials)),
                    msg=f"Num trials iter {iteration}",
                )

                # The following two tests were relaxed for reuse_actors=True
                # so that up to `max_num_parallel` more placement groups can
                # exist than we would expect. This is because caching
                # relies on reconciliation for cleanup to avoid overscheduling
                # of new placement groups.
                num_parallel_reuse = int(reuse_actors) * max_num_parallel

                # The number of PGs should decrease when trials finish
                # We allow a constant excess of 1 here because the trial will
                # be TERMINATED and the resources only returned after the trainable
                # cleanup future succeeded. Because num_finished will increase,
                # this still asserts that the number of PGs goes down over time.
                this.assertGreaterEqual(
                    max(scheduled, len(trials)) - num_finished + 1 + num_parallel_reuse,
                    total_num_tracked,
                    msg=f"Num tracked iter {iteration}, {len(trials)}, "
                    f"{scheduled}, {num_finished}, {num_parallel_reuse}",
                )

        start = time.time()
        out = tune.run(
            train,
            config={"start_time": start},
            resources_per_trial=placement_group_factory,
            num_samples=10,
            trial_executor=trial_executor,
            callbacks=[_TestCallback()],
            reuse_actors=reuse_actors,
            verbose=2,
        )

        trial_end_times = sorted(t.last_result["end"] for t in out.trials)
        print("Trial end times:", trial_end_times)
        max_diff = trial_end_times[-1] - trial_end_times[0]

        # Not all trials have been run in parallel
        self.assertGreater(max_diff, 3)

        # Some trials should have run in parallel
        # Todo: Re-enable when using buildkite
        # self.assertLess(max_diff, 10)

        self._assertCleanup(trial_executor)

    def testPlacementGroupRequestsWithActorReuse(self):
        """Assert that reuse actors doesn't leak placement groups"""
        self.testPlacementGroupRequests(reuse_actors=True)

    def testPlacementGroupLimitedRequests(self):
        """Assert that maximum number of placement groups is enforced."""
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "6"
        self.testPlacementGroupRequests(scheduled=6)

    def testPlacementGroupLimitedRequestsWithActorReuse(self):
        os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "6"
        self.testPlacementGroupRequests(reuse_actors=True, scheduled=6)

    def testPlacementGroupDistributedTraining(self, reuse_actors=False):
        """Run distributed training using placement groups.

        Each trial requests 4 CPUs and starts 4 remote training workers.
        """

        head_bundle = {"CPU": 1, "GPU": 0, "custom": 0}
        child_bundle = {"CPU": 1}

        placement_group_factory = PlacementGroupFactory(
            [head_bundle, child_bundle, child_bundle, child_bundle]
        )

        @ray.remote
        class TrainingActor:
            def train(self, val):
                time.sleep(1)
                return val

        def train(config):
            base = config["base"]
            actors = [TrainingActor.remote() for _ in range(4)]
            futures = [
                actor.train.remote(base + 2 * i) for i, actor in enumerate(actors)
            ]
            results = ray.get(futures)

            end = time.time() - config["start_time"]
            tune.report(avg=np.mean(results), end=end)

        trial_executor = RayTrialExecutor(reuse_actors=reuse_actors)

        start = time.time()
        out = tune.run(
            train,
            config={
                "start_time": start,
                "base": tune.grid_search(list(range(0, 100, 10))),
            },
            resources_per_trial=placement_group_factory,
            num_samples=1,
            trial_executor=trial_executor,
            reuse_actors=reuse_actors,
            verbose=2,
        )

        avgs = sorted(t.last_result["avg"] for t in out.trials)
        self.assertSequenceEqual(avgs, list(range(3, 103, 10)))

        trial_end_times = sorted(t.last_result["end"] for t in out.trials)
        print("Trial end times:", trial_end_times)
        max_diff = trial_end_times[-1] - trial_end_times[0]

        # Not all trials have been run in parallel
        self.assertGreater(max_diff, 3)

        # Some trials should have run in parallel
        # Todo: Re-enable when using buildkite
        # self.assertLess(max_diff, 10)

        self._assertCleanup(trial_executor)

    def testPlacementGroupDistributedTrainingWithActorReuse(self):
        self.testPlacementGroupDistributedTraining(reuse_actors=True)


class TrialRunnerPlacementGroupHeterogeneousTest(unittest.TestCase):
    def tearDown(self) -> None:
        if ray.is_initialized:
            ray.shutdown()

    def testResourceDeadlock(self):
        """Tests that resource deadlock is avoided for heterogeneous PGFs.

        We start 4 trials in a cluster with 2 CPUs. The first two trials
        require 1 CPU each, the third trial 2 CPUs, the fourth trial 1 CPU.

        The second trial needs a bit more time to finish. This means that the
        resources from the first trial will be freed, and the PG of the
        _fourth_ trial becomes ready (not that of the third trial, because that
        requires 2 CPUs - however, one is still occupied by trial 2).

        After the first two trials finished, the FIFOScheduler tries to start
        the third trial. However, it can't be started because its placement
        group is not ready. Instead, the placement group of the fourth
        trial is ready. Thus, we opt to run the fourth trial instead.
        """

        def train(config):
            time.sleep(config["sleep"])
            return 4

        ray.init(num_cpus=2)

        tune.register_trainable("het", train)
        pgf1 = PlacementGroupFactory([{"CPU": 1}])
        pgf2 = PlacementGroupFactory([{"CPU": 2}])

        trial1 = Trial("het", config={"sleep": 0}, placement_group_factory=pgf1)
        trial2 = Trial("het", config={"sleep": 2}, placement_group_factory=pgf1)
        trial3 = Trial("het", config={"sleep": 0}, placement_group_factory=pgf2)
        trial4 = Trial("het", config={"sleep": 0}, placement_group_factory=pgf1)

        runner = TrialRunner(fail_fast=True)
        runner.add_trial(trial1)
        runner.add_trial(trial2)
        runner.add_trial(trial3)
        runner.add_trial(trial4)

        timeout = time.monotonic() + 30
        while not runner.is_finished():
            # We enforce a timeout here
            self.assertLess(
                time.monotonic(), timeout, msg="Ran into a resource deadlock"
            )

            runner.step()


def test_placement_group_no_cpu_trainer():
    """Bundles with only GPU:1 but no CPU should work"""
    ray.init(num_gpus=1, num_cpus=1)
    pgf = PlacementGroupFactory([{"GPU": 1, "CPU": 0}, {"CPU": 1}])

    def train(config):
        time.sleep(1)
        return 5

    tune.run(train, resources_per_trial=pgf)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
