import unittest

import pytest

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.test_utils import check


class TestIMPALA(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_impala_minibatch_size_check(self):
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .training(minibatch_size=100)
            .env_runners(rollout_fragment_length=30)
        )

        with pytest.raises(
            ValueError,
            match=r"`minibatch_size` \(100\) must either be None or a multiple of `rollout_fragment_length` \(30\)",
        ):
            config.validate()

    def test_impala_lr_schedule(self):
        # Test whether we correctly ignore the "lr" setting.
        # The first lr should be 0.05.
        config = (
            impala.IMPALAConfig()
            .learners(num_learners=0)
            .experimental(_validate_config=False)  #
            .training(
                lr=[
                    [0, 0.05],
                    [100000, 0.000001],
                ],
                train_batch_size=100,
            )
            .env_runners(num_envs_per_env_runner=2)
            .environment(env="CartPole-v1")
        )

        def get_lr(result):
            return result[LEARNER_RESULTS][DEFAULT_POLICY_ID][
                "default_optimizer_learning_rate"
            ]

        algo = config.build()
        optim = algo.learner_group._learner.get_optimizer()

        try:
            check(optim.param_groups[0]["lr"], 0.05)
            for _ in range(1):
                r1 = algo.train()
            for _ in range(2):
                r2 = algo.train()
            for _ in range(2):
                r3 = algo.train()
            # Due to the asynch'ness of IMPALA, learner-stats metrics
            # could be delayed by one iteration. Do 3 train() calls here
            # and measure guaranteed decrease in lr between 1st and 3rd.
            lr1 = get_lr(r1)
            lr2 = get_lr(r2)
            lr3 = get_lr(r3)
            assert lr2 <= lr1, (lr1, lr2)
            assert lr3 <= lr2, (lr2, lr3)
            assert lr3 < lr1, (lr1, lr3)
        finally:
            algo.stop()

    def test_aggregator_colocation_warns_outside_tune(self):
        """`colocate_aggregator_actors_with_learners=True` (the default)
        builds cleanly outside a Tune trial, but logs a warning telling
        the user co-location is best-effort and how to silence it.
        Strict co-location is only achievable inside a Tune trial
        (the trial's placement group is what pins aggregators to their
        Learner's bundle), which is covered by the
        `learning_tests_impala_cartpole_local` integration test.
        """
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(
                num_learners=0,
                num_aggregator_actors_per_learner=2,
                # Default is True; passed explicitly to make the intent
                # obvious.
                colocate_aggregator_actors_with_learners=True,
            )
            .env_runners(num_env_runners=0)
        )
        with self.assertLogs("ray.rllib.algorithms.algorithm", level="WARNING") as cm:
            algo = config.build()
        try:
            self.assertTrue(
                any(
                    "colocate_aggregator_actors_with_learners=False" in line
                    for line in cm.output
                ),
                f"Expected fallback warning to mention the opt-out flag; "
                f"got: {cm.output}",
            )
            # Aggregators should still come up; we just don't guarantee
            # where they land.
            self.assertEqual(len(algo._aggregator_actor_manager._actors), 2)
        finally:
            algo.stop()

    def test_aggregator_colocation_disabled(self):
        """`colocate_aggregator_actors_with_learners=False` keeps the
        old behaviour: no scheduling hint, aggregators land wherever
        Ray's default scheduler puts them. We only assert the algo
        builds cleanly and the aggregator manager is wired up. We do
        *not* assert on which node each aggregator lands -- that's the
        whole point of the opt-out.
        """
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(
                num_learners=0,
                num_aggregator_actors_per_learner=2,
                colocate_aggregator_actors_with_learners=False,
            )
            .env_runners(num_env_runners=0)
        )
        algo = config.build()
        try:
            mgr = algo._aggregator_actor_manager
            self.assertIsNotNone(mgr)
            self.assertEqual(len(mgr._actors), 2)
        finally:
            algo.stop()

    def test_local_learner_thread_stops_on_algo_stop(self):
        # Regression test: `algo.stop()` -> `LearnerGroup.shutdown()` ->
        # `IMPALALearner.shutdown()` must stop and join the local IMPALA
        # `_LearnerThread`. Otherwise the daemon thread keeps spinning and
        # can race against interpreter shutdown inside an auto_init-wrapped
        # Ray API.
        config = (
            impala.IMPALAConfig()
            .environment("CartPole-v1")
            .learners(num_learners=0)
            .env_runners(num_env_runners=0)
        )
        algo = config.build()
        learner_thread = algo.learner_group._learner._learner_thread
        self.assertTrue(learner_thread.is_alive())

        algo.stop()

        # `Learner.shutdown()` joins the thread, so it must be dead by the
        # time `algo.stop()` returns — no extra `join()` needed here.
        self.assertFalse(learner_thread.is_alive())


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
