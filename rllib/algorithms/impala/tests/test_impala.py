import unittest

import pytest

import ray
import ray.rllib.algorithms.impala as impala
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.test_utils import check
from ray.util.debug import reset_log_once


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

    def test_aggregator_colocation_warns_only_when_enabled(self):
        """Outside a Tune trial, ``colocate=True`` (the default) builds
        cleanly but logs a warning pointing at the opt-out flag, while
        ``colocate=False`` builds silently. Strict co-location itself
        requires a Tune PG and is covered by the
        ``learning_tests_impala_cartpole_local`` integration test.

        Parametrized over ``num_learners`` to exercise both
        local-learner (``num_learners=0`` -> aggregators get their own
        bundles) and remote-learner (``num_learners >= 1`` -> aggregator
        CPU is baked into the learner's bundle) code paths.
        """
        n_agg = 2

        def _config(colocate, num_learners):
            return (
                impala.IMPALAConfig()
                .environment("CartPole-v1")
                .learners(
                    num_learners=num_learners,
                    num_aggregator_actors_per_learner=n_agg,
                    colocate_aggregator_actors_with_learners=colocate,
                )
                .env_runners(num_env_runners=0)
            )

        for num_learners in (0, 1, 2):
            with self.subTest(num_learners=num_learners):
                # The warning is gated by `log_once` so it only fires
                # once per process; reset the cache so each subtest can
                # observe the warning.
                reset_log_once("aggregator_colocation_no_pg_fallback")
                with self.assertLogs(
                    "ray.rllib.algorithms.algorithm", level="WARNING"
                ) as cm:
                    _config(colocate=True, num_learners=num_learners).build()
                self.assertTrue(
                    any(
                        "colocate_aggregator_actors_with_learners=False" in line
                        for line in cm.output
                    )
                )

                # `colocate=False` must still wire up the aggregator
                # manager. Total aggregator actors = max(1, num_learners)
                # * n_agg (the local-learner case still produces one
                # logical learner).
                algo = _config(colocate=False, num_learners=num_learners).build()
                self.assertEqual(
                    len(algo._aggregator_actor_manager._actors),
                    max(1, num_learners) * n_agg,
                )

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
