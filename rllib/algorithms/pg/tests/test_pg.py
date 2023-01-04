import gymnasium as gym
from gymnasium.spaces import Box, Dict, Discrete, Tuple
import numpy as np
import unittest

import ray
import ray.rllib.algorithms.pg as pg
from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.examples.env.random_env import RandomEnv
from ray.rllib.models.tf.tf_action_dist import Categorical
from ray.rllib.models.torch.torch_action_dist import TorchCategorical
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import fc
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)
from ray import tune
from ray.rllib.utils.metrics.learner_info import (
    LEARNER_INFO,
    LEARNER_STATS_KEY,
    DEFAULT_POLICY_ID,
)


class TestPG(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_pg_compilation(self):
        """Test whether PG can be built with all frameworks."""
        config = pg.PGConfig()

        # Test with filter to see whether they work w/o preprocessing.
        config.rollouts(
            num_rollout_workers=1,
            observation_filter="MeanStdFilter",
        ).training(train_batch_size=500)
        num_iterations = 1

        image_space = Box(-1.0, 1.0, shape=(84, 84, 3))
        simple_space = Box(-1.0, 1.0, shape=(3,))

        tune.gym.register(
            "random_dict_env",
            lambda: RandomEnv(
                {
                    "observation_space": Dict(
                        {
                            "a": simple_space,
                            "b": Discrete(2),
                            "c": image_space,
                        }
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )
        gym.register(
            "random_tuple_env",
            lambda: RandomEnv(
                {
                    "observation_space": Tuple(
                        [simple_space, Discrete(2), image_space]
                    ),
                    "action_space": Box(-1.0, 1.0, shape=(1,)),
                }
            ),
        )

        for _ in framework_iterator(config, with_eager_tracing=True):
            # Test for different env types (discrete w/ and w/o image, + cont).
            for env in [
                "random_dict_env",
                "random_tuple_env",
                "ALE/MsPacman-v5",
                "CartPole-v1",
                "FrozenLake-v1",
            ]:
                print(f"env={env}")
                config.environment(env)

                algo = config.build()
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)

                check_compute_single_action(algo, include_prev_action_reward=True)

    def test_pg_loss_functions(self):
        """Tests the PG loss function math."""
        config = (
            pg.PGConfig()
            .rollouts(num_rollout_workers=0)
            .training(
                gamma=0.99,
                model={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": "linear",
                },
            )
        )

        # Fake CartPole episode of n time steps.
        train_batch = SampleBatch(
            {
                SampleBatch.OBS: np.array(
                    [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]]
                ),
                SampleBatch.ACTIONS: np.array([0, 1, 1]),
                SampleBatch.REWARDS: np.array([1.0, 1.0, 1.0]),
                SampleBatch.TERMINATEDS: np.array([False, False, True]),
                SampleBatch.EPS_ID: np.array([1234, 1234, 1234]),
                SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
            }
        )

        for fw, sess in framework_iterator(config, session=True):
            dist_cls = Categorical if fw != "torch" else TorchCategorical
            algo = config.build(env="CartPole-v1")
            policy = algo.get_policy()
            vars = policy.model.trainable_variables()
            if sess:
                vars = policy.get_session().run(vars)

            # Post-process (calculate simple (non-GAE) advantages) and attach
            # to train_batch dict.
            # A = [0.99^2 * 1.0 + 0.99 * 1.0 + 1.0, 0.99 * 1.0 + 1.0, 1.0] =
            # [2.9701, 1.99, 1.0]
            train_batch_ = pg.post_process_advantages(policy, train_batch.copy())
            if fw == "torch":
                train_batch_ = policy._lazy_tensor_dict(train_batch_)

            # Check Advantage values.
            check(train_batch_[Postprocessing.ADVANTAGES], [2.9701, 1.99, 1.0])

            # Actual loss results.
            if sess:
                results = policy.get_session().run(
                    policy._loss,
                    feed_dict=policy._get_loss_inputs_dict(train_batch_, shuffle=False),
                )
            else:

                results = policy.loss(
                    policy.model, dist_class=dist_cls, train_batch=train_batch_
                )

            # Calculate expected results.
            if fw != "torch":
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS], vars[0], vars[1], framework=fw),
                    vars[2],
                    vars[3],
                    framework=fw,
                )
            else:
                expected_logits = fc(
                    fc(train_batch_[SampleBatch.OBS], vars[2], vars[3], framework=fw),
                    vars[0],
                    vars[1],
                    framework=fw,
                )
            expected_logp = dist_cls(expected_logits, policy.model).logp(
                train_batch_[SampleBatch.ACTIONS]
            )
            adv = train_batch_[Postprocessing.ADVANTAGES]
            if sess:
                expected_logp = sess.run(expected_logp)
            elif fw == "torch":
                expected_logp = expected_logp.detach().cpu().numpy()
                adv = adv.detach().cpu().numpy()
            else:
                expected_logp = expected_logp.numpy()
            expected_loss = -np.mean(expected_logp * adv)
            check(results, expected_loss, decimals=4)

    def test_pg_lr(self):
        """Test PG with learning rate schedule."""
        config = pg.PGConfig()
        config.reporting(
            min_sample_timesteps_per_iteration=10,
            # Make sure that results contain info on default policy
            min_train_timesteps_per_iteration=10,
            # 0 metrics reporting delay, this makes sure timestep,
            # which lr depends on, is updated after each worker rollout.
            min_time_s_per_iteration=0,
        )
        config.rollouts(
            num_rollout_workers=1,
        )
        config.training(
            lr=0.2,
            lr_schedule=[[0, 0.2], [500, 0.001]],
            train_batch_size=50,
        )

        def _step_n_times(algo, n: int):
            """Step trainer n times.

            Returns:
                learning rate at the end of the execution.
            """
            for _ in range(n):
                results = algo.train()
            return results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY][
                "cur_lr"
            ]

        for _ in framework_iterator(config):
            algo = config.build(env="CartPole-v1")

            lr = _step_n_times(algo, 1)  # 50 timesteps
            # Close to 0.2
            self.assertGreaterEqual(lr, 0.15)

            lr = _step_n_times(algo, 8)  # Close to 500 timesteps
            # LR Annealed to 0.001
            self.assertLessEqual(float(lr), 0.5)

            lr = _step_n_times(algo, 2)  # > 500 timesteps
            # LR == 0.001
            self.assertAlmostEqual(lr, 0.001)

            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
