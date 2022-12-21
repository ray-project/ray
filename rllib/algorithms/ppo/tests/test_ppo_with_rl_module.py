import unittest

import numpy as np

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


class MyCallbacks(DefaultCallbacks):
    @staticmethod
    def _check_lr_torch(policy, policy_id):
        for j, opt in enumerate(policy._optimizers):
            for p in opt.param_groups:
                assert p["lr"] == policy.cur_lr, "LR scheduling error!"

    @staticmethod
    def _check_lr_tf(policy, policy_id):
        lr = policy.cur_lr
        sess = policy.get_session()
        if sess:
            lr = sess.run(lr)
            optim_lr = sess.run(policy._optimizer._lr)
        else:
            lr = lr.numpy()
            optim_lr = policy._optimizer.lr.numpy()
        assert lr == optim_lr, "LR scheduling error!"

    def on_train_result(self, *, algorithm, result: dict, **kwargs):
        stats = result["info"][LEARNER_INFO][DEFAULT_POLICY_ID][LEARNER_STATS_KEY]
        # Learning rate should go to 0 after 1 iter.
        check(stats["cur_lr"], 5e-5 if algorithm.iteration == 1 else 0.0)
        # Entropy coeff goes to 0.05, then 0.0 (per iter).
        check(stats["entropy_coeff"], 0.1 if algorithm.iteration == 1 else 0.05)

        algorithm.workers.foreach_policy(
            self._check_lr_torch
            if algorithm.config.framework_str == "torch"
            else self._check_lr_tf
        )


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ppo_compilation_and_schedule_mixins(self):
        """Test whether PPO can be built with all frameworks."""

        # Build a PPOConfig object.
        config = (
            ppo.PPOConfig()
            .training(
                num_sgd_iter=2,
                # Setup lr schedule for testing.
                lr_schedule=[[0, 5e-5], [128, 0.0]],
                # Set entropy_coeff to a faulty value to proof that it'll get
                # overridden by the schedule below (which is expected).
                entropy_coeff=100.0,
                entropy_coeff_schedule=[[0, 0.1], [256, 0.0]],
                train_batch_size=128,
                model=dict(
                    # Settings in case we use an LSTM.
                    lstm_cell_size=10,
                    max_seq_len=20,
                ),
            )
            .rollouts(
                num_rollout_workers=1,
                # Test with compression.
                compress_observations=True,
                enable_connectors=True,
            )
            .callbacks(MyCallbacks)
            .rl_module(_enable_rl_module_api=True)
        )  # For checking lr-schedule correctness.

        num_iterations = 2

        # TODO (Kourosh): for now just do torch
        for fw in framework_iterator(
            config, frameworks=("torch"), with_eager_tracing=True
        ):
            # TODO (Kourosh) Bring back "FrozenLake-v1" and "MsPacmanNoFrameskip-v4"
            for env in ["CartPole-v1", "Pendulum-v1"]:
                print("Env={}".format(env))
                # TODO (Kourosh): for now just do lstm=False
                for lstm in [False]:
                    print("LSTM={}".format(lstm))
                    config.training(
                        model=dict(
                            use_lstm=lstm,
                            lstm_use_prev_action=lstm,
                            lstm_use_prev_reward=lstm,
                            vf_share_layers=lstm,
                        )
                    )

                    algo = config.build(env=env)
                    policy = algo.get_policy()
                    entropy_coeff = algo.get_policy().entropy_coeff
                    lr = policy.cur_lr
                    if fw == "tf":
                        entropy_coeff, lr = policy.get_session().run(
                            [entropy_coeff, lr]
                        )
                    check(entropy_coeff, 0.1)
                    check(lr, config.lr)

                    for i in range(num_iterations):
                        results = algo.train()
                        check_train_results(results)
                        print(results)

                    check_compute_single_action(
                        algo, include_prev_action_reward=True, include_state=lstm
                    )
                    algo.stop()

    def test_ppo_exploration_setup(self):
        """Tests, whether PPO runs with different exploration setups."""
        config = (
            ppo.PPOConfig()
            .environment(
                "FrozenLake-v1",
                env_config={"is_slippery": False, "map_name": "4x4"},
            )
            .rollouts(
                # Run locally.
                num_rollout_workers=0,
                enable_connectors=True,
            )
            .rl_module(_enable_rl_module_api=True)
        )
        obs = np.array(0)

        # TODO (Kourosh) Test against all frameworks.
        for fw in framework_iterator(config, frameworks=("torch")):
            # Default Agent should be setup with StochasticSampling.
            trainer = config.build()
            # explore=False, always expect the same (deterministic) action.
            a_ = trainer.compute_single_action(
                obs, explore=False, prev_action=np.array(2), prev_reward=np.array(1.0)
            )

            for _ in range(50):
                a = trainer.compute_single_action(
                    obs,
                    explore=False,
                    prev_action=np.array(2),
                    prev_reward=np.array(1.0),
                )
                check(a, a_)

            # With explore=True (default), expect stochastic actions.
            actions = []
            for _ in range(300):
                actions.append(
                    trainer.compute_single_action(
                        obs, prev_action=np.array(2), prev_reward=np.array(1.0)
                    )
                )
            check(np.mean(actions), 1.5, atol=0.2)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
