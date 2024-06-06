import unittest

import numpy as np

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
)
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.algorithms.ppo.tests.test_ppo import PENDULUM_FAKE_BATCH
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import DEFAULT_OPTIMIZER, LR_KEY
from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import (
    check,
    check_compute_single_action,
    check_train_results,
    framework_iterator,
)


def get_model_config(framework, lstm=False):
    return (
        dict(
            use_lstm=True,
            lstm_use_prev_action=True,
            lstm_use_prev_reward=True,
            lstm_cell_size=10,
            max_seq_len=20,
        )
        if lstm
        else {"use_lstm": False}
    )


class MyCallbacks(DefaultCallbacks):
    def on_train_result(self, *, algorithm, result: dict, **kwargs):
        stats = result["info"][LEARNER_INFO][DEFAULT_MODULE_ID]
        # Entropy coeff goes to 0.05, then 0.0 (per iter).
        check(
            stats[LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY],
            0.05 if algorithm.iteration == 1 else 0.0,
        )

        # Learning rate should decrease by 0.0001/4 per iteration.
        check(
            stats[DEFAULT_OPTIMIZER + "_" + LR_KEY],
            0.0000075 if algorithm.iteration == 1 else 0.000005,
        )
        # Compare reported curr lr vs the actual lr found in the optimizer object.
        optim = algorithm.learner_group._learner.get_optimizer()
        actual_optimizer_lr = (
            optim.param_groups[0]["lr"]
            if algorithm.config.framework_str == "torch"
            else optim.lr
        )
        check(stats[DEFAULT_OPTIMIZER + "_" + LR_KEY], actual_optimizer_lr)


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
            .api_stack(enable_rl_module_and_learner=True)
            .training(
                num_sgd_iter=2,
                # Setup lr schedule for testing lr-scheduling correctness.
                lr=[[0, 0.00001], [512, 0.0]],  # 512=4x128
                # Set entropy_coeff to a faulty value to proof that it'll get
                # overridden by the schedule below (which is expected).
                entropy_coeff=[[0, 0.1], [256, 0.0]],  # 256=2x128,
                train_batch_size=128,
            )
            .env_runners(
                num_env_runners=1,
                # Test with compression.
                # compress_observations=True,
                enable_connectors=True,
            )
            .callbacks(MyCallbacks)
        )

        num_iterations = 2

        for fw in framework_iterator(config, frameworks=("tf2", "torch")):
            # TODO (Kourosh) Bring back "FrozenLake-v1"
            for env in ["CartPole-v1", "Pendulum-v1", "ALE/Breakout-v5"]:
                print("Env={}".format(env))
                for lstm in [False]:
                    print("LSTM={}".format(lstm))
                    config.rl_module(model_config_dict=get_model_config(fw, lstm=lstm))

                    algo = config.build(env=env)
                    # TODO: Maybe add an API to get the Learner(s) instances within
                    #  a learner group, remote or not.
                    learner = algo.learner_group._learner
                    optim = learner.get_optimizer()
                    # Check initial LR directly set in optimizer vs the first (ts=0)
                    # value from the schedule.
                    lr = optim.param_groups[0]["lr"] if fw == "torch" else optim.lr
                    check(lr, config.lr[0][1])

                    # Check current entropy coeff value using the respective Scheduler.
                    entropy_coeff = learner.entropy_coeff_schedulers_per_module[
                        DEFAULT_MODULE_ID
                    ].get_current_value()
                    check(entropy_coeff, 0.1)

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
            .api_stack(enable_rl_module_and_learner=True)
            .environment(
                "FrozenLake-v1",
                env_config={"is_slippery": False, "map_name": "4x4"},
            )
            .env_runners(
                # Run locally.
                num_env_runners=0,
            )
        )
        obs = np.array(0)

        for _ in framework_iterator(config, frameworks=("torch", "tf2")):
            # Default Agent should be setup with StochasticSampling.
            algo = config.build()
            # explore=False, always expect the same (deterministic) action.
            a_ = algo.compute_single_action(
                obs, explore=False, prev_action=np.array(2), prev_reward=np.array(1.0)
            )

            for _ in range(50):
                a = algo.compute_single_action(
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
                    algo.compute_single_action(
                        obs, prev_action=np.array(2), prev_reward=np.array(1.0)
                    )
                )
            check(np.mean(actions), 1.5, atol=0.49)
            algo.stop()

    def test_ppo_free_log_std_with_rl_modules(self):
        """Tests the free log std option works."""
        config = (
            ppo.PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("Pendulum-v1")
            .env_runners(
                num_env_runners=1,
            )
            .rl_module(
                model_config_dict={
                    "fcnet_hiddens": [10],
                    "fcnet_activation": "linear",
                    "free_log_std": True,
                    "vf_share_layers": True,
                }
            )
            .training(
                gamma=0.99,
            )
        )

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            algo = config.build()
            policy = algo.get_policy()
            learner = algo.learner_group._learner
            module = learner.module[DEFAULT_MODULE_ID]

            # Check the free log std var is created.
            if fw == "torch":
                matching = [v for (n, v) in module.named_parameters() if "log_std" in n]
            else:
                matching = [
                    v for v in module.trainable_variables if "log_std" in str(v)
                ]
            assert len(matching) == 1, matching
            log_std_var = matching[0]

            def get_value(fw=fw, log_std_var=log_std_var):
                if fw == "torch":
                    return log_std_var.detach().cpu().numpy()[0]
                else:
                    return log_std_var.numpy()[0]

            # Check the variable is initially zero.
            init_std = get_value()
            assert init_std == 0.0, init_std
            batch = compute_gae_for_sample_batch(policy, PENDULUM_FAKE_BATCH.copy())
            batch = policy._lazy_tensor_dict(batch)
            algo.learner_group.update_from_batch(batch=batch.as_multi_agent())

            # Check the variable is updated.
            post_std = get_value()
            assert post_std != 0.0, post_std
            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
