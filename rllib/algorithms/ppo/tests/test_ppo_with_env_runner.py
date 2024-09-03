import unittest

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo_learner import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
)
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.learner.learner import DEFAULT_OPTIMIZER, LR_KEY

from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.test_utils import check, check_train_results_new_api_stack


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
        stats = result[LEARNER_RESULTS][DEFAULT_MODULE_ID]
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

        # Build a PPOConfig object with the `SingleAgentEnvRunner` class.
        config = (
            ppo.PPOConfig()
            # Enable new API stack and use EnvRunner.
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .env_runners(num_env_runners=0)
            .training(
                num_epochs=2,
                # Setup lr schedule for testing lr-scheduling correctness.
                lr=[[0, 0.00001], [512, 0.0]],  # 512=4x128
                # Setup `entropy_coeff` schedule for testing whether it's scheduled
                # correctly.
                entropy_coeff=[[0, 0.1], [256, 0.0]],  # 256=2x128,
                train_batch_size=128,
            )
            .callbacks(MyCallbacks)
            .evaluation(
                # Also test evaluation with remote workers.
                evaluation_num_env_runners=2,
                evaluation_duration=3,
                evaluation_duration_unit="episodes",
                evaluation_parallel_to_training=True,
            )
        )

        num_iterations = 2

        # TODO (sven) Bring back "FrozenLake-v1"
        for env in [
            # "CliffWalking-v0",
            "CartPole-v1",
            "Pendulum-v1",
        ]:  # "ALE/Breakout-v5"]:
            print("Env={}".format(env))
            for lstm in [False]:
                print("LSTM={}".format(lstm))
                config.rl_module(
                    model_config_dict=get_model_config("torch", lstm=lstm)
                ).framework(eager_tracing=False)

                algo = config.build(env=env)
                # TODO: Maybe add an API to get the Learner(s) instances within
                #  a learner group, remote or not.
                learner = algo.learner_group._learner
                optim = learner.get_optimizer()
                # Check initial LR directly set in optimizer vs the first (ts=0)
                # value from the schedule.
                lr = optim.param_groups[0]["lr"]
                check(lr, config.lr[0][1])

                # Check current entropy coeff value using the respective Scheduler.
                entropy_coeff = learner.entropy_coeff_schedulers_per_module[
                    DEFAULT_MODULE_ID
                ].get_current_value()
                check(entropy_coeff, 0.1)

                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results_new_api_stack(results)
                    print(results)

                # algo.evaluate()
                algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
