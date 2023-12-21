import unittest

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo_learner import (
    LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY,
)
from ray.rllib.algorithms.callbacks import DefaultCallbacks

from ray.rllib.core.learner.learner import (
    LEARNER_RESULTS_CURR_LR_KEY,
)

from ray.rllib.env.single_agent_env_runner import SingleAgentEnvRunner
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import (
    check,
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
        stats = result["info"][LEARNER_INFO][DEFAULT_POLICY_ID]
        # Entropy coeff goes to 0.05, then 0.0 (per iter).
        check(
            stats[LEARNER_RESULTS_CURR_ENTROPY_COEFF_KEY],
            0.05 if algorithm.iteration == 1 else 0.0,
        )

        # Learning rate should decrease by 0.0001/4 per iteration.
        check(
            stats[LEARNER_RESULTS_CURR_LR_KEY],
            0.0000075 if algorithm.iteration == 1 else 0.000005,
        )
        # Compare reported curr lr vs the actual lr found in the optimizer object.
        optim = algorithm.learner_group._learner.get_optimizer()
        actual_optimizer_lr = (
            optim.param_groups[0]["lr"]
            if algorithm.config.framework_str == "torch"
            else optim.lr
        )
        check(stats[LEARNER_RESULTS_CURR_LR_KEY], actual_optimizer_lr)


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
            .experimental(_enable_new_api_stack=True)
            .rollouts(
                env_runner_cls=SingleAgentEnvRunner,
                num_rollout_workers=0,
            )
            .training(
                num_sgd_iter=2,
                # Setup lr schedule for testing lr-scheduling correctness.
                lr=[[0, 0.00001], [512, 0.0]],  # 512=4x128
                # Set entropy_coeff to a faulty value to proof that it'll get
                # overridden by the schedule below (which is expected).
                entropy_coeff=[[0, 0.1], [256, 0.0]],  # 256=2x128,
                train_batch_size=128,
            )
            .callbacks(MyCallbacks)
            .evaluation(
                # Also test evaluation with remote workers.
                evaluation_num_workers=2,
                evaluation_duration=3,
                evaluation_duration_unit="episodes",
                # Has to be used if `env_runner_cls` is not RolloutWorker.
                enable_async_evaluation=True,
            )
        )

        num_iterations = 2

        for fw in framework_iterator(config, frameworks=("torch", "tf2")):
            # TODO (Kourosh) Bring back "FrozenLake-v1"
            for env in [
                # "CliffWalking-v0",
                "CartPole-v1",
                "Pendulum-v1",
            ]:  # "ALE/Breakout-v5"]:
                print("Env={}".format(env))
                for lstm in [False]:
                    print("LSTM={}".format(lstm))
                    config.training(model=get_model_config(fw, lstm=lstm)).framework(
                        eager_tracing=False
                    )

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
                        DEFAULT_POLICY_ID
                    ].get_current_value()
                    check(entropy_coeff, 0.1)

                    for i in range(num_iterations):
                        results = algo.train()
                        check_train_results(results)
                        print(results)

                    # algo.evaluate()
                    algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
