from typing import Counter
import gym
from gym.spaces import Discrete, Box
import numpy as np
import unittest

import ray
from ray.rllib.algorithms.bandit import bandit
from ray.rllib.algorithms.bandit.bandit import BanditLinTS, BanditLinUCB
from ray.rllib.examples.env.bandit_envs_discrete import SimpleContextualBandit
from ray.rllib.utils.test_utils import check_train_results, framework_iterator

# from ray.rllib.utils.test_utils import check
from ray.rllib.env import EnvContext


class TestBandits(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_bandit_lin_ts_compilation(self):
        """Test whether BanditLinTS can be built on all frameworks."""
        config = (
            bandit.BanditLinTSConfig()
            .environment(env=SimpleContextualBandit)
            .rollouts(num_rollout_workers=2, num_envs_per_worker=2)
        )
        num_iterations = 5

        for _ in framework_iterator(
            config, frameworks=("tf2", "torch"), with_eager_tracing=True
        ):
            for train_batch_size in [1, 10]:
                config.training(train_batch_size=train_batch_size)
                algo = config.build()
                results = None
                for _ in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                algo.stop()

    def test_bandit_lin_ucb_compilation(self):
        """Test whether BanditLinUCB can be built on all frameworks."""
        config = (
            bandit.BanditLinUCBConfig()
            .environment(env=SimpleContextualBandit)
            .rollouts(num_envs_per_worker=2)
        )

        num_iterations = 5

        for _ in framework_iterator(
            config, frameworks=("tf2", "torch"), with_eager_tracing=True
        ):
            for train_batch_size in [1, 10]:
                config.training(train_batch_size=train_batch_size)
                trainer = config.build()
                results = None
                for i in range(num_iterations):
                    results = trainer.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                trainer.stop()

    def test_bandit_convergence(self):
        TRAINING_ITERATIONS = 50
        ARM_COUNT = 2
        BETTER_ARM_PROBABILITY = 0.3
        ACTION_TO_REWARD_PROBABILITY = {0: 0.1, 1: BETTER_ARM_PROBABILITY}

        class NonContextualBanditEnv(gym.Env):
            def __init__(self, config: EnvContext):
                self.action_space = Discrete(ARM_COUNT)
                # actually no features - can remove?
                self.observation_space = Box(0.0, 1.0, shape=(1,), dtype=np.float32)
                self.seed(0)

            def reset(self):
                return [1.0]

            def step(self, action):
                reward = self.rng.binomial(1.0, ACTION_TO_REWARD_PROBABILITY[action])
                return ([1.0], reward, True, {})

            def seed(self, seed=0):
                self._seed = seed
                if seed is not None:
                    self.rng = np.random.default_rng(self._seed)

        for algo_cls in [BanditLinUCB, BanditLinTS]:
            algotf = algo_cls(
                env=NonContextualBanditEnv, config={"framework": "tf2", "seed": 0}
            )
            policytf = algotf.get_policy()
            # modeltf = policytf.model

            algotorch = algo_cls(
                env=NonContextualBanditEnv, config={"framework": "torch", "seed": 0}
            )
            policytorch = algotorch.get_policy()
            # modeltorch = policytorch.model
            for _ in range(TRAINING_ITERATIONS):
                algotf.train()
                algotorch.train()
            # n = 10000
            # # TODO: Kourosh. not checking arm 0 because it isn't converging to
            # # same values for tf and torch
            # for arm_idx in [1]:
            #     samplestf = modeltf.arms[arm_idx].dist.sample(sample_shape=n)
            #     samplestorch = [
            #         float(modeltorch.arms[1].dist.sample()) for _ in range(n)
            #     ]

            #     mean_of_arm_tf_analytical = np.mean(samplestf)
            #     mean_of_arm_torch_analytical = np.mean(samplestorch)
            #     variance_of_arm_tf = np.var(samplestf)
            #     variance_of_arm_torch = np.var(samplestorch)

            # check(
            #     mean_of_arm_tf_analytical, mean_of_arm_torch_analytical, atol=3e-3
            # )
            # check(variance_of_arm_tf, variance_of_arm_torch, atol=1e-4)

            # checking against values that are currently being outputted
            # check(variance_of_arm_tf, 2e-4, atol=1e-5)
            # check(variance_of_arm_torch, 2e-4, atol=1e-5)
            # check(mean_of_arm_tf_analytical, 0.301, 5e-4)
            # check(mean_of_arm_torch_analytical, 0.301, 5e-4)

            # check that after training the bandit favors the arm giving
            # better reward (arm 1)
            env = NonContextualBanditEnv({})
            env.seed(0)

            for policy in [policytorch, policytf]:
                obs = env.reset()
                counter = Counter({1: 0, 0: 0})
                for _ in range(100):
                    action, _, _ = policy.compute_single_action(obs)
                    obs, _, _, _ = env.step(action)
                    counter[action] += 1
                assert counter[1] > 95, "Arm 1 should be selected almost " "every time"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
