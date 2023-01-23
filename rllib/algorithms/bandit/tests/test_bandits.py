import gymnasium as gym
from gymnasium.spaces import Discrete, Box
import numpy as np
import unittest

import ray
from ray.rllib.algorithms.bandit.bandit import BanditLinTSConfig, BanditLinUCBConfig
from ray.rllib.examples.env.bandit_envs_discrete import SimpleContextualBandit
from ray.rllib.env import EnvContext
from ray.rllib.utils.test_utils import check_train_results, framework_iterator, check
from ray.rllib.utils.numpy import convert_to_numpy


class NonContextualBanditEnv(gym.Env):
    def __init__(self, config: EnvContext):
        best_arm_prob = config.get("best_arm_prob", 0.5)
        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 1.0, shape=(1,), dtype=np.float32)
        self.reset(seed=0)
        self._arm_probs = {0: 0.1, 1: best_arm_prob}

    def reset(self, *, seed=0, options=None):
        self._seed = seed
        if seed is not None:
            self.rng = np.random.default_rng(self._seed)
        return [1.0], {}

    def step(self, action):
        reward = self.rng.binomial(1, self._arm_probs[action])
        return [1.0], reward, True, False, {}


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
            BanditLinTSConfig()
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
            BanditLinUCBConfig()
            .environment(env=SimpleContextualBandit)
            .rollouts(num_envs_per_worker=2)
        )

        num_iterations = 5

        for _ in framework_iterator(
            config, frameworks=("tf2", "torch"), with_eager_tracing=True
        ):
            for train_batch_size in [1, 10]:
                config.training(train_batch_size=train_batch_size)
                algo = config.build()
                results = None
                for i in range(num_iterations):
                    results = algo.train()
                    check_train_results(results)
                    print(results)
                # Force good learning behavior (this is a very simple env).
                self.assertTrue(results["episode_reward_mean"] == 10.0)
                algo.stop()

    def test_bandit_convergence(self):
        # test whether in a simple bandit environment, the bandit algorithm
        # distribution converge to the optimal distribution empirically

        std_threshold = 0.1
        best_arm_prob = 0.5

        for config_cls in [BanditLinUCBConfig, BanditLinTSConfig]:
            config = (
                config_cls()
                .debugging(seed=0)
                .environment(
                    env=NonContextualBanditEnv,
                    env_config={"best_arm_prob": best_arm_prob},
                )
            )
            for _ in framework_iterator(
                config, frameworks=("tf2", "torch"), with_eager_tracing=True
            ):
                algo = config.build()
                model = algo.get_policy().model
                arm_means, arm_stds = [], []
                for _ in range(50):
                    # TODO the internals of the model is leaking here.
                    # We should revisit this once the RLModule is merged in.
                    samples = [model.arms[i].dist.sample((1000,)) for i in range(2)]
                    arm_means.append(
                        [float(convert_to_numpy(s).mean(0)) for s in samples]
                    )
                    arm_stds.append(
                        [float(convert_to_numpy(s).std(0)) for s in samples]
                    )
                    algo.train()

                best_arm = np.argmax(arm_means[-1])
                print(
                    f"best arm: {best_arm}, arm means: {arm_means[-1]}, "
                    f"arm stds: {arm_stds[-1]}"
                )

                # the better arm (according to the learned model) should be
                # sufficiently exploited so it should have a low variance at convergence
                self.assertLess(arm_stds[-1][best_arm], std_threshold)

                # best arm should also have a good estimate of its actual mean
                # Note that this may not be true for non-optimal arms as they may not
                # have been explored enough
                check(arm_means[-1][best_arm], best_arm_prob, decimals=1)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
