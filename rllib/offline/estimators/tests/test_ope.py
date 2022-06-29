import unittest
import ray
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.policy.sample_batch import concat_samples
from pathlib import Path
import os
import numpy as np
import gym


class TestOPE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)
        rllib_dir = Path(__file__).parent.parent.parent.parent
        eval_data = os.path.join(rllib_dir, "tests/data/cartpole/large.json")

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        train_steps = 200000
        n_batches = 20  # Approx. equal to n_episodes
        n_eval_episodes = 20

        config = (
            DQNConfig()
            .environment(env=env_name)
            .training(gamma=cls.gamma)
            .rollouts(num_rollout_workers=3, batch_mode="complete_episodes")
            .exploration(
                explore=True,
                exploration_config={
                    "type": "SoftQ",
                    "temperature": 1.0,
                },
            )
            .framework("torch")
            .evaluation(
                evaluation_interval=1,
                evaluation_duration=n_eval_episodes,
                evaluation_num_workers=1,
                evaluation_duration_unit="episodes",
                evaluation_config={
                    "input": "dataset",
                    "input_config": {"format": "json", "path": eval_data},
                },
                off_policy_estimation_methods={
                    "is": {"type": ImportanceSampling},
                    "wis": {"type": WeightedImportanceSampling},
                    "dm": {
                        "type": DirectMethod,
                    },
                    "dr": {
                        "type": DoublyRobust,
                    },
                },
            )
        )
        cls.algo = config.build()

        # Train DQN for evaluation policy
        timesteps_total = 0
        while timesteps_total < train_steps:
            results = cls.algo.train()
            timesteps_total = results["timesteps_total"]

        # Read n_batches of data
        reader = JsonReader(eval_data)
        cls.batch = reader.next()
        for _ in range(n_batches - 1):
            cls.batch = concat_samples([cls.batch, reader.next()])
        cls.n_episodes = len(cls.batch.split_by_episode())
        print("Episodes:", cls.n_episodes, "Steps:", cls.batch.count)

        cls.mean_ret = {}
        cls.std_ret = {}

        # Simulate Monte-Carlo rollouts
        mc_ret = []
        env = gym.make(env_name)
        for _ in range(n_eval_episodes):
            obs = env.reset()
            done = False
            rewards = []
            while not done:
                act = cls.algo.compute_single_action(obs)
                obs, reward, done, _ = env.step(act)
                rewards.append(reward)
            ret = 0
            for r in reversed(rewards):
                ret = r + cls.gamma * ret
            mc_ret.append(ret)

        cls.mean_ret["simulation"] = np.mean(mc_ret)
        cls.std_ret["simulation"] = np.std(mc_ret)

    @classmethod
    def tearDownClass(cls):
        print("Mean:", cls.mean_ret)
        print("Stddev:", cls.std_ret)
        ray.shutdown()

    def test_is(self):
        name = "is"
        estimator = ImportanceSampling(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_new"]
        self.std_ret[name] = estimates["v_new_std"]

    def test_wis(self):
        name = "wis"
        estimator = WeightedImportanceSampling(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_new"]
        self.std_ret[name] = estimates["v_new_std"]

    def test_dm(self):
        name = "dm"
        estimator = DirectMethod(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_new"]
        self.std_ret[name] = estimates["v_new_std"]

    def test_dr(self):
        name = "dr"
        estimator = DoublyRobust(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_new"]
        self.std_ret[name] = estimates["v_new_std"]

    def test_ope_in_algo(self):
        results = self.algo.evaluate()
        print(
            *list(results["evaluation"]["off_policy_estimator"].items()),
            sep="\n",
            end="\n\n\n"
        )

    def test_multiple_inputs(self):
        # TODO (Rohan138): Test with multiple input files
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
