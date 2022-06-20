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
        ray.init(num_cpus=8)
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        train_data = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("train_data={} exists={}".format(train_data, os.path.isfile(train_data)))
        eval_data = train_data

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        train_steps = 200000
        n_batches = 20  # Approx. equal to n_episodes
        n_eval_episodes = 20
        # Optional configs for the model-based estimators
        cls.model_config = {"train_test_split_val": 0.0, "k": 2, "n_iters": 10}

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
            .offline_data(
                input_="dataset",
                input_config={"format": "json", "path": train_data},
            )
            .evaluation(
                evaluation_interval=None,
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
                    "dm_qreg": {
                        "type": DirectMethod,
                        "q_model_type": "qreg",
                        **cls.model_config,
                    },
                    "dm_fqe": {
                        "type": DirectMethod,
                        "q_model_type": "fqe",
                        **cls.model_config,
                    },
                    "dr_qreg": {
                        "type": DoublyRobust,
                        "q_model_type": "qreg",
                        **cls.model_config,
                    },
                    "dr_fqe": {
                        "type": DoublyRobust,
                        "q_model_type": "fqe",
                        **cls.model_config,
                    },
                },
            )
        )
        cls.algo = config.build()

        # Train DQN for evaluation policy
        timesteps_total = 0
        while timesteps_total < train_steps:
            results = cls.trainer.train()
            timesteps_total = results["timesteps_total"]

        # Read n_batches of data
        reader = JsonReader(train_data)
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
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_wis(self):
        name = "wis"
        estimator = WeightedImportanceSampling(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_dm_qreg(self):
        name = "dm_qreg"
        estimator = DirectMethod(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_dm_fqe(self):
        name = "dm_fqe"
        estimator = DirectMethod(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_dr_qreg(self):
        name = "dr_qreg"
        estimator = DoublyRobust(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_dr_fqe(self):
        name = "dr_fqe"
        estimator = DoublyRobust(
            name=name,
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = np.mean(estimates["v_new"])
        self.std_ret[name] = np.std(estimates["v_new"])

    def test_ope_in_trainer(self):
        results = self.trainer.evaluate()
        print(results["evaluation"]["off_policy_estimator"])
        print("\n\n\n")

    def test_multiple_inputs(self):
        # TODO (Rohan138): Test with multiple input files
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
