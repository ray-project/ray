import unittest
import ray
from ray import tune
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.offline.json_reader import JsonReader
from pathlib import Path
import os
import numpy as np
import gym


class TestOPE(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4)

    def tearDown(self):
        ray.shutdown()

    @classmethod
    def setUpClass(cls):
        ray.init(ignore_reinit_error=True)
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        train_steps = 20000
        n_batches = 20  # Approx. equal to n_episodes
        n_eval_episodes = 100

        config = (
            DQNConfig()
            .environment(env=env_name)
            .training(gamma=cls.gamma)
            .rollouts(num_rollout_workers=3)
            .exploration(
                explore=True,
                exploration_config={
                    "type": "SoftQ",
                    "temperature": 1.0,
                },
            )
            .framework("torch")
            .rollouts(batch_mode="complete_episodes")
        )
        cls.trainer = config.build()

        # Train DQN for evaluation policy
        tune.run(
            "DQN",
            config=config.to_dict(),
            stop={"timesteps_total": train_steps},
            verbose=0,
        )

        # Read n_batches of data
        reader = JsonReader(data_file)
        cls.batch = reader.next()
        for _ in range(n_batches - 1):
            cls.batch = cls.batch.concat(reader.next())
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
                act = cls.trainer.compute_single_action(obs)
                obs, reward, done, _ = env.step(act)
                rewards.append(reward)
            ret = 0
            for r in reversed(rewards):
                ret = r + cls.gamma * ret
            mc_ret.append(ret)

        cls.mean_ret["simulation"] = np.mean(mc_ret)
        cls.std_ret["simulation"] = np.std(mc_ret)

        # Optional configs for the model-based estimators
        cls.model_config = {"k": 2, "n_iters": 10}
        ray.shutdown()

    @classmethod
    def tearDownClass(cls):
        print("Mean:", cls.mean_ret)
        print("Stddev:", cls.std_ret)
        ray.shutdown()

    def test_is(self):
        name = "is"
        estimator = ImportanceSampling(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_wis(self):
        name = "wis"
        estimator = WeightedImportanceSampling(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dm_qreg(self):
        name = "dm_qreg"
        estimator = DirectMethod(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dm_fqe(self):
        name = "dm_fqe"
        estimator = DirectMethod(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dr_qreg(self):
        name = "dr_qreg"
        estimator = DoublyRobust(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="qreg",
            **self.model_config,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_dr_fqe(self):
        name = "dr_fqe"
        estimator = DoublyRobust(
            name=name,
            policy=self.trainer.get_policy(),
            gamma=self.gamma,
            q_model_type="fqe",
            **self.model_config,
        )
        estimator.process(self.batch)
        estimates = estimator.get_metrics()
        assert len(estimates) == self.n_episodes
        self.mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
        self.std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

    def test_ope_in_trainer(self):
        # TODO (rohan): Add performance tests for off_policy_estimation_methods,
        # with fixed seeds and hyperparameters
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
