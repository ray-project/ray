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
from pathlib import Path
import os
import numpy as np
import gym

from ray.rllib.offline.estimators.off_policy_estimator import OffPolicyEstimator


class TestOPE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_build_ope_methods(self):
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        env_name = "CartPole-v0"
        gamma = 0.99
        train_steps = 200000
        n_batches = 20  # Approx. equal to n_episodes
        n_eval_episodes = 100

        config = (
            DQNConfig()
            .environment(env=env_name)
            .offline_data(
                input_=data_file,
                off_policy_estimation_methods={"simulation": {"type": "simulation"}},
            )
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
        trainer = config.build()

        # Train DQN for evaluation policy
        while trainer._timesteps_total and trainer._timesteps_total < train_steps:
            trainer.train()

        # Read n_batches of data
        reader = JsonReader(data_file)
        batch = reader.next()
        for _ in range(n_batches - 1):
            batch = batch.concat(reader.next())
        n_episodes = len(batch.split_by_episode())
        print("Episodes:", n_episodes, "Steps:", batch.count)
        estimators = {
            "is": {"type": ImportanceSampling},
            "wis": {"type": WeightedImportanceSampling},
            "dm_qreg": {
                "type": DirectMethod,
                "q_model_type": "qreg",
            },
            "dm_fqe": {
                "type": DirectMethod,
                "q_model_type": "fqe",
            },
            "dr_qreg": {
                "type": DoublyRobust,
                "q_model_type": "qreg",
            },
            "dr_fqe": {
                "type": DoublyRobust,
                "q_model_type": "fqe",
            },
        }
        mean_ret = {}
        std_ret = {}
        # Run estimators on data
        for name, method_config in estimators.items():
            estimator_cls = method_config.pop("type")
            estimator: OffPolicyEstimator = estimator_cls(
                name=name,
                policy=trainer.get_policy(),
                gamma=gamma,
                **method_config,
            )
            estimator.process(batch)
            estimates = estimator.get_metrics()
            assert len(estimates) == n_episodes
            mean_ret[name] = np.mean([e.metrics["v_new"] for e in estimates])
            std_ret[name] = np.std([e.metrics["v_new"] for e in estimates])

        # Simulate Monte-Carlo rollouts
        mc_ret = []
        env = gym.make(env_name)
        for _ in range(n_eval_episodes):
            obs = env.reset()
            done = False
            rewards = []
            while not done:
                act = trainer.compute_single_action(obs)
                obs, reward, done, _ = env.step(act)
                rewards.append(reward)
            ret = 0
            for r in reversed(rewards):
                ret = r + gamma * ret
            mc_ret.append(ret)

        mean_ret["simulation"] = np.mean(mc_ret)
        std_ret["simulation"] = np.std(mc_ret)
        print("mean: ", mean_ret)
        print("stddev: ", std_ret)

    def test_ope_in_trainer(self):
        # TODO (rohan): Add performance tests for off_policy_estimation_methods,
        # with fixed seeds and hyperparameters
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
