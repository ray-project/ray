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


class TestOPE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=4)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_build_ope_methods(self):
        rllib_dir = Path(__file__).parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        env_name = "CartPole-v0"
        gamma = 0.99
        train_steps = 200000
        n_batches = 100 # Approx. equal to n_episodes
        n_eval_episodes = 10000
        estimator_config = {
            "model": {
                "fcnet_hiddens": [8, 4],
                "fcnet_activation": "relu",
                "vf_share_layers": True,
            },
            "q_model_type": "fqe", "clip_grad_norm": 100,
            "k": 5, "n_iters": 160, "lr": 1e-3, "delta": 1e-5, "batch_size": 32, "tau": 0.05,
        }

        config = (
            DQNConfig()
            .environment(env=env_name)
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

        # Train DQN for evaluation policy
        trainer = config.build()
        while trainer._timesteps_total and trainer._timesteps_total < train_steps:
            trainer.train()
        
        # Read n_batches of data
        reader = JsonReader(data_file)
        batch = reader.next()
        for _ in range(n_batches - 1):
            batch = batch.concat(reader.next())
        n_episodes = len(batch.split_by_episode())
        print("Episodes:", n_episodes, "Steps:", batch.count)
        estimators = [
            ImportanceSampling,
            WeightedImportanceSampling,
            DirectMethod,
            DoublyRobust,
        ]
        mean_ret = {}
        # Run estimators on data
        for estimator_cls in estimators:
            estimator = estimator_cls(
                trainer.get_policy(),
                gamma=gamma,
                config=estimator_config,
            )
            estimates = estimator.estimate(batch)
            assert len(estimates) == n_episodes
            if estimator_cls.__name__ == "WeightedImportanceSampling":
                # wis estimator improves with data, use last few episodes
                mean_ret[estimator_cls.__name__] = np.mean(
                    [e.metrics["v_new"] for e in estimates[-10:]]
                )
            else:
                mean_ret[estimator_cls.__name__] = np.mean(
                    [e.metrics["v_new"] for e in estimates]
                )

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
        print(mean_ret)
        
        def test_ope_in_trainer(self):
            # TODO (rohan): Test off_policy_estimation_methods in trainer config
            pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
