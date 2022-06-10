import unittest
import ray
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from pathlib import Path
import os
import numpy as np
import gym


class TestOPE(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=8)

    def tearDown(self):
        ray.shutdown()

    def test_rllib_cartpole_large(self):
        # Test on rllib/tests/data/cartpole/large.json
        rllib_dir = Path(__file__).parent.parent.parent.parent
        print("rllib dir={}".format(rllib_dir))
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/large.json")

        env_name = "CartPole-v0"
        gamma = 0.99
        train_iters = 25
        num_workers = 2
        eval_num_workers = 5
        train_test_split_val = 0.8
        eval_episodes = 100

        config = (
            DQNConfig()
            .rollouts(num_rollout_workers=num_workers)
            .training(gamma=gamma)
            .environment(env=env_name)
            .offline_data(
                input_="dataset",
                input_config={"format": "json", "path": data_file},
            )
            .exploration(
                explore=True,
                exploration_config={
                    "type": "SoftQ",
                    "temperature": 1.0,
                },
            )
            .evaluation(
                # Evaluate once after training for train_iters
                evaluation_interval=train_iters,
                evaluation_duration_unit="episodes",
                evaluation_duration=eval_episodes,
                evaluation_num_workers=eval_num_workers,
                evaluation_config={
                    "input": "dataset",
                    "input_config": {"format": "json", "path": data_file},
                },
                off_policy_estimation_methods={
                    "train_test_split_val": train_test_split_val,
                    "is": {"type": ImportanceSampling},
                    "wis": {"type": WeightedImportanceSampling},
                    "dm_qreg": {"type": DirectMethod, "q_model_type": "qreg"},
                    "dm_fqe": {"type": DirectMethod, "q_model_type": "fqe"},
                    "dr_qreg": {"type": DoublyRobust, "q_model_type": "qreg"},
                    "dr_fqe": {"type": DoublyRobust, "q_model_type": "fqe"},
                },
            )
            .framework("torch")
            .rollouts(batch_mode="complete_episodes")
        )

        trainer = config.build()

        for _ in range(train_iters - 1):
            results = trainer.train()
        print("Trained for ", results["timesteps_total"], "timesteps")

        # Final .train() will run trainer.evaluate() as well
        results = trainer.train()

        # Simulate Monte-Carlo rollouts
        mc_ret = []
        env = gym.make(env_name)
        for _ in range(eval_episodes):
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

        estimates = results["evaluation"]["off_policy_estimator"]
        print("Simulation", "mean:", np.mean(mc_ret), "std:", np.std(mc_ret))
        for k, v in estimates.items():
            print(k, v)

    def test_d3rply_cartpole_random(self):
        # Test OPE methods on d3rlpy cartpole-random
        pass

    def test_d3rlpy_cartpole_replay(self):
        # Test OPE methods on d3rlpy cartpole-random
        pass

    def test_cobs_mountaincar(self):
        # Test OPE methods on COBS MountainCar
        pass

    def test_input_evaluation_backwards_compatible(self):
        # Test with deprecated `input_evaluation` config key
        pass

    def test_multiple_input_sources(self):
        # Test multiple input sources e.g. input = {data_file : 0.5, "sampler": 0.5}
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
