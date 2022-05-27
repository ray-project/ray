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
        data_file = os.path.join(rllib_dir, "tests/data/cartpole/small.json")
        print("data_file={} exists={}".format(data_file, os.path.isfile(data_file)))

        config = (
            DQNConfig()
            .environment(env="CartPole-v0")
            .offline_data(input_=data_file, input_evaluation=[])
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

        timesteps_total = 200000
        n_batches = 20
        while trainer._timesteps_total and trainer._timesteps_total < timesteps_total:
            trainer.train()
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
        for estimator_cls in estimators:
            estimator = estimator_cls(
                trainer.get_policy(),
                gamma=0.99,
                config={
                    "model": {
                        "fcnet_hiddens": [8, 4],
                        "fcnet_activation": "relu",
                        "vf_share_layers": True,
                    },
                    "q_model_type": "fqe",
                    "clip_grad_norm": 100,
                    "k": 5,
                    "n_iters": 160,
                    "lr": 1e-3,
                    "delta": 1e-5,
                    "batch_size": 32,
                    "tau": 0.01,
                },
            )
            estimates = estimator.estimate(batch)
            assert len(estimates) == n_episodes


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
