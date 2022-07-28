import unittest
import ray
from ray.data import read_json
from ray.rllib.algorithms.dqn import DQNConfig
from ray.rllib.offline.estimators import (
    ImportanceSampling,
    WeightedImportanceSampling,
    DirectMethod,
    DoublyRobust,
)
from ray.rllib.offline.dataset_reader import DatasetReader
from ray.rllib.policy.sample_batch import concat_samples
from pathlib import Path
import os


class TestOPE(unittest.TestCase):
    """Compilation tests for using OPE both standalone and in an Offline Algorithm"""

    @classmethod
    def setUpClass(cls):
        ray.init()
        rllib_dir = Path(__file__).parent.parent.parent.parent
        train_data = os.path.join(rllib_dir, "tests/data/cartpole/small.json")
        eval_data = train_data

        env_name = "CartPole-v0"
        cls.gamma = 0.99
        n_episodes = 3
        cls.q_model_config = {"n_iters": 160}

        config = (
            DQNConfig()
            .environment(env=env_name)
            .training(gamma=cls.gamma)
            .rollouts(num_rollout_workers=3, batch_mode="complete_episodes")
            .framework("torch")
            .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", 0)))
            .offline_data(input_=train_data)
            .evaluation(
                evaluation_interval=1,
                evaluation_duration=n_episodes,
                evaluation_num_workers=1,
                evaluation_duration_unit="episodes",
                evaluation_config={"input": eval_data},
                off_policy_estimation_methods={
                    "is": {"type": ImportanceSampling},
                    "wis": {"type": WeightedImportanceSampling},
                    "dm_fqe": {"type": DirectMethod},
                    "dr_fqe": {"type": DoublyRobust},
                },
            )
        )
        cls.algo = config.build()

        # Read n_episodes of data, assuming that one line is one episode
        reader = DatasetReader(read_json(eval_data))
        batches = [reader.next() for _ in range(n_episodes)]
        cls.batch = concat_samples(batches)
        cls.n_episodes = len(cls.batch.split_by_episode())
        print("Episodes:", cls.n_episodes, "Steps:", cls.batch.count)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_ope_standalone(self):
        # Test all OPE methods standalone
        estimator = ImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        assert estimates is not None, "IS estimator did not compute estimates"

        estimator = WeightedImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        assert estimates is not None, "WIS estimator did not compute estimates"

        estimator = DirectMethod(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        losses = estimator.train(self.batch)
        assert losses, "DM estimator did not return mean loss"
        estimates = estimator.estimate(self.batch)
        assert estimates is not None, "DM estimator did not compute estimates"

        estimator = DoublyRobust(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        losses = estimator.train(self.batch)
        assert losses, "DM estimator did not return mean loss"
        estimates = estimator.estimate(self.batch)
        assert estimates is not None, "DM estimator did not compute estimates"

    def test_ope_in_algo(self):
        # Test OPE in DQN, during training as well as by calling evaluate()
        ope_results = self.algo.train()["evaluation"]["off_policy_estimator"]
        # Check that key exists AND is not {}
        assert ope_results, "Did not run OPE in training!"
        assert set(ope_results["evaluation"]["off_policy_estimator"].keys()) == set(
            "is", "wis", "dm_fqe", "dr_fqe"
        ), "Missing keys in OPE result dict"

        # Check algo.evaluate() manually as well
        ope_results = self.algo.evaluate()["evaluation"]["off_policy_estimator"]
        assert ope_results, "Did not run OPE on call to Algorithm.evaluate()!"
        assert set(ope_results["evaluation"]["off_policy_estimator"].keys()) == set(
            "is", "wis", "dm_fqe", "dr_fqe"
        ), "Missing keys in OPE result dict"


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
