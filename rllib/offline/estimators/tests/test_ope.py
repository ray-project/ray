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

    def test_is(self):
        # Test IS standalone
        name = "is"
        estimator = ImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_target"]
        self.std_ret[name] = estimates["v_target_std"]

    def test_wis(self):
        # Test WIS standalone
        name = "wis"
        estimator = WeightedImportanceSampling(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
        )
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_target"]
        self.std_ret[name] = estimates["v_target_std"]

    def test_dm_fqe(self):
        # Test DM standalone
        name = "dm_fqe"
        estimator = DirectMethod(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        self.losses[name] = estimator.train(self.batch)
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_target"]
        self.std_ret[name] = estimates["v_target_std"]

    def test_dr_fqe(self):
        # Test DR standalone
        name = "dr_fqe"
        estimator = DoublyRobust(
            policy=self.algo.get_policy(),
            gamma=self.gamma,
            q_model_config=self.q_model_config,
        )
        self.losses[name] = estimator.train(self.batch)
        estimates = estimator.estimate(self.batch)
        self.mean_ret[name] = estimates["v_target"]
        self.std_ret[name] = estimates["v_target_std"]

    def test_ope_in_algo(self):
        # Test OPE in DQN, during training as well as by calling evaluate()
        results = self.algo.train()
        # Check that key exists AND is not {}
        assert results["evaluation"][
            "off_policy_estimator"
        ], "Did not run OPE during training!"
        # Check algo.evaluate() manually as well
        results = self.algo.evaluate()
        print("OPE in Algorithm results")
        estimates = results["evaluation"]["off_policy_estimator"]
        mean_est = {k: v["v_target"] for k, v in estimates.items()}
        std_est = {k: v["v_target_std"] for k, v in estimates.items()}

        print("Mean:")
        print(*list(mean_est.items()), sep="\n")
        print("Stddev:")
        print(*list(std_est.items()), sep="\n")
        print("\n\n\n")

    def test_multiple_inputs(self):
        # TODO (Rohan138): Test with multiple input files
        pass


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
