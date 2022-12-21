import gym
import unittest

import ray

from ray.rllib.algorithms import AlgorithmConfig
from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_reader import (
    DatasetReader,
    get_dataset_and_shards,
)

from ray.rllib.core.rl_trainer.trainer_runner import TrainerRunner
from ray.rllib.core.rl_trainer.tf.tf_rl_trainer import TfRLTrainer
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule
from ray.rllib.core.testing.tf.bc_optimizer import BCTFOptimizer


class TestTfRLTrainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_update_multigpu(self):
        """Test training in a 2 gpu setup and that weights are synchronized."""
        env = gym.make("CartPole-v1")
        trainer_class = TfRLTrainer
        trainer_cfg = dict(
            module_class=DiscreteBCTFModule,
            module_config={
                "observation_space": env.observation_space,
                "action_space": env.action_space,
                "model_config": {"hidden_dim": 32},
            },
            optimizer_class=BCTFOptimizer,
            optimizer_config={},
            debug=True,
        )
        runner = TrainerRunner(
            trainer_class, trainer_cfg, compute_config=dict(num_gpus=2)
        )

        # path = "s3://air-example-data/rllib/cartpole/large.json"

        path = "tests/data/cartpole/large.json"
        input_config = {"format": "json", "paths": path}
        dataset, _ = get_dataset_and_shards(
            AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
        )
        batch_size = 500
        ioctx = IOContext(
            config=(
                AlgorithmConfig()
                .training(train_batch_size=batch_size)
                .offline_data(actions_in_input_normalized=True)
            ),
            worker_index=0,
        )
        reader = DatasetReader(dataset, ioctx)
        # num_epochs = 100
        # total_timesteps_of_training = 1000000
        # inter_steps = total_timesteps_of_training // (num_epochs * batch_size)
        # for _ in range(num_epochs):
        # for _ in range(inter_steps):
        batch = reader.next()
        for _ in range(5):
            results_worker_0, results_worker_1 = runner.update(batch.as_multi_agent())
            self.assertEqual(
                results_worker_0["mean_weight"]["default_policy"],
                results_worker_1["mean_weight"]["default_policy"],
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))

    # test 2: check add remove module / optimizer ok
    # check if memory usage all right after adding / removing module

    # env = gym.make("CartPole-v1")
    # trainer_class = TfRLTrainer
    # trainer_cfg = dict(
    #     module_class=DiscreteBCTFModule,
    #     module_config={
    #         "observation_space": env.observation_space,
    #         "action_space": env.action_space,
    #         "model_config": {"hidden_dim": 32},
    #     },
    #     optimizer_class=BCTFOptimizer,
    #     optimizer_config={},
    #     debug=True,
    # )
    # runner = TrainerRunner(
    #     trainer_class, trainer_cfg, compute_config=dict(num_gpus=2)
    # )

    # # path = "s3://air-example-data/rllib/cartpole/large.json"

    # path = "tests/data/cartpole/large.json"
    # input_config = {"format": "json", "paths": path}
    # dataset, _ = get_dataset_and_shards(
    #     AlgorithmConfig().offline_data(input_="dataset", input_config=input_config)
    # )
    # batch_size = 500
    # ioctx = IOContext(
    #     config=(
    #         AlgorithmConfig()
    #         .training(train_batch_size=batch_size)
    #         .offline_data(actions_in_input_normalized=True)
    #     ),
    #     worker_index=0,
    # )
    # reader = DatasetReader(dataset, ioctx)
    # # num_epochs = 100
    # # total_timesteps_of_training = 1000000
    # # inter_steps = total_timesteps_of_training // (num_epochs * batch_size)
    # # for _ in range(num_epochs):
    # # for _ in range(inter_steps):
    # batch = reader.next()
    # for _ in range(5):
    #     results_worker_0, results_worker_1 = runner.update(batch.as_multi_agent())
