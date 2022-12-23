import gymnasium as gym
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
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch


class TestTfRLTrainer(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
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

        batch = reader.next()
        for _ in range(5):
            results_worker_0, results_worker_1 = runner.update(batch.as_multi_agent())
            self.assertEqual(
                results_worker_0["mean_weight"]["default_policy"],
                results_worker_1["mean_weight"]["default_policy"],
            )

    def test_add_remove_module(self):
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
        batch = reader.next()
        results = runner.update(batch.as_multi_agent())
        module_ids_before_add = {DEFAULT_POLICY_ID}
        new_module_id = "test_module"

        # add a test_module
        runner.add_module(
            module_id=new_module_id,
            module_cls=DiscreteBCTFModule,
            module_config={
                "observation_space": env.observation_space,
                "action_space": env.action_space,
                "model_config": {"hidden_dim": 32},
            },
            optimizer_cls=BCTFOptimizer,
            optimizer_config={},
        )

        # do training that includes the test_module
        results = runner.update(
            MultiAgentBatch(
                {new_module_id: batch, DEFAULT_POLICY_ID: batch}, batch.count
            )
        )

        # check that module weights are updated across workers and synchronized
        for i in range(1, len(results)):
            for module_id in results[i]["mean_weight"].keys():
                assert (
                    results[i]["mean_weight"][module_id]
                    == results[i - 1]["mean_weight"][module_id]
                )

        # check that module ids are updated to include the new module
        module_ids_after_add = {DEFAULT_POLICY_ID, new_module_id}
        for result in results:
            for module_result in result.values():
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(module_result) - {"total_loss"}, module_ids_after_add
                )

        # remove the test_module
        runner.remove_module(module_id=new_module_id)

        # run training without the test_module
        results = runner.update(batch.as_multi_agent())

        # check that module weights are updated across workers and synchronized
        for i in range(1, len(results)):
            for module_id in results[i]["mean_weight"].keys():
                assert (
                    results[i]["mean_weight"][module_id]
                    == results[i - 1]["mean_weight"][module_id]
                )

        # check that module ids are updated after remove operation to not
        # include the new module
        for result in results:
            for module_result in result.values():
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(module_result) - {"total_loss"}, module_ids_before_add
                )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
