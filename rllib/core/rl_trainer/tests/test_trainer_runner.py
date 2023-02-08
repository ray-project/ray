import gymnasium as gym
import itertools
import numpy as np
import unittest

import ray
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.test_utils import check, get_cartpole_dataset_reader
from ray.rllib.core.rl_trainer.scaling_config import TrainerScalingConfig
from ray.rllib.core.testing.utils import (
    get_trainer_runner,
    get_rl_trainer,
    add_module_to_runner_or_trainer,
)


REMOTE_SCALING_CONFIGS = {
    "remote-cpu": TrainerScalingConfig(num_workers=1),
    "remote-gpu": TrainerScalingConfig(num_workers=1, num_gpus_per_worker=0.5),
    "multi-gpu-ddp": TrainerScalingConfig(num_workers=2, num_gpus_per_worker=1),
    "multi-cpu-ddp": TrainerScalingConfig(num_workers=2, num_cpus_per_worker=2),
    # "multi-gpu-ddp-pipeline": TrainerScalingConfig(
    #     num_workers=2, num_gpus_per_worker=2
    # ),
}


LOCAL_SCALING_CONFIGS = {
    "local-cpu": TrainerScalingConfig(num_workers=0, num_gpus_per_worker=0),
    "local-gpu": TrainerScalingConfig(num_workers=0, num_gpus_per_worker=0.5),
}


class TestTrainerRunner(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    @staticmethod
    def local_training_helper(fw, scaling_mode) -> None:
        env = gym.make("CartPole-v1")
        scaling_config = LOCAL_SCALING_CONFIGS[scaling_mode]
        runner = get_trainer_runner(fw, env, scaling_config)
        local_trainer = get_rl_trainer(fw, env)
        local_trainer.build()

        # make the state of the trainer and the local runner identical
        local_trainer.set_state(runner.get_state()[0])

        reader = get_cartpole_dataset_reader(batch_size=500)
        batch = reader.next()
        batch = batch.as_multi_agent()
        check(local_trainer.update(batch), runner.update(batch)[0])

        new_module_id = "test_module"

        add_module_to_runner_or_trainer(fw, env, new_module_id, runner)
        add_module_to_runner_or_trainer(fw, env, new_module_id, local_trainer)

        # make the state of the trainer and the local runner identical
        local_trainer.set_state(runner.get_state()[0])

        # do another update
        batch = reader.next()
        ma_batch = MultiAgentBatch(
            {new_module_id: batch, DEFAULT_POLICY_ID: batch}, env_steps=batch.count
        )
        check(local_trainer.update(ma_batch), runner.update(ma_batch)[0])

        check(local_trainer.get_state(), runner.get_state()[0])

    def test_trainer_runner_local(self):
        fws = ["tf", "torch"]
        test_iterator = itertools.product(fws, LOCAL_SCALING_CONFIGS)
        # run the logic of this test inside of a ray actor because we want tensorflow
        # resources to be gracefully released. Tensorflow blocks the gpu resources
        # otherwise between test cases, causing a gpu oom error.
        remote_helper_fn = ray.remote(self.local_training_helper)
        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}")
            ray.get(remote_helper_fn.remote(fw, scaling_mode))

    def test_update_multigpu(self):
        fws = ["tf", "torch"]
        scaling_modes = REMOTE_SCALING_CONFIGS.keys()
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")

            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            runner = get_trainer_runner(fw, env, scaling_config)
            reader = get_cartpole_dataset_reader(batch_size=1024)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                results = runner.update(batch.as_multi_agent())

                loss = np.mean([res["loss"]["total_loss"] for res in results])
                min_loss = min(loss, min_loss)
                print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break

                for res1, res2 in zip(results, results[1:]):
                    self.assertEqual(
                        res1["mean_weight"]["default_policy"],
                        res2["mean_weight"]["default_policy"],
                    )

            self.assertLess(min_loss, 0.57)

            # make sure the runner resources are freed up so that we don't autoscale
            runner.shutdown()
            del runner

    def test_add_remove_module(self):
        fws = ["tf", "torch"]
        scaling_modes = REMOTE_SCALING_CONFIGS.keys()
        test_iterator = itertools.product(fws, scaling_modes)

        for fw, scaling_mode in test_iterator:
            print(f"Testing framework: {fw}, scaling mode: {scaling_mode}.")
            env = gym.make("CartPole-v1")
            scaling_config = REMOTE_SCALING_CONFIGS[scaling_mode]
            runner = get_trainer_runner(fw, env, scaling_config)
            reader = get_cartpole_dataset_reader(batch_size=500)
            batch = reader.next()

            # update once with the default policy
            results = runner.update(batch.as_multi_agent())
            module_ids_before_add = {DEFAULT_POLICY_ID}
            new_module_id = "test_module"

            # add a test_module
            add_module_to_runner_or_trainer(fw, env, new_module_id, runner)

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
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(result["loss"]) - {"total_loss"}, module_ids_after_add
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
                # remove the total_loss key since its not a module key
                self.assertEqual(
                    set(result["loss"]) - {"total_loss"}, module_ids_before_add
                )

            # make sure the runner resources are freed up so that we don't autoscale
            runner.shutdown()
            del runner


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
