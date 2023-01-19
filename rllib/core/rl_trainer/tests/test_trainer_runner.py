import gymnasium as gym
import unittest
import ray
import time

from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, MultiAgentBatch
from ray.rllib.utils.test_utils import get_cartpole_dataset_reader
from ray.rllib.core.testing.utils import get_trainer_runner, add_module_to_runner_or_trainer


class TestTrainerRunner(unittest.TestCase):
    """This test is setup for 2 gpus."""

    # TODO: This unittest should also test other resource allocations like multi-cpu,
    # multi-node multi-gpu, etc.

    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_update_multigpu(self):
        """Test training in a 2 gpu setup and that weights are synchronized."""

        for fw in ["tf", "torch"]:
            ray.init(ignore_reinit_error=True)
            print(f"Testing framework: {fw}.")
            env = gym.make("CartPole-v1")
            runner = get_trainer_runner(fw, env, compute_config=dict(num_gpus=2))
            reader = get_cartpole_dataset_reader(batch_size=500)

            min_loss = float("inf")
            for iter_i in range(1000):
                batch = reader.next()
                res_0, res_1 = runner.update(batch.as_multi_agent())

                loss = (res_0["loss"]["total_loss"] + res_1["loss"]["total_loss"]) / 2
                min_loss = min(loss, min_loss)
                print(f"[iter = {iter_i}] Loss: {loss:.3f}, Min Loss: {min_loss:.3f}")
                # The loss is initially around 0.69 (ln2). When it gets to around
                # 0.57 the return of the policy gets to around 100.
                if min_loss < 0.57:
                    break
                self.assertEqual(
                    res_0["mean_weight"]["default_policy"],
                    res_1["mean_weight"]["default_policy"],
                )
            self.assertLess(min_loss, 0.57)

            # make sure the runner resources are freed up so that we don't autoscale
            del runner
            ray.shutdown()
            time.sleep(10)

    def test_add_remove_module(self):

        for fw in ["tf", "torch"]:
            ray.init(ignore_reinit_error=True)
            print(f"Testing framework: {fw}.")
            env = gym.make("CartPole-v1")
            runner = get_trainer_runner(fw, env, compute_config=dict(num_gpus=2))
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
            del runner
            ray.shutdown()
            time.sleep(10)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
