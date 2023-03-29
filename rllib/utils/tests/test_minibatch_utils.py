import unittest
import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.minibatch_utils import MiniBatchCyclicIterator

CONFIGS = [
    {"mini_batch_size": 128, "num_sgd_iter": 3, "agent_steps": (56, 56)},
    {"mini_batch_size": 128, "num_sgd_iter": 7, "agent_steps": (56, 56)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (56, 56)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (56, 3)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (56, 4)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (56, 55)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (400, 400)},
    {"mini_batch_size": 128, "num_sgd_iter": 10, "agent_steps": (64, 64)},
]


class TestMinibatchUtils(unittest.TestCase):
    def test_minibatch_cyclic_iterator(self):

        for config in CONFIGS:
            mini_batch_size = config["mini_batch_size"]
            num_sgd_iter = config["num_sgd_iter"]
            agent_steps = config["agent_steps"]

            num_env_steps = max(agent_steps)
            sample_batches = {
                f"pol{i}": SampleBatch({"obs": np.arange(agent_steps[i])})
                for i in range(len(agent_steps))
            }
            mb = MultiAgentBatch(sample_batches, num_env_steps)
            batch_iter = MiniBatchCyclicIterator(mb, mini_batch_size, num_sgd_iter)
            print(config)
            iteration_counter = 0
            for batch in batch_iter:
                print(batch)
                print("-" * 80)
                print(batch["pol0"]["obs"])
                print("*" * 80)
                # Check that for each policy the batch size is equal to the
                # mini_batch_size
                for policy_batch in batch.policy_batches.values():
                    self.assertEqual(policy_batch.count, mini_batch_size)
                iteration_counter += 1

            # for each policy check that the last item in batch matches the expected
            # values, i.e. iteration_counter * mini_batch_size % agent_steps - 1
            total_steps = iteration_counter * mini_batch_size
            for policy_idx, policy_batch in enumerate(batch.policy_batches.values()):
                expected_last_item = (total_steps - 1) % agent_steps[policy_idx]
                self.assertEqual(policy_batch["obs"][-1], expected_last_item)

            # check iteration counter (should be
            # ceil(num_gsd_iter * max(agent_steps) / mini_batch_size))
            expected_iteration_counter = np.ceil(
                num_sgd_iter * max(agent_steps) / mini_batch_size
            )
            self.assertEqual(iteration_counter, expected_iteration_counter)
            print(f"iteration_counter: {iteration_counter}")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
