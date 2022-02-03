import unittest

import ray
from ray.rllib.agents.callbacks import DefaultCallbacks
import ray.rllib.agents.dqn as dqn
from ray.rllib.utils.test_utils import framework_iterator


class OnSubEnvironmentCreatedCallback(DefaultCallbacks):
    def on_sub_environment_created(
        self, *, worker, sub_environment, env_context, **kwargs
    ):
        # Create a vector-index-sum property per remote worker.
        if not hasattr(worker, "sum_sub_env_vector_indices"):
            worker.sum_sub_env_vector_indices = 0
        # Add the sub-env's vector index to the counter.
        worker.sum_sub_env_vector_indices += env_context.vector_index
        print(
            f"sub-env {sub_environment} created; "
            f"worker={worker.worker_index}; "
            f"vector-idx={env_context.vector_index}"
        )


class TestCallbacks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_on_sub_environment_created(self):
        config = {
            "env": "CartPole-v1",
            # Create 4 sub-environments per remote worker.
            "num_envs_per_worker": 4,
            # Create 2 remote workers.
            "num_workers": 2,
            "callbacks": OnSubEnvironmentCreatedCallback,
        }

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = dqn.DQNTrainer(config=config)
            # Fake the counter on the local worker (doesn't have an env) and
            # set it to -1 so the below `foreach_worker()` won't fail.
            trainer.workers.local_worker().sum_sub_env_vector_indices = -1

            # Get sub-env vector index sums from the 2 remote workers:
            sum_sub_env_vector_indices = trainer.workers.foreach_worker(
                lambda w: w.sum_sub_env_vector_indices
            )
            # Local worker has no environments -> Expect the -1 special
            # value returned by the above lambda.
            self.assertTrue(sum_sub_env_vector_indices[0] == -1)
            # Both remote workers (index 1 and 2) have a vector index counter
            # of 6 (sum of vector indices: 0 + 1 + 2 + 3).
            self.assertTrue(sum_sub_env_vector_indices[1] == 6)
            self.assertTrue(sum_sub_env_vector_indices[2] == 6)
            trainer.stop()

    def test_on_sub_environment_created_with_remote_envs(self):
        config = {
            "env": "CartPole-v1",
            # Make each sub-environment a ray actor.
            "remote_worker_envs": True,
            # Create 4 sub-environments (ray remote actors) per remote
            # worker.
            "num_envs_per_worker": 4,
            # Create 2 remote workers.
            "num_workers": 2,
            "callbacks": OnSubEnvironmentCreatedCallback,
        }

        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            trainer = dqn.DQNTrainer(config=config)
            # Fake the counter on the local worker (doesn't have an env) and
            # set it to -1 so the below `foreach_worker()` won't fail.
            trainer.workers.local_worker().sum_sub_env_vector_indices = -1

            # Get sub-env vector index sums from the 2 remote workers:
            sum_sub_env_vector_indices = trainer.workers.foreach_worker(
                lambda w: w.sum_sub_env_vector_indices
            )
            # Local worker has no environments -> Expect the -1 special
            # value returned by the above lambda.
            self.assertTrue(sum_sub_env_vector_indices[0] == -1)
            # Both remote workers (index 1 and 2) have a vector index counter
            # of 6 (sum of vector indices: 0 + 1 + 2 + 3).
            self.assertTrue(sum_sub_env_vector_indices[1] == 6)
            self.assertTrue(sum_sub_env_vector_indices[2] == 6)
            trainer.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
