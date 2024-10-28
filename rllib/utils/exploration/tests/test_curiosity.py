import numpy as np
import sys
import unittest

import ray
from ray.rllib.algorithms.callbacks import DefaultCallbacks
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MAX,
)


class MyCallBack(DefaultCallbacks):
    def __init__(self):
        super().__init__()
        self.deltas = []

    def on_postprocess_trajectory(
        self,
        *,
        worker,
        episode,
        agent_id,
        policy_id,
        policies,
        postprocessed_batch,
        original_batches,
        **kwargs,
    ):
        pos = np.argmax(postprocessed_batch["obs"], -1)
        x, y = pos % 8, pos // 8
        self.deltas.extend((x**2 + y**2) ** 0.5)

    def on_sample_end(self, *, worker, samples, **kwargs):
        print("mean. distance from origin={}".format(np.mean(self.deltas)))
        self.deltas = []


class TestCuriosity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=3)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_curiosity_on_frozen_lake(self):

        config = (
            ppo.PPOConfig()
            .api_stack(
                enable_env_runner_and_connector_v2=False,
                enable_rl_module_and_learner=False,
            )
            # A very large frozen-lake that's hard for a random policy to solve
            # due to 0.0 feedback.
            .environment(
                "FrozenLake-v1",
                env_config={
                    "desc": [
                        "SFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFF",
                        "FFFFFFFG",
                    ],
                    "is_slippery": False,
                    "max_episode_steps": 16,
                },
            )
            # Print out observations to see how far we already get inside the Env.
            .callbacks(MyCallBack)
            # Limit horizon to make it really hard for non-curious agent to reach
            # the goal state.
            .env_runners(
                num_env_runners=0,
                exploration_config={
                    "type": "Curiosity",
                    "eta": 0.2,
                    "lr": 0.001,
                    "feature_dim": 128,
                    "feature_net_config": {
                        "fcnet_hiddens": [],
                        "fcnet_activation": "relu",
                    },
                    "sub_exploration": {
                        "type": "StochasticSampling",
                    },
                },
            )
            .training(lr=0.001)
        )

        num_iterations = 10
        # W/ Curiosity. Expect to learn something.
        algo = config.build()
        learnt = False
        for i in range(num_iterations):
            result = algo.train()
            print(result)
            if result[ENV_RUNNER_RESULTS][EPISODE_RETURN_MAX] > 0.0:
                print("Reached goal after {} iters!".format(i))
                learnt = True
                break
        algo.stop()
        self.assertTrue(learnt)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
