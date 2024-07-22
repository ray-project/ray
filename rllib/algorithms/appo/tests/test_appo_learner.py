import unittest
import numpy as np

import tree  # pip install dm_tree

import ray
import ray.rllib.algorithms.appo as appo
from ray.rllib.algorithms.appo.tf.appo_tf_learner import (
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
)
from ray.rllib.core import DEFAULT_MODULE_ID
from ray.rllib.core.columns import Columns
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.metrics import LEARNER_RESULTS
from ray.rllib.utils.torch_utils import convert_to_torch_tensor


frag_length = 50

FAKE_BATCH = {
    Columns.OBS: np.random.uniform(low=0, high=1, size=(frag_length, 4)).astype(
        np.float32
    ),
    Columns.ACTIONS: np.random.choice(2, frag_length).astype(np.float32),
    Columns.REWARDS: np.random.uniform(low=-1, high=1, size=(frag_length,)).astype(
        np.float32
    ),
    Columns.TERMINATEDS: np.array(
        [False for _ in range(frag_length - 1)] + [True]
    ).astype(np.float32),
    Columns.VF_PREDS: np.array(list(reversed(range(frag_length))), dtype=np.float32),
    Columns.ACTION_LOGP: np.log(
        np.random.uniform(low=0, high=1, size=(frag_length,))
    ).astype(np.float32),
}


class TestAPPOTfLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_appo_loss(self):
        """Test that appo_policy_rlm loss matches the appo learner loss."""
        config = (
            appo.APPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=frag_length,
            )
            .resources(num_gpus=0)
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )
        # We have to set exploration_config here manually because setting it through
        # config.env_runners() only deep-updates it
        config.exploration_config = {}

        algo = config.build()

        train_batch = SampleBatch(
            tree.map_structure(lambda x: convert_to_torch_tensor(x), FAKE_BATCH)
        )

        algo_config = config.copy(copy_frozen=False)
        algo_config.learners(num_learners=0)
        algo_config.validate()

        learner_group = algo_config.build_learner_group(env=algo.env_runner.env)
        learner_group.update_from_batch(batch=train_batch.as_multi_agent())

        algo.stop()

    def test_kl_coeff_changes(self):
        initial_kl_coeff = 0.01
        config = (
            appo.APPOConfig()
            .api_stack(
                enable_rl_module_and_learner=True,
                enable_env_runner_and_connector_v2=True,
            )
            .environment("CartPole-v1")
            # Asynchronous Algo, make sure we have some results after 1 iteration.
            .reporting(min_time_s_per_iteration=10)
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=frag_length,
                exploration_config={},
            )
            .resources(num_gpus=0)
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
                use_kl_loss=True,
                kl_coeff=initial_kl_coeff,
            )
        )
        algo = config.build()
        # Call train while results aren't returned because this is
        # a asynchronous algorithm and results are returned asynchronously.
        while True:
            results = algo.train()
            print(results)
            if results.get(LEARNER_RESULTS, {}).get(DEFAULT_MODULE_ID):
                break
        curr_kl_coeff = results[LEARNER_RESULTS][DEFAULT_MODULE_ID][
            LEARNER_RESULTS_CURR_KL_COEFF_KEY
        ]
        self.assertNotEqual(curr_kl_coeff, initial_kl_coeff)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
