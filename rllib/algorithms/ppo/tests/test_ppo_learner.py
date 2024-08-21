import unittest
import tempfile

import gymnasium as gym
import numpy as np
import tensorflow as tf
import torch
import tree  # pip install dm-tree

import ray
import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo import LEARNER_RESULTS_CURR_KL_COEFF_KEY
from ray.rllib.core.columns import Columns
from ray.rllib.evaluation.postprocessing import compute_gae_for_sample_batch
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.tune.registry import register_env


# Fake CartPole episode of n time steps.
FAKE_BATCH = {
    Columns.OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    Columns.NEXT_OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    Columns.ACTIONS: np.array([0, 1, 1]),
    Columns.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    Columns.TERMINATEDS: np.array([False, False, True]),
    Columns.TRUNCATEDS: np.array([False, False, False]),
    Columns.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    Columns.ACTION_DIST_INPUTS: np.array(
        [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
    ),
    Columns.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
    Columns.EPS_ID: np.array([0, 0, 0]),
}


class TestPPO(unittest.TestCase):
    ENV = gym.make("CartPole-v1")

    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_loss(self):
        config = (
            ppo.PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("CartPole-v1")
            .env_runners(num_env_runners=0)
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )

        for fw in framework_iterator(config, ("tf2", "torch")):
            algo = config.build()
            policy = algo.get_policy()

            train_batch = SampleBatch(FAKE_BATCH)
            train_batch = compute_gae_for_sample_batch(policy, train_batch)

            # convert to proper tensors with tree.map_structure
            if fw == "torch":
                train_batch = tree.map_structure(
                    lambda x: torch.as_tensor(x).float(), train_batch
                )
            else:
                train_batch = tree.map_structure(
                    lambda x: tf.convert_to_tensor(x), train_batch
                )

            algo_config = config.copy(copy_frozen=False)
            algo_config.validate()
            algo_config.freeze()

            learner_group = algo_config.build_learner_group(env=self.ENV)

            # Load the algo weights onto the learner_group.
            learner_group.set_weights(algo.get_weights())
            learner_group.update_from_batch(batch=train_batch.as_multi_agent())

            algo.stop()

    def test_save_to_path_and_restore_from_path(self):
        """Tests saving and loading the state of the PPO Learner Group."""
        config = (
            ppo.PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
        )

        for _ in framework_iterator(config, ("tf2", "torch")):
            algo_config = config.copy(copy_frozen=False)
            algo_config.validate()
            algo_config.freeze()
            learner_group1 = algo_config.build_learner_group(env=self.ENV)
            learner_group2 = algo_config.build_learner_group(env=self.ENV)
            with tempfile.TemporaryDirectory() as tmpdir:
                learner_group1.save_to_path(tmpdir)
                learner_group2.restore_from_path(tmpdir)
                # Remove functions from state b/c they are not comparable via `check`.
                s1 = learner_group1.get_state()
                s2 = learner_group2.get_state()
                check(s1, s2)

    def test_kl_coeff_changes(self):
        # Simple environment with 4 independent cartpole entities
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
        )

        initial_kl_coeff = 0.01
        config = (
            ppo.PPOConfig()
            .api_stack(enable_rl_module_and_learner=True)
            .environment("CartPole-v1")
            .env_runners(
                num_env_runners=0,
                rollout_fragment_length=50,
                exploration_config={},
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
                kl_coeff=initial_kl_coeff,
            )
            .environment("multi_agent_cartpole")
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: (
                    "p{}".format(agent_id % 2)
                ),
            )
        )

        for _ in framework_iterator(config, ("torch", "tf2")):
            algo = config.build()
            # Call train while results aren't returned because this is
            # a asynchronous Algorithm and results are returned asynchronously.
            curr_kl_coeff_1 = None
            curr_kl_coeff_2 = None
            while not curr_kl_coeff_1 or not curr_kl_coeff_2:
                results = algo.train()

                # Attempt to get the current KL coefficient from the learner.
                # Iterate until we have found both coefficients at least once.
                if results and "info" in results and LEARNER_INFO in results["info"]:
                    if "p0" in results["info"][LEARNER_INFO]:
                        curr_kl_coeff_1 = results["info"][LEARNER_INFO]["p0"][
                            LEARNER_RESULTS_CURR_KL_COEFF_KEY
                        ]
                    if "p1" in results["info"][LEARNER_INFO]:
                        curr_kl_coeff_2 = results["info"][LEARNER_INFO]["p1"][
                            LEARNER_RESULTS_CURR_KL_COEFF_KEY
                        ]

            self.assertNotEqual(curr_kl_coeff_1, initial_kl_coeff)
            self.assertNotEqual(curr_kl_coeff_2, initial_kl_coeff)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
