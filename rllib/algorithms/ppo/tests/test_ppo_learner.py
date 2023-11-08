import ray
import unittest
import numpy as np
import torch
import tempfile
import tensorflow as tf
import tree  # pip install dm-tree

import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

from ray.rllib.algorithms.ppo.ppo_learner import LEARNER_RESULTS_CURR_KL_COEFF_KEY
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import SampleBatch
from ray.tune.registry import register_env
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO
from ray.rllib.utils.test_utils import check, framework_iterator

from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
)

# Fake CartPole episode of n time steps.
FAKE_BATCH = {
    SampleBatch.OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.NEXT_OBS: np.array(
        [[0.1, 0.2, 0.3, 0.4], [0.5, 0.6, 0.7, 0.8], [0.9, 1.0, 1.1, 1.2]],
        dtype=np.float32,
    ),
    SampleBatch.ACTIONS: np.array([0, 1, 1]),
    SampleBatch.PREV_ACTIONS: np.array([0, 1, 1]),
    SampleBatch.REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.PREV_REWARDS: np.array([1.0, -1.0, 0.5], dtype=np.float32),
    SampleBatch.TERMINATEDS: np.array([False, False, True]),
    SampleBatch.TRUNCATEDS: np.array([False, False, False]),
    SampleBatch.VF_PREDS: np.array([0.5, 0.6, 0.7], dtype=np.float32),
    SampleBatch.ACTION_DIST_INPUTS: np.array(
        [[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32
    ),
    SampleBatch.ACTION_LOGP: np.array([-0.5, -0.1, -0.2], dtype=np.float32),
    SampleBatch.EPS_ID: np.array([0, 0, 0]),
    SampleBatch.AGENT_INDEX: np.array([0, 0, 0]),
}


class TestPPO(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_loss(self):

        config = (
            ppo.PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
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

            learner_group_config = algo_config.get_learner_group_config(
                SingleAgentRLModuleSpec(
                    module_class=algo_config.rl_module_spec.module_class,
                    observation_space=policy.observation_space,
                    action_space=policy.action_space,
                    model_config_dict=policy.config["model"],
                    catalog_class=PPOCatalog,
                )
            )
            learner_group = learner_group_config.build()

            # Load the algo weights onto the learner_group.
            learner_group.set_weights(algo.get_weights())
            learner_group.update(train_batch.as_multi_agent())

            algo.stop()

    def test_save_load_state(self):
        """Tests saving and loading the state of the PPO Learner Group."""
        config = (
            ppo.PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
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
        algo = config.build()
        policy = algo.get_policy()

        for _ in framework_iterator(config, ("tf2", "torch")):
            algo_config = config.copy(copy_frozen=False)
            algo_config.validate()
            algo_config.freeze()
            learner_group_config = algo_config.get_learner_group_config(
                SingleAgentRLModuleSpec(
                    module_class=algo_config.rl_module_spec.module_class,
                    observation_space=policy.observation_space,
                    action_space=policy.action_space,
                    model_config_dict=policy.config["model"],
                    catalog_class=PPOCatalog,
                )
            )
            learner_group1 = learner_group_config.build()
            learner_group2 = learner_group_config.build()
            with tempfile.TemporaryDirectory() as tmpdir:
                learner_group1.save_state(tmpdir)
                learner_group2.load_state(tmpdir)
                check(learner_group1.get_state(), learner_group2.get_state())

    def test_kl_coeff_changes(self):
        # Simple environment with 4 independent cartpole entities
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
        )

        initial_kl_coeff = 0.01
        config = (
            ppo.PPOConfig()
            .experimental(_enable_new_api_stack=True)
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
                rollout_fragment_length=50,
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
            .exploration(exploration_config={})
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
