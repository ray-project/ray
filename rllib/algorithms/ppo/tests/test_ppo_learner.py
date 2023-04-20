import ray
import unittest
import numpy as np
import torch
import tensorflow as tf
import tree  # pip install dm-tree

import ray.rllib.algorithms.ppo as ppo
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog

from ray.rllib.algorithms.appo.tf.appo_tf_learner import (
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.examples.env.multi_agent import MultiAgentCartPole
from ray.rllib.policy.sample_batch import SampleBatch
from ray.tune.registry import register_env
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.test_utils import check, framework_iterator
from ray.rllib.utils.metrics import ALL_MODULES

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
            .rl_module(
                _enable_rl_module_api=True,
            )
        )

        for fw in framework_iterator(config, ("tf2", "torch"), with_eager_tracing=True):
            trainer = config.build()
            policy = trainer.get_policy()

            train_batch = SampleBatch(FAKE_BATCH)
            train_batch = compute_gae_for_sample_batch(policy, train_batch)

            # convert to proper tensors with tree.map_structure
            if fw == "torch":
                train_batch = tree.map_structure(
                    lambda x: torch.as_tensor(x).float(), train_batch
                )
            else:
                # tf
                train_batch = tree.map_structure(
                    lambda x: tf.convert_to_tensor(x), train_batch
                )

            policy_loss = policy.loss(policy.model, policy.dist_class, train_batch)

            algo_config = config.copy(copy_frozen=False)
            algo_config.training(_enable_learner_api=True)
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

            # load the trainer weights onto the learner_group
            learner_group.set_weights(trainer.get_weights())
            results = learner_group.update(train_batch.as_multi_agent())

            learner_group_loss = results[ALL_MODULES]["total_loss"]

            check(learner_group_loss, policy_loss)

    def test_kl_coeff_changes(self):
        # Simple environment with 4 independent cartpole entities
        register_env(
            "multi_agent_cartpole", lambda _: MultiAgentCartPole({"num_agents": 2})
        )

        initial_kl_coeff = 0.01
        config = (
            ppo.PPOConfig()
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
                rollout_fragment_length=50,
            )
            .resources(num_gpus=0)
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10, 10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
                _enable_learner_api=True,
                kl_coeff=initial_kl_coeff,
            )
            .rl_module(
                _enable_rl_module_api=True,
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

        for _ in framework_iterator(config, "tf2", with_eager_tracing=True):
            algo = config.build()
            # Call train while results aren't returned because this is
            # a asynchronous trainer and results are returned asynchronously.
            curr_kl_coeff_1 = None
            curr_kl_coeff_2 = None
            while not curr_kl_coeff_1 or not curr_kl_coeff_2:
                results = algo.train()

                # Attempt to get the current KL coefficient from the learner.
                # Iterate until we have found both coefficients at least once.
                if results and "info" in results and LEARNER_INFO in results["info"]:
                    if "p0" in results["info"][LEARNER_INFO]:
                        curr_kl_coeff_1 = results["info"][LEARNER_INFO]["p0"][
                            LEARNER_STATS_KEY
                        ][LEARNER_RESULTS_CURR_KL_COEFF_KEY]
                    if "p1" in results["info"][LEARNER_INFO]:
                        curr_kl_coeff_2 = results["info"][LEARNER_INFO]["p1"][
                            LEARNER_STATS_KEY
                        ][LEARNER_RESULTS_CURR_KL_COEFF_KEY]

            self.assertNotEqual(curr_kl_coeff_1, initial_kl_coeff)
            self.assertNotEqual(curr_kl_coeff_2, initial_kl_coeff)
            self.assertEqual(curr_kl_coeff_1, curr_kl_coeff_2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
