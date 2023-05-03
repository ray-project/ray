import unittest
import numpy as np

import ray
import ray.rllib.algorithms.appo as appo
from ray.rllib.algorithms.appo.tf.appo_tf_learner import (
    LEARNER_RESULTS_CURR_KL_COEFF_KEY,
)
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, LEARNER_STATS_KEY
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check, framework_iterator


tf1, tf, _ = try_import_tf()

tf1.enable_eager_execution()

frag_length = 50

FAKE_BATCH = {
    SampleBatch.OBS: np.random.uniform(low=0, high=1, size=(frag_length, 4)).astype(
        np.float32
    ),
    SampleBatch.ACTIONS: np.random.choice(2, frag_length).astype(np.float32),
    SampleBatch.REWARDS: np.random.uniform(low=-1, high=1, size=(frag_length,)).astype(
        np.float32
    ),
    SampleBatch.TERMINATEDS: np.array(
        [False for _ in range(frag_length - 1)] + [True]
    ).astype(np.float32),
    SampleBatch.VF_PREDS: np.array(
        list(reversed(range(frag_length))), dtype=np.float32
    ),
    SampleBatch.ACTION_LOGP: np.log(
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
            .environment("CartPole-v1")
            .rollouts(
                num_rollout_workers=0,
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
            .rl_module(
                _enable_rl_module_api=True,
            )
        )
        # We have to set exploration_config here manually because setting it through
        # config.exploration() only deepupdates it
        config.exploration_config = {}

        for fw in framework_iterator(config, ("tf2")):
            trainer = config.build()
            policy = trainer.get_policy()

            if fw == "tf2":
                train_batch = SampleBatch(
                    tf.nest.map_structure(lambda x: tf.convert_to_tensor(x), FAKE_BATCH)
                )
            else:
                train_batch = SampleBatch(FAKE_BATCH)
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
                    catalog_class=algo_config.rl_module_spec.catalog_class,
                )
            )
            learner_group_config.num_learner_workers = 0
            learner_group = learner_group_config.build()
            learner_group.set_weights(trainer.get_weights())
            results = learner_group.update(train_batch.as_multi_agent())
            learner_group_loss = results[ALL_MODULES]["total_loss"]

            check(learner_group_loss, policy_loss)

    def test_kl_coeff_changes(self):
        initial_kl_coeff = 0.01
        config = (
            appo.APPOConfig()
            .environment("CartPole-v1")
            .framework(eager_tracing=True)
            .rollouts(
                num_rollout_workers=0,
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
                _enable_learner_api=True,
                kl_coeff=initial_kl_coeff,
            )
            .rl_module(
                _enable_rl_module_api=True,
            )
            .exploration(exploration_config={})
        )
        for _ in framework_iterator(config, frameworks="tf2"):
            algo = config.build()
            # Call train while results aren't returned because this is
            # a asynchronous trainer and results are returned asynchronously.
            while True:
                results = algo.train()
                if results.get("info", {}).get(LEARNER_INFO, {}).get(DEFAULT_POLICY_ID):
                    break
            curr_kl_coeff = results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][
                LEARNER_STATS_KEY
            ][LEARNER_RESULTS_CURR_KL_COEFF_KEY]
            self.assertNotEqual(curr_kl_coeff, initial_kl_coeff)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
