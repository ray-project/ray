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

    def test_appo_model(self):
        """Tests that the new AppoCatalog models are the same as the old ModelV2s."""
        # Base config.
        config = (
            appo.APPOConfig()
            .environment("ALE/Pong-v5")
            .framework("tf2", eager_tracing=True)
            .rollouts(num_rollout_workers=1, rollout_fragment_length=50)
            .resources(
                num_learner_workers=2,
                num_cpus_per_learner_worker=1,
                num_gpus_per_learner_worker=0,
                num_gpus=0,
            )
            .exploration(exploration_config={})
            .training(
                _enable_learner_api=True,
                train_batch_size=1600,
                minibatch_size="auto",
                vtrace=True,
                num_sgd_iter=2,
                model={
                    "dim": 42,
                    "conv_filters": [[16, 4, 2], [32, 4, 2], [256, 11, 1, "valid"]],
                    #conv_activation": "relu",
                    "conv_add_final_dense": False,
                    "conv_flattened_dim": 256,
                    "use_cnn_heads": True,
                })
                .rl_module(_enable_rl_module_api=True)
        )
        algo_new = config.build()
        algo_new.train()
        model_new = algo_new.get_policy().model

        # Old stack config.
        (
            config
            .training(
                _enable_learner_api=False,
                model={"conv_filters": None},
            )
            .rl_module(_enable_rl_module_api=False)
            .exploration(exploration_config={"type": "StochasticSampling"})
        )
        algo_old = config.build()
        algo_old.train()
        model_old = algo_old.get_policy().model.base_model

        # Set the weights of both models to the exact same values.
        model_new.encoder._set_to_dummy_weights(value_sequence=(0.1,))
        model_new.pi._set_to_dummy_weights(value_sequence=(0.1,))
        model_new.vf._set_to_dummy_weights(value_sequence=(0.1,))
        for v in model_old.trainable_variables:
            v.assign(tf.fill(v.shape, 0.1))
        # Check actual values/shapes of all trainable variables.
        for i in range(len(model_new.trainable_variables)):
            check(model_old.trainable_variables[i], model_new.trainable_variables[i])
        # Try sending a test batch through the model.
        obs = np.random.random((10, 42, 42, 4))
        model_new.forward_inference({"obs": obs})
        model_old({"observations": obs})

        algo_new.stop()
        algo_old.stop()

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
        for _ in framework_iterator(config, "tf2", with_eager_tracing=True):
            algo = config.build()
            # Call train while results aren't returned because this is
            # a asynchronous trainer and results are returned asynchronously.
            while 1:
                results = algo.train()
                if results and "info" in results and LEARNER_INFO in results["info"]:
                    break
            curr_kl_coeff = results["info"][LEARNER_INFO][DEFAULT_POLICY_ID][
                LEARNER_STATS_KEY
            ][LEARNER_RESULTS_CURR_KL_COEFF_KEY]
            self.assertNotEqual(curr_kl_coeff, initial_kl_coeff)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
