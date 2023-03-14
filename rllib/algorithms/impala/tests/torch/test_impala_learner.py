import unittest
import numpy as np

import ray
from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.metrics import ALL_MODULES
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.test_utils import check, framework_iterator

torch, nn = try_import_torch()

frag_length = 32

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
    SampleBatch.ACTION_DIST_INPUTS: np.random.randn(frag_length).astype(np.float32),
}


class TestImpalaTorchLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init(local_mode=True)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_impala_loss(self):
        """Test that impala_policy_rlm loss matches the impala learner loss."""
        config = (
            ImpalaConfig()
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
            )
            .rl_module(
                _enable_rl_module_api=True,
            )
        )

        for _ in framework_iterator(config, frameworks=["torch"]):
            trainer = config.build()
            policy = trainer.get_policy()

            train_batch = SampleBatch(FAKE_BATCH)
            policy_loss = policy.loss(policy.model, policy.dist_class, train_batch)
            algo_config = config.copy(copy_frozen=False)
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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
