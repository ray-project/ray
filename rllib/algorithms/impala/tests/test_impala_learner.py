import unittest

import numpy as np
import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.impala import ImpalaConfig
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.framework import try_import_torch, try_import_tf
from ray.rllib.utils.test_utils import framework_iterator
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()
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
    SampleBatch.VALUES_BOOTSTRAPPED: np.array(
        list(reversed(range(frag_length))), dtype=np.float32
    ),
    SampleBatch.ACTION_LOGP: np.log(
        np.random.uniform(low=0, high=1, size=(frag_length,))
    ).astype(np.float32),
    SampleBatch.ACTION_DIST_INPUTS: np.random.normal(
        0, 1, size=(frag_length, 2)
    ).astype(np.float32),
}


class TestImpalaLearner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_impala_loss(self):
        """Test that impala_policy_rlm loss matches the impala learner loss.

        Correctness of V-Trance is tested in test_vtrace_v2.py.
        """
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
        # TODO (Artur): See if setting Impala's default to {} pose any issues.
        #  Deprecate the current default and set it to {}.
        config.exploration_config = {}

        for fw in framework_iterator(config, frameworks=["torch", "tf2"]):
            algo = config.build()
            policy = algo.get_policy()

            if fw == "torch":
                train_batch = convert_to_torch_tensor(SampleBatch(FAKE_BATCH))
            else:
                train_batch = SampleBatch(
                    tree.map_structure(lambda x: tf.convert_to_tensor(x), FAKE_BATCH)
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
                    catalog_class=algo_config.rl_module_spec.catalog_class,
                )
            )
            learner_group_config.num_learner_workers = 0
            learner_group = learner_group_config.build()
            learner_group.set_weights(algo.get_weights())
            learner_group.update(train_batch.as_multi_agent())

            algo.stop()


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
