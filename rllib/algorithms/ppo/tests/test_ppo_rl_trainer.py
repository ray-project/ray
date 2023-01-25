import ray
import unittest
import numpy as np
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
import torch
import tree  # pip install dm-tree

import ray.rllib.algorithms.ppo as ppo
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.test_utils import check

from ray.rllib.evaluation.postprocessing import (
    compute_gae_for_sample_batch,
)

# Fake CartPole episode of n time steps.
FAKE_BATCH = {
    SampleBatch.OBS: np.array(
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
    SampleBatch.ACTION_DIST_INPUTS: {
        "logits": np.array([[-2.0, 0.5], [-3.0, -0.3], [-0.1, 2.5]], dtype=np.float32)
    },
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
            .framework("torch")
            .rollouts(
                num_rollout_workers=0,
            )
            .training(
                gamma=0.99,
                model=dict(
                    fcnet_hiddens=[10],
                    fcnet_activation="linear",
                    vf_share_layers=False,
                ),
            )
            .rl_module(
                _enable_rl_module_api=True,
            )
        )

        trainer = config.build()
        policy = trainer.get_policy()

        train_batch = SampleBatch(FAKE_BATCH)
        train_batch = compute_gae_for_sample_batch(policy, train_batch)

        # convert to torch tensors with tree.map_structure
        train_batch = tree.map_structure(
            lambda x: torch.as_tensor(x).float(), train_batch
        )

        policy_loss = policy.loss(policy.model, policy.dist_class, train_batch)

        config.training(_enable_rl_trainer_api=True)
        config.validate()
        config.freeze()
        trainer_runner_config = config.get_trainer_runner_config(
            SingleAgentRLModuleSpec(
                observation_space=policy.observation_space,
                action_space=policy.action_space,
            )
        )
        trainer_runner = trainer_runner_config.build()

        # load the policy weights into the trainer runner
        state_dict = {"module_state": {"default_policy": policy.get_weights()}}
        state_dict = convert_to_torch_tensor(state_dict)
        trainer_runner.set_state(state_dict)
        results = trainer_runner.update(train_batch.as_multi_agent())

        trainer_runner_loss = results[0]["loss"]["total_loss"]

        check(trainer_runner_loss, policy_loss)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
