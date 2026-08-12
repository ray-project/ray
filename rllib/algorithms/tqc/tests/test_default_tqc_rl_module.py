import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.sac.sac_learner import QF_PREDS
from ray.rllib.algorithms.tqc.torch.default_tqc_torch_rl_module import (
    DefaultTQCTorchRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.framework import try_import_torch

torch, _ = try_import_torch()


class TestDefaultTQCTorchRLModule(unittest.TestCase):
    """Regression tests for TQC's actor-loss Q-value pass.

    The actor loss maximizes Q-values of resampled actions (``qf_curr``). Its
    gradients must reach the policy only (through the actions), never the
    critic parameters. TQC was missing the critic-parameter freeze SAC uses
    for this pass, so the actor loss's critic gradients leaked into the critic
    optimizer's update and pushed the critics to inflate Q-values at the
    policy's actions (unbounded critic growth on e.g. Humanoid).
    """

    def _build_module(self):
        torch.manual_seed(0)
        spec = RLModuleSpec(
            module_class=DefaultTQCTorchRLModule,
            observation_space=gym.spaces.Box(-1.0, 1.0, (8,), np.float32),
            action_space=gym.spaces.Box(-1.0, 1.0, (3,), np.float32),
            model_config={"fcnet_hiddens": [32, 32]},
        )
        module = spec.build()
        # No NEXT_OBS -> the target-network branch of `_forward_train` is
        # skipped, so target networks are not needed for these tests.
        batch = {
            Columns.OBS: torch.randn(16, 8),
            Columns.ACTIONS: torch.rand(16, 3) * 2.0 - 1.0,
        }
        return module, batch

    def _critic_params(self, module):
        return list(module.qf_encoders.parameters()) + list(
            module.qf_heads.parameters()
        )

    def test_actor_q_pass_produces_no_critic_gradients(self):
        module, batch = self._build_module()
        fwd_out = module._forward_train(batch)

        # Backprop through the actor-loss Q-values only.
        fwd_out["qf_curr"].mean().backward()

        # No gradients may have reached the critics...
        for param in self._critic_params(module):
            self.assertIsNone(param.grad)
        # ...while the policy received gradients through the actions.
        pi_params = list(module.pi_encoder.parameters()) + list(module.pi.parameters())
        self.assertTrue(any(p.grad is not None for p in pi_params))
        # And the critics must be trainable again after the forward pass.
        for param in self._critic_params(module):
            self.assertTrue(param.requires_grad)

    def test_critic_pass_still_produces_critic_gradients(self):
        module, batch = self._build_module()
        fwd_out = module._forward_train(batch)

        # Backprop through the critic-loss Q-value predictions.
        fwd_out[QF_PREDS].mean().backward()

        self.assertTrue(any(p.grad is not None for p in self._critic_params(module)))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
