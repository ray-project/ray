import unittest

import numpy as np
from gymnasium.spaces import Box

from ray.rllib.algorithms.iql.iql_learner import QF_TARGET_PREDS
from ray.rllib.algorithms.iql.torch.default_iql_torch_rl_module import (
    DefaultIQLTorchRLModule,
)
from ray.rllib.core.columns import Columns
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TestIQLRLModule(unittest.TestCase):
    def test_forward_train_target_preds_are_frozen(self):
        """QF_TARGET_PREDS must derive only from frozen target nets (#64931)."""
        obs_space = Box(-1.0, 1.0, (4,), np.float32)
        action_space = Box(-1.0, 1.0, (2,), np.float32)

        for twin_q in [True, False]:
            module = DefaultIQLTorchRLModule(
                observation_space=obs_space,
                action_space=action_space,
                model_config={"twin_q": twin_q},
            )
            module.make_target_networks()

            batch = {
                Columns.OBS: torch.randn(2, 4),
                Columns.ACTIONS: torch.randn(2, 2),
                Columns.NEXT_OBS: torch.randn(2, 4),
            }
            outputs = module._forward_train(batch)

            self.assertIn(QF_TARGET_PREDS, outputs)
            self.assertEqual(outputs[QF_TARGET_PREDS].shape, (2,))
            # Bug #64931 used the online `qf_twin` head here, which makes this
            # tensor require grad and leaks gradients into the twin-Q update.
            self.assertFalse(outputs[QF_TARGET_PREDS].requires_grad)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
