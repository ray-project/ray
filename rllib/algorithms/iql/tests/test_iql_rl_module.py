import unittest
import numpy as np
from gymnasium.spaces import Box

import ray
from ray.rllib.algorithms.iql.torch.default_iql_torch_rl_module import DefaultIQLTorchRLModule
from ray.rllib.algorithms.iql.iql_learner import QF_TARGET_PREDS
from ray.rllib.core.columns import Columns
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

torch, nn = try_import_torch()


class TestIQLRLModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_iql_torch_rl_module_forward_train_twin_q(self):
        """Verify DefaultIQLTorchRLModule forward_train behavior with twin_q=True and twin_q=False."""
        obs_space = Box(low=-1.0, high=1.0, shape=(4,), dtype=np.float32)
        action_space = Box(low=-1.0, high=1.0, shape=(2,), dtype=np.float32)

        for twin_q in [True, False]:
            model_config = {"twin_q": twin_q}
            module = DefaultIQLTorchRLModule(
                observation_space=obs_space,
                action_space=action_space,
                model_config=model_config,
            )
            module.make_target_networks()

            # In twin_q mode, set distinct weights for target_qf_twin and qf_twin to ensure target_qf_twin is evaluated
            if twin_q:
                with torch.no_grad():
                    for p in module.target_qf_twin.parameters():
                        p.fill_(100.0)
                    for p in module.qf_twin.parameters():
                        p.fill_(-100.0)

            batch = convert_to_torch_tensor({
                Columns.OBS: torch.randn(2, 4),
                Columns.ACTIONS: torch.randn(2, 2),
                Columns.NEXT_OBS: torch.randn(2, 4),
            })

            outputs = module._forward_train(batch)
            self.assertIn(QF_TARGET_PREDS, outputs)
            self.assertEqual(outputs[QF_TARGET_PREDS].shape, (2,))


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
