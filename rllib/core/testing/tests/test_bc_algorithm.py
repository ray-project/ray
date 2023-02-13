import unittest

import ray
from ray.rllib.core.testing.torch.bc_module import DiscreteBCTorchModule
from ray.rllib.core.testing.tf.bc_module import DiscreteBCTFModule

from ray.rllib.core.testing.bc_algorithm import BCConfigTest
from ray.rllib.utils.test_utils import framework_iterator


class TestRLTrainer(unittest.TestCase):
    @classmethod
    def setUp(cls) -> None:
        ray.init()

    @classmethod
    def tearDown(cls) -> None:
        ray.shutdown()

    def test_bc_algorithm(self):

        config = (
            BCConfigTest()
            .rl_module(_enable_rl_module_api=True)
            .training(_enable_rl_trainer_api=True, model={"fcnet_hiddens": [32, 32]})
        )

        # TODO (Kourosh): Add tf2 support
        for fw in framework_iterator(config, frameworks=("torch")):
            algo = config.build(env="CartPole-v1")
            policy = algo.get_policy()
            rl_module = policy.model

            if fw == "torch":
                assert isinstance(rl_module, DiscreteBCTorchModule)
            elif fw == "tf":
                assert isinstance(rl_module, DiscreteBCTFModule)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
