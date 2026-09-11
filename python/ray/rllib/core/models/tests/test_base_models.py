import unittest
from dataclasses import dataclass

import gymnasium as gym

from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import _dynamo_is_available

torch, nn = try_import_torch()

"""
TODO(Artur): There are a couple of tests for torch.compile that are outstanding:
- Loading the states of a compile RLModule to a non-compile RLModule and vica-versa
- Removing a Compiled and non-compiled module to make sure there is no leak
- ...
"""


class TestModelBase(unittest.TestCase):

    # Todo (rllib-team): Fix for torch 2.0+
    @unittest.skip("Failing with torch >= 2.0")
    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_no_breaks(self):
        """Tests if torch.compile() does not encounter any breaks.

        torch.compile() should not encounter any breaks when model is on its
        code path by default. This test checks if this is the case.
        """

        class SomeTorchModel(TorchModel):
            """Simple model that produces bad outputs."""

            def __init__(self, config):
                super().__init__(config)
                self._model = torch.nn.Linear(1, 1)

            def _forward(self, input_dict):
                return {"out": self._model(input_dict["in"])}

        @dataclass
        class SomeTorchModelConfig(ModelConfig):
            def build(self, framework: str):
                return SomeTorchModel(self)

        config = SomeTorchModelConfig()

        model = config.build(framework="spam")

        # This could be the forward method of an RL Module that we torch compile
        def compile_me(input_dict):
            return model(input_dict)

        import torch._dynamo as dynamo

        # The followingcall to dynamo.explain() breaks depending on the torch version.
        # It works for torch==2.0.0.
        # TODO(Artur): Fit this to to the correct torch version once it is enabled on
        #  CI.
        # This is a helper method of dynamo to analyze where breaks occur.
        (
            explanation,
            out_guards,
            graphs,
            ops_per_graph,
            break_reasons,
            explanation_verbose,
        ) = dynamo.explain(compile_me, {"in": torch.Tensor([[1]])})

        print(explanation_verbose)

        # There should be only one break reason - `return_value` - since inputs and
        # outputs are not checked
        self.assertEqual(len(break_reasons), 1)

    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_forwards(self):
        """Test if logic around TorchCompileConfig works as intended."""

        spec = RLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=gym.spaces.Box(low=0, high=1, shape=(32,)),
            action_space=gym.spaces.Box(low=0, high=1, shape=(1,)),
            catalog_class=PPOCatalog,
        )
        torch_module = spec.build()

        compile_config = TorchCompileConfig()

        torch_module.compile(compile_config)

        # We should still be able to call the forward methods
        torch_module._forward_train({"obs": torch.randn(1, 32)})
        torch_module._forward_inference({"obs": torch.randn(1, 32)})
        torch_module._forward_exploration({"obs": torch.randn(1, 32)})


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
