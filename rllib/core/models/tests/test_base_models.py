import unittest
from dataclasses import dataclass

import gymnasium as gym

from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.core.models.specs.checker import SpecCheckingError
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.torch.torch_compile_config import TorchCompileConfig
from ray.rllib.utils.torch_utils import _dynamo_is_available

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()

"""
TODO(Artur): There are a couple of tests for torch.compile that are outstanding:
- Loading the states of a compile RLModule to a non-compile RLModule and vica-versa
- Removing a Compiled and non-compiled module to make sure there is no leak
- ...
"""


class TestModelBase(unittest.TestCase):
    def test_model_input_spec_checking(self):
        """Tests if model input spec checking works correctly.

        This test is centered around the `always_check_shapes` flag of the
        ModelConfig class. If this flag is set to True, the model will always
        check if the inputs conform to the specs. If this flag is set to False,
        the model will only check the input if we encounter an error in side
        the forward call.
        """

        for fw in ["torch", "tf2"]:

            class CatModel:
                """Simple model that concatenates parts of its input."""

                def __init__(self, config):
                    super().__init__(config)

                def get_output_specs(self):
                    return SpecDict(
                        {
                            "out_1": TensorSpec("b, h", h=1, framework=fw),
                            # out_2 is simply 2x stacked in_1
                            "out_2": TensorSpec("b, h", h=4, framework=fw),
                        }
                    )

                def get_input_specs(self):
                    return SpecDict(
                        {
                            "in_1": TensorSpec("b, h", h=1, framework=fw),
                            "in_2": TensorSpec("b, h", h=2, framework=fw),
                        }
                    )

            if fw == "tf2":

                class TestModel(CatModel, TfModel):
                    def _forward(self, input_dict):
                        out_2 = tf.concat(
                            [input_dict["in_2"], input_dict["in_2"]], axis=1
                        )
                        return {"out_1": input_dict["in_1"], "out_2": out_2}

            else:

                class TestModel(CatModel, TorchModel):
                    def _forward(self, input_dict):
                        out_2 = torch.cat(
                            [input_dict["in_2"], input_dict["in_2"]], dim=1
                        )
                        return {"out_1": input_dict["in_1"], "out_2": out_2}

            @dataclass
            class CatModelConfig(ModelConfig):
                def build(self, framework: str):
                    # Since we define the correct model above anyway, we don't need
                    # to distinguish between frameworks here.
                    return TestModel(self)

            # 1) Check if model behaves correctly with always_check_shapes=True first
            # We expect model to raise an error if the input shapes are not correct.
            # This is the behaviour we use for debugging with model specs.

            config = CatModelConfig(always_check_shapes=True)

            model = config.build(framework="spam")

            # We want to raise an input spec validation error here since the input
            # consists of lists and not torch Tensors
            with self.assertRaisesRegex(
                SpecCheckingError, "input spec validation failed"
            ):
                model({"in_1": [1], "in_2": [1, 2]})

            # We don't want to raise an input spec validation error here since the
            # input consists of valid tensors
            if fw == "torch":
                model({"in_1": torch.Tensor([[1]]), "in_2": torch.Tensor([[1, 2]])})
            else:
                model({"in_1": tf.constant([[1]]), "in_2": tf.constant([[1, 2]])})

            # 2) Check if model behaves correctly with always_check_shapes=False.
            # We don't expect model to raise an error if the input shapes are not
            # correct.
            # This is the more performant default behaviour

            config = CatModelConfig(always_check_shapes=False)

            model = config.build(framework="spam")

            # This should not raise an error since the specs are correct and the
            # model does not raise an error either.
            if fw == "torch":
                model({"in_1": torch.Tensor([[1]]), "in_2": torch.Tensor([[1, 2]])})
            else:
                model({"in_1": tf.constant([[1]]), "in_2": tf.constant([[1, 2]])})

            # This should not raise an error since specs would be violated, but they
            # are not checked and the model does not raise an error.
            if fw == "torch":
                model(
                    {"in_1": torch.Tensor([[1]]), "in_2": torch.Tensor([[1, 2, 3, 4]])}
                )
            else:
                model({"in_1": tf.constant([[1]]), "in_2": tf.constant([[1, 2, 3, 4]])})

            # We want to raise an input spec validation error here since the model
            # raises an exception that stems from inputs that could have been caught
            # with input spec checking.
            with self.assertRaisesRegex(
                SpecCheckingError, "input spec validation failed"
            ):
                model({"in_1": [1], "in_2": [1, 2]})

    def test_model_output_spec_checking(self):
        """Tests if model output spec checking works correctly.

        This test is centered around the `always_check_shapes` flag of the
        ModelConfig class. If this flag is set to True, the model will always
        check if the outputs conform to the specs. If this flag is set to False,
        the model will never check the outputs.
        """

        for fw in ["torch", "tf2"]:

            class BadModel:
                """Simple model that produces bad outputs."""

                def get_output_specs(self):
                    return SpecDict(
                        {
                            "out": TensorSpec("b, h", h=1),
                        }
                    )

                def get_input_specs(self):
                    return SpecDict(
                        {
                            "in": TensorSpec("b, h", h=1),
                        }
                    )

            if fw == "tf2":

                class TestModel(BadModel, TfModel):
                    def _forward(self, input_dict):
                        return {"out": torch.Tensor([[1, 2]])}

            else:

                class TestModel(BadModel, TfModel):
                    def _forward(self, input_dict):
                        return {"out": tf.constant([[1, 2]])}

            @dataclass
            class CatModelConfig(ModelConfig):
                def build(self, framework: str):
                    # Since we define the correct model above anyway, we don't need
                    # to distinguish between frameworks here.
                    return TestModel(self)

            # 1) Check if model behaves correctly with always_check_shapes=True first.
            # We expect model to raise an error if the output shapes are not correct.
            # This is the behaviour we use for debugging with model specs.

            config = CatModelConfig(always_check_shapes=True)

            model = config.build(framework="spam")

            # We want to raise an output spec validation error here since the output
            # has the wrong shape
            with self.assertRaisesRegex(
                SpecCheckingError, "output spec validation failed"
            ):
                model({"in": torch.Tensor([[1]])})

            # 2) Check if model behaves correctly with always_check_shapes=False.
            # We don't expect model to raise an error.
            # This is the more performant default behaviour

            config = CatModelConfig(always_check_shapes=False)

            model = config.build(framework="spam")

            model({"in_1": [[1]]})

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

            def get_output_specs(self):
                return SpecDict(
                    {
                        "out": TensorSpec("b, h", h=1, framework="torch"),
                    }
                )

            def get_input_specs(self):
                return SpecDict(
                    {
                        "in": TensorSpec("b, h", h=1, framework="torch"),
                    }
                )

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
        self.assertEquals(len(break_reasons), 1)

    @unittest.skipIf(not _dynamo_is_available(), "torch._dynamo not available")
    def test_torch_compile_forwards(self):
        """Test if logic around TorchCompileConfig works as intended."""

        spec = SingleAgentRLModuleSpec(
            module_class=PPOTorchRLModule,
            observation_space=gym.spaces.Box(low=0, high=1, shape=(32,)),
            action_space=gym.spaces.Box(low=0, high=1, shape=(1,)),
            model_config_dict={},
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
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
