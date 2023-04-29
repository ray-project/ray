import unittest
from dataclasses import dataclass

from ray.rllib.core.models.base import ModelConfig, Model
from ray.rllib.core.models.specs.specs_base import TensorSpec
from ray.rllib.core.models.specs.specs_dict import SpecDict
from ray.rllib.core.models.tf.base import TfModel
from ray.rllib.core.models.torch.base import TorchModel
from ray.rllib.utils.framework import try_import_tf, try_import_torch

_, tf, _ = try_import_tf()
torch, nn = try_import_torch()


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
                            [input_dict["in_2"], input_dict["in_2"]], axis=1)
                        return {"out_1": input_dict["in_1"], "out_2": out_2}
            else:
                class TestModel(CatModel, TorchModel):
                    def _forward(self, input_dict):
                        out_2 = torch.cat(
                            [input_dict["in_2"], input_dict["in_2"]], dim=1)
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
            with self.assertRaisesRegex(ValueError, "input spec validation failed"):
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

            # We want to raise an input spec validation error here since the input
            # consists of lists and not torch Tensors
            if fw == "torch":
                model({"in_1": torch.Tensor([[1]]), "in_2": torch.Tensor([[1, 2]])})
            else:
                model({"in_1": tf.constant([[1]]), "in_2": tf.constant([[1, 2]])})
            # We want to raise an input spec validation error here since the model
            # itself does not raise an exception, so there is no need to double check
            # the inputs.
            if fw == "torch":
                model({"in_1": torch.Tensor([[1]]), "in_2": torch.Tensor([[1, 2, 3, 4
                                                                           ]])})
            else:
                model({"in_1": tf.constant([[1]]), "in_2": tf.constant([[1, 2, 3, 4]])})

            # We want to raise an input spec validation error here since the model
            # raises an exception that stems from inputs that could have been caught
            # with input spec checking.
            with self.assertRaisesRegex(ValueError, "input spec validation failed"):
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
            with self.assertRaisesRegex(ValueError, "output spec validation failed"):
                model({"in": torch.Tensor([[1]])})

            # 2) Check if model behaves correctly with always_check_shapes=False.
            # We don't expect model to raise an error.
            # This is the more performant default behaviour

            config = CatModelConfig(always_check_shapes=False)

            model = config.build(framework="spam")

            model({"in_1": [[1]]})


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
