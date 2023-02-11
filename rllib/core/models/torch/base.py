import abc
from typing import Union

from ray.rllib.core.models.base import Model, ModelConfig
from ray.rllib.core.models.torch.primitives import nn
from ray.rllib.models.specs.checker import is_input_decorated, is_output_decorated, \
    check_input_specs, check_output_specs
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType


def _raise_not_decorated_exception(input_or_output):
    raise ValueError(
        f"`TorchModel.forward()` not decorated with {input_or_output} specification. "
        f"Decorate it with @check_{input_or_output}_specs() to define a specification."
    )


class TorchModel(nn.Module, Model, abc.ABC):
    """Base class for RLlib's PyTorch models.

    This class defines the interface for RLlib's PyTorch models and checks
    whether inputs and outputs of forward are checked with `check_input_specs()` and
    `check_output_specs()` respectively.

    Example usage for a single Flattening layer:

    .. testcode::

        class FlattenModelConfig(ModelConfig):
            def build(self, framework: str):
                assert framework == "torch"
                return TorchFlattenModel(self)

        class TorchFlattenModel(TorchModel):
            def __init__(self, config):
                TorchModel.__init__(self, config)
                self.flatten_layer = nn.Flatten()

            def _forward(self, inputs, **kwargs):
                return self.flatten_layer(inputs)

        model = FlattenModelConfig().build("torch")
        inputs = torch.Tensor([[[1, 2]]])
        print(model(inputs))

    .. testoutput::

        tensor([[1., 2.]])

    """

    def __init__(self, config: ModelConfig):
        nn.Module.__init__(self)
        Model.__init__(self, config)

        # automatically apply spec checking
        if not is_input_decorated(self.forward):
            _raise_not_decorated_exception("input")
        if not is_output_decorated(self.forward):
            _raise_not_decorated_exception("output")

    @check_input_specs("input_spec", cache=True)
    @check_output_specs("output_spec", cache=True)
    def forward(self, inputs: Union[NestedDict, TensorType], **kwargs) -> Union[
        NestedDict, TensorType]:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            inputs: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        return self._forward(inputs, **kwargs)