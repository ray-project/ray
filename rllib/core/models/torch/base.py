import abc
from typing import Tuple, Union

import numpy as np

from ray.rllib.core.models.base import (
    Model,
    ModelConfig,
    _raise_not_decorated_exception,
)
from ray.rllib.core.models.torch.primitives import nn
from ray.rllib.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
    check_input_specs,
    check_output_specs,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.typing import TensorType


class TorchModel(nn.Module, Model, abc.ABC):
    """Base class for RLlib's PyTorch models.

    This class defines the interface for RLlib's PyTorch models and checks
    whether inputs and outputs of forward are checked with `check_input_specs()` and
    `check_output_specs()` respectively.

    Example usage for a single Flattening layer:

    .. testcode::

        from ray.rllib.core.models.torch.base import TorchModel
        from ray.rllib.core.models.base import ModelConfig
        import torch

        class FlattenModelConfig(ModelConfig):
            def build(self, framework: str):
                assert framework == "torch"
                return TorchFlattenModel(self)

        class TorchFlattenModel(TorchModel):
            def __init__(self, config):
                TorchModel.__init__(self, config)
                self.flatten_layer = torch.nn.Flatten()

            def _forward(self, inputs, **kwargs):
                return self.flatten_layer(inputs)

        model = FlattenModelConfig().build("torch")
        inputs = torch.Tensor([[[1, 2]]])
        print(model(inputs))

    .. testoutput::

        tensor([[1., 2.]])

    """

    def __init__(self, config: ModelConfig, skip_nn_module_init=False):
        """Initialized a TorchModel.

        Args:
            config: The ModelConfig to use
            skip_nn_module_init: Whether to skip the call to nn.Module.__init__.
                This should be used if the child class has called nn.Module.__init__
                already to avoid deleting sub-modules that were added between the two
                nn.Module.__init__ calls.
        """
        nn.Module.__init__(self)
        Model.__init__(self, config)

        # Raise errors if forward method is not decorated to check specs.
        if not is_input_decorated(self.forward):
            _raise_not_decorated_exception(type(self).__name__ + ".forward()", "input")
        if not is_output_decorated(self.forward):
            _raise_not_decorated_exception(type(self).__name__ + ".forward()", "output")

    @check_input_specs("input_spec")
    @check_output_specs("output_spec")
    def forward(
        self, inputs: Union[NestedDict, TensorType], **kwargs
    ) -> Union[NestedDict, TensorType]:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            inputs: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            NestedDict: The output tensors.
        """
        return self._forward(inputs, **kwargs)

    @override(Model)
    def get_num_parameters(self) -> Tuple[int, int]:
        num_all_params = sum(int(np.prod(p.size())) for p in self.parameters())
        trainable_params = filter(lambda p: p.requires_grad, self.parameters())
        num_trainable_params = sum(int(np.prod(p.size())) for p in trainable_params)
        return (
            num_trainable_params,
            num_all_params - num_trainable_params,
        )
