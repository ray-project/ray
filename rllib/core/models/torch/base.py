import abc
import logging
from typing import Tuple, Union

import numpy as np

from ray.rllib.core.models.base import Model
from ray.rllib.core.models.configs import ModelConfig
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


class TorchModel(nn.Module, Model, abc.ABC):
    """Base class for RLlib's PyTorch models.

    This class defines the interface for RLlib's PyTorch models and checks
    whether inputs and outputs of forward are checked with `check_input_specs()` and
    `check_output_specs()` respectively.

    Example usage for a single Flattening layer:

    .. testcode::

        from ray.rllib.core.models.configs import ModelConfig
        from ray.rllib.core.models.torch.base import TorchModel
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

    def __init__(self, config: ModelConfig):
        """Initialized a TorchModel.

        Args:
            config: The ModelConfig to use.
        """
        nn.Module.__init__(self)
        Model.__init__(self, config)

    def forward(
        self, inputs: Union[dict, TensorType], **kwargs
    ) -> Union[dict, TensorType]:
        """Returns the output of this model for the given input.

        This method only makes sure that we have a spec-checked _forward() method.

        Args:
            inputs: The input tensors.
            **kwargs: Forward compatibility kwargs.

        Returns:
            dict: The output tensors.
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

    @override(Model)
    def _set_to_dummy_weights(self, value_sequence=(-0.02, -0.01, 0.01, 0.02)):
        trainable_weights = [p for p in self.parameters() if p.requires_grad]
        non_trainable_weights = [p for p in self.parameters() if not p.requires_grad]
        for i, w in enumerate(trainable_weights + non_trainable_weights):
            fill_val = value_sequence[i % len(value_sequence)]
            with torch.no_grad():
                w.fill_(fill_val)
