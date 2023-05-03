import abc
import logging
from typing import Tuple, Union

import numpy as np

from ray.rllib.core.models.base import (
    Model,
    ModelConfig,
)
from ray.rllib.core.models.specs.checker import (
    is_input_decorated,
    is_output_decorated,
    check_input_specs,
    check_output_specs,
)
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from ray.util import log_once

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


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

    def __init__(self, config: ModelConfig):
        """Initialized a TorchModel.

        Args:
            config: The ModelConfig to use.
        """
        nn.Module.__init__(self)
        Model.__init__(self, config)

        # Raise errors if forward method is not decorated to check input specs.
        if not is_input_decorated(self.forward):
            raise ValueError(
                f"`{type(self).__name__}.forward()` not decorated with input "
                f"specification. Decorate it with @check_input_specs() to define a "
                f"specification and resolve this Error. If you don't want to check "
                f"anything, you can use an empty spec."
            )

        if is_output_decorated(self.forward):
            if log_once("torch_model_forward_output_decorated"):
                logger.warning(
                    f"`{type(self).__name__}.forward()` decorated with output "
                    f"specification. This is not recommended for torch models "
                    f"that are used with torch.compile() because it breaks "
                    f"torch dynamo's graph. This can lead lead to slower execution."
                    f"Remove @check_output_specs() from the forward() method to "
                    f"resolve this."
                )

    @check_input_specs("input_specs")
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

        # When `always_check_shapes` is set, we always check input and output specs.
        # Note that we check the input specs twice because we need the following
        # check to always check the input specs.
        if self.config.always_check_shapes:

            @check_input_specs("input_specs", only_check_on_retry=False)
            @check_output_specs("output_specs")
            def checked_forward(self, input_data, **kwargs):
                return self._forward(input_data, **kwargs)

            return checked_forward(self, inputs, **kwargs)

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
