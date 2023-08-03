from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import TensorType
from typing import Optional

torch, nn = try_import_torch()


class SkipConnection(nn.Module):
    """Skip connection layer.

    Adds the original input to the output (regular residual layer) OR uses
    input as hidden state input to a given fan_in_layer.
    """

    def __init__(
        self, layer: nn.Module, fan_in_layer: Optional[nn.Module] = None, **kwargs
    ):
        """Initializes a SkipConnection nn Module object.

        Args:
            layer (nn.Module): Any layer processing inputs.
            fan_in_layer (Optional[nn.Module]): An optional
                layer taking two inputs: The original input and the output
                of `layer`.
        """
        super().__init__(**kwargs)
        self._layer = layer
        self._fan_in_layer = fan_in_layer

    def forward(self, inputs: TensorType, **kwargs) -> TensorType:
        # del kwargs
        outputs = self._layer(inputs, **kwargs)
        # Residual case, just add inputs to outputs.
        if self._fan_in_layer is None:
            outputs = outputs + inputs
        # Fan-in e.g. RNN: Call fan-in with `inputs` and `outputs`.
        else:
            # NOTE: In the GRU case, `inputs` is the state input.
            outputs = self._fan_in_layer((inputs, outputs))

        return outputs
