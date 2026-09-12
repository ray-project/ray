from typing import Any, Optional

from ray._common.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType
from ray.util import log_once

tf1, tf, tfv = try_import_tf()


class SkipConnection(tf.keras.layers.Layer if tf else object):
    """Skip connection layer.

    Adds the original input to the output (regular residual layer) OR uses
    input as hidden state input to a given fan_in_layer.
    """

    def __init__(self, layer: Any, fan_in_layer: Optional[Any] = None, **kwargs):
        """Initializes a SkipConnection keras layer object.

        Args:
            layer (tf.keras.layers.Layer): Any layer processing inputs.
            fan_in_layer (Optional[tf.keras.layers.Layer]): An optional
                layer taking two inputs: The original input and the output
                of `layer`.
        """
        if log_once("skip_connection"):
            deprecation_warning(
                old="rllib.models.tf.layers.SkipConnection",
            )
        super().__init__(**kwargs)
        self._layer = layer
        self._fan_in_layer = fan_in_layer

    def call(self, inputs: TensorType, **kwargs) -> TensorType:
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
