from ray._common.deprecation import deprecation_warning
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorShape, TensorType
from ray.util import log_once

tf1, tf, tfv = try_import_tf()


class GRUGate(tf.keras.layers.Layer if tf else object):
    def __init__(self, init_bias: float = 0.0, **kwargs):
        super().__init__(**kwargs)
        self._init_bias = init_bias
        if log_once("gru_gate"):
            deprecation_warning(
                old="rllib.models.tf.layers.GRUGate",
            )

    def build(self, input_shape: TensorShape):
        h_shape, x_shape = input_shape
        if x_shape[-1] != h_shape[-1]:
            raise ValueError(
                "Both inputs to GRUGate must have equal size in last axis!"
            )

        dim = int(h_shape[-1])
        self._w_r = self.add_weight(shape=(dim, dim))
        self._w_z = self.add_weight(shape=(dim, dim))
        self._w_h = self.add_weight(shape=(dim, dim))

        self._u_r = self.add_weight(shape=(dim, dim))
        self._u_z = self.add_weight(shape=(dim, dim))
        self._u_h = self.add_weight(shape=(dim, dim))

        def bias_initializer(shape, dtype):
            return tf.fill(shape, tf.cast(self._init_bias, dtype=dtype))

        self._bias_z = self.add_weight(shape=(dim,), initializer=bias_initializer)

    def call(self, inputs: TensorType, **kwargs) -> TensorType:
        # Pass in internal state first.
        h, X = inputs

        r = tf.tensordot(X, self._w_r, axes=1) + tf.tensordot(h, self._u_r, axes=1)
        r = tf.nn.sigmoid(r)

        z = (
            tf.tensordot(X, self._w_z, axes=1)
            + tf.tensordot(h, self._u_z, axes=1)
            - self._bias_z
        )
        z = tf.nn.sigmoid(z)

        h_next = tf.tensordot(X, self._w_h, axes=1) + tf.tensordot(
            (h * r), self._u_h, axes=1
        )
        h_next = tf.nn.tanh(h_next)

        return (1 - z) * h + z * h_next
