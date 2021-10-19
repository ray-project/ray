import numpy as np

from ray.rllib.models.utils import get_activation_fn
from ray.rllib.utils.framework import get_variable, try_import_tf, \
    TensorType, TensorShape

tf1, tf, tfv = try_import_tf()


class NoisyLayer(tf.keras.layers.Layer if tf else object):
    """A Layer that adds learnable Noise to some previous layer's outputs.

    Consists of:
    - a common dense layer: y = w^{T}x + b
    - a noisy layer: y = (w + \\epsilon_w*\\sigma_w)^{T}x +
        (b+\\epsilon_b*\\sigma_b)
    , where \epsilon are random variables sampled from factorized normal
    distributions and \\sigma are trainable variables which are expected to
    vanish along the training procedure.
    """

    def __init__(self,
                 prefix: str,
                 out_size: int,
                 sigma0: float,
                 activation: str = "relu"):
        """Initializes a NoisyLayer object.

        Args:
            prefix:
            out_size: Output size for Noisy Layer
            sigma0: Initialization value for sigma_b (bias noise)
            non_linear: Non-linear activation for Noisy Layer
        """
        super().__init__()
        self.prefix = prefix
        self.out_size = out_size
        # TF noise generation can be unreliable on GPU
        # If generating the noise on the CPU,
        # lowering sigma0 to 0.1 may be helpful
        self.sigma0 = sigma0  # 0.5~GPU, 0.1~CPU
        self.activation = activation
        # Variables.
        self.w = None  # Weight matrix.
        self.b = None  # Biases.
        self.sigma_w = None  # Noise for weight matrix
        self.sigma_b = None  # Noise for biases.

    def build(self, input_shape: TensorShape):
        in_size = int(input_shape[1])

        self.sigma_w = get_variable(
            value=tf.keras.initializers.RandomUniform(
                minval=-1.0 / np.sqrt(float(in_size)),
                maxval=1.0 / np.sqrt(float(in_size))),
            trainable=True,
            tf_name=self.prefix + "_sigma_w",
            shape=[in_size, self.out_size],
            dtype=tf.float32)

        self.sigma_b = get_variable(
            value=tf.keras.initializers.Constant(
                self.sigma0 / np.sqrt(float(in_size))),
            trainable=True,
            tf_name=self.prefix + "_sigma_b",
            shape=[self.out_size],
            dtype=tf.float32,
        )

        self.w = get_variable(
            value=tf.keras.initializers.GlorotUniform(),
            tf_name=self.prefix + "_fc_w",
            trainable=True,
            shape=[in_size, self.out_size],
            dtype=tf.float32,
        )

        self.b = get_variable(
            value=tf.keras.initializers.Zeros(),
            tf_name=self.prefix + "_fc_b",
            trainable=True,
            shape=[self.out_size],
            dtype=tf.float32,
        )

    def call(self, inputs: TensorType) -> TensorType:
        in_size = int(inputs.shape[1])
        epsilon_in = tf.random.normal(shape=[in_size])
        epsilon_out = tf.random.normal(shape=[self.out_size])
        epsilon_in = self._f_epsilon(epsilon_in)
        epsilon_out = self._f_epsilon(epsilon_out)
        epsilon_w = tf.matmul(
            a=tf.expand_dims(epsilon_in, -1), b=tf.expand_dims(epsilon_out, 0))
        epsilon_b = epsilon_out

        action_activation = tf.matmul(
            inputs,
            self.w + self.sigma_w * epsilon_w) + \
            self.b + self.sigma_b * epsilon_b

        fn = get_activation_fn(self.activation, framework="tf")
        if fn is not None:
            action_activation = fn(action_activation)
        return action_activation

    def _f_epsilon(self, x: TensorType) -> TensorType:
        return tf.math.sign(x) * tf.math.sqrt(tf.math.abs(x))
