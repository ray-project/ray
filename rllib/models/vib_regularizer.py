import logging
from tensorflow.keras.layers import Layer
from ray.rllib.models.loss_functions import compute_vib_loss
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

tf1, tf, tfv = try_import_tf()

logger = logging.getLogger(__name__)


class VIB_Decoder(Layer):
    def __init__(self, decoder_layers=(64,), dropout_rate=0.0):
        super().__init__()
        self._decoder_layers = decoder_layers
        self._mu_size = None
        self._dropout_rate = dropout_rate
        self._encoding = None

    def call(self, inputs, **kwargs):
        if self._mu_size is None:
            self._mu_size = inputs.get_shape().as_list()[-1] // 2
        mu, rho = inputs[:, :self._mu_size], inputs[:, self._mu_size:]
        self._encoding = tf.distributions.Normal(mu, tf.nn.softplus(rho))
        outputs = self._encoding.sample()
        for layer in self._decoder_layers:
            outputs = tf.keras.layers.Dense(units=layer, activation="relu",
                                            name="q_out_decoder")(outputs)
            if self._dropout_rate > 0:
                outputs = tf.keras.layers.Dropout(self._dropout_rate)(outputs)

        return outputs
