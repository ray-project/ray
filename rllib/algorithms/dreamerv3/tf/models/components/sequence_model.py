"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import numpy as np
import tensorflow as tf

from models.components.mlp import MLP
from utils.model_dimensions import get_gru_units


# TODO: de-hardcode the discrete action processing (currently one-hot).
class SequenceModel(tf.keras.Model):
    """The "sequence model" of the RSSM, computing ht+1 given (ht, zt, at).

    Here, h is the GRU unit output.
    """
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        action_space: gym.Space,
        num_gru_units: Optional[int] = None,
    ):
        super().__init__()

        num_gru_units = get_gru_units(model_dimension, override=num_gru_units)

        self.action_space = action_space
        # In Danijar's code, there is an additional layer (units=[model_size])
        # prior to the GRU (but always only with 1 layer), which is not mentioned in
        # the paper.
        self.pre_gru_layer = MLP(
            num_dense_layers=1,
            model_dimension=model_dimension,
            output_layer_size=None,
        )
        self.gru_unit = tf.keras.layers.GRU(
            num_gru_units,
            return_sequences=False,
            return_state=False,
            time_major=True,
            # Note: Changing these activations is most likely a bad idea!
            # In experiments, setting one of both of them to silu deteriorated
            # performance significantly.
            # activation=tf.nn.silu,
            # recurrent_activation=tf.nn.silu,
        )
        # Add layer norm after the GRU output.
        # self.layer_norm = tf.keras.layers.LayerNormalization()

    def call(self, z, a, h=None):
        """

        Args:
            z: The previous stochastic discrete representations of the original
                observation input. (B, num_categoricals, num_classes_per_categorical).
            a: The previous action (already one-hot'd if applicable). (B, ...).
            h: The previous deterministic hidden state of the sequence model.
                (B, num_gru_units)
        """
        # Flatten last two dims of z.
        z_shape = tf.shape(z)
        z = tf.reshape(tf.cast(z, tf.float32), shape=(z_shape[0], -1))
        out = tf.concat([z, a], axis=-1)
        # Pass through pre-GRU layer.
        out = self.pre_gru_layer(out)
        # Pass through (time-major) GRU.
        out = self.gru_unit(tf.expand_dims(out, axis=0), initial_state=h)
        # Pass through LayerNorm and return both non-normed and normed h-states.
        return out


if __name__ == "__main__":
    # DreamerV2/3 Atari input space: B x 32 (num_categoricals) x 32 (num_classes)
    B = 1
    T = 3
    h_dim = 32
    num_categoricals = num_classes = 8

    h_tm1 = tf.convert_to_tensor(np.random.random(size=(B, h_dim)), dtype=tf.float32)
    z_seq = np.random.random(size=(B, T, num_categoricals, num_classes))
    a_space = gym.spaces.Discrete(4)
    a_seq = np.array([[a_space.sample() for t in range(T)] for b in range(B)])

    model = SequenceModel(action_space=a_space, num_gru_units=h_dim)

    #h, layer_normed_h = model(z=z_seq, a=a_seq, h=h_tm1)
    h = model(z=z_seq, a=a_seq, h=h_tm1)
    print(h.shape)
