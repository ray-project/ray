"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import tensorflow as tf
import tensorflow_probability as tfp

from models.components.mlp import MLP


class VectorDecoder(tf.keras.Model):
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        observation_space: gym.Space,
    ):
        super().__init__()

        assert (
            isinstance(observation_space, gym.spaces.Box)
            and len(observation_space.shape) == 1
        )

        self.mlp = MLP(
            model_dimension=model_dimension,
            output_layer_size=observation_space.shape[0],
        )

    def call(self, h, z):
        """TODO

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
        """
        # Flatten last two dims of z.
        assert len(z.shape) == 3
        z_shape = tf.shape(z)
        z = tf.reshape(tf.cast(z, tf.float32), shape=(z_shape[0], -1))
        assert len(z.shape) == 2
        out = tf.concat([h, z], axis=-1)
        # Send h-cat-z through MLP to get mean values of diag gaussian.
        loc = self.mlp(out)

        # Create the Gaussian diag distribution.
        distribution = tfp.distributions.MultivariateNormalDiag(
            loc=loc,
            # Scale == 1.0.
            scale_diag=tf.ones_like(loc),
        )
        pred_obs = distribution.sample()

        # Always return both predicted observations (sample0 and distribution.
        return pred_obs, distribution
