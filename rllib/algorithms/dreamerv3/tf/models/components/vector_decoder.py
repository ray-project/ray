"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import tensorflow as tf
import tensorflow_probability as tfp

from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP


class VectorDecoder(tf.keras.Model):
    """A simple vector decoder to reproduce non-image (1D vector) observations.

    Wraps an MLP for mean parameter computations and a Gaussian distribution,
    from which we then sample using these mean values and a fixed stddev of 1.0.
    """

    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        observation_space: gym.Space,
    ):
        """Initializes a VectorDecoder instance.

        Args:
            model_dimension: The "Model Size" used according to [1] Appendinx B.
                Determines the exact size of the underlying MLP.
            observation_space: The observation space to decode back into. This must
                be a Box of shape (d,), where d >= 1.
        """
        super().__init__(name="vector_decoder")

        assert (
            isinstance(observation_space, gym.spaces.Box)
            and len(observation_space.shape) == 1
        )

        self.mlp = MLP(
            model_dimension=model_dimension,
            output_layer_size=observation_space.shape[0],
        )

    def call(self, h, z):
        """Performs a forward pass through the vector encoder.

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
