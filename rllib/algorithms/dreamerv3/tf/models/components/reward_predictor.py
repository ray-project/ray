"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import numpy as np
import tensorflow as tf

from models.components.mlp import MLP
from models.components.reward_predictor_layer import RewardPredictorLayer


class RewardPredictor(tf.keras.Model):
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        num_buckets: int = 255,
        lower_bound: float = -20.0,
        upper_bound: float = 20.0,
    ):
        super().__init__()

        self.mlp = MLP(
            model_dimension=model_dimension,
            output_layer_size=None,
        )
        self.reward_layer = RewardPredictorLayer(
            num_buckets=num_buckets,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )

    def call(self, h, z, return_logits=False):
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
        # Send h-cat-z through MLP.
        out = self.mlp(out)
        # Return reward OR (reward, weighted bucket values).
        return self.reward_layer(out, return_logits=return_logits)


if __name__ == "__main__":
    h_dim = 8
    h = np.random.random(size=(1, 8))
    z = np.random.random(size=(1, 8, 8))

    model = RewardPredictor(num_buckets=5, lower_bound=-2.0, upper_bound=2.0)

    out = model(h, z)
    print(out)

    out = model(h, z, return_weighted_values=True)
    print(out)
