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


class CriticNetwork(tf.keras.Model):
    def __init__(
        self,
        *,
        model_dimension: Optional[str] = "XS",
        num_buckets: int = 255,
        lower_bound: float = -20.0,
        upper_bound: float = 20.0,
        ema_decay: float = 0.98,
    ):
        super().__init__()

        self.model_dimension = model_dimension
        self.ema_decay = ema_decay

        # "Fast" critic network(s) (mlp + reward-pred-layer). This is the network
        # we actually train with our critic loss.
        # IMPORTANT: We also use this to compute the return-targets, BUT we regularize
        # the critic loss term such that the weights of this fast critic stay close
        # to the EMA weights (see below).
        self.mlp = MLP(model_dimension=self.model_dimension, output_layer_size=None)
        self.return_layer = RewardPredictorLayer(
            num_buckets=num_buckets,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )

        # Weights-EMA (EWMA) containing networks for critic loss (similar to a
        # target net, BUT not used to compute anything, just for the
        # weights regularizer term inside the critic loss).
        self.mlp_ema = MLP(
            model_dimension=self.model_dimension,
            output_layer_size=None,
            trainable=False,
        )
        self.return_layer_ema = RewardPredictorLayer(
            num_buckets=num_buckets,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            trainable=False,
        )

        # Optimizer.
        self.optimizer = tf.keras.optimizers.Adam(learning_rate=3e-5, epsilon=1e-5)

    def call(self, h, z, return_logits=False, use_ema=False):
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

        if not use_ema:
            # Send h-cat-z through MLP.
            out = self.mlp(out)
            # Return expected return OR (expected return, probs of bucket values).
            return self.return_layer(out, return_logits=return_logits)
        else:
            out = self.mlp_ema(out)
            return self.return_layer_ema(out, return_logits=return_logits)

    def init_ema(self):
        vars = self.mlp.trainable_variables + self.return_layer.trainable_variables
        vars_ema = (
            self.mlp_ema.variables + self.return_layer_ema.variables
        )
        assert len(vars) == len(vars_ema)
        for var, var_ema in zip(vars, vars_ema):
            var_ema.assign(var)

    def update_ema(self):
        vars = self.mlp.trainable_variables + self.return_layer.trainable_variables
        vars_ema = (
            self.mlp_ema.variables + self.return_layer_ema.variables
        )
        assert len(vars) == len(vars_ema)
        for var, var_ema in zip(vars, vars_ema):
            var_ema.assign(self.ema_decay * var_ema + (1.0 - self.ema_decay) * var)


if __name__ == "__main__":
    h_dim = 8
    h = np.random.random(size=(1, 8))
    z = np.random.random(size=(1, 8, 8))

    model = CriticNetwork(num_buckets=5, lower_bound=-2.0, upper_bound=2.0)

    out = model(h, z)
    print(out)

    out = model(h, z, return_weighted_values=True)
    print(out)
