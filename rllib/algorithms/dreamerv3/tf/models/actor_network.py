"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
from gymnasium.spaces import Box, Discrete
import numpy as np
import tensorflow as tf
import tensorflow_probability as tfp

from models.components.mlp import MLP


class ActorNetwork(tf.keras.Model):
    def __init__(
        self,
        *,
        action_space: gym.Space,
        model_dimension: Optional[str] = "XS",
        return_normalization_decay: float = 0.99,
    ):
        super().__init__()

        self.model_dimension = model_dimension
        # The EMA decay rate used for the [Percentile(R, 95%) - Percentile(R, 5%)]
        # diff to scale value targets for the actor loss.
        self.return_normalization_decay = return_normalization_decay
        self.ema_value_target_pct5 = tf.Variable(
            np.nan, dtype=tf.float32, trainable=False
        )
        self.ema_value_target_pct95 = tf.Variable(
            np.nan, dtype=tf.float32, trainable=False
        )

        self.action_space = action_space

        if isinstance(self.action_space, Discrete):
            self.mlp = MLP(
                model_dimension=self.model_dimension,
                output_layer_size=self.action_space.n,
            )
        elif isinstance(action_space, Box):
            #assert np.all(action_space.low) == 0.0 and np.all(action_space.high) == 1.0
            output_layer_size = np.prod(action_space.shape)
            self.mlp = MLP(
                model_dimension=self.model_dimension,
                output_layer_size=output_layer_size,
            )
            self.std_mlp = MLP(
                model_dimension=self.model_dimension,
                output_layer_size=output_layer_size,
            )
        else:
            raise ValueError(f"Invalid action space: {action_space}")

        # Optimizer.
        self.optimizer = tf.keras.optimizers.Adam(learning_rate=3e-5, epsilon=1e-5)

    @tf.function
    def call(self, h, z, return_distribution=False):
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
        action_logits = self.mlp(out)

        if isinstance(self.action_space, Discrete):
            action_probs = tf.nn.softmax(action_logits)

            # Add the unimix weighting (1% uniform) to the probs.
            # See [1]: "Unimix categoricals: We parameterize the categorical distributions
            # for the world model representations and dynamics, as well as for the actor
            # network, as mixtures of 1% uniform and 99% neural network output to ensure
            # a minimal amount of probability mass on every class and thus keep log
            # probabilities and KL divergences well behaved."
            action_probs = 0.99 * action_probs + 0.01 * (1.0 / self.action_space.n)

            # Danijar's code does: distr = [Distr class](logits=tf.log(probs)).
            # Not sure why we don't directly use the already available probs instead.
            action_logits = tf.math.log(action_probs)

            # Create the distribution object using the unimix'd logits.
            distr = tfp.distributions.OneHotCategorical(logits=action_logits)

            action = tf.cast(tf.stop_gradient(distr.sample()), tf.float32) + (
                action_probs - tf.stop_gradient(action_probs)
            )

        elif isinstance(self.action_space, Box):
            # Send h-cat-z through MLP to compute stddev logits for Normal dist
            std_logits = self.std_mlp(out)
            # minstd, maxstd taken from [1] from configs.yaml
            minstd = 0.1
            maxstd = 1.0
            # squash std_logits from (-inf, inf) to (minstd, maxstd)
            std_logits = (maxstd - minstd) * tf.sigmoid(std_logits + 2.0) + minstd
            # Compute Normal distribution from action_logits and std_logits
            distr = tfp.distributions.Normal(tf.tanh(action_logits), std_logits)
            # If action_space is a box with multiple dims, make individual dims independent
            distr = tfp.distributions.Independent(distr, len(self.action_space.shape))
            action = distr.sample()

        if return_distribution:
            return action, distr
        return action


if __name__ == "__main__":
    action_space = gym.spaces.Discrete(5)
    print("action space: ", action_space)

    b_dim = 2
    h_dim = 8
    h = np.random.random(size=(b_dim, h_dim))
    z = np.random.random(size=(b_dim, h_dim, h_dim))

    model = ActorNetwork(action_space=action_space, model_dimension="XS")

    actions = model(h, z)
    print(actions)

    actions, distr = model(h, z, return_distribution=True)
    print(actions, distr.sample())
    print(distr.log_prob(actions))
    print(distr.logits)

    action_space = gym.spaces.Box(0, 1, (5,))
    print("action space: ", action_space)

    model = ActorNetwork(action_space=action_space, model_dimension="XS")

    actions = model(h, z)
    print(actions)

    actions, distr = model(h, z, return_distribution=True)
    print(actions, distr.sample())
    print(distr.log_prob(actions))
    print(distr.distribution.loc, distr.distribution.scale)

    print("Test log_prob computation used in actor_loss.py")
    h2 = np.random.random(size=(b_dim, h_dim))
    z2 = np.random.random(size=(b_dim, h_dim, h_dim))
    actions2, distr2 = model(h, z, return_distribution=True)

    actions = [actions, actions2]
    distr = [distr, distr2]

    log_probs = tf.stack([dist.log_prob(action) for dist, action in zip(distr, actions)])
    print(log_probs)
