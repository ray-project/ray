"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
from gymnasium.spaces import Box, Discrete
import numpy as np

from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.utils import (
    get_gru_units,
    get_num_z_categoricals,
    get_num_z_classes,
)
from ray.rllib.utils.framework import try_import_tf, try_import_tfp

_, tf, _ = try_import_tf()
tfp = try_import_tfp()


class ActorNetwork(tf.keras.Model):
    """The `actor` (policy net) of DreamerV3.

    Consists of a simple MLP for Discrete actions and two MLPs for cont. actions (mean
    and stddev).
    Also contains two scalar variables to keep track of the percentile-5 and
    percentile-95 values of the computed value targets within a batch. This is used to
    compute the "scaled value targets" for actor learning. These two variables decay
    over time exponentially (see [1] for more details).
    """

    def __init__(
        self,
        *,
        model_size: Optional[str] = "XS",
        action_space: gym.Space,
    ):
        """Initializes an ActorNetwork instance.

        Args:
             model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             action_space: The action space the our environment used.
        """
        super().__init__(name="actor")

        self.model_size = model_size
        self.action_space = action_space

        # The EMA decay variables used for the [Percentile(R, 95%) - Percentile(R, 5%)]
        # diff to scale value targets for the actor loss.
        self.ema_value_target_pct5 = tf.Variable(
            np.nan, trainable=False, name="value_target_pct5"
        )
        self.ema_value_target_pct95 = tf.Variable(
            np.nan, trainable=False, name="value_target_pct95"
        )

        # For discrete actions, use a single MLP that computes logits.
        if isinstance(self.action_space, Discrete):
            self.mlp = MLP(
                model_size=self.model_size,
                output_layer_size=self.action_space.n,
                name="actor_mlp",
            )
        # For cont. actions, use separate MLPs for Gaussian mean and stddev.
        # TODO (sven): In the author's original code repo, this is NOT the case,
        #  inputs are pushed through a shared MLP, then only the two output linear
        #  layers are separate for std- and mean logits.
        elif isinstance(action_space, Box):
            output_layer_size = np.prod(action_space.shape)
            self.mlp = MLP(
                model_size=self.model_size,
                output_layer_size=output_layer_size,
                name="actor_mlp_mean",
            )
            self.std_mlp = MLP(
                model_size=self.model_size,
                output_layer_size=output_layer_size,
                name="actor_mlp_std",
            )
        else:
            raise ValueError(f"Invalid action space: {action_space}")

        # Trace self.call.
        dl_type = tf.keras.mixed_precision.global_policy().compute_dtype or tf.float32
        self.call = tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, get_gru_units(model_size)], dtype=dl_type),
                tf.TensorSpec(
                    shape=[
                        None,
                        get_num_z_categoricals(model_size),
                        get_num_z_classes(model_size),
                    ],
                    dtype=dl_type,
                ),
            ]
        )(self.call)

    def call(self, h, z):
        """Performs a forward pass through this policy network.

        Args:
            h: The deterministic hidden state of the sequence model. [B, dim(h)].
            z: The stochastic discrete representations of the original
                observation input. [B, num_categoricals, num_classes].
        """
        # Flatten last two dims of z.
        assert len(z.shape) == 3
        z_shape = tf.shape(z)
        z = tf.reshape(z, shape=(z_shape[0], -1))
        assert len(z.shape) == 2
        out = tf.concat([h, z], axis=-1)
        out.set_shape(
            [
                None,
                (
                    get_num_z_categoricals(self.model_size)
                    * get_num_z_classes(self.model_size)
                    + get_gru_units(self.model_size)
                ),
            ]
        )
        # Send h-cat-z through MLP.
        action_logits = tf.cast(self.mlp(out), tf.float32)

        if isinstance(self.action_space, Discrete):
            action_probs = tf.nn.softmax(action_logits)

            # Add the unimix weighting (1% uniform) to the probs.
            # See [1]: "Unimix categoricals: We parameterize the categorical
            # distributions for the world model representations and dynamics, as well as
            # for the actor network, as mixtures of 1% uniform and 99% neural network
            # output to ensure a minimal amount of probability mass on every class and
            # thus keep log probabilities and KL divergences well behaved."
            action_probs = 0.99 * action_probs + 0.01 * (1.0 / self.action_space.n)

            # Danijar's code does: distr = [Distr class](logits=tf.log(probs)).
            # Not sure why we don't directly use the already available probs instead.
            action_logits = tf.math.log(action_probs)

            # Distribution parameters are the log(probs) directly.
            distr_params = action_logits
            distr = self.get_action_dist_object(distr_params)

            action = tf.stop_gradient(distr.sample()) + (
                action_probs - tf.stop_gradient(action_probs)
            )

        elif isinstance(self.action_space, Box):
            # Send h-cat-z through MLP to compute stddev logits for Normal dist
            std_logits = tf.cast(self.std_mlp(out), tf.float32)
            # minstd, maxstd taken from [1] from configs.yaml
            minstd = 0.1
            maxstd = 1.0

            # Distribution parameters are the squashed std_logits and the tanh'd
            # mean logits.
            # squash std_logits from (-inf, inf) to (minstd, maxstd)
            std_logits = (maxstd - minstd) * tf.sigmoid(std_logits + 2.0) + minstd
            mean_logits = tf.tanh(action_logits)

            distr_params = tf.concat([mean_logits, std_logits], axis=-1)
            distr = self.get_action_dist_object(distr_params)

            action = distr.sample()

        return action, distr_params

    def get_action_dist_object(self, action_dist_params_T_B):
        """Helper method to create an action distribution object from (T, B, ..) params.

        Args:
            action_dist_params_T_B: The time-major action distribution parameters.
                This could be simply the logits (discrete) or a to-be-split-in-2
                tensor for mean and stddev (continuous).

        Returns:
            The tfp action distribution object, from which one can sample, compute
            log probs, entropy, etc..
        """
        if isinstance(self.action_space, gym.spaces.Discrete):
            # Create the distribution object using the unimix'd logits.
            distr = tfp.distributions.OneHotCategorical(
                logits=action_dist_params_T_B,
                dtype=tf.float32,
            )

        elif isinstance(self.action_space, gym.spaces.Box):
            # Compute Normal distribution from action_logits and std_logits
            loc, scale = tf.split(action_dist_params_T_B, 2, axis=-1)
            distr = tfp.distributions.Normal(loc=loc, scale=scale)

            # If action_space is a box with multiple dims, make individual dims
            # independent.
            distr = tfp.distributions.Independent(distr, len(self.action_space.shape))

        else:
            raise ValueError(f"Action space {self.action_space} not supported!")

        return distr
