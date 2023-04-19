"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import tensorflow as tf
import tree  # pip install dm_tree

from models.components.continue_predictor import ContinuePredictor
from models.components.dynamics_predictor import DynamicsPredictor
from models.components.mlp import MLP
from models.components.representation_layer import RepresentationLayer
from models.components.reward_predictor import RewardPredictor
from models.components.sequence_model import SequenceModel
from utils.model_dimensions import get_gru_units
from utils.symlog import symlog


class WorldModel(tf.keras.Model):
    """TODO
    """
    def __init__(
        self,
        *,
        model_dimension: str = "XS",
        action_space: gym.Space,
        batch_length_T: int = 64,
        encoder: tf.keras.Model,
        decoder: tf.keras.Model,
        num_gru_units: Optional[int] = None,
        symlog_obs: bool = True,
    ):
        """TODO

        Args:
             model_dimension: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             action_space: The action space the our environment used.
             batch_length_T: The length (T) of the sequences used for training. The
                actual shape of the input data (e.g. rewards) is then: [B, T, ...],
                where B is the "batch size", T is the "batch length" (this arg) and
                "..." is the dimension of the data (e.g. (64, 64, 3) for Atari image
                observations). Note that a single sequence (within a batch) only ever
                contains continuous time-step data from one episode. Should an
                episode have ended inside a sequence, the reset of that sequence will be
                filled with zero-data.
            encoder: The encoder Model taking symlogged observations as input and
                outputting a 1D vector that will beused as input into the
                z-representation generating layer (either together with an h-state
                (sequence model) or not (dynamics network)).
            decoder: The decoder Model taking h- and z- states as input and generating
                a (symlogged) predicted observation.
            num_gru_units: The number of GRU units to use. If None, use
                `model_dimension` to figure out this parameter.
            symlog_obs: Whether to predict decoded observations in symlog space.
                This should be False for image based observations.
                According to the paper [1] Appendix E: "NoObsSymlog: This ablation
                removes the symlog encoding of inputs to the world model and also
                changes the symlog MSE loss in the decoder to a simple MSE loss.
                *Because symlog encoding is only used for vector observations*, this
                ablation is equivalent to DreamerV3 on purely image-based environments".
        """
        super().__init__()

        self.model_dimension = model_dimension
        self.batch_length_T = batch_length_T
        self.symlog_obs = symlog_obs
        self.action_space = action_space

        # Encoder + z-generator (x, h -> z).
        self.encoder = encoder

        # Posterior predictor: [h, encoder-out] -> z
        self.posterior_mlp = MLP(
            model_dimension=self.model_dimension,
            output_layer_size=None,
            # TODO: In Danijar's code, the posterior predictor only has a single layer,
            #  no matter the model size.
            num_dense_layers=1,
        )
        self.posterior_representation_layer = RepresentationLayer(
            model_dimension=self.model_dimension
        )

        # Dynamics (prior) predictor: h -> z^
        self.dynamics_predictor = DynamicsPredictor(
            model_dimension=self.model_dimension
        )

        # Initial state learner.
        self.num_gru_units = get_gru_units(
            model_dimension=self.model_dimension,
            override=num_gru_units,
        )
        self.initial_h = tf.Variable(
            tf.zeros(shape=(self.num_gru_units,), dtype=tf.float32),
            trainable=True,
        )
        # -> tanh(self.initial_h) -> deterministic state
        # Use our Dynamics predictor for initial stochastic state, BUT with greedy
        # (mode) instead of sampling.

        # Sequence Model (h-1, a-1, z-1 -> h).
        self.sequence_model = SequenceModel(
            model_dimension=self.model_dimension,
            action_space=self.action_space,
            num_gru_units=self.num_gru_units,
        )

        # Reward Predictor.
        self.reward_predictor = RewardPredictor(model_dimension=self.model_dimension)
        # Continue Predictor.
        self.continue_predictor = ContinuePredictor(
            model_dimension=self.model_dimension
        )

        # Decoder (h, z -> x^).
        self.decoder = decoder

        # Optimizer.
        self.optimizer = tf.keras.optimizers.Adam(learning_rate=1e-4, epsilon=1e-8)

    @tf.function
    def get_initial_state(self):
        h = tf.expand_dims(tf.math.tanh(self.initial_h), 0)
        # Use the mode, NOT a sample for the initial z-state.
        _, z_probs = self.dynamics_predictor(h, return_z_probs=True)
        z = tf.argmax(z_probs, axis=-1)
        z = tf.one_hot(z, depth=z_probs.shape[-1])

        return {"h": h, "z": z}

    @tf.function
    def call(self, inputs, *args, **kwargs):
        return self.forward_inference(inputs, *args, **kwargs)

    @tf.function#(experimental_relax_shapes=True)
    def forward_inference(self, previous_states, observations, is_first, training=None):
        """Performs a forward step for inference.

        Works analogous to `forward_train`, except that all inputs are provided
        for a single timestep in the shape of [B, ...] (no time dimension!).

        Args:
            observations: The batch (B, ...) of observations to be passed through
                the encoder network to yield the inputs to the representation layer
                (which then can compute the z-states).
            actions: The batch (B, ...) of actions to be used in combination with
                h-states and computed z-states to yield the next h-states.
            initial_h: The initial h-states (B, ...) (h(t)) to be
                used in combination with the observations to yield the
                z-states and then - in combination with the actions and z-states -
                to yield the next h-states (h(t+1)) via the RSSM.

        Returns:
            The next deterministic h-state (h(t+1)) as predicted by the sequence model.
        """
        initial_states = tree.map_structure(
            lambda s: tf.repeat(s, tf.shape(observations)[0], axis=0),
            self.get_initial_state()
        )

        # If first, mask it with initial state/actions.
        previous_h = self._mask(previous_states["h"], 1.0 - is_first)  # zero out
        previous_h = previous_h + self._mask(initial_states["h"], is_first)  # add init

        previous_z = self._mask(previous_states["z"], 1.0 - is_first)  # zero out
        previous_z = previous_z + self._mask(initial_states["z"], is_first)  # add init

        # Zero out actions (no special learnt initial state).
        previous_a = self._mask(previous_states["a"], 1.0 - is_first)

        # Compute new states.
        h = self.sequence_model(z=previous_z, a=previous_a, h=previous_h)
        z = self.compute_posterior_z(observations=observations, initial_h=h)

        return {"h": h, "z": z}

    @tf.function
    def forward_train(self, observations, actions, is_first, training=None):
        """Performs a forward step for training.

        1) Forwards all observations [B, T, ...] through the encoder network to yield
        o_processed[B, T, ...].
        2) Uses `initial_h` (h[B, 0, ...]) and o_processed[B, 0, ...] to
        compute z[B, 0, ...].
        3) Uses action a[B, 0, ...] and z[B, 0, ...] and h[B, 0, ...] to compute the
        next h-state (h[B, 1, ...]).
        4) Repeats 2) and 3) until t=T.
        5) Uses all h[B, T, ...] and z[B, T, ...] to compute predicted observations,
        rewards, and continue signals.
        6) Returns predictions from 5) along with all z-states z[B, T, ...] and
        the final h-state (h[B, ...] for t=T).

        Args:
            observations: The batch (B, T, ...) of observations to be passed through
                the encoder network to yield the inputs to the representation layer
                (which then can compute the z-states).
            actions: The batch (B, T, ...) of actions to be used in combination with
                h-states and computed z-states to yield the next h-states.
            is_first: The batch (B, T) of `is_first` flags.
        """
        if self.symlog_obs:
            observations = symlog(observations)

        # Compute bare encoder outs (not z; this is done later with involvement of the
        # sequence model and the h-states).
        # Fold time dimension for CNN pass.
        shape = tf.shape(observations)
        B, T = shape[0], shape[1]
        observations = tf.reshape(observations, shape=tf.concat([[-1], shape[2:]], axis=0))
        encoder_out = self.encoder(observations)
        # Unfold time dimension.
        encoder_out = tf.reshape(encoder_out, shape=tf.concat([[B, T], tf.shape(encoder_out)[1:]], axis=0))
        # Make time major for faster upcoming loop.
        encoder_out = tf.transpose(
            encoder_out, perm=[1, 0] + list(range(2, len(encoder_out.shape.as_list())))
        )
        # encoder_out=[T, B, ...]

        initial_states = tree.map_structure(
            lambda s: tf.repeat(s, B, axis=0),
            self.get_initial_state()
        )

        # Make actions and `is_first` time-major.
        actions = tf.transpose(
            actions,
            perm=[1, 0] + list(range(2, len(actions.shape.as_list()))),
        )
        is_first = tf.transpose(is_first, perm=[1, 0])

        # Loop through the T-axis of our samples and perform one computation step at
        # a time. This is necessary because the sequence model's output (h(t+1)) depends
        # on the current z(t), but z(t) depends on the current sequence model's output
        # h(t).
        z_t0_to_T = [initial_states["z"]]
        z_posterior_probs = []
        z_prior_probs = []
        h_t0_to_T = [initial_states["h"]]
        for t in range(self.batch_length_T):
            # If first, mask it with initial state/actions.
            h_tm1 = self._mask(h_t0_to_T[-1], 1.0 - is_first[t])  # zero out
            h_tm1 = h_tm1 + self._mask(initial_states["h"], is_first[t])  # add init

            z_tm1 = self._mask(z_t0_to_T[-1], 1.0 - is_first[t])  # zero out
            z_tm1 = z_tm1 + self._mask(initial_states["z"], is_first[t])  # add init

            # Zero out actions (no special learnt initial state).
            a_tm1 = self._mask(actions[t - 1], 1.0 - is_first[t])

            # Perform one RSSM (sequence model) step to get the current h.
            h_t = self.sequence_model(z=z_tm1, a=a_tm1, h=h_tm1)
            h_t0_to_T.append(h_t)

            posterior_mlp_input = tf.concat([encoder_out[t], h_t], axis=-1)
            repr_input = self.posterior_mlp(posterior_mlp_input)
            # Draw one z-sample (z(t)) and also get the z-distribution for dynamics and
            # representation loss computations.
            z_t, z_probs = self.posterior_representation_layer(
                repr_input,
                return_z_probs=True,
            )
            # z_t=[B, num_categoricals, num_classes]
            z_posterior_probs.append(z_probs)
            z_t0_to_T.append(z_t)

            # Compute the predicted z_t (z^) using the dynamics model.
            _, z_probs = self.dynamics_predictor(h_t, return_z_probs=True)
            z_prior_probs.append(z_probs)

        # Stack at time dimension to yield: [B, T, ...].
        h_t1_to_T = tf.stack(h_t0_to_T[1:], axis=1)
        z_t1_to_T = tf.stack(z_t0_to_T[1:], axis=1)

        # Fold time axis to retrieve the final (loss ready) Independent distribution
        # (over `num_categoricals` Categoricals).
        z_posterior_probs = tf.stack(z_posterior_probs, axis=1)
        z_posterior_probs = tf.reshape(
            z_posterior_probs,
            shape=[-1] + z_posterior_probs.shape.as_list()[2:],
        )
        # Fold time axis to retrieve the final (loss ready) Independent distribution
        # (over `num_categoricals` Categoricals).
        z_prior_probs = tf.stack(z_prior_probs, axis=1)
        z_prior_probs = tf.reshape(
            z_prior_probs,
            shape=[-1] + z_prior_probs.shape.as_list()[2:],
        )

        # Fold time dimension for parallelization of all dependent predictions:
        # observations (reproduction via decoder), rewards, continues.
        h_BxT = tf.reshape(h_t1_to_T, shape=[-1] + h_t1_to_T.shape.as_list()[2:])
        z_BxT = tf.reshape(z_t1_to_T, shape=[-1] + z_t1_to_T.shape.as_list()[2:])

        _, obs_distribution = self.decoder(h=h_BxT, z=z_BxT)

        # Compute (predicted) reward distributions.
        rewards, reward_logits = self.reward_predictor(
            h=h_BxT, z=z_BxT, return_logits=True
        )

        # Compute (predicted) continue distributions.
        continues, continue_distribution = self.continue_predictor(
            h=h_BxT, z=z_BxT, return_distribution=True
        )

        # Return outputs for loss computation.
        # Note that all shapes are [B, ...] (no time axis).
        return {
            "sampled_obs_symlog_BxT": observations,
            "obs_distribution_BxT": obs_distribution,
            "reward_logits_BxT": reward_logits,
            "rewards_BxT": rewards,
            "continue_distribution_BxT": continue_distribution,
            "continues_BxT": continues,
            "z_posterior_probs_BxT": z_posterior_probs,
            "z_prior_probs_BxT": z_prior_probs,
            # Deterministic, continuous h-states (t1 to T).
            "h_states_BxT": h_BxT,
            # Sampled, discrete z-states (t1 to T).
            "z_states_BxT": z_BxT,
        }

    @tf.function
    def compute_posterior_z(self, observations, initial_h):
        # Compute bare encoder outs (not z; this done in next step with involvement of
        # the previous output (initial_h) of the sequence model).
        # encoder_outs=[B, ...]
        if self.symlog_obs:
            observations = symlog(observations)
        encoder_out = self.encoder(observations)
        # Concat encoder outs with the h-states.
        posterior_mlp_input = tf.concat([encoder_out, initial_h], axis=-1)
        # Compute z.
        repr_input = self.posterior_mlp(posterior_mlp_input)
        # Draw one z-sample (no need to return the distribution here).
        z_t = self.posterior_representation_layer(repr_input, return_z_probs=False)
        return z_t

    @staticmethod
    def _mask(value, mask):
        return tf.einsum("b...,b->b...", value, tf.cast(mask, value.dtype))
