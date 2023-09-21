"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.algorithms.dreamerv3.tf.models.components.continue_predictor import (
    ContinuePredictor,
)
from ray.rllib.algorithms.dreamerv3.tf.models.components.dynamics_predictor import (
    DynamicsPredictor,
)
from ray.rllib.algorithms.dreamerv3.tf.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.tf.models.components.representation_layer import (
    RepresentationLayer,
)
from ray.rllib.algorithms.dreamerv3.tf.models.components.reward_predictor import (
    RewardPredictor,
)
from ray.rllib.algorithms.dreamerv3.tf.models.components.sequence_model import (
    SequenceModel,
)
from ray.rllib.algorithms.dreamerv3.utils import get_gru_units
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.tf_utils import symlog


_, tf, _ = try_import_tf()


class WorldModel(tf.keras.Model):
    """WorldModel component of [1] w/ encoder, decoder, RSSM, reward/cont. predictors.

    See eq. 3 of [1] for all components and their respective in- and outputs.
    Note that in the paper, the "encoder" includes both the raw encoder plus the
    "posterior net", which produces posterior z-states from observations and h-states.

    Note: The "internal state" of the world model always consists of:
    The actions `a` (initially, this is a zeroed-out action), `h`-states (deterministic,
    continuous), and `z`-states (stochastic, discrete).
    There are two versions of z-states: "posterior" for world model training and "prior"
    for creating the dream data.

    Initial internal state values (`a`, `h`, and `z`) are inserted where ever a new
    episode starts within a batch row OR at the beginning of each train batch's B rows,
    regardless of whether there was an actual episode boundary or not. Thus, internal
    states are not required to be stored in or retrieved from the replay buffer AND
    retrieved batches from the buffer must not be zero padded.

    Initial `a` is the zero "one hot" action, e.g. [0.0, 0.0] for Discrete(2), initial
    `h` is a separate learned variable, and initial `z` are computed by the "dynamics"
    (or "prior") net, using only the initial-h state as input.
    """

    def __init__(
        self,
        *,
        model_size: str = "XS",
        observation_space: gym.Space,
        action_space: gym.Space,
        batch_length_T: int = 64,
        encoder: tf.keras.Model,
        decoder: tf.keras.Model,
        num_gru_units: Optional[int] = None,
        symlog_obs: bool = True,
    ):
        """Initializes a WorldModel instance.

        Args:
             model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             observation_space: The observation space of the environment used.
             action_space: The action space of the environment used.
             batch_length_T: The length (T) of the sequences used for training. The
                actual shape of the input data (e.g. rewards) is then: [B, T, ...],
                where B is the "batch size", T is the "batch length" (this arg) and
                "..." is the dimension of the data (e.g. (64, 64, 3) for Atari image
                observations). Note that a single row (within a batch) may contain data
                from different episodes, but an already on-going episode is always
                finished, before a new one starts within the same row.
            encoder: The encoder Model taking observations as inputs and
                outputting a 1D latent vector that will be used as input into the
                posterior net (z-posterior state generating layer). Inputs are symlogged
                if inputs are NOT images. For images, we use normalization between -1.0
                and 1.0 (x / 128 - 1.0)
            decoder: The decoder Model taking h- and z-states as inputs and generating
                a (possibly symlogged) predicted observation. Note that for images,
                the last decoder layer produces the exact, normalized pixel values
                (not a Gaussian as described in [1]!).
            num_gru_units: The number of GRU units to use. If None, use
                `model_size` to figure out this parameter.
            symlog_obs: Whether to predict decoded observations in symlog space.
                This should be False for image based observations.
                According to the paper [1] Appendix E: "NoObsSymlog: This ablation
                removes the symlog encoding of inputs to the world model and also
                changes the symlog MSE loss in the decoder to a simple MSE loss.
                *Because symlog encoding is only used for vector observations*, this
                ablation is equivalent to DreamerV3 on purely image-based environments".
        """
        super().__init__(name="world_model")

        self.model_size = model_size
        self.batch_length_T = batch_length_T
        self.symlog_obs = symlog_obs
        self.observation_space = observation_space
        self.action_space = action_space
        self._comp_dtype = (
            tf.keras.mixed_precision.global_policy().compute_dtype or tf.float32
        )

        # Encoder (latent 1D vector generator) (xt -> lt).
        self.encoder = encoder

        # Posterior predictor consisting of an MLP and a RepresentationLayer:
        # [ht, lt] -> zt.
        self.posterior_mlp = MLP(
            model_size=self.model_size,
            output_layer_size=None,
            # In Danijar's code, the posterior predictor only has a single layer,
            # no matter the model size:
            num_dense_layers=1,
            name="posterior_mlp",
        )
        # The (posterior) z-state generating layer.
        self.posterior_representation_layer = RepresentationLayer(
            model_size=self.model_size,
        )

        # Dynamics (prior z-state) predictor: ht -> z^t
        self.dynamics_predictor = DynamicsPredictor(model_size=self.model_size)

        # GRU for the RSSM: [at, ht, zt] -> ht+1
        self.num_gru_units = get_gru_units(
            model_size=self.model_size,
            override=num_gru_units,
        )
        # Initial h-state variable (learnt).
        # -> tanh(self.initial_h) -> deterministic state
        # Use our Dynamics predictor for initial stochastic state, BUT with greedy
        # (mode) instead of sampling.
        self.initial_h = tf.Variable(
            tf.zeros(shape=(self.num_gru_units,)),
            trainable=True,
            name="initial_h",
        )
        # The actual sequence model containing the GRU layer.
        self.sequence_model = SequenceModel(
            model_size=self.model_size,
            action_space=self.action_space,
            num_gru_units=self.num_gru_units,
        )

        # Reward Predictor: [ht, zt] -> rt.
        self.reward_predictor = RewardPredictor(model_size=self.model_size)
        # Continue Predictor: [ht, zt] -> ct.
        self.continue_predictor = ContinuePredictor(model_size=self.model_size)

        # Decoder: [ht, zt] -> x^t.
        self.decoder = decoder

        # Trace self.call.
        self.forward_train = tf.function(
            input_signature=[
                tf.TensorSpec(shape=[None, None] + list(self.observation_space.shape)),
                tf.TensorSpec(
                    shape=[None, None]
                    + (
                        [self.action_space.n]
                        if isinstance(action_space, gym.spaces.Discrete)
                        else list(self.action_space.shape)
                    )
                ),
                tf.TensorSpec(shape=[None, None], dtype=tf.bool),
            ]
        )(self.forward_train)

    @tf.function
    def get_initial_state(self):
        """Returns the (current) initial state of the world model (h- and z-states).

        An initial state is generated using the tanh of the (learned) h-state variable
        and the dynamics predictor (or "prior net") to compute z^0 from h0. In this last
        step, it is important that we do NOT sample the z^-state (as we would usually
        do during dreaming), but rather take the mode (argmax, then one-hot again).
        """
        h = tf.expand_dims(tf.math.tanh(tf.cast(self.initial_h, self._comp_dtype)), 0)
        # Use the mode, NOT a sample for the initial z-state.
        _, z_probs = self.dynamics_predictor(h)
        z = tf.argmax(z_probs, axis=-1)
        z = tf.one_hot(z, depth=z_probs.shape[-1], dtype=self._comp_dtype)

        return {"h": h, "z": z}

    def forward_inference(self, observations, previous_states, is_first, training=None):
        """Performs a forward step for inference (e.g. environment stepping).

        Works analogous to `forward_train`, except that all inputs are provided
        for a single timestep in the shape of [B, ...] (no time dimension!).

        Args:
            observations: The batch (B, ...) of observations to be passed through
                the encoder network to yield the inputs to the representation layer
                (which then can compute the z-states).
            previous_states: A dict with `h`, `z`, and `a` keys mapping to the
                respective previous states/actions. All of the shape (B, ...), no time
                rank.
            is_first: The batch (B) of `is_first` flags.

        Returns:
            The next deterministic h-state (h(t+1)) as predicted by the sequence model.
        """
        observations = tf.cast(observations, self._comp_dtype)

        initial_states = tree.map_structure(
            lambda s: tf.repeat(s, tf.shape(observations)[0], axis=0),
            self.get_initial_state(),
        )

        # If first, mask it with initial state/actions.
        previous_h = self._mask(previous_states["h"], 1.0 - is_first)  # zero out
        previous_h = previous_h + self._mask(initial_states["h"], is_first)  # add init

        previous_z = self._mask(previous_states["z"], 1.0 - is_first)  # zero out
        previous_z = previous_z + self._mask(initial_states["z"], is_first)  # add init

        # Zero out actions (no special learnt initial state).
        previous_a = self._mask(previous_states["a"], 1.0 - is_first)

        # Compute new states.
        h = self.sequence_model(a=previous_a, h=previous_h, z=previous_z)
        z = self.compute_posterior_z(observations=observations, initial_h=h)

        return {"h": h, "z": z}

    def forward_train(self, observations, actions, is_first):
        """Performs a forward step for training.

        1) Forwards all observations [B, T, ...] through the encoder network to yield
        o_processed[B, T, ...].
        2) Uses initial state (h0/z^0/a0[B, 0, ...]) and sequence model (RSSM) to
        compute the first internal state (h1 and z^1).
        3) Uses action a[B, 1, ...], z[B, 1, ...] and h[B, 1, ...] to compute the
        next h-state (h[B, 2, ...]), etc..
        4) Repeats 2) and 3) until t=T.
        5) Uses all h[B, T, ...] and z[B, T, ...] to compute predicted/reconstructed
        observations, rewards, and continue signals.
        6) Returns predictions from 5) along with all z-states z[B, T, ...] and
        the final h-state (h[B, ...] for t=T).

        Should we encounter is_first=True flags in the middle of a batch row (somewhere
        within an ongoing sequence of length T), we insert this world model's initial
        state again (zero-action, learned init h-state, and prior-computed z^) and
        simply continue (no zero-padding).

        Args:
            observations: The batch (B, T, ...) of observations to be passed through
                the encoder network to yield the inputs to the representation layer
                (which then can compute the posterior z-states).
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
        observations = tf.reshape(
            observations, shape=tf.concat([[-1], shape[2:]], axis=0)
        )

        encoder_out = self.encoder(tf.cast(observations, self._comp_dtype))
        # Unfold time dimension.
        encoder_out = tf.reshape(
            encoder_out, shape=tf.concat([[B, T], tf.shape(encoder_out)[1:]], axis=0)
        )
        # Make time major for faster upcoming loop.
        encoder_out = tf.transpose(
            encoder_out, perm=[1, 0] + list(range(2, len(encoder_out.shape.as_list())))
        )
        # encoder_out=[T, B, ...]

        initial_states = tree.map_structure(
            lambda s: tf.repeat(s, B, axis=0), self.get_initial_state()
        )

        # Make actions and `is_first` time-major.
        actions = tf.transpose(
            tf.cast(actions, self._comp_dtype),
            perm=[1, 0] + list(range(2, tf.shape(actions).shape.as_list()[0])),
        )
        is_first = tf.transpose(tf.cast(is_first, self._comp_dtype), perm=[1, 0])

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
            h_t = self.sequence_model(a=a_tm1, h=h_tm1, z=z_tm1)
            h_t0_to_T.append(h_t)

            posterior_mlp_input = tf.concat([encoder_out[t], h_t], axis=-1)
            repr_input = self.posterior_mlp(posterior_mlp_input)
            # Draw one z-sample (z(t)) and also get the z-distribution for dynamics and
            # representation loss computations.
            z_t, z_probs = self.posterior_representation_layer(repr_input)
            # z_t=[B, num_categoricals, num_classes]
            z_posterior_probs.append(z_probs)
            z_t0_to_T.append(z_t)

            # Compute the predicted z_t (z^) using the dynamics model.
            _, z_probs = self.dynamics_predictor(h_t)
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

        obs_distribution_means = tf.cast(self.decoder(h=h_BxT, z=z_BxT), tf.float32)

        # Compute (predicted) reward distributions.
        rewards, reward_logits = self.reward_predictor(h=h_BxT, z=z_BxT)

        # Compute (predicted) continue distributions.
        continues, continue_distribution = self.continue_predictor(h=h_BxT, z=z_BxT)

        # Return outputs for loss computation.
        # Note that all shapes are [BxT, ...] (time axis already folded).
        return {
            # Obs.
            "sampled_obs_symlog_BxT": observations,
            "obs_distribution_means_BxT": obs_distribution_means,
            # Rewards.
            "reward_logits_BxT": reward_logits,
            "rewards_BxT": rewards,
            # Continues.
            "continue_distribution_BxT": continue_distribution,
            "continues_BxT": continues,
            # Deterministic, continuous h-states (t1 to T).
            "h_states_BxT": h_BxT,
            # Sampled, discrete posterior z-states and their probs (t1 to T).
            "z_posterior_states_BxT": z_BxT,
            "z_posterior_probs_BxT": z_posterior_probs,
            # Probs of the prior z-states (t1 to T).
            "z_prior_probs_BxT": z_prior_probs,
        }

    def compute_posterior_z(self, observations, initial_h):
        # Compute bare encoder outputs (not including z, which is computed in next step
        # with involvement of the previous output (initial_h) of the sequence model).
        # encoder_outs=[B, ...]
        if self.symlog_obs:
            observations = symlog(observations)
        encoder_out = self.encoder(observations)
        # Concat encoder outs with the h-states.
        posterior_mlp_input = tf.concat([encoder_out, initial_h], axis=-1)
        # Compute z.
        repr_input = self.posterior_mlp(posterior_mlp_input)
        # Draw a z-sample.
        z_t, _ = self.posterior_representation_layer(repr_input)
        return z_t

    @staticmethod
    def _mask(value, mask):
        return tf.einsum("b...,b->b...", value, tf.cast(mask, value.dtype))
