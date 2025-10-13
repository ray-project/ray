"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
from typing import Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.algorithms.dreamerv3.torch.models.components.continue_predictor import (
    ContinuePredictor,
)
from ray.rllib.algorithms.dreamerv3.torch.models.components.dynamics_predictor import (
    DynamicsPredictor,
)
from ray.rllib.algorithms.dreamerv3.torch.models.components.mlp import MLP
from ray.rllib.algorithms.dreamerv3.torch.models.components import (
    representation_layer,
)
from ray.rllib.algorithms.dreamerv3.torch.models.components.reward_predictor import (
    RewardPredictor,
)
from ray.rllib.algorithms.dreamerv3.torch.models.components.sequence_model import (
    SequenceModel,
)
from ray.rllib.algorithms.dreamerv3.utils import get_dense_hidden_units, get_gru_units
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import symlog

torch, nn = try_import_torch()
if torch:
    F = nn.functional


class WorldModel(nn.Module):
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
        encoder: nn.Module,
        decoder: nn.Module,
        num_gru_units: Optional[int] = None,
        symlog_obs: bool = True,
    ):
        """Initializes a WorldModel instance.

        Args:
             model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             action_space: The action space the our environment used.
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
        super().__init__()

        self.model_size = model_size
        self.batch_length_T = batch_length_T
        self.symlog_obs = symlog_obs
        self.action_space = action_space
        a_flat = (
            action_space.n
            if isinstance(action_space, gym.spaces.Discrete)
            else (np.prod(action_space.shape))
        )

        # Encoder (latent 1D vector generator) (xt -> lt).
        self.encoder = encoder

        self.num_gru_units = get_gru_units(
            model_size=self.model_size,
            override=num_gru_units,
        )

        # Posterior predictor consisting of an MLP and a RepresentationLayer:
        # [ht, lt] -> zt.
        # In Danijar's code, this is called: `obs_out`.
        self.posterior_mlp = MLP(
            input_size=(self.num_gru_units + encoder.output_size[0]),
            model_size=self.model_size,
            output_layer_size=None,
            # In Danijar's code, the posterior predictor only has a single layer,
            # no matter the model size:
            num_dense_layers=1,
        )
        # The (posterior) z-state generating layer.
        # In Danijar's code, this is called: `obs_stats`.
        self.posterior_representation_layer = representation_layer.RepresentationLayer(
            input_size=get_dense_hidden_units(self.model_size),
            model_size=self.model_size,
        )

        z_flat = (
            self.posterior_representation_layer.num_categoricals
            * self.posterior_representation_layer.num_classes_per_categorical
        )
        h_plus_z_flat = self.num_gru_units + z_flat

        # Dynamics (prior z-state) predictor: ht -> z^t
        # In Danijar's code, the layers in this network are called:
        # `img_out` (1 Linear) and `img_stats` (representation layer).
        self.dynamics_predictor = DynamicsPredictor(
            input_size=self.num_gru_units, model_size=self.model_size
        )

        # GRU for the RSSM: [at, ht, zt] -> ht+1
        # Initial h-state variable (learnt).
        # -> tanh(self.initial_h) -> deterministic state
        # Use our Dynamics predictor for initial stochastic state, BUT with greedy
        # (mode) instead of sampling.
        self.initial_h = nn.Parameter(
            torch.zeros(self.num_gru_units), requires_grad=True
        )
        # The actual sequence model containing the GRU layer.
        # In Danijar's code, the layers in this network are called:
        # `img_in` (1 Linear) and `gru` (custom GRU implementation).
        self.sequence_model = SequenceModel(
            # Only z- and a-state go into pre-layer. The output of that goes then
            # into GRU (together with h-state).
            input_size=int(z_flat + a_flat),
            model_size=self.model_size,
            action_space=self.action_space,
            num_gru_units=self.num_gru_units,
        )

        # Reward Predictor: [ht, zt] -> rt.
        self.reward_predictor = RewardPredictor(
            input_size=h_plus_z_flat,
            model_size=self.model_size,
        )
        # Continue Predictor: [ht, zt] -> ct.
        self.continue_predictor = ContinuePredictor(
            input_size=h_plus_z_flat,
            model_size=self.model_size,
        )

        # Decoder: [ht, zt] -> x^t.
        self.decoder = decoder

    def get_initial_state(self) -> dict:
        """Returns the (current) initial state of the world model (h- and z-states).

        An initial state is generated using the tanh of the (learned) h-state variable
        and the dynamics predictor (or "prior net") to compute z^0 from h0. In this last
        step, it is important that we do NOT sample the z^-state (as we would usually
        do during dreaming), but rather take the mode (argmax, then one-hot again).
        """
        h = torch.tanh(self.initial_h)
        # Use the mode, NOT a sample for the initial z-state.
        _, z_probs = self.dynamics_predictor(h.unsqueeze(0), return_z_probs=True)
        z = z_probs.squeeze(0).argmax(dim=-1)
        z = F.one_hot(z, num_classes=z_probs.shape[-1])

        return {"h": h, "z": z}

    def forward_inference(
        self,
        observations: "torch.Tensor",
        previous_states: dict,
        is_first: "torch.Tensor",
    ) -> dict:
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
        B = observations.shape[0]
        initial_states = tree.map_structure(
            # Repeat only the batch dimension (B times).
            lambda s: s.unsqueeze(0).repeat(B, *([1] * len(s.shape))),
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

    def forward_train(
        self,
        observations: "torch.Tensor",
        actions: "torch.Tensor",
        is_first: "torch.Tensor",
    ) -> dict:
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
        shape = observations.shape
        B, T = shape[0], shape[1]
        observations = observations.view((-1,) + shape[2:])
        encoder_out = self.encoder(observations)
        # Unfold time dimension.
        encoder_out = encoder_out.view(
            (
                B,
                T,
            )
            + encoder_out.shape[1:]
        )
        # Make time major for faster upcoming loop.
        encoder_out = encoder_out.transpose(0, 1)
        # encoder_out=[T, B, ...]

        initial_states = tree.map_structure(
            # Repeat only the batch dimension (B times).
            lambda s: s.unsqueeze(0).repeat(B, *([1] * len(s.shape))),
            self.get_initial_state(),
        )

        # Make actions and `is_first` time-major.
        actions = actions.transpose(0, 1)
        is_first = is_first.transpose(0, 1).float()

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

            posterior_mlp_input = torch.cat([encoder_out[t], h_t], dim=-1)
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
        h_t1_to_T = torch.stack(h_t0_to_T[1:], dim=1)
        z_t1_to_T = torch.stack(z_t0_to_T[1:], dim=1)

        # Fold time axis to retrieve the final (loss ready) Independent distribution
        # (over `num_categoricals` Categoricals).
        z_posterior_probs = torch.stack(z_posterior_probs, dim=1)
        z_posterior_probs = z_posterior_probs.view(
            (-1,) + z_posterior_probs.shape[2:],
        )
        # Fold time axis to retrieve the final (loss ready) Independent distribution
        # (over `num_categoricals` Categoricals).
        z_prior_probs = torch.stack(z_prior_probs, dim=1)
        z_prior_probs = z_prior_probs.view((-1,) + z_prior_probs.shape[2:])

        # Fold time dimension for parallelization of all dependent predictions:
        # observations (reproduction via decoder), rewards, continues.
        h_BxT = h_t1_to_T.view((-1,) + h_t1_to_T.shape[2:])
        z_BxT = z_t1_to_T.view((-1,) + z_t1_to_T.shape[2:])

        obs_distribution_means = self.decoder(h=h_BxT, z=z_BxT)

        # Compute (predicted) reward distributions.
        rewards, reward_logits = self.reward_predictor(
            h=h_BxT, z=z_BxT, return_logits=True
        )

        # Compute (predicted) continue distributions.
        continues, continue_distribution = self.continue_predictor(
            h=h_BxT, z=z_BxT, return_distribution=True
        )

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

    def compute_posterior_z(
        self, observations: "torch.Tensor", initial_h: "torch.Tensor"
    ) -> "torch.Tensor":
        # Fold time dimension for possible CNN pass.
        shape = observations.shape
        observations = observations.view((-1,) + shape[2:])
        # Compute bare encoder outputs (not including z, which is computed in next step
        # with involvement of the previous output (initial_h) of the sequence model).
        # encoder_outs=[B, ...]
        if self.symlog_obs:
            observations = symlog(observations)
        encoder_out = self.encoder(observations)
        # Concat encoder outs with the h-states.
        posterior_mlp_input = torch.cat([encoder_out, initial_h], dim=-1)
        # Compute z.
        repr_input = self.posterior_mlp(posterior_mlp_input)
        # Draw one z-sample (no need to return the distribution here).
        z_t = self.posterior_representation_layer(repr_input, return_z_probs=False)
        return z_t

    @staticmethod
    def _mask(value: "torch.Tensor", mask: "torch.Tensor") -> "torch.Tensor":
        return torch.einsum("b...,b->b...", value, mask)
