"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
import re

import gymnasium as gym
import numpy as np

from ray.rllib.algorithms.dreamerv3.torch.models.actor_network import ActorNetwork
from ray.rllib.algorithms.dreamerv3.torch.models.critic_network import CriticNetwork
from ray.rllib.algorithms.dreamerv3.torch.models.world_model import WorldModel
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.torch_utils import inverse_symlog

torch, nn = try_import_torch()


class DreamerModel(nn.Module):
    """The main PyTorch model containing all necessary components for DreamerV3.

    Includes:
    - The world model with encoder, decoder, sequence-model (RSSM), dynamics
    (generates prior z-state), and "posterior" model (generates posterior z-state).
    Predicts env dynamics and produces dreamed trajectories for actor- and critic
    learning.
    - The actor network (policy).
    - The critic network for value function prediction.
    """

    def __init__(
        self,
        *,
        model_size: str = "XS",
        action_space: gym.Space,
        world_model: WorldModel,
        actor: ActorNetwork,
        critic: CriticNetwork,
        use_curiosity: bool = False,
        intrinsic_rewards_scale: float = 0.1,
    ):
        """Initializes a DreamerModel instance.

        Args:
             model_size: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             action_space: The action space the our environment used.
             world_model: The WorldModel component.
             actor: The ActorNetwork component.
             critic: The CriticNetwork component.
        """
        super().__init__()

        self.model_size = model_size
        self.action_space = action_space
        self.use_curiosity = use_curiosity

        self.world_model = world_model
        self.actor = actor
        self.critic = critic

        self.disagree_nets = None
        if self.use_curiosity:
            raise NotImplementedError

    def forward_inference(self, observations, previous_states, is_first):
        """Performs a (non-exploring) action computation step given obs and states.

        Note that all input data should not have a time rank (only a batch dimension).

        Args:
            observations: The current environment observation with shape (B, ...).
            previous_states: Dict with keys `a`, `h`, and `z` used as input to the RSSM
                to produce the next h-state, from which then to compute the action
                using the actor network. All values in the dict should have shape
                (B, ...) (no time rank).
            is_first: Batch of is_first flags. These should be True if a new episode
                has been started at the current timestep (meaning `observations` is the
                reset observation from the environment).
        """
        # Perform one step in the world model (starting from `previous_state` and
        # using the observations to yield a current (posterior) state).
        states = self.world_model.forward_inference(
            observations=observations,
            previous_states=previous_states,
            is_first=is_first,
        )
        # Compute action using our actor network and the current states.
        _, distr_params = self.actor(
            h=states["h"], z=states["z"], return_distr_params=True
        )
        # Use the mode of the distribution (Discrete=argmax, Normal=mean).
        distr = self.actor.get_action_dist_object(distr_params)
        actions = distr.mode
        return actions, {"h": states["h"], "z": states["z"], "a": actions}

    def forward_exploration(self, observations, previous_states, is_first):
        """Performs an exploratory action computation step given obs and states.

        Note that all input data should not have a time rank (only a batch dimension).

        Args:
            observations: The current environment observation with shape (B, ...).
            previous_states: Dict with keys `a`, `h`, and `z` used as input to the RSSM
                to produce the next h-state, from which then to compute the action
                using the actor network. All values in the dict should have shape
                (B, ...) (no time rank).
            is_first: Batch of is_first flags. These should be True if a new episode
                has been started at the current timestep (meaning `observations` is the
                reset observation from the environment).
        """
        # Perform one step in the world model (starting from `previous_state` and
        # using the observations to yield a current (posterior) state).
        states = self.world_model.forward_inference(
            observations=observations,
            previous_states=previous_states,
            is_first=is_first,
        )
        # Compute action using our actor network and the current states.
        actions = self.actor(h=states["h"], z=states["z"])
        return actions, {"h": states["h"], "z": states["z"], "a": actions}

    def forward_train(self, observations, actions, is_first):
        """Performs a training forward pass given observations and actions.

        Note that all input data must have a time rank (batch-major: [B, T, ...]).

        Args:
            observations: The environment observations with shape (B, T, ...). Thus,
                the batch has B rows of T timesteps each. Note that it's ok to have
                episode boundaries (is_first=True) within a batch row. DreamerV3 will
                simply insert an initial state before these locations and continue the
                sequence modelling (with the RSSM). Hence, there will be no zero
                padding.
            actions: The actions actually taken in the environment with shape
                (B, T, ...). See `observations` docstring for details on how B and T are
                handled.
            is_first: Batch of is_first flags. These should be True:
                - if a new episode has been started at the current timestep (meaning
                `observations` is the reset observation from the environment).
                - in each batch row at T=0 (first timestep of each of the B batch
                rows), regardless of whether the actual env had an episode boundary
                there or not.
        """
        return self.world_model.forward_train(
            observations=observations,
            actions=actions,
            is_first=is_first,
        )

    def get_initial_state(self):
        """Returns the initial state of the dreamer model (a, h-, z-states).

        An initial state is generated using the previous action, the tanh of the
        (learned) h-state variable and the dynamics predictor (or "prior net") to
        compute z^0 from h0. In this last step, it is important that we do NOT sample
        the z^-state (as we would usually do during dreaming), but rather take the mode
        (argmax, then one-hot again).

        Note that the initial state is returned without batch dimension.
        """
        states = self.world_model.get_initial_state()

        action_dim = (
            self.action_space.n
            if isinstance(self.action_space, gym.spaces.Discrete)
            else np.prod(self.action_space.shape)
        )
        states["a"] = torch.zeros((action_dim,), dtype=torch.float32)
        return states

    def dream_trajectory(self, start_states, start_is_terminated, timesteps_H, gamma):
        """Dreams trajectories of length H from batch of h- and z-states.

        Note that incoming data will have the shapes (BxT, ...), where the original
        batch- and time-dimensions are already folded together. Beginning from this
        new batch dim (BxT), we will unroll `timesteps_H` timesteps in a time-major
        fashion, such that the dreamed data will have shape (H, BxT, ...).

        Args:
            start_states: Dict of `h` and `z` states in the shape of (B, ...) and
                (B, num_categoricals, num_classes), respectively, as
                computed by a train forward pass. From each individual h-/z-state pair
                in the given batch, we will branch off a dreamed trajectory of len
                `timesteps_H`.
            start_is_terminated: Float flags of shape (B,) indicating whether the
                first timesteps of each batch row is already a terminated timestep
                (given by the actual environment).
            timesteps_H: The number of timesteps to dream for.
            gamma: The discount factor gamma.
        """
        # Dreamed actions (one-hot encoded for discrete actions).
        a_dreamed_t0_to_H = []
        a_dreamed_dist_params_t0_to_H = []

        h = start_states["h"].detach()
        z = start_states["z"].detach()

        # GRU outputs.
        h_states_t0_to_H = [h]
        # Dynamics model outputs.
        z_states_prior_t0_to_H = [z]

        # Compute `a` using actor network (already the first step uses a dreamed action,
        # not a sampled one).
        a, a_dist_params = self.actor(
            # We have to stop the gradients through the states. B/c we are using a
            # differentiable Discrete action distribution (straight through gradients
            # with `a = stop_gradient(sample(probs)) + probs - stop_gradient(probs)`,
            # we otherwise would add dependencies of the `-log(pi(a|s))` REINFORCE loss
            # term on actions further back in the trajectory.
            h=h.detach(),
            z=z.detach(),
            return_distr_params=True,
        )
        a_dreamed_t0_to_H.append(a)
        a_dreamed_dist_params_t0_to_H.append(a_dist_params)

        # Disable all gradients from the world model so they don't get backprop'd
        # through twice when computing the actor loss (for cont. actions).
        for p in self.world_model.parameters():
            p.requires_grad_(False)

        for i in range(timesteps_H):
            # Move one step in the dream using the RSSM.
            h = self.world_model.sequence_model(a=a, h=h, z=z)
            h_states_t0_to_H.append(h)

            # Compute prior z using dynamics model.
            z = self.world_model.dynamics_predictor(h=h)
            z_states_prior_t0_to_H.append(z)

            # Compute `a` using actor network.
            a, a_dist_params = self.actor(
                h=h.detach(),
                z=z.detach(),
                return_distr_params=True,
            )
            a_dreamed_t0_to_H.append(a)
            a_dreamed_dist_params_t0_to_H.append(a_dist_params)

        h_states_H_B = torch.stack(h_states_t0_to_H, dim=0)  # (T, B, ...)
        h_states_HxB = h_states_H_B.reshape([-1] + list(h_states_H_B.shape[2:]))

        z_states_prior_H_B = torch.stack(z_states_prior_t0_to_H, dim=0)  # (T, B, ...)
        z_states_prior_HxB = z_states_prior_H_B.reshape(
            [-1] + list(z_states_prior_H_B.shape[2:])
        )

        a_dreamed_H_B = torch.stack(a_dreamed_t0_to_H, dim=0)  # (T, B, ...)
        a_dreamed_dist_params_H_B = torch.stack(a_dreamed_dist_params_t0_to_H, dim=0)

        # Compute r using reward predictor.
        r_dreamed_H_B = inverse_symlog(
            self.world_model.reward_predictor(h=h_states_HxB, z=z_states_prior_HxB)
        )
        r_dreamed_H_B = r_dreamed_H_B.reshape([timesteps_H + 1, -1])

        # Compute intrinsic rewards.
        if self.use_curiosity:
            results_HxB = self.disagree_nets.compute_intrinsic_rewards(
                h=h_states_HxB,
                z=z_states_prior_HxB,
                a=a_dreamed_H_B.reshape([-1] + a_dreamed_H_B.shape[2:]),
            )
            r_intrinsic_H_B = results_HxB["rewards_intrinsic"]
            r_intrinsic_H_B = r_intrinsic_H_B.reshape([timesteps_H + 1, -1])[1:]
            curiosity_forward_train_outs = results_HxB["forward_train_outs"]
            del results_HxB

        # Compute continues using continue predictor.
        c_dreamed_HxB = self.world_model.continue_predictor(
            h=h_states_HxB,
            z=z_states_prior_HxB,
        )
        c_dreamed_H_B = c_dreamed_HxB.reshape([timesteps_H + 1, -1])
        # Force-set first `continue` flags to False iff `start_is_terminated`.
        # Note: This will cause the loss-weights for this row in the batch to be
        # completely zero'd out. In general, we don't use dreamed data past any
        # predicted (or actual first) continue=False flags.
        c_dreamed_H_B = torch.cat(
            [1.0 - start_is_terminated.unsqueeze(0).float(), c_dreamed_H_B[1:]], dim=0
        )

        # Loss weights for each individual dreamed timestep. Zero-out all timesteps
        # that lie past continue=False flags. B/c our world model does NOT learn how
        # to skip terminal/reset episode boundaries, dreamed data crossing such a
        # boundary should not be used for critic/actor learning either.
        dream_loss_weights_H_B = torch.cumprod(gamma * c_dreamed_H_B, dim=0) / gamma

        # Reactivate world model gradients.
        for p in self.world_model.parameters():
            p.requires_grad_(True)

        # Compute the symlog'd value logits (w/o world model gradients; used for the
        # critic loss).
        _, v_symlog_dreamed_logits_HxB_wm_detached = self.critic(
            h=h_states_HxB.detach(),
            z=z_states_prior_HxB.detach(),
            use_ema=False,
            return_logits=True,
        )

        # Compute the value estimates (including world model gradients -> 1 sequence
        # model step after the action has been computed; used for the scaled value
        # target used in the actor loss for cont. actions).
        # Disable all gradients from the critic so they don't get backprop'd
        # through twice when computing the actor loss (for cont. actions).
        for p in self.critic.parameters():
            p.requires_grad_(False)
        v, _ = self.critic(
            h=h_states_HxB,
            z=z_states_prior_HxB,
            use_ema=False,
            return_logits=True,
        )
        # Reactivate critic gradients.
        for p in self.critic.parameters():
            p.requires_grad_(True)
        v_dreamed_HxB = inverse_symlog(v)
        v_dreamed_H_B = v_dreamed_HxB.reshape([timesteps_H + 1, -1])

        # Compute the EMA net outputs w/o any gradients.
        with torch.no_grad():
            v_symlog_dreamed_ema_HxB = self.critic(
                h=h_states_HxB.detach(),
                z=z_states_prior_HxB.detach(),
                return_logits=False,
                use_ema=True,
            )
        v_symlog_dreamed_ema_H_B = v_symlog_dreamed_ema_HxB.reshape(
            [timesteps_H + 1, -1]
        )

        ret = {
            "h_states_t0_to_H_BxT": h_states_H_B,
            "z_states_prior_t0_to_H_BxT": z_states_prior_H_B,
            "rewards_dreamed_t0_to_H_BxT": r_dreamed_H_B,
            "continues_dreamed_t0_to_H_BxT": c_dreamed_H_B,
            "actions_dreamed_t0_to_H_BxT": a_dreamed_H_B,
            "actions_dreamed_dist_params_t0_to_H_BxT": a_dreamed_dist_params_H_B,
            # Critic (w/ world-model grads for actor loss).
            "values_dreamed_t0_to_H_BxT": v_dreamed_H_B,
            # Critic (world-model detached, for critic loss).
            "values_symlog_dreamed_logits_t0_to_HxBxT_wm_detached": v_symlog_dreamed_logits_HxB_wm_detached,
            # Critic EMA.
            "v_symlog_dreamed_ema_t0_to_H_BxT": v_symlog_dreamed_ema_H_B,
            # Loss weights for critic- and actor losses.
            "dream_loss_weights_t0_to_H_BxT": dream_loss_weights_H_B,
        }

        if self.use_curiosity:
            ret["rewards_intrinsic_t1_to_H_B"] = r_intrinsic_H_B
            ret.update(curiosity_forward_train_outs)

        if isinstance(self.action_space, gym.spaces.Discrete):
            ret["actions_ints_dreamed_t0_to_H_B"] = torch.argmax(a_dreamed_H_B, dim=-1)

        return ret

    def dream_trajectory_with_burn_in(
        self,
        *,
        start_states,
        timesteps_burn_in: int,
        timesteps_H: int,
        observations,  # [B, >=timesteps_burn_in]
        actions,  # [B, timesteps_burn_in (+timesteps_H)?]
        use_sampled_actions_in_dream: bool = False,
        use_random_actions_in_dream: bool = False,
    ):
        """Dreams trajectory from N initial observations and initial states.

        Note: This is only used for reporting and debugging, not for actual world-model
        or policy training.

        Args:
            start_states: The batch of start states (dicts with `a`, `h`, and `z` keys)
                to begin dreaming with. These are used to compute the first h-state
                using the sequence model.
            timesteps_burn_in: For how many timesteps should be use the posterior
                z-states (computed by the posterior net and actual observations from
                the env)?
            timesteps_H: For how many timesteps should we dream using the prior
                z-states (computed by the dynamics (prior) net and h-states only)?
                Note that the total length of the returned trajectories will
                be `timesteps_burn_in` + `timesteps_H`.
            observations: The batch (B, T, ...) of observations (to be used only during
                burn-in over `timesteps_burn_in` timesteps).
            actions: The batch (B, T, ...) of actions to use during a) burn-in over the
                first `timesteps_burn_in` timesteps and - possibly - b) during
                actual dreaming, iff use_sampled_actions_in_dream=True.
            use_sampled_actions_in_dream: If True, instead of using our actor network
                to compute fresh actions, we will use the one provided via the `actions`
                argument. Note that in the latter case, the `actions` time dimension
                must be at least `timesteps_burn_in` + `timesteps_H` long.
            use_random_actions_in_dream: Whether to use randomly sampled actions in the
                dream. Note that this does not apply to the burn-in phase, during which
                we will always use the actions given in the `actions` argument.
        """
        assert not (use_sampled_actions_in_dream and use_random_actions_in_dream)

        B = observations.shape[0]

        # Produce initial N internal posterior states (burn-in) using the given
        # observations:
        states = start_states
        for i in range(timesteps_burn_in):
            states = self.world_model.forward_inference(
                observations=observations[:, i : i + 1],
                previous_states=states,
                is_first=torch.full((B,), 1.0 if i == 0 else 0.0),
            )
            states["a"] = actions[:, i]

        # Start producing the actual dream, using prior states and either the given
        # actions, dreamed, or random ones.
        h_states_t0_to_H = [states["h"]]
        z_states_prior_t0_to_H = [states["z"]]
        a_t0_to_H = [states["a"]]

        for j in range(timesteps_H):
            # Compute next h using sequence model.
            h = self.world_model.sequence_model(
                a=states["a"],
                h=states["h"],
                z=states["z"],
            )
            h_states_t0_to_H.append(h)
            # Compute z from h, using the dynamics model (we don't have an actual
            # observation at this timestep).
            z = self.world_model.dynamics_predictor(h=h)
            z_states_prior_t0_to_H.append(z)

            # Compute next dreamed action or use sampled one or random one.
            if use_sampled_actions_in_dream:
                a = actions[:, timesteps_burn_in + j]
            elif use_random_actions_in_dream:
                if isinstance(self.action_space, gym.spaces.Discrete):
                    a = torch.randint(self.action_space.n, (B,), dtype=torch.int64)
                    a = torch.nn.functional.one_hot(a, num_classes=self.action_space.n)
                else:
                    a = torch.rand(
                        (B,) + self.action_space.shape, dtype=self.action_space.dtype
                    )
            else:
                a = self.actor(h=h, z=z)
            a_t0_to_H.append(a)

            states = {"h": h, "z": z, "a": a}

        # Fold time-rank for upcoming batch-predictions (no sequences needed anymore).
        h_states_t0_to_H_B = torch.stack(h_states_t0_to_H, dim=0)
        h_states_t0_to_HxB = h_states_t0_to_H_B.reshape(
            [-1] + list(h_states_t0_to_H_B.shape[2:])
        )

        z_states_prior_t0_to_H_B = torch.stack(z_states_prior_t0_to_H, dim=0)
        z_states_prior_t0_to_HxB = z_states_prior_t0_to_H_B.reshape(
            [-1] + list(z_states_prior_t0_to_H_B.shape[2:])
        )

        a_t0_to_H_B = torch.stack(a_t0_to_H, dim=0)

        # Compute o using decoder.
        o_dreamed_t0_to_HxB = self.world_model.decoder(
            h=h_states_t0_to_HxB,
            z=z_states_prior_t0_to_HxB,
        )
        if self.world_model.symlog_obs:
            o_dreamed_t0_to_HxB = inverse_symlog(o_dreamed_t0_to_HxB)

        # Compute r using reward predictor.
        r_dreamed_t0_to_H_B = inverse_symlog(
            self.world_model.reward_predictor(
                h=h_states_t0_to_HxB,
                z=z_states_prior_t0_to_HxB,
            )
        ).reshape([-1, B])

        # Compute continues using continue predictor.
        c_dreamed_t0_to_H_B = self.world_model.continue_predictor(
            h=h_states_t0_to_HxB,
            z=z_states_prior_t0_to_HxB,
        ).reshape([-1, B])

        # Return everything as time-major (H, B, ...), where H is the timesteps dreamed
        # (NOT burn-in'd) and B is a batch dimension (this might or might not include
        # an original time dimension from the real env, from all of which we then branch
        # out our dream trajectories).
        ret = {
            "h_states_t0_to_H_BxT": h_states_t0_to_H_B,
            "z_states_prior_t0_to_H_BxT": z_states_prior_t0_to_H_B,
            # Unfold time-ranks in predictions.
            "observations_dreamed_t0_to_H_BxT": torch.reshape(
                o_dreamed_t0_to_HxB, [-1, B] + list(observations.shape)[2:]
            ),
            "rewards_dreamed_t0_to_H_BxT": r_dreamed_t0_to_H_B,
            "continues_dreamed_t0_to_H_BxT": c_dreamed_t0_to_H_B,
        }

        # Figure out action key (random, sampled from env, dreamed?).
        if use_sampled_actions_in_dream:
            key = "actions_sampled_t0_to_H_BxT"
        elif use_random_actions_in_dream:
            key = "actions_random_t0_to_H_BxT"
        else:
            key = "actions_dreamed_t0_to_H_BxT"
        ret[key] = a_t0_to_H_B

        # Also provide int-actions, if discrete action space.
        if isinstance(self.action_space, gym.spaces.Discrete):
            ret[re.sub("^actions_", "actions_ints_", key)] = torch.argmax(
                a_t0_to_H_B, dim=-1
            )

        return ret
