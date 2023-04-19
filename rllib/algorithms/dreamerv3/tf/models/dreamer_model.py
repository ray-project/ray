"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf
"""
import re

import gymnasium as gym
import numpy as np
import tensorflow as tf
import tree  # pip install dm_tree

from models.actor_network import ActorNetwork
from models.critic_network import CriticNetwork
from models.disagree_networks import DisagreeNetworks
from utils.model_dimensions import get_num_curiosity_nets
from utils.symlog import inverse_symlog


class DreamerModel(tf.keras.Model):
    """TODO
    """
    def __init__(
            self,
            *,
            model_dimension: str = "XS",
            action_space: gym.Space,
            batch_size_B,
            batch_length_T,
            horizon_H,
            world_model,
            use_curiosity: bool = False,
            intrinsic_rewards_scale: float = 0.1,
    ):
        """TODO

        Args:
             model_dimension: The "Model Size" used according to [1] Appendinx B.
                Use None for manually setting the different network sizes.
             action_space: The action space the our environment used.
        """
        super().__init__()

        self.model_dimension = model_dimension
        self.action_space = action_space
        self.use_curiosity = use_curiosity
        self.batch_size_B = batch_size_B
        self.batch_length_T = batch_length_T
        self.horizon_H = horizon_H

        self.world_model = world_model

        self.actor = ActorNetwork(
            action_space=self.action_space,
            model_dimension=self.model_dimension,
        )
        self.critic = CriticNetwork(
            model_dimension=self.model_dimension,
        )

        self.disagree_nets = None
        if self.use_curiosity:
            self.disagree_nets = DisagreeNetworks(
                num_networks=8,
                model_dimension=self.model_dimension,
                intrinsic_rewards_scale=intrinsic_rewards_scale,
            )

    @tf.function
    def call(self, inputs, actions, previous_states, is_first, training=None):
        observations = inputs
        self.get_initial_state()
        # Single action inference.
        self.forward_inference(
            previous_states,
            observations,
            is_first,
        )
        previous_states_B = tree.map_structure(
            lambda s: tf.repeat(s, self.batch_size_B, axis=0),
            previous_states,
        )
        observations_B = tf.repeat(observations, self.batch_size_B, axis=0)
        is_first_B = tf.repeat(is_first, self.batch_size_B, axis=0)
        # Inference on "get_action" batch size (parallel envs).
        self.forward_inference(
            previous_states_B,
            observations_B,
            is_first_B,
        )
        previous_states_BxT = tree.map_structure(
            lambda s: tf.repeat(s, self.batch_size_B * self.batch_length_T, axis=0),
            previous_states,
        )
        self.dream_trajectory(
            start_states=previous_states_BxT,
            start_is_terminated=tf.fill((self.batch_size_B * self.batch_length_T,), 0.0),
            timesteps_H=self.horizon_H,
            gamma=0.99,
        )
        observations_B_T = tf.repeat(tf.expand_dims(observations_B, axis=1), self.batch_length_T, axis=1)
        actions_B = tf.repeat(actions, self.batch_size_B, axis=0)
        actions_B_T = tree.map_structure(
            lambda s: tf.repeat(tf.expand_dims(s, axis=1), self.batch_length_T, axis=1),
            actions_B,
        )
        is_first_B_T = tf.repeat(tf.expand_dims(is_first_B, axis=1), self.batch_length_T, axis=1)
        # For train pass, add time dimension.
        ret = self.forward_train(observations_B_T, actions_B_T, is_first_B_T)
        return ret

    @tf.function
    def forward_inference(self, previous_states, observations, is_first, training=None):
        """TODO"""
        # Perform one step in the world model (starting from `previous_state` and
        # using the observations to yield a current (posterior) state).
        states = self.world_model(
            previous_states, observations, is_first
        )

        # Compute action using our actor network and the current states.
        actions = self.actor(h=states["h"], z=states["z"])

        return actions, {
            "h": states["h"],
            "z": states["z"],
            "a": actions,
        }

    @tf.function
    def forward_train(self, observations, actions, is_first, training=None):
        """TODO"""
        return self.world_model.forward_train(observations, actions, is_first)

    @tf.function
    def get_initial_state(self):
        states = self.world_model.get_initial_state()

        action_dim = (
            self.action_space.n if isinstance(self.action_space, gym.spaces.Discrete)
            else np.prod(self.action_space.shape)
        )
        states["a"] = tf.zeros((1, action_dim,), dtype=tf.float32)
        return states

    @tf.function
    def dream_trajectory(self, *, start_states, start_is_terminated, timesteps_H, gamma):
        """Dreams trajectories from batch of h- and z-states.

        Args:
            h: The h-states (B, ...) as computed by a train forward pass. From
                each individual h-state in the given batch, we will branch off
                a dreamed trajectory of len `timesteps`.
            z: The posterior z-states (B, num_categoricals, num_classes) as computed
                by a train forward pass. From each individual z-state in the given
                batch,, we will branch off a dreamed trajectory of len `timesteps`.
            timesteps_H: The number of timesteps to dream for.
        """
        # Dreamed actions (one-hot for discrete actions).
        a_dreamed_t0_to_H = []
        a_dreamed_distributions_t0_to_H = []

        h = start_states["h"]
        z = start_states["z"]

        # GRU outputs.
        h_states_t0_to_H = [h]
        # Dynamics model outputs.
        z_states_prior_t0_to_H = [z]

        # Compute `a` using actor network (already the first step uses a dreamed action,
        # not a sampled one).
        a, a_dist = self.actor(
            # We have to stop the gradients through the states. B/c we are using a
            # differentiable Discrete action distribution (straight through gradients
            # with `a = stop_gradient(sample(probs)) + probs - stop_gradient(probs)`,
            # we otherwise would add dependencies of the `-log(pi(a|s))` REINFORCE loss
            # term on actions further back in the trajectory.
            h=tf.stop_gradient(h),
            z=tf.stop_gradient(z),
            return_distribution=True,
        )
        # TEST: Use random actions instead of actor-computed ones.
        # a = tf.random.uniform(tf.shape(h)[0:1], 0, self.action_space.n, tf.int64)
        # END TEST: random actions
        a_dreamed_t0_to_H.append(a)
        a_dreamed_distributions_t0_to_H.append(a_dist)

        for i in range(timesteps_H):
            # Move one step in the dream using the RSSM.
            h = self.world_model.sequence_model(z=z, a=a, h=h)
            h_states_t0_to_H.append(h)

            # Compute prior z using dynamics model.
            z = self.world_model.dynamics_predictor(h=h)
            z_states_prior_t0_to_H.append(z)

            # Compute `a` using actor network.
            a, a_dist = self.actor(
                h=tf.stop_gradient(h),
                z=tf.stop_gradient(z),
                return_distribution=True,
            )
            # TEST: Use random actions instead of actor-computed ones.
            # a = tf.random.uniform(tf.shape(h)[0:1], 0, self.action_space.n, tf.int64)
            # END TEST: random actions
            a_dreamed_t0_to_H.append(a)
            a_dreamed_distributions_t0_to_H.append(a_dist)

        h_states_H_B = tf.stack(h_states_t0_to_H, axis=0)  # (T, B, ...)
        h_states_HxB = tf.reshape(h_states_H_B, [-1] + h_states_H_B.shape.as_list()[2:])

        z_states_prior_H_B = tf.stack(z_states_prior_t0_to_H, axis=0)  # (T, B, ...)
        z_states_prior_HxB = tf.reshape(z_states_prior_H_B, [-1] + z_states_prior_H_B.shape.as_list()[2:])

        a_dreamed_H_B = tf.stack(a_dreamed_t0_to_H, axis=0)  # (T, B, ...)

        # Compute r using reward predictor.
        r_dreamed_H_B = tf.reshape(
            inverse_symlog(
                self.world_model.reward_predictor(h=h_states_HxB, z=z_states_prior_HxB)
            ),
            shape=[timesteps_H+1, -1],
        )

        # Compute intrinsic rewards.
        if self.use_curiosity:
            results_HxB = self.disagree_nets.compute_intrinsic_rewards(
                h=h_states_HxB,
                z=z_states_prior_HxB,
                a=tf.reshape(a_dreamed_H_B, [-1] + a_dreamed_H_B.shape.as_list()[2:]),
            )
            ## Wrong? -> Cut out last timestep as we always predict z-states for the NEXT timestep
            ## and derive ri (for the NEXT timestep) from the disagreement between
            ## our N disagreee nets.
            r_intrinsic_H_B = tf.reshape(
                results_HxB["rewards_intrinsic"], shape=[timesteps_H+1, -1]
            )[1:]   # cut out first ts instead
            curiosity_forward_train_outs = results_HxB["forward_train_outs"]
            del results_HxB

        # Compute continues using continue predictor.
        c_dreamed_HxB = self.world_model.continue_predictor(
            h=h_states_HxB,
            z=z_states_prior_HxB,
        )
        c_dreamed_H_B = tf.reshape(c_dreamed_HxB, [timesteps_H+1, -1])
        # Force-set first continue to False iff `start_is_terminated`.
        # Note: This will cause the loss-weights for this row in the batch to be
        # completely zero'd out. In general, we don't use dreamed data past any
        # predicted (or actual first) continue=False flags.
        c_dreamed_H_B = tf.concat(
            [1.0 - tf.expand_dims(start_is_terminated, 0), c_dreamed_H_B[1:]],
            axis=0,
        )

        # Loss weights for each individual dreamed timestep. Zero-out all timesteps
        # that lie past continue=False flags. B/c our world model does NOT learn how
        # to skip terminal/reset episode boundaries, dreamed data crossing such a
        # boundary should not be used for critic/actor learning either.
        dream_loss_weights_H_B = tf.math.cumprod(gamma * c_dreamed_H_B, axis=0) / gamma

        # Compute the value estimates.
        v, v_symlog_dreamed_logits_HxB = self.critic(
            h=h_states_HxB,
            z=z_states_prior_HxB,
            return_logits=True,
        )
        v_dreamed_HxB = inverse_symlog(v)
        v_dreamed_H_B = tf.reshape(v_dreamed_HxB, shape=[timesteps_H+1, -1])

        v_symlog_dreamed_ema_HxB = self.critic(
            h=h_states_HxB,
            z=z_states_prior_HxB,
            return_logits=False,
            use_ema=True,
        )
        v_symlog_dreamed_ema_H_B = tf.reshape(
            v_symlog_dreamed_ema_HxB, shape=[timesteps_H+1, -1]
        )

        ret = {
            "h_states_t0_to_H_B": h_states_H_B,
            "z_states_prior_t0_to_H_B": z_states_prior_H_B,
            "rewards_dreamed_t0_to_H_B": r_dreamed_H_B,
            "continues_dreamed_t0_to_H_B": c_dreamed_H_B,
            "actions_dreamed_t0_to_H_B": a_dreamed_H_B,
            "actions_dreamed_distributions_t0_to_H_B": a_dreamed_distributions_t0_to_H,
            "values_dreamed_t0_to_H_B": v_dreamed_H_B,
            "values_symlog_dreamed_logits_t0_to_HxB": v_symlog_dreamed_logits_HxB,
            "v_symlog_dreamed_ema_t0_to_H_B": v_symlog_dreamed_ema_H_B,
            # Loss weights for critic- and actor losses.
            "dream_loss_weights_t0_to_H_B": dream_loss_weights_H_B,
        }

        if self.use_curiosity:
            ret["rewards_intrinsic_t1_to_H_B"] = r_intrinsic_H_B
            ret.update(curiosity_forward_train_outs)

        if isinstance(self.action_space, gym.spaces.Discrete):
            ret["actions_ints_dreamed_t0_to_H_B"] = tf.argmax(a_dreamed_H_B, axis=-1)

        return ret

    @tf.function
    def dream_trajectory_with_burn_in(
        self,
        *,
        start_states,
        timesteps_burn_in,
        timesteps_H,
        observations,  # [B, >=timesteps_burn_in]
        actions,  # [B, timesteps_burn_in (+timesteps_H)?]
        use_sampled_actions_in_dream=False,
        use_random_actions_in_dream=False,
    ):
        """Dreams trajectory from N initial observations and initial states.

        TODO: docstring
        """
        assert not (use_sampled_actions_in_dream and use_random_actions_in_dream)

        B, T = observations.shape[0], observations.shape[1]

        # Produce initial N internal posterior states (burn-in) using the given
        # observations:
        states = start_states
        for i in range(timesteps_burn_in):
            states = self.world_model(
                states,
                observations[:, i],
                tf.fill((B,), 1.0 if i == 0 else 0.0),
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
                z=states["z"],
                a=states["a"],
                h=states["h"],
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
                a = tf.random.randint((B,), 0, self.action_space.n, tf.int64)
                a = tf.one_hot(a, depth=self.action_space.n)
            else:
                a = self.actor(h=h, z=z)
            a_t0_to_H.append(a)

            states = {"h": h, "z": z, "a": a}

        h_states_t0_to_H_B = tf.stack(h_states_t0_to_H, axis=0)
        h_states_t0_to_HxB = tf.reshape(
            h_states_t0_to_H_B, shape=[-1] + h_states_t0_to_H_B.shape.as_list()[2:]
        )

        z_states_prior_t0_to_H_B = tf.stack(z_states_prior_t0_to_H, axis=0)
        z_states_prior_t0_to_HxB = tf.reshape(
            z_states_prior_t0_to_H_B,
            shape=[-1] + z_states_prior_t0_to_H_B.shape.as_list()[2:],
        )

        a_t0_to_H_B = tf.stack(a_t0_to_H, axis=0)

        # Compute r using reward predictor.
        r_dreamed_t0_to_HxB = inverse_symlog(
            self.world_model.reward_predictor(
                h=h_states_t0_to_HxB,
                z=z_states_prior_t0_to_HxB,
            )
        )
        # Compute continues using continue predictor.
        c_dreamed_t0_to_HxB = self.world_model.continue_predictor(
            h=h_states_t0_to_HxB,
            z=z_states_prior_t0_to_HxB,
        )

        ret = {
            "h_states_t0_to_H_B": h_states_t0_to_H_B,
            "z_states_prior_t0_to_H_B": z_states_prior_t0_to_H_B,
            "rewards_dreamed_t0_to_H_B": tf.reshape(r_dreamed_t0_to_HxB, (-1, B)),
            "continues_dreamed_t0_to_H_B": tf.reshape(c_dreamed_t0_to_HxB, (-1, B)),
        }

        if use_sampled_actions_in_dream:
            key = "actions_sampled_t0_to_H_B"
        elif use_random_actions_in_dream:
            key = "actions_random_t0_to_H_B"
        else:
            key = "actions_dreamed_t0_to_H_B"

        ret[key] = a_t0_to_H_B
        if isinstance(self.action_space, gym.spaces.Discrete):
            ret[re.sub("^actions_", "actions_ints_", key)] = tf.argmax(
                a_t0_to_H_B, axis=-1
            )

        return ret


if __name__ == "__main__":

    from IPython.display import display, Image
    from moviepy.editor import ImageSequenceClip
    import time

    from ray.rllib.algorithms.algorithm_config import AlgorithmConfig

    from utils.env_runner import EnvRunner

    B = 1
    T = 64
    burn_in_T = 5

    config = (
        AlgorithmConfig()
            .environment("ALE/MsPacman-v5", env_config={
            # [2]: "We follow the evaluation protocol of Machado et al. (2018) with 200M
            # environment steps, action repeat of 4, a time limit of 108,000 steps per
            # episode that correspond to 30 minutes of game play, no access to life
            # information, full action space, and sticky actions. Because the world model
            # integrates information over time, DreamerV2 does not use frame stacking.
            # The experiments use a single-task setup where a separate agent is trained
            # for each game. Moreover, each agent uses only a single environment instance.
            "repeat_action_probability": 0.25,  # "sticky actions"
            "full_action_space": True,  # "full action space"
            "frameskip": 1,  # already done by MaxAndSkip wrapper: "action repeat" == 4
        })
        .rollouts(num_envs_per_worker=16, rollout_fragment_length=burn_in_T + T)
    )
    # The vectorized gymnasium EnvRunner to collect samples of shape (B, T, ...).
    env_runner = EnvRunner(model=None, config=config, max_seq_len=None, continuous_episodes=True)

    # Our DreamerV3 world model.
    #from_checkpoint = 'C:\Dropbox\Projects\dreamer_v3\examples\checkpoints\mspacman_world_model_170'
    from_checkpoint = "/Users/sven/Dropbox/Projects/dreamer_v3/examples/checkpoints/mspacman_dreamer_model_60"
    dreamer_model = tf.keras.models.load_model(from_checkpoint)
    world_model = dreamer_model.world_model
    # TODO: ugly hack (resulting from the insane fact that you cannot know
    #  an env's spaces prior to actually constructing an instance of it) :(
    env_runner.model = dreamer_model

    #obs = np.random.randint(0, 256, size=(B, burn_in_T, 64, 64, 3), dtype=np.uint8)
    #actions = np.random.randint(0, 2, size=(B, burn_in_T), dtype=np.uint8)
    #initial_h = np.random.random(size=(B, 256)).astype(np.float32)

    sampled_obs, _, sampled_actions, _, _, _, sampled_h = env_runner.sample(random_actions=False)

    dreamed_trajectory = dreamer_model.dream_trajectory(
        sampled_obs[:, :burn_in_T],
        sampled_actions.astype(np.int64),  # use all sampled actions, not random or actor-computed ones
        sampled_h,
        timesteps=T,
        # Use same actions as in the sample such that we can 100% compare
        # predicted vs actual observations.
        use_sampled_actions=True,
    )
    print(dreamed_trajectory)

    # Compute observations using h and z and the decoder net.
    # Note that the last h-state is NOT used here as it's already part of
    # a new trajectory.
    _, dreamed_images_distr = world_model.cnn_transpose_atari(
        tf.reshape(dreamed_trajectory["h_states_t1_to_Hp1"][:,:-1], (B * T, -1)),
        tf.reshape(dreamed_trajectory["z_dreamed_t1_to_Hp1"], (B * T) + dreamed_trajectory["z_dreamed_t1_to_Hp1"].shape[2:]),
    )
    # Use mean() of the Gaussian, no sample!
    #
    dreamed_images = dreamed_images_distr.mean()
    dreamed_images = tf.reshape(
        tf.cast(
            tf.clip_by_value(
                inverse_symlog(dreamed_images), 0.0, 255.0
            ),
            tf.uint8,
        ),
        shape=(B, T, 64, 64, 3),
    ).numpy()

    # Stitch dreamed_obs and sampled_obs together for better comparison.
    images = np.concatenate([dreamed_images, sampled_obs[:, burn_in_T:]], axis=2)

    # Save sequence a gif.
    clip = ImageSequenceClip(list(images[0]), fps=2)
    clip.write_gif("test.gif", fps=2)
    Image("test.gif")
    time.sleep(10)
