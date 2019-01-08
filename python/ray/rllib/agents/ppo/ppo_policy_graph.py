from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import tensorflow as tf

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph, \
    LearningRateSchedule
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.explained_variance import explained_variance


class PPOLoss(object):
    def __init__(self,
                 action_space,
                 value_targets,
                 advantages,
                 actions,
                 logits,
                 vf_preds,
                 curr_action_dist,
                 value_fn,
                 cur_kl_coeff,
                 valid_mask,
                 entropy_coeff=0,
                 clip_param=0.1,
                 vf_clip_param=0.1,
                 vf_loss_coeff=1.0,
                 use_gae=True,
                 use_central_vf=False,
                 central_value_fn=None,
                 central_vf_preds=None,
                 central_value_targets=None,
                 ):
        """Constructs the loss for Proximal Policy Objective.

        Arguments:
            action_space: Environment observation space specification.
            value_targets (Placeholder): Placeholder for target values; used
                for GAE.
            actions (Placeholder): Placeholder for actions taken
                from previous model evaluation.
            advantages (Placeholder): Placeholder for calculated advantages
                from previous model evaluation.
            logits (Placeholder): Placeholder for logits output from
                previous model evaluation.
            vf_preds (Placeholder): Placeholder for value function output
                from previous model evaluation.
            curr_action_dist (ActionDistribution): ActionDistribution
                of the current model.
            value_fn (Tensor): Current value function output Tensor.
            cur_kl_coeff (Variable): Variable holding the current PPO KL
                coefficient.
            valid_mask (Tensor): A bool mask of valid input elements (#2992).
            entropy_coeff (float): Coefficient of the entropy regularizer.
            clip_param (float): Clip parameter
            vf_clip_param (float): Clip parameter for the value function
            vf_loss_coeff (float): Coefficient of the value function loss
            use_gae (bool): If true, use the Generalized Advantage Estimator.
            use_central_vf (bool): If true, train a centralized value function
            central_value_fn (Tensor): Current centralized value function
                output Tensor.
            central_vf_preds (Placeholder): Placeholder for central value
                function output from previous model evaluation.
            central_value_targets (Placeholder): Placeholder for central
                value function target values; used for GA
        """

        def reduce_mean_valid(t):
            return tf.reduce_mean(tf.boolean_mask(t, valid_mask))

        dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        prev_dist = dist_cls(logits)
        # Make loss functions.
        logp_ratio = tf.exp(
            curr_action_dist.logp(actions) - prev_dist.logp(actions))
        action_kl = prev_dist.kl(curr_action_dist)
        self.mean_kl = reduce_mean_valid(action_kl)

        curr_entropy = curr_action_dist.entropy()
        self.mean_entropy = reduce_mean_valid(curr_entropy)

        surrogate_loss = tf.minimum(
            advantages * logp_ratio,
            advantages * tf.clip_by_value(logp_ratio, 1 - clip_param,
                                          1 + clip_param))
        self.mean_policy_loss = reduce_mean_valid(-surrogate_loss)

        if use_gae:
            vf_loss1 = tf.square(value_fn - value_targets)
            vf_clipped = vf_preds + tf.clip_by_value(
                value_fn - vf_preds, -vf_clip_param, vf_clip_param)
            vf_loss2 = tf.square(vf_clipped - value_targets)
            vf_loss = tf.maximum(vf_loss1, vf_loss2)
            self.mean_vf_loss = reduce_mean_valid(vf_loss)
            loss = reduce_mean_valid(
                -surrogate_loss + cur_kl_coeff * action_kl +
                vf_loss_coeff * vf_loss - entropy_coeff * curr_entropy)

            # TODO(ev) add a centralized value function loss
            if use_central_vf:
                central_vf_loss1 = tf.square(central_value_fn
                                             - central_value_targets)
                central_vf_clipped = central_vf_preds + tf.clip_by_value(
                    central_value_fn - central_vf_preds, -vf_clip_param,
                    vf_clip_param)
                central_vf_loss2 = tf.square(central_vf_clipped -
                                             central_value_targets)
                central_vf_loss = tf.maximum(central_vf_loss1,
                                             central_vf_loss2)
                self.central_mean_vf_loss = reduce_mean_valid(central_vf_loss)
                loss += reduce_mean_valid(vf_loss_coeff * central_vf_loss)
        else:
            self.mean_vf_loss = tf.constant(0.0)
            loss = reduce_mean_valid(-surrogate_loss +
                                     cur_kl_coeff * action_kl -
                                     entropy_coeff * curr_entropy)
        self.loss = loss


class PPOPolicyGraph(LearningRateSchedule, TFPolicyGraph):
    def __init__(self,
                 observation_space,
                 action_space,
                 config,
                 existing_inputs=None):
        """
        Arguments:
            observation_space: Environment observation space specification.
            action_space: Environment action space specification.
            config (dict): Configuration values for PPO graph.
            existing_inputs (list): Optional list of tuples that specify the
                placeholders upon which the graph should be built upon.
        """
        config = dict(ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG, **config)
        self.sess = tf.get_default_session()
        self.action_space = action_space
        self.config = config
        self.kl_coeff_val = self.config["kl_coeff"]
        self.kl_target = self.config["kl_target"]
        dist_cls, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])

        if existing_inputs:
            obs_ph, value_targets_ph, adv_ph, act_ph, \
                logits_ph, vf_preds_ph, prev_actions_ph, prev_rewards_ph = \
                existing_inputs[:8]
            if self.config["use_centralized_vf"]:
                central_obs_ph, central_value_targets_ph, \
                    central_vf_preds_ph = existing_inputs[8:11]
                existing_state_in = existing_inputs[11:-1]
                existing_seq_lens = existing_inputs[-1]
            else:
                existing_state_in = existing_inputs[8:-1]
                existing_seq_lens = existing_inputs[-1]
        else:
            obs_ph = tf.placeholder(
                tf.float32,
                name="obs",
                shape=(None,) + observation_space.shape)
            if self.config["use_centralized_vf"]:
                # TODO(ev) this assumes all observation spaces are the same
                # import ipdb; ipdb.set_trace()
                obs_shape = self.config["max_vf_agents"] * \
                            np.product(observation_space.shape)
                central_obs_ph = tf.placeholder(
                    tf.float32,
                    name="central_obs",
                    shape=(None, obs_shape))
                central_vf_preds_ph = tf.placeholder(
                    tf.float32, name="central_vf_preds", shape=(None,))

                central_value_targets_ph = tf.placeholder(
                    tf.float32, name="central_value_targets", shape=(None,))

                self.central_observations = central_obs_ph

            adv_ph = tf.placeholder(
                tf.float32, name="advantages", shape=(None,))
            act_ph = ModelCatalog.get_action_placeholder(action_space)
            logits_ph = tf.placeholder(
                tf.float32, name="logits", shape=(None, logit_dim))
            vf_preds_ph = tf.placeholder(
                tf.float32, name="vf_preds", shape=(None,))
            value_targets_ph = tf.placeholder(
                tf.float32, name="value_targets", shape=(None,))
            prev_actions_ph = ModelCatalog.get_action_placeholder(action_space)
            prev_rewards_ph = tf.placeholder(
                tf.float32, [None], name="prev_reward")
            existing_state_in = None
            existing_seq_lens = None
        self.observations = obs_ph

        self.loss_in = [
            ("obs", obs_ph),
            ("value_targets", value_targets_ph),
            ("advantages", adv_ph),
            ("actions", act_ph),
            ("logits", logits_ph),
            ("vf_preds", vf_preds_ph),
            ("prev_actions", prev_actions_ph),
            ("prev_rewards", prev_rewards_ph),
        ]

        if self.config["use_centralized_vf"]:
            self.loss_in.append(("central_obs", central_obs_ph))
            self.loss_in.append(("central_value_targets",
                                 central_value_targets_ph))
            self.loss_in.append(("central_vf_preds",
                                 central_vf_preds_ph))

        self.model = ModelCatalog.get_model(
            {
                "obs": obs_ph,
                "prev_actions": prev_actions_ph,
                "prev_rewards": prev_rewards_ph,
                "is_training": self._get_is_training_placeholder(),
            },
            observation_space,
            logit_dim,
            self.config["model"],
            state_in=existing_state_in,
            seq_lens=existing_seq_lens)

        # KL Coefficient
        self.kl_coeff = tf.get_variable(
            initializer=tf.constant_initializer(self.kl_coeff_val),
            name="kl_coeff",
            shape=(),
            trainable=False,
            dtype=tf.float32)

        self.logits = self.model.outputs
        curr_action_dist = dist_cls(self.logits)
        self.sampler = curr_action_dist.sample()
        if self.config["use_gae"]:
            if self.config["vf_share_layers"]:
                self.value_function = self.model.value_function()
            else:
                vf_config = self.config["model"].copy()
                # Do not split the last layer of the value function into
                # mean parameters and standard deviation parameters and
                # do not make the standard deviations free variables.
                vf_config["free_log_std"] = False
                vf_config["use_lstm"] = False
                with tf.variable_scope("value_function"):
                    # FIXME(ev, kp) it is trying to evaluate this but can't
                    self.value_function = ModelCatalog.get_model({
                        "obs": obs_ph,
                        "prev_actions": prev_actions_ph,
                        "prev_rewards": prev_rewards_ph,
                        "is_training": self._get_is_training_placeholder(),
                    }, observation_space, 1, vf_config).outputs
                    self.value_function = tf.reshape(self.value_function, [-1])

                # TODO(ev) should we change the scope?
                if self.config["use_centralized_vf"]:
                    with tf.variable_scope("central_value_function"):
                        # TODO(ev) do we need to remove observation space
                        self.central_value_function = ModelCatalog.get_model({
                            "obs": central_obs_ph,
                            "prev_actions": prev_actions_ph,
                            "prev_rewards": prev_rewards_ph,
                            "is_training": self._get_is_training_placeholder(),
                        }, observation_space, 1, vf_config).outputs
                        reshaped_val = tf.reshape(self.central_value_function,
                                                  [-1])
                        self.central_value_function = reshaped_val
        else:
            self.value_function = tf.zeros(shape=tf.shape(obs_ph)[:1])
            # TODO(ev) we need to place the global value function here as well
            # TODO(ev) or later code will break if GAE
            # TODO(ev) is on, but central vf is off

        if self.model.state_in:
            max_seq_len = tf.reduce_max(self.model.seq_lens)
            mask = tf.sequence_mask(self.model.seq_lens, max_seq_len)
            mask = tf.reshape(mask, [-1])
        else:
            mask = tf.ones_like(adv_ph, dtype=tf.bool)

        if self.config["use_centralized_vf"]:
            self.loss_obj = PPOLoss(
                action_space,
                value_targets_ph,
                adv_ph,
                act_ph,
                logits_ph,
                vf_preds_ph,
                curr_action_dist,
                self.value_function,
                self.kl_coeff,
                mask,
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"],
                vf_clip_param=self.config["vf_clip_param"],
                vf_loss_coeff=self.config["vf_loss_coeff"],
                use_gae=self.config["use_gae"],
                use_central_vf=True,
                central_value_fn=self.central_value_function,
                central_vf_preds=central_vf_preds_ph,
                central_value_targets=central_value_targets_ph,
            )
        else:
            self.loss_obj = PPOLoss(
                action_space,
                value_targets_ph,
                adv_ph,
                act_ph,
                logits_ph,
                vf_preds_ph,
                curr_action_dist,
                self.value_function,
                self.kl_coeff,
                mask,
                entropy_coeff=self.config["entropy_coeff"],
                clip_param=self.config["clip_param"],
                vf_clip_param=self.config["vf_clip_param"],
                vf_loss_coeff=self.config["vf_loss_coeff"],
                use_gae=self.config["use_gae"],
            )

        LearningRateSchedule.__init__(self, self.config["lr"],
                                      self.config["lr_schedule"])
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=obs_ph,
            action_sampler=self.sampler,
            loss=self.model.loss() + self.loss_obj.loss,
            loss_inputs=self.loss_in,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out,
            prev_action_input=prev_actions_ph,
            prev_reward_input=prev_rewards_ph,
            seq_lens=self.model.seq_lens,
            max_seq_len=config["model"]["max_seq_len"])

        self.sess.run(tf.global_variables_initializer())
        self.explained_variance = explained_variance(value_targets_ph,
                                                     self.value_function)
        self.stats_fetches = {
            "cur_kl_coeff": self.kl_coeff,
            "cur_lr": tf.cast(self.cur_lr, tf.float64),
            "total_loss": self.loss_obj.loss,
            "policy_loss": self.loss_obj.mean_policy_loss,
            "vf_loss": self.loss_obj.mean_vf_loss,
            "vf_explained_var": self.explained_variance,
            "kl": self.loss_obj.mean_kl,
            "entropy": self.loss_obj.mean_entropy
        }
        if self.config["use_centralized_vf"]:
            central_vf_loss = self.loss_obj.central_mean_vf_loss
            self.stats_fetches["central_vf_loss"] = central_vf_loss
            # TODO(ev, kp) add central vf explained var

    @override(TFPolicyGraph)
    def copy(self, existing_inputs):
        """Creates a copy of self using existing input placeholders."""
        return PPOPolicyGraph(
            self.observation_space,
            self.action_space,
            self.config,
            existing_inputs=existing_inputs)

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            next_state = []
            for i in range(len(self.model.state_in)):
                next_state.append([sample_batch["state_out_{}".format(i)][-1]])
            last_r = self._value(sample_batch["new_obs"][-1], *next_state)

        # if needed, add a centralized value function to the sample batch
        if self.config["use_centralized_vf"]:
            # TODO(ev) do we need to sort this?
            time_span = (sample_batch['t'][0], sample_batch['t'][-1])
            other_agent_times = {agent_id:
                                 (other_agent_batches[agent_id][1]["t"][0],
                                  other_agent_batches[agent_id][1]["t"][-1])
                                 for agent_id in other_agent_batches.keys()}
            rel_agents = {agent_id: other_agent_time for agent_id,
                          other_agent_time in
                          other_agent_times.items()
                          if self.time_overlap(time_span, other_agent_time)}
            if len(rel_agents) > 0:
                other_obs = {agent_id: other_agent_batches[agent_id][1]["obs"].copy()
                             for agent_id in rel_agents.keys()}
                padded_agent_obs = {agent_id:
                                    self.overlap_and_pad_agent(time_span,
                                                               rel_agent_time,
                                                               other_obs[agent_id])
                                    for agent_id,
                                    rel_agent_time in rel_agents.items()}
                central_obs_batch = np.hstack(
                    [padded_obs for padded_obs in padded_agent_obs.values()])
                central_obs_batch = np.hstack(
                    (central_obs_batch, sample_batch["obs"]))
            else:
                central_obs_batch = sample_batch["obs"]
            # TODO(ev) this is almost certainly broken
            # TODO(ev) pad with zeros as needed
            max_vf_agents = self.config["max_vf_agents"]
            num_agents = len(rel_agents) + 1
            if num_agents < max_vf_agents:
                diff = max_vf_agents - num_agents
                zero_pad = np.zeros((central_obs_batch.shape[0],
                                     self.observation_space.shape[0]*diff))
                central_obs_batch = np.hstack((central_obs_batch,
                                               zero_pad))
            elif num_agents > max_vf_agents:
                print("Too many agents!")

            # add the central obs and central critic value
            sample_batch["central_obs"] = central_obs_batch
            sample_batch["central_vf_preds"] = self.sess.run(
                self.central_value_function,
                feed_dict={self.central_observations: central_obs_batch})
        batch = compute_advantages(
            sample_batch,
            last_r,
            self.config["gamma"],
            self.config["lambda"],
            use_gae=self.config["use_gae"],
            use_centralized_vf=self.config["use_centralized_vf"])
        return batch

    @override(TFPolicyGraph)
    def gradients(self, optimizer):
        return optimizer.compute_gradients(
            self._loss, colocate_gradients_with_ops=True)

    @override(PolicyGraph)
    def get_initial_state(self):
        return self.model.state_init

    @override(TFPolicyGraph)
    def extra_compute_action_fetches(self):
        fetch_dict = {"vf_preds": self.value_function, "logits": self.logits}
        return fetch_dict

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        return self.stats_fetches

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5
        self.kl_coeff.load(self.kl_coeff_val, session=self.sess)
        return self.kl_coeff_val

    def time_overlap(self, time_span, agent_time):
        """Check if agent_time overlaps with time_span"""
        if agent_time[0] <= time_span[1] and agent_time[1] >= time_span[0]:
            return True
        else:
            return False

    def overlap_and_pad_agent(self, time_span, agent_time, obs):
        """returns only the portion of obs that overlaps with time_span and pads it
        Arguments:
            time_span (tuple): tuple of the first and last time that the agent
                of interest is in the system
            agent_time (tuple): tuple of the first and last time that the
                agent whose obs we are padding is in the system
            obs (np.ndarray): observations of the agent whose time is
                agent_time
        """
        assert self.time_overlap(time_span, agent_time)
        # FIXME(ev) some of these conditions can be combined
        # no padding needed
        if agent_time[0] == time_span[0] and agent_time[1] == time_span[1]:
            return obs
        # agent enters before time_span starts and exits before time_span end
        if agent_time[0] < time_span[0] and agent_time[1] < time_span[1]:
            non_overlap_time = time_span[0] - agent_time[0]
            missing_time = time_span[1] - agent_time[1]
            overlap_obs = obs[non_overlap_time:]
            padding = np.zeros((missing_time, obs.shape[1]))
            return np.concatenate((overlap_obs, padding))
        # agent enters after time_span starts and exits after time_span ends
        elif agent_time[0] > time_span[0] and agent_time[1] > time_span[1]:
            non_overlap_time = agent_time[1] - time_span[1]
            overlap_obs = obs[:-non_overlap_time]
            missing_time = agent_time[0] - time_span[0]
            padding = np.zeros((missing_time, obs.shape[1]))
            return np.concatenate((padding, overlap_obs))
        # agent time is entirely contained in time_span
        elif agent_time[0] >= time_span[0] and agent_time[1] <= time_span[1]:
            missing_left = agent_time[0] - time_span[0]
            missing_right = time_span[1] - agent_time[1]
            obs_concat = obs
            if missing_left > 0:
                padding = np.zeros((missing_left, obs.shape[1]))
                obs_concat = np.concatenate((padding, obs_concat))
            if missing_right > 0:
                padding = np.zeros((missing_right, obs.shape[1]))
                obs_concat = np.concatenate((obs_concat, padding))
            return obs_concat
        # agent time totally contains time_span
        elif agent_time[0] <= time_span[0] and agent_time[1] >= time_span[1]:
            non_overlap_left = time_span[0] - agent_time[0]
            non_overlap_right = agent_time[1] - time_span[1]
            overlap_obs = obs
            if non_overlap_left > 0:
                overlap_obs = overlap_obs[non_overlap_left:]
            if non_overlap_right > 0:
                overlap_obs = overlap_obs[:-non_overlap_right]
            return overlap_obs

    def _value(self, ob, *args):
        feed_dict = {self.observations: [ob], self.model.seq_lens: [1]}
        assert len(args) == len(self.model.state_in), \
            (args, self.model.state_in)
        for k, v in zip(self.model.state_in, args):
            feed_dict[k] = v
        vf = self.sess.run(self.value_function, feed_dict)
        return vf[0]
