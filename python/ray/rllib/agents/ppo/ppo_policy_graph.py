from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import tensorflow as tf

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph, \
    LearningRateSchedule
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.explained_variance import explained_variance

logger = logging.getLogger(__name__)


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
                 use_gae=True):
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
            existing_state_in = existing_inputs[8:-1]
            existing_seq_lens = existing_inputs[-1]
        else:
            obs_ph = tf.placeholder(
                tf.float32,
                name="obs",
                shape=(None, ) + observation_space.shape)
            adv_ph = tf.placeholder(
                tf.float32, name="advantages", shape=(None, ))
            act_ph = ModelCatalog.get_action_placeholder(action_space)
            logits_ph = tf.placeholder(
                tf.float32, name="logits", shape=(None, logit_dim))
            vf_preds_ph = tf.placeholder(
                tf.float32, name="vf_preds", shape=(None, ))
            value_targets_ph = tf.placeholder(
                tf.float32, name="value_targets", shape=(None, ))
            prev_actions_ph = ModelCatalog.get_action_placeholder(action_space)
            prev_rewards_ph = tf.placeholder(
                tf.float32, [None], name="prev_reward")
            existing_state_in = None
            existing_seq_lens = None
        self.observations = obs_ph
        self.prev_actions = prev_actions_ph
        self.prev_rewards = prev_rewards_ph

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
        self.model = ModelCatalog.get_model(
            {
                "obs": obs_ph,
                "prev_actions": prev_actions_ph,
                "prev_rewards": prev_rewards_ph,
                "is_training": self._get_is_training_placeholder(),
            },
            observation_space,
            action_space,
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
                if vf_config["use_lstm"]:
                    vf_config["use_lstm"] = False
                    logger.warning(
                        "It is not recommended to use a LSTM model with "
                        "vf_share_layers=False (consider setting it to True). "
                        "If you want to not share layers, you can implement "
                        "a custom LSTM model that overrides the "
                        "value_function() method.")
                with tf.variable_scope("value_function"):
                    self.value_function = ModelCatalog.get_model({
                        "obs": obs_ph,
                        "prev_actions": prev_actions_ph,
                        "prev_rewards": prev_rewards_ph,
                        "is_training": self._get_is_training_placeholder(),
                    }, observation_space, action_space, 1, vf_config).outputs
                    self.value_function = tf.reshape(self.value_function, [-1])
        else:
            self.value_function = tf.zeros(shape=tf.shape(obs_ph)[:1])

        if self.model.state_in:
            max_seq_len = tf.reduce_max(self.model.seq_lens)
            mask = tf.sequence_mask(self.model.seq_lens, max_seq_len)
            mask = tf.reshape(mask, [-1])
        else:
            mask = tf.ones_like(adv_ph, dtype=tf.bool)

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
            use_gae=self.config["use_gae"])

        LearningRateSchedule.__init__(self, self.config["lr"],
                                      self.config["lr_schedule"])
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=obs_ph,
            action_sampler=self.sampler,
            action_prob=curr_action_dist.sampled_action_prob(),
            loss=self.loss_obj.loss,
            model=self.model,
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
            last_r = self._value(sample_batch["new_obs"][-1],
                                 sample_batch["actions"][-1],
                                 sample_batch["rewards"][-1], *next_state)
        batch = compute_advantages(
            sample_batch,
            last_r,
            self.config["gamma"],
            self.config["lambda"],
            use_gae=self.config["use_gae"])
        return batch

    @override(TFPolicyGraph)
    def gradients(self, optimizer, loss):
        if self.config["grad_clip"] is not None:
            self.var_list = tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES,
                                              tf.get_variable_scope().name)
            grads = tf.gradients(loss, self.var_list)
            self.grads, _ = tf.clip_by_global_norm(grads,
                                                   self.config["grad_clip"])
            clipped_grads = list(zip(self.grads, self.var_list))
            return clipped_grads
        else:
            return optimizer.compute_gradients(
                loss, colocate_gradients_with_ops=True)

    @override(PolicyGraph)
    def get_initial_state(self):
        return self.model.state_init

    @override(TFPolicyGraph)
    def extra_compute_action_fetches(self):
        return dict(
            TFPolicyGraph.extra_compute_action_fetches(self), **{
                "vf_preds": self.value_function,
                "logits": self.logits
            })

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

    def _value(self, ob, prev_action, prev_reward, *args):
        feed_dict = {
            self.observations: [ob],
            self.prev_actions: [prev_action],
            self.prev_rewards: [prev_reward],
            self.model.seq_lens: [1]
        }
        assert len(args) == len(self.model.state_in), \
            (args, self.model.state_in)
        for k, v in zip(self.model.state_in, args):
            feed_dict[k] = v
        vf = self.sess.run(self.value_function, feed_dict)
        return vf[0]
