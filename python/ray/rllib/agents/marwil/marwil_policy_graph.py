from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import tensorflow as tf

import ray
from ray.rllib.models import ModelCatalog
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.utils.annotations import override
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.agents.dqn.dqn_policy_graph import _scope_vars
from ray.rllib.utils.explained_variance import explained_variance

P_SCOPE = "p_func"
V_SCOPE = "v_func"


class ValueLoss(object):
    def __init__(self, state_values, cumulative_rewards):
        self.loss = 0.5 * tf.reduce_mean(
            tf.square(state_values - cumulative_rewards))


class ReweightedImitationLoss(object):
    def __init__(self, state_values, cumulative_rewards, logits, actions,
                 action_space, beta):
        ma_adv_norm = tf.get_variable(
            name="moving_average_of_advantage_norm",
            dtype=tf.float32,
            initializer=100.0,
            trainable=False)
        # advantage estimation
        adv = cumulative_rewards - state_values
        # update averaged advantage norm
        update_adv_norm = tf.assign_add(
            ref=ma_adv_norm,
            value=1e-6 * (tf.reduce_mean(tf.square(adv)) - ma_adv_norm))

        # exponentially weighted advantages
        with tf.control_dependencies([update_adv_norm]):
            exp_advs = tf.exp(
                beta * tf.divide(adv, 1e-8 + tf.sqrt(ma_adv_norm)))

        # log\pi_\theta(a|s)
        dist_cls, _ = ModelCatalog.get_action_dist(action_space, {})
        action_dist = dist_cls(logits)
        logprobs = action_dist.logp(actions)

        self.loss = -1.0 * tf.reduce_mean(
            tf.stop_gradient(exp_advs) * logprobs)


class MARWILPolicyGraph(TFPolicyGraph):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.agents.dqn.dqn.DEFAULT_CONFIG, **config)
        self.config = config

        dist_cls, logit_dim = ModelCatalog.get_action_dist(
            action_space, self.config["model"])

        # Action inputs
        self.obs_t = tf.placeholder(
            tf.float32, shape=(None, ) + observation_space.shape)

        with tf.variable_scope(P_SCOPE) as scope:
            self.model = self._build_policy_network(
                self.obs_t, observation_space, logit_dim)
            logits = self.model.outputs
            self.p_func_vars = _scope_vars(scope.name)

        # Action outputs
        action_dist = dist_cls(logits)
        self.output_actions = action_dist.sample()

        # Training inputs
        self.act_t = tf.placeholder(tf.int32, [None], name="action")
        self.cum_rew_t = tf.placeholder(tf.float32, [None], name="reward")

        # v network evaluation
        with tf.variable_scope(V_SCOPE) as scope:
            state_values = self._build_value_network(self.obs_t,
                                                     observation_space)
            self.v_func_vars = _scope_vars(scope.name)
        self.v_loss = self._build_value_loss(state_values, self.cum_rew_t)
        self.p_loss = self._build_policy_loss(state_values, self.cum_rew_t,
                                              logits, self.act_t, action_space)

        # which kind of objective to optimize
        objective = (
            self.p_loss.loss + self.config["vf_coeff"] * self.v_loss.loss)
        self.explained_variance = tf.reduce_mean(
            explained_variance(self.cum_rew_t, state_values))

        # initialize TFPolicyGraph
        self.sess = tf.get_default_session()
        self.loss_inputs = [
            ("obs", self.obs_t),
            ("actions", self.act_t),
            ("advantages", self.cum_rew_t),
        ]
        TFPolicyGraph.__init__(
            self,
            observation_space,
            action_space,
            self.sess,
            obs_input=self.obs_t,
            action_sampler=self.output_actions,
            loss=self.model.loss() + objective,
            loss_inputs=self.loss_inputs,
            state_inputs=self.model.state_in,
            state_outputs=self.model.state_out)
        self.sess.run(tf.global_variables_initializer())

        self.stats_fetches = {
            "total_loss": objective,
            "vf_explained_var": self.explained_variance,
            "policy_loss": self.p_loss.loss,
            "vf_loss": self.v_loss.loss
        }

    def _build_policy_network(self, obs, obs_space, logit_dim):
        return ModelCatalog.get_model({
            "obs": obs,
            "is_training": self._get_is_training_placeholder(),
        }, obs_space, logit_dim, self.config["model"])

    def _build_value_network(self, obs, obs_space):
        value_model = ModelCatalog.get_model({
            "obs": obs,
            "is_training": self._get_is_training_placeholder(),
        }, obs_space, 1, self.config["model"])
        return value_model.outputs

    def _build_value_loss(self, state_values, cum_rwds):
        return ValueLoss(state_values, cum_rwds)

    def _build_policy_loss(self, state_values, cum_rwds, logits, actions,
                           action_space):
        return ReweightedImitationLoss(state_values, cum_rwds, logits, actions,
                                       action_space, self.config["beta"])

    @override(TFPolicyGraph)
    def extra_compute_grad_fetches(self):
        return self.stats_fetches

    @override(PolicyGraph)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        completed = sample_batch["dones"][-1]
        if completed:
            last_r = 0.0
        else:
            raise NotImplementedError(
                "last done mask in a batch should be True. "
                "For now, we only support reading experience batches produced "
                "with batch_mode='complete_episodes'.",
                len(sample_batch["dones"]), sample_batch["dones"][-1])
        batch = compute_advantages(
            sample_batch, last_r, gamma=self.config["gamma"], use_gae=False)
        return batch

    @override(PolicyGraph)
    def get_initial_state(self):
        return self.model.state_init
