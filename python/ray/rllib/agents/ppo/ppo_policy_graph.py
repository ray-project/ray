from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.rllib.evaluation.postprocessing import compute_advantages, \
    Postprocessing
from ray.rllib.evaluation.dynamic_tf_policy_graph import DynamicTFPolicyGraph
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph, \
    LearningRateSchedule
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.utils.annotations import override
from ray.rllib.utils.explained_variance import explained_variance
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)

# Frozen logits of the policy that computed the action
BEHAVIOUR_LOGITS = "behaviour_logits"


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


def loss_fn(graph, postprocessed_batch):
    if graph.model.state_in:
        max_seq_len = tf.reduce_max(graph.model.seq_lens)
        mask = tf.sequence_mask(graph.model.seq_lens, max_seq_len)
        mask = tf.reshape(mask, [-1])
    else:
        mask = tf.ones_like(
            postprocessed_batch[Postprocessing.ADVANTAGES], dtype=tf.bool)

    loss_obj = PPOLoss(
        graph.action_space,
        postprocessed_batch[Postprocessing.VALUE_TARGETS],
        postprocessed_batch[Postprocessing.ADVANTAGES],
        postprocessed_batch[SampleBatch.ACTIONS],
        postprocessed_batch[BEHAVIOUR_LOGITS],
        postprocessed_batch[SampleBatch.VF_PREDS],
        graph.action_dist,
        graph.value_function,
        graph.kl_coeff,
        mask,
        entropy_coeff=graph.config["entropy_coeff"],
        clip_param=graph.config["clip_param"],
        vf_clip_param=graph.config["vf_clip_param"],
        vf_loss_coeff=graph.config["vf_loss_coeff"],
        use_gae=graph.config["use_gae"])

    graph.explained_variance = explained_variance(
        postprocessed_batch[Postprocessing.VALUE_TARGETS],
        graph.value_function)

    graph.stats_fetches = {
        "cur_kl_coeff": graph.kl_coeff,
        "cur_lr": tf.cast(graph.cur_lr, tf.float64),
        "total_loss": loss_obj.loss,
        "policy_loss": loss_obj.mean_policy_loss,
        "vf_loss": loss_obj.mean_vf_loss,
        "vf_explained_var": graph.explained_variance,
        "kl": loss_obj.mean_kl,
        "entropy": loss_obj.mean_entropy,
    }

    return loss_obj.loss


class PPOPostprocessing(object):
    """Adds the policy logits, VF preds, and advantages to the trajectory."""

    @override(TFPolicyGraph)
    def extra_compute_action_fetches(self):
        return dict(
            TFPolicyGraph.extra_compute_action_fetches(self), **{
                SampleBatch.VF_PREDS: self.value_function,
                BEHAVIOUR_LOGITS: self.model.outputs,
            })

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
            last_r = self._value(sample_batch[SampleBatch.NEXT_OBS][-1],
                                 sample_batch[SampleBatch.ACTIONS][-1],
                                 sample_batch[SampleBatch.REWARDS][-1],
                                 *next_state)
        batch = compute_advantages(
            sample_batch,
            last_r,
            self.config["gamma"],
            self.config["lambda"],
            use_gae=self.config["use_gae"])
        return batch


class PPOPolicyGraph(
        LearningRateSchedule, PPOPostprocessing, DynamicTFPolicyGraph):

    def __init__(self,
                 observation_space,
                 action_space,
                 config,
                 existing_inputs=None):
        config = dict(ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG, **config)

        # KL Coefficient
        self.kl_coeff_val = config["kl_coeff"]
        self.kl_target = config["kl_target"]
        self.kl_coeff = tf.get_variable(
            initializer=tf.constant_initializer(self.kl_coeff_val),
            name="kl_coeff",
            shape=(),
            trainable=False,
            dtype=tf.float32)

        DynamicTFPolicyGraph.__init__(
            self, observation_space, action_space, config, loss_fn,
            existing_inputs=existing_inputs)

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
                        "obs": self._obs_input,
                        "prev_actions": self._prev_action_input,
                        "prev_rewards": self._prev_reward_input,
                        "is_training": self._get_is_training_placeholder(),
                    }, observation_space, action_space, 1, vf_config).outputs
                    self.value_function = tf.reshape(self.value_function, [-1])
        else:
            self.value_function = tf.zeros(shape=tf.shape(self._obs_input)[:1])

        LearningRateSchedule.__init__(self, self.config["lr"],
                                      self.config["lr_schedule"])
        self._sess.run(tf.global_variables_initializer())

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
    def extra_compute_grad_fetches(self):
        return {LEARNER_STATS_KEY: self.stats_fetches}

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5
        self.kl_coeff.load(self.kl_coeff_val, session=self._sess)
        return self.kl_coeff_val

    def _value(self, ob, prev_action, prev_reward, *args):
        feed_dict = {
            self._obs_input: [ob],
            self._prev_action_input: [prev_action],
            self._prev_reward_input: [prev_reward],
            self.model.seq_lens: [1]
        }
        assert len(args) == len(self.model.state_in), \
            (args, self.model.state_in)
        for k, v in zip(self.model.state_in, args):
            feed_dict[k] = v
        vf = self._sess.run(self.value_function, feed_dict)
        return vf[0]
