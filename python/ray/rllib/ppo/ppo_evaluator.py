from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import tensorflow as tf
from collections import OrderedDict

import ray
from ray.rllib.optimizers import SampleBatch, TFMultiGPUSupport
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.sampler import SyncSampler
from ray.rllib.utils.filter import get_filter, MeanStdFilter
from ray.rllib.utils.process_rollout import compute_advantages
from ray.rllib.ppo.loss import ProximalPolicyGraph


class PPOEvaluator(TFMultiGPUSupport):
    """
    Runner class that holds the simulator environment and the policy.

    Initializes the tensorflow graphs for both training and evaluation.
    One common policy graph is initialized on '/cpu:0' and holds all the shared
    network weights. When run as a remote agent, only this graph is used.
    """

    def __init__(self, registry, env_creator, config, logdir, is_remote):
        self.registry = registry
        self.config = config
        self.logdir = logdir
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])
        if is_remote:
            config_proto = tf.ConfigProto()
        else:
            config_proto = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=config_proto)
        self.kl_coeff_val = self.config["kl_coeff"]
        self.kl_target = self.config["kl_target"]

        # Defines the training inputs:
        # The coefficient of the KL penalty.
        self.kl_coeff = tf.placeholder(
            name="newkl", shape=(), dtype=tf.float32)

        # The input observations.
        self.observations = tf.placeholder(
            tf.float32, shape=(None,) + self.env.observation_space.shape)
        # Targets of the value function.
        self.value_targets = tf.placeholder(tf.float32, shape=(None,))
        # Advantage values in the policy gradient estimator.
        self.advantages = tf.placeholder(tf.float32, shape=(None,))

        action_space = self.env.action_space
        self.actions = ModelCatalog.get_action_placeholder(action_space)
        self.distribution_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space)
        # Log probabilities from the policy before the policy update.
        self.prev_logits = tf.placeholder(
            tf.float32, shape=(None, self.logit_dim))
        # Value function predictions before the policy update.
        self.prev_vf_preds = tf.placeholder(tf.float32, shape=(None,))

        self.inputs = [
            ("obs", self.observations),
            ("value_targets", self.value_targets),
            ("advantages", self.advantages),
            ("actions", self.actions),
            ("logprobs", self.prev_logits),
            ("vf_preds", self.prev_vf_preds)
        ]
        self.common_policy = self.build_tf_loss([ph for _, ph in self.inputs])

        # References to the model weights
        self.variables = ray.experimental.TensorFlowVariables(
            self.common_policy.loss, self.sess)
        self.obs_filter = get_filter(
            config["observation_filter"], self.env.observation_space.shape)
        self.rew_filter = MeanStdFilter((), clip=5.0)
        self.filters = {"obs_filter": self.obs_filter,
                        "rew_filter": self.rew_filter}
        self.sampler = SyncSampler(
            self.env, self.common_policy, self.obs_filter,
            self.config["horizon"], self.config["horizon"])

    def tf_loss_inputs(self):
        return self.inputs

    def build_tf_loss(self, input_placeholders):
        obs, vtargets, advs, acts, plog, pvf_preds = input_placeholders
        return ProximalPolicyGraph(
            self.env.observation_space, self.env.action_space,
            obs, vtargets, advs, acts, plog, pvf_preds, self.logit_dim,
            self.kl_coeff, self.distribution_class, self.config,
            self.sess, self.registry)

    def init_extra_ops(self, device_losses):
        self.extra_ops = OrderedDict()
        with tf.name_scope("test_outputs"):
            policies = device_losses
            self.extra_ops["loss"] = tf.reduce_mean(
                tf.stack(values=[
                    policy.loss for policy in policies]), 0)
            self.extra_ops["policy_loss"] = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_policy_loss for policy in policies]), 0)
            self.extra_ops["vf_loss"] = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_vf_loss for policy in policies]), 0)
            self.extra_ops["kl"] = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_kl for policy in policies]), 0)
            self.extra_ops["entropy"] = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_entropy for policy in policies]), 0)

    def extra_apply_grad_fetches(self):
        return list(self.extra_ops.values())

    def extra_apply_grad_feed_dict(self):
        return {self.kl_coeff: self.kl_coeff_val}

    def update_kl(self, sampled_kl):
        if sampled_kl > 2.0 * self.kl_target:
            self.kl_coeff_val *= 1.5
        elif sampled_kl < 0.5 * self.kl_target:
            self.kl_coeff_val *= 0.5

    def save(self):
        filters = self.get_filters(flush_after=True)
        return pickle.dumps({
            "filters": filters,
            "kl_coeff_val": self.kl_coeff_val,
            "kl_target": self.kl_target,

        })

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])
        self.kl_coeff_val = objs["kl_coeff_val"]
        self.kl_target = objs["kl_target"]

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def sample(self):
        """Returns experience samples from this Evaluator. Observation
        filter and reward filters are flushed here.

        Returns:
            SampleBatch: A columnar batch of experiences.
        """
        num_steps_so_far = 0
        all_samples = []

        while num_steps_so_far < self.config["min_steps_per_task"]:
            rollout = self.sampler.get_data()
            last_r = 0.0  # note: not needed since we don't truncate rollouts
            samples = compute_advantages(
                rollout, last_r, self.config["gamma"],
                self.config["lambda"], use_gae=self.config["use_gae"])
            num_steps_so_far += samples.count
            all_samples.append(samples)
        return SampleBatch.concat_samples(all_samples)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()

    def sync_filters(self, new_filters):
        """Changes self's filter to given and rebases any accumulated delta.

        Args:
            new_filters (dict): Filters with new state to update local copy.
        """
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def get_filters(self, flush_after=False):
        """Returns a snapshot of filters.

        Args:
            flush_after (bool): Clears the filter buffer state.

        Returns:
            return_filters (dict): Dict for serializable filters
        """
        return_filters = {}
        for k, f in self.filters.items():
            return_filters[k] = f.as_serializable()
            if flush_after:
                f.clear_buffer()
        return return_filters
