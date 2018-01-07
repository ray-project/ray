from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym.spaces
import pickle
import tensorflow as tf
import os

from tensorflow.python import debug as tf_debug

import numpy as np

import ray
from ray.rllib.optimizers import Evaluator, SampleBatch, TFMultiGPUSupport
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.sampler import SyncSampler
from ray.rllib.utils.filter import get_filter, MeanStdFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.ppo.loss import ProximalPolicyLoss


# TODO(rliaw): Move this onto LocalMultiGPUOptimizer
class PPOEvaluator(TFMultiGPUSupport):
    """
    Runner class that holds the simulator environment and the policy.

    Initializes the tensorflow graphs for both training and evaluation.
    One common policy graph is initialized on '/cpu:0' and holds all the shared
    network weights. When run as a remote agent, only this graph is used.
    """

    def __init__(self, registry, env_creator, config, logdir, is_remote):
        self.registry = registry
        self.is_remote = is_remote
        if is_remote:
            os.environ["CUDA_VISIBLE_DEVICES"] = ""
            devices = ["/cpu:0"]
        else:
            devices = config["devices"]
        self.devices = devices
        self.config = config
        self.logdir = logdir
        self.env = ModelCatalog.get_preprocessor_as_wrapper(
            registry, env_creator(config["env_config"]), config["model"])

        # TODO(rliaw): Evaluator should not be the model!!
        if is_remote:
            config_proto = tf.ConfigProto()
        else:
            config_proto = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=config_proto)
        if config["tf_debug_inf_or_nan"] and not is_remote:
            self.sess = tf_debug.LocalCLIDebugWrapperSession(self.sess)
            self.sess.add_tensor_filter(
                "has_inf_or_nan", tf_debug.has_inf_or_nan)

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
        # TODO(rliaw): pull this into model_catalog
        if isinstance(action_space, gym.spaces.Box):
            self.actions = tf.placeholder(
                tf.float32, shape=(None, action_space.shape[0]))
        elif isinstance(action_space, gym.spaces.Discrete):
            self.actions = tf.placeholder(tf.int64, shape=(None,))
        else:
            raise NotImplemented(
                "action space" + str(type(action_space)) +
                "currently not supported")
        self.distribution_class, self.logit_dim = ModelCatalog.get_action_dist(
            action_space)
        # Log probabilities from the policy before the policy update.
        self.prev_logits = tf.placeholder(
            tf.float32, shape=(None, self.logit_dim))
        # Value function predictions before the policy update.
        self.prev_vf_preds = tf.placeholder(tf.float32, shape=(None,))

        # assert config["sgd_batchsize"] % len(devices) == 0, \
        #     "Batch size must be evenly divisible by devices"
        # if is_remote:
        #     self.batch_size = config["rollout_batchsize"]
        #     self.per_device_batch_size = config["rollout_batchsize"]
        # else:
        #     self.batch_size = config["sgd_batchsize"]
        #     self.per_device_batch_size = int(self.batch_size / len(devices))


        # self.par_opt = LocalSyncParallelOptimizer(
        #     tf.train.AdamOptimizer(self.config["sgd_stepsize"]),
        #     self.devices,
        if is_remote:
            # TODO(rliaw): figure out how many times the graph is created
            self.common_policy = self.build_tf_loss(
                [self.observations, self.value_targets, self.advantages,
                self.actions, self.prev_logits, self.prev_vf_preds])
            self.variables = ray.experimental.TensorFlowVariables(
                self.common_policy.loss, self.sess)
            self.sampler = SyncSampler(
                self.env, self.common_policy, self.obs_filter,
                self.config["horizon"], self.config["horizon"])
        #     self.per_device_batch_size,
        #     build_loss,
        #     self.logdir)

        # Metric ops

        # References to the model weights
            # self.common_policy = self.par_opt.get_common_loss()
        self.obs_filter = get_filter(
            config["observation_filter"], self.env.observation_space.shape)
        self.rew_filter = MeanStdFilter((), clip=5.0)
        self.filters = {"obs_filter": self.obs_filter,
                        "rew_filter": self.rew_filter}
        # self.sess.run(tf.global_variables_initializer())

    def update_kl(self, new_kl):
        if new_kl > 2.0 * self.config["kl_target"]:
            self.kl_coeff_val *= 1.5
        elif new_kl < 0.5 * self.config["kl_target"]:
            self.kl_coeff_val *= 0.5
        return self.kl_coeff_val

    def extra_feed_dict(self):
        return {self.kl_coeff: self.kl_coeff_value}

    def build_tf_loss(placeholders):
        obs, vtargets, advs, acts, plog, pvf_preds = placeholders
        return ProximalPolicyLoss(
            self.env.observation_space, self.env.action_space,
            obs, vtargets, advs, acts, plog, pvf_preds, self.logit_dim,
            self.kl_coeff, self.distribution_class, self.config,
            self.sess, self.registry)

    def tf_loss_inputs(self):
        loss_inputs = [
            ("observations", self.observations),
            ("value_targets", self.value_targets),
            ("advantages", self.advantages),
            ("actions", self.actions),
            ("logprobs", self.prev_logits),
            ("vf_preds", self.prev_vf_preds)]
        return loss_inputs

    def tf_extra_ops(self, loss_objects):
        with tf.name_scope("test_outputs"):
            extra_ops_list = [k.extra_ops() for k in loss_objects]
            import ipdb; ipdb.set_trace()
            extra_ops = defaultdict(list)
            for op in extra_ops_list:
                for k in op:
                    extra_ops[k].append(op[k])

            for k in extra_ops:
                extra_ops[k] = tf.reduce_mean(
                    tf.stack(values=extra_ops[k]), 0)
        return extra_ops

    # def run_sgd_minibatch(
    #         self, batch_index, kl_coeff, full_trace, file_writer):
    #     return self.par_opt.optimize(
    #         self.sess,
    #         batch_index,
    #         extra_ops=[
    #             self.mean_loss, self.mean_policy_loss, self.mean_vf_loss,
    #             self.mean_kl, self.mean_entropy],
    #         extra_feed_dict={self.kl_coeff: kl_coeff},
    #         file_writer=file_writer if full_trace else None)

    def compute_gradients(self, samples):
        raise NotImplementedError

    def apply_gradients(self, grads):
        raise NotImplementedError

    def save(self):
        filters = self.get_filters(flush_after=True)
        return pickle.dumps({"filters": filters})

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.sync_filters(objs["filters"])

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
            samples = process_rollout(
                rollout, self.rew_filter, self.config["gamma"],
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


RemotePPOEvaluator = ray.remote(PPOEvaluator)
