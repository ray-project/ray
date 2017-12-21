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

from ray.rllib.evaluator import Evaluator
from ray.rllib.parallel import LocalSyncParallelOptimizer
from ray.rllib.models import ModelCatalog
from ray.rllib.envs import create_and_wrap
from ray.rllib.utils.sampler import SyncSampler
from ray.rllib.utils.filter import get_filter, MeanStdFilter
from ray.rllib.utils.process_rollout import process_rollout
from ray.rllib.ppo.loss import ProximalPolicyLoss
from ray.rllib.ppo.utils import concatenate


class PPOEvaluator(Evaluator):
    """
    Runner class that holds the simulator environment and the policy.

    Initializes the tensorflow graphs for both training and evaluation.
    One common policy graph is initialized on '/cpu:0' and holds all the shared
    network weights. When run as a remote agent, only this graph is used.
    """

    def __init__(self, env_creator, config, logdir, is_remote):
        self.is_remote = is_remote
        if is_remote:
            os.environ["CUDA_VISIBLE_DEVICES"] = ""
            devices = ["/cpu:0"]
        else:
            devices = config["devices"]
        self.devices = devices
        self.config = config
        self.logdir = logdir
        self.env = create_and_wrap(env_creator, config["preprocessing"])
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

        assert config["sgd_batchsize"] % len(devices) == 0, \
            "Batch size must be evenly divisible by devices"
        if is_remote:
            self.batch_size = config["rollout_batchsize"]
            self.per_device_batch_size = config["rollout_batchsize"]
        else:
            self.batch_size = config["sgd_batchsize"]
            self.per_device_batch_size = int(self.batch_size / len(devices))

        def build_loss(obs, vtargets, advs, acts, plog, pvf_preds):
            return ProximalPolicyLoss(
                self.env.observation_space, self.env.action_space,
                obs, vtargets, advs, acts, plog, pvf_preds, self.logit_dim,
                self.kl_coeff, self.distribution_class, self.config,
                self.sess)

        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.config["sgd_stepsize"]),
            self.devices,
            [self.observations, self.value_targets, self.advantages,
             self.actions, self.prev_logits, self.prev_vf_preds],
            self.per_device_batch_size,
            build_loss,
            self.logdir)

        # Metric ops
        with tf.name_scope("test_outputs"):
            policies = self.par_opt.get_device_losses()
            self.mean_loss = tf.reduce_mean(
                tf.stack(values=[
                    policy.loss for policy in policies]), 0)
            self.mean_policy_loss = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_policy_loss for policy in policies]), 0)
            self.mean_vf_loss = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_vf_loss for policy in policies]), 0)
            self.mean_kl = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_kl for policy in policies]), 0)
            self.mean_entropy = tf.reduce_mean(
                tf.stack(values=[
                    policy.mean_entropy for policy in policies]), 0)

        # References to the model weights
        self.common_policy = self.par_opt.get_common_loss()
        self.variables = ray.experimental.TensorFlowVariables(
            self.common_policy.loss, self.sess)
        self.obs_filter = get_filter(
            config["observation_filter"], self.env.observation_space.shape)
        self.sampler = SyncSampler(
            self.env, self.common_policy, self.obs_filter,
            self.config["horizon"], self.config["horizon"])
        self.rew_filter = MeanStdFilter((), clip=5.0)
        self.sess.run(tf.global_variables_initializer())

    def load_data(self, trajectories, full_trace):
        use_gae = self.config["use_gae"]
        dummy = np.zeros_like(trajectories["advantages"])
        return self.par_opt.load_data(
            self.sess,
            [trajectories["observations"],
             trajectories["value_targets"] if use_gae else dummy,
             trajectories["advantages"],
             trajectories["actions"],
             trajectories["logprobs"],
             trajectories["vf_preds"] if use_gae else dummy],
            full_trace=full_trace)

    def run_sgd_minibatch(
            self, batch_index, kl_coeff, full_trace, file_writer):
        return self.par_opt.optimize(
            self.sess,
            batch_index,
            extra_ops=[
                self.mean_loss, self.mean_policy_loss, self.mean_vf_loss,
                self.mean_kl, self.mean_entropy],
            extra_feed_dict={self.kl_coeff: kl_coeff},
            file_writer=file_writer if full_trace else None)

    def compute_gradients(self, rollout):
        raise NotImplementedError

    def apply_gradients(self, grads):
        raise NotImplementedError

    def save(self):
        obs_filter, rew_filter = self.get_filters()
        return pickle.dumps([obs_filter, rew_filter])

    def restore(self, objs):
        objs = pickle.loads(objs)
        obs_filter = objs[0]
        rew_filter = objs[1]
        self.sync_filters(obs_filter, rew_filter)

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def sample(self):
        """Compute multiple rollouts and concatenate the results.

        Returns:
            trajectory
        """
        num_steps_so_far = 0
        trajectories = []

        while num_steps_so_far < self.config["min_steps_per_task"]:
            rollout = self.sampler.get_data()
            trajectory = process_rollout(
                rollout, self.rew_filter, self.config["gamma"],
                self.config["lambda"], use_gae=self.config["use_gae"])
            num_steps_so_far += trajectory["rewards"].shape[0]
            trajectories.append(trajectory)
        return concatenate(trajectories)

    def get_completed_rollout_metrics(self):
        """Returns metrics on previously completed rollouts.

        Calling this clears the queue of completed rollout metrics.
        """
        return self.sampler.get_metrics()


RemotePPOEvaluator = ray.remote(PPOEvaluator)
