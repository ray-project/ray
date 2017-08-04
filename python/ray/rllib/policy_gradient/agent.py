from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym.spaces
import tensorflow as tf
import os

from tensorflow.python import debug as tf_debug

import numpy as np
import ray

from ray.rllib.parallel import LocalSyncParallelOptimizer
from ray.rllib.models import ModelCatalog
from ray.rllib.policy_gradient.env import BatchedEnv
from ray.rllib.policy_gradient.loss import ProximalPolicyLoss
from ray.rllib.policy_gradient.filter import MeanStdFilter
from ray.rllib.policy_gradient.rollout import rollouts, add_advantage_values
from ray.rllib.policy_gradient.utils import flatten, concatenate

# TODO(pcm): Make sure that both observation_filter and reward_filter
# are correctly handled, i.e. (a) the values are accumulated accross
# workers (if necessary), (b) they are passed between all the methods
# correctly and no default arguments are used, and (c) they are saved
# as part of the checkpoint so training can resume properly.


class Agent(object):
    """
    Agent class that holds the simulator environment and the policy.

    Initializes the tensorflow graphs for both training and evaluation.
    One common policy graph is initialized on '/cpu:0' and holds all the shared
    network weights. When run as a remote agent, only this graph is used.
    """

    def __init__(
            self, name, batchsize, preprocessor, config, logdir, is_remote):
        if is_remote:
            os.environ["CUDA_VISIBLE_DEVICES"] = ""
            devices = ["/cpu:0"]
        else:
            devices = config["devices"]
        self.devices = devices
        self.config = config
        self.logdir = logdir
        self.env = BatchedEnv(name, batchsize, preprocessor=preprocessor)
        if preprocessor.shape is None:
            preprocessor.shape = self.env.observation_space.shape
        if is_remote:
            config_proto = tf.ConfigProto()
        else:
            config_proto = tf.ConfigProto(**config["tf_session_args"])
        self.preprocessor = preprocessor
        self.sess = tf.Session(config=config_proto)
        if config["use_tf_debugger"] and not is_remote:
            self.sess = tf_debug.LocalCLIDebugWrapperSession(self.sess)
            self.sess.add_tensor_filter(
                "has_inf_or_nan", tf_debug.has_inf_or_nan)

        # Defines the training inputs.
        self.kl_coeff = tf.placeholder(
            name="newkl", shape=(), dtype=tf.float32)
        self.observations = tf.placeholder(
            tf.float32, shape=(None,) + preprocessor.shape)
        self.advantages = tf.placeholder(tf.float32, shape=(None,))

        action_space = self.env.action_space
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
        self.prev_logits = tf.placeholder(
            tf.float32, shape=(None, self.logit_dim))

        assert config["sgd_batchsize"] % len(devices) == 0, \
            "Batch size must be evenly divisible by devices"
        if is_remote:
            self.batch_size = 1
            self.per_device_batch_size = 1
        else:
            self.batch_size = config["sgd_batchsize"]
            self.per_device_batch_size = int(self.batch_size / len(devices))

        def build_loss(obs, advs, acts, plog):
            return ProximalPolicyLoss(
                self.env.observation_space, self.env.action_space,
                obs, advs, acts, plog, self.logit_dim,
                self.kl_coeff, self.distribution_class, self.config,
                self.sess)

        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.config["sgd_stepsize"]),
            self.devices,
            [self.observations, self.advantages, self.actions,
             self.prev_logits],
            self.per_device_batch_size,
            build_loss,
            self.logdir)

        # Metric ops
        with tf.name_scope("test_outputs"):
            policies = self.par_opt.get_device_losses()
            self.mean_loss = tf.reduce_mean(
                tf.stack(values=[policy.loss for policy in policies]), 0)
            self.mean_kl = tf.reduce_mean(
                tf.stack(values=[policy.mean_kl for policy in policies]), 0)
            self.mean_entropy = tf.reduce_mean(
                tf.stack(
                    values=[policy.mean_entropy for policy in policies]), 0)

        # References to the model weights
        self.common_policy = self.par_opt.get_common_loss()
        self.variables = ray.experimental.TensorFlowVariables(
            self.common_policy.loss, self.sess)
        self.observation_filter = MeanStdFilter(preprocessor.shape, clip=None)
        self.reward_filter = MeanStdFilter((), clip=5.0)
        self.sess.run(tf.global_variables_initializer())

    def load_data(self, trajectories, full_trace):
        return self.par_opt.load_data(
            self.sess,
            [trajectories["observations"],
             trajectories["advantages"],
             trajectories["actions"].squeeze(),
             trajectories["logprobs"]],
            full_trace=full_trace)

    def run_sgd_minibatch(
            self, batch_index, kl_coeff, full_trace, file_writer):
        return self.par_opt.optimize(
            self.sess,
            batch_index,
            extra_ops=[self.mean_loss, self.mean_kl, self.mean_entropy],
            extra_feed_dict={self.kl_coeff: kl_coeff},
            file_writer=file_writer if full_trace else None)

    def get_weights(self):
        return self.variables.get_weights()

    def load_weights(self, weights):
        self.variables.set_weights(weights)

    def compute_trajectory(self, gamma, lam, horizon):
        """Compute a single rollout on the agent and return."""
        trajectory = rollouts(
            self.common_policy,
            self.env, horizon, self.observation_filter, self.reward_filter)
        add_advantage_values(trajectory, gamma, lam, self.reward_filter)
        return trajectory

    def compute_steps(self, gamma, lam, horizon, num_steps=-1):
        """Compute multiple rollouts and concatenate the results.

        Parameters:
            num_states: Lower bound on the number of states to be collected.
        Returns:
            states: List of states.
            total_rewards: Total rewards of the trajectories.
            trajectory_lengths: Lengths of the trajectories.
        """
        num_states_so_far = 0
        trajectories = []
        total_rewards = []
        traj_lengths = []
        while num_states_so_far < num_steps:
            trajectory = rollouts(
                self.common_policy,
                self.env, horizon, self.observation_filter, self.reward_filter)
            add_advantage_values(trajectory, gamma, lam, self.reward_filter)
            trajectory = flatten(trajectory)
            not_done = np.logical_not(trajectory["dones"])
            total_rewards.append(
                trajectory["raw_rewards"][not_done].sum(axis=0).mean())
            traj_lengths.append(not_done.sum(axis=0).mean())
            trajectory = {key: val[not_done] for key, val in trajectory.items()}
            num_states_so_far += len(trajectory["dones"])
            trajectories.append(trajectory)
        return concatenate(trajectories), total_rewards, traj_lengths


RemoteAgent = ray.remote(Agent)
