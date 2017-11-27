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

from ray.rllib.parallel import LocalSyncParallelOptimizer
from ray.rllib.models import ModelCatalog
from ray.rllib.ppo.env import BatchedEnv
from ray.rllib.ppo.loss import ProximalPolicyLoss
from ray.rllib.ppo.filter import NoFilter, MeanStdFilter
from ray.rllib.ppo.rollout import (
    rollouts, add_return_values, add_advantage_values)
from ray.rllib.ppo.utils import flatten, concatenate

# TODO(pcm): Make sure that both observation_filter and reward_filter
# are correctly handled, i.e. (a) the values are accumulated accross
# workers (if necessary), (b) they are passed between all the methods
# correctly and no default arguments are used, and (c) they are saved
# as part of the checkpoint so training can resume properly.


class Runner(object):
    """
    Runner class that holds the simulator environment and the policy.

    Initializes the tensorflow graphs for both training and evaluation.
    One common policy graph is initialized on '/cpu:0' and holds all the shared
    network weights. When run as a remote agent, only this graph is used.
    """

    def __init__(self, env_creator, batchsize, config, logdir, is_remote):
        if is_remote:
            os.environ["CUDA_VISIBLE_DEVICES"] = ""
            devices = ["/cpu:0"]
        else:
            devices = config["devices"]
        self.devices = devices
        self.config = config
        self.logdir = logdir
        self.env = BatchedEnv(env_creator, batchsize, config)
        if is_remote:
            config_proto = tf.ConfigProto()
        else:
            config_proto = tf.ConfigProto(**config["tf_session_args"])
        self.preprocessor = self.env.preprocessor
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
            tf.float32, shape=(None,) + self.preprocessor.shape)
        # Targets of the value function.
        self.returns = tf.placeholder(tf.float32, shape=(None,))
        # Advantage values in the policy gradient estimator.
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

        def build_loss(obs, rets, advs, acts, plog, pvf_preds):
            return ProximalPolicyLoss(
                self.env.observation_space, self.env.action_space,
                obs, rets, advs, acts, plog, pvf_preds, self.logit_dim,
                self.kl_coeff, self.distribution_class, self.config,
                self.sess)

        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.config["sgd_stepsize"]),
            self.devices,
            [self.observations, self.returns, self.advantages,
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
        if config["observation_filter"] == "MeanStdFilter":
            self.observation_filter = MeanStdFilter(
                self.preprocessor.shape, clip=None)
        elif config["observation_filter"] == "NoFilter":
            self.observation_filter = NoFilter()
        else:
            raise Exception("Unknown observation_filter: " +
                            str(config["observation_filter"]))
        self.reward_filter = MeanStdFilter((), clip=5.0)
        self.sess.run(tf.global_variables_initializer())

    def load_data(self, trajectories, full_trace):
        if self.config["use_gae"]:
            return self.par_opt.load_data(
                self.sess,
                [trajectories["observations"],
                 trajectories["td_lambda_returns"],
                 trajectories["advantages"],
                 trajectories["actions"].squeeze(),
                 trajectories["logprobs"],
                 trajectories["vf_preds"]],
                full_trace=full_trace)
        else:
            dummy = np.zeros((trajectories["observations"].shape[0],))
            return self.par_opt.load_data(
                self.sess,
                [trajectories["observations"],
                 dummy,
                 trajectories["returns"],
                 trajectories["actions"].squeeze(),
                 trajectories["logprobs"],
                 dummy],
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

    def save(self):
        return pickle.dumps([self.observation_filter, self.reward_filter])

    def restore(self, objs):
        objs = pickle.loads(objs)
        self.observation_filter = objs[0]
        self.reward_filter = objs[1]

    def get_weights(self):
        return self.variables.get_weights()

    def load_weights(self, weights):
        self.variables.set_weights(weights)

    def compute_trajectory(self, gamma, lam, horizon):
        """Compute a single rollout on the agent and return."""
        trajectory = rollouts(
            self.common_policy,
            self.env, horizon, self.observation_filter, self.reward_filter)
        if self.config["use_gae"]:
            add_advantage_values(trajectory, gamma, lam, self.reward_filter)
        else:
            add_return_values(trajectory, gamma, self.reward_filter)
        return trajectory

    def compute_steps(
            self, gamma, lam, horizon, min_steps_per_task,
            observation_filter, reward_filter):
        """Compute multiple rollouts and concatenate the results.

        Args:
            gamma: MDP discount factor
            lam: GAE(lambda) parameter
            horizon: Number of steps after which a rollout gets cut
            min_steps_per_task: Lower bound on the number of states to be
                collected.
            observation_filter: Function that is applied to each of the
                observations.
            reward_filter: Function that is applied to each of the rewards.

        Returns:
            states: List of states.
            total_rewards: Total rewards of the trajectories.
            trajectory_lengths: Lengths of the trajectories.
        """

        # Update our local filters
        self.observation_filter = observation_filter.copy()
        self.reward_filter = reward_filter.copy()

        num_steps_so_far = 0
        trajectories = []
        total_rewards = []
        trajectory_lengths = []
        while True:
            trajectory = self.compute_trajectory(gamma, lam, horizon)
            total_rewards.append(
                trajectory["raw_rewards"].sum(axis=0).mean())
            trajectory_lengths.append(
                np.logical_not(trajectory["dones"]).sum(axis=0).mean())
            trajectory = flatten(trajectory)
            not_done = np.logical_not(trajectory["dones"])
            # Filtering out states that are done. We do this because
            # trajectories are batched and cut only if all the trajectories
            # in the batch terminated, so we can potentially get rid of
            # some of the states here.
            trajectory = {key: val[not_done]
                          for key, val in trajectory.items()}
            num_steps_so_far += trajectory["raw_rewards"].shape[0]
            trajectories.append(trajectory)
            if num_steps_so_far >= min_steps_per_task:
                break
        return (
            concatenate(trajectories),
            total_rewards,
            trajectory_lengths,
            self.observation_filter,
            self.reward_filter)


RemoteRunner = ray.remote(Runner)
