from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime
import time

import numpy as np
import tensorflow as tf

import ray
from ray.rllib.common import Algorithm, TrainingResult
from ray.rllib.policy_gradient.agent import Agent, RemoteAgent
from ray.rllib.policy_gradient.env import (
    NoPreprocessor, AtariRamPreprocessor, AtariPixelPreprocessor)
from ray.rllib.policy_gradient.rollout import collect_samples
from ray.rllib.policy_gradient.utils import shuffle


DEFAULT_CONFIG = {
    "kl_coeff": 0.2,
    "num_sgd_iter": 30,
    "max_iterations": 1000,
    "sgd_stepsize": 5e-5,
    # TODO(pcm): Expose the choice between gpus and cpus
    # as a command line argument.
    "devices": ["/cpu:%d" % i for i in range(4)],
    "tf_session_args": {
        "device_count": {"CPU": 4},
        "log_device_placement": False,
        "allow_soft_placement": True,
    },
    "sgd_batchsize": 128,  # total size across all devices
    "entropy_coeff": 0.0,
    "clip_param": 0.3,
    "kl_target": 0.01,
    "timesteps_per_batch": 40000,
    "num_agents": 5,
    "tensorboard_log_dir": "/tmp/ray",
    "full_trace_nth_sgd_batch": -1,
    "full_trace_data_load": False,
    "use_tf_debugger": False,
    "model_checkpoint_file": "/tmp/iteration-%s.ckpt"}


class PolicyGradient(Algorithm):
  def __init__(self, env_name, config):
    Algorithm.__init__(self, env_name, config)

    # TODO(ekl) the preprocessor should be associated with the env elsewhere
    if self.env_name == "Pong-v0":
      preprocessor = AtariPixelPreprocessor()
    elif self.env_name == "Pong-ram-v3":
      preprocessor = AtariRamPreprocessor()
    elif self.env_name == "CartPole-v0":
      preprocessor = NoPreprocessor()
    elif self.env_name == "Walker2d-v1":
      preprocessor = NoPreprocessor()
    else:
      preprocessor = AtariPixelPreprocessor()

    self.preprocessor = preprocessor
    self.global_step = 0
    self.j = 0
    self.kl_coeff = config["kl_coeff"]
    self.model = Agent(
        self.env_name, 1, self.preprocessor, self.config, False)
    self.agents = [
        RemoteAgent.remote(
            self.env_name, 1, self.preprocessor, self.config, True)
        for _ in range(config["num_agents"])]

  def train(self):
    agents = self.agents
    config = self.config
    model = self.model
    j = self.j
    self.j += 1

    saver = tf.train.Saver(max_to_keep=None)
    if "load_checkpoint" in config:
      saver.restore(model.sess, config["load_checkpoint"])

    file_writer = tf.summary.FileWriter(
        "{}/trpo_{}_{}".format(
            config["tensorboard_log_dir"], self.env_name,
            str(datetime.today()).replace(" ", "_")),
        model.sess.graph)
    iter_start = time.time()
    if config["model_checkpoint_file"]:
      checkpoint_path = saver.save(
          model.sess, config["model_checkpoint_file"] % j)
      print("Checkpoint saved in file: %s" % checkpoint_path)
    checkpointing_end = time.time()
    weights = ray.put(model.get_weights())
    [a.load_weights.remote(weights) for a in agents]
    trajectory, total_reward, traj_len_mean = collect_samples(
        agents, config["timesteps_per_batch"], 0.995, 1.0, 2000)
    print("total reward is ", total_reward)
    print("trajectory length mean is ", traj_len_mean)
    print("timesteps:", trajectory["dones"].shape[0])
    traj_stats = tf.Summary(value=[
        tf.Summary.Value(
            tag="policy_gradient/rollouts/mean_reward",
            simple_value=total_reward),
        tf.Summary.Value(
            tag="policy_gradient/rollouts/traj_len_mean",
            simple_value=traj_len_mean)])
    file_writer.add_summary(traj_stats, self.global_step)
    self.global_step += 1
    trajectory["advantages"] = ((trajectory["advantages"] -
                                 trajectory["advantages"].mean()) /
                                trajectory["advantages"].std())
    rollouts_end = time.time()
    print("Computing policy (iterations=" + str(config["num_sgd_iter"]) +
          ", stepsize=" + str(config["sgd_stepsize"]) + "):")
    names = ["iter", "loss", "kl", "entropy"]
    print(("{:>15}" * len(names)).format(*names))
    trajectory = shuffle(trajectory)
    shuffle_end = time.time()
    tuples_per_device = model.load_data(
        trajectory, j == 0 and config["full_trace_data_load"])
    load_end = time.time()
    checkpointing_time = checkpointing_end - iter_start
    rollouts_time = rollouts_end - checkpointing_end
    shuffle_time = shuffle_end - rollouts_end
    load_time = load_end - shuffle_end
    sgd_time = 0
    for i in range(config["num_sgd_iter"]):
      sgd_start = time.time()
      batch_index = 0
      num_batches = int(tuples_per_device) // int(model.per_device_batch_size)
      loss, kl, entropy = [], [], []
      permutation = np.random.permutation(num_batches)
      while batch_index < num_batches:
        full_trace = (
            i == 0 and j == 0 and
            batch_index == config["full_trace_nth_sgd_batch"])
        batch_loss, batch_kl, batch_entropy = model.run_sgd_minibatch(
            permutation[batch_index] * model.per_device_batch_size,
            self.kl_coeff, full_trace, file_writer)
        loss.append(batch_loss)
        kl.append(batch_kl)
        entropy.append(batch_entropy)
        batch_index += 1
      loss = np.mean(loss)
      kl = np.mean(kl)
      entropy = np.mean(entropy)
      sgd_end = time.time()
      print("{:>15}{:15.5e}{:15.5e}{:15.5e}".format(i, loss, kl, entropy))

      values = []
      if i == config["num_sgd_iter"] - 1:
        metric_prefix = "policy_gradient/sgd/final_iter/"
        values.append(tf.Summary.Value(
            tag=metric_prefix + "kl_coeff",
            simple_value=self.kl_coeff))
      else:
        metric_prefix = "policy_gradient/sgd/intermediate_iters/"
      values.extend([
          tf.Summary.Value(
              tag=metric_prefix + "mean_entropy",
              simple_value=entropy),
          tf.Summary.Value(
              tag=metric_prefix + "mean_loss",
              simple_value=loss),
          tf.Summary.Value(
              tag=metric_prefix + "mean_kl",
              simple_value=kl)])
      sgd_stats = tf.Summary(value=values)
      file_writer.add_summary(sgd_stats, self.global_step)
      self.global_step += 1
      sgd_time += sgd_end - sgd_start
    if kl > 2.0 * config["kl_target"]:
      self.kl_coeff *= 1.5
    elif kl < 0.5 * config["kl_target"]:
      self.kl_coeff *= 0.5

    print("kl div:", kl)
    print("kl coeff:", self.kl_coeff)
    print("checkpointing time:", checkpointing_time)
    print("rollouts time:", rollouts_time)
    print("shuffle time:", shuffle_time)
    print("load time:", load_time)
    print("sgd time:", sgd_time)
    print("sgd examples/s:", len(trajectory["observations"]) / sgd_time)

    return TrainingResult(j, total_reward, traj_len_mean)
