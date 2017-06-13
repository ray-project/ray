from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import argparse
import time

import ray
import numpy as np
import tensorflow as tf

from reinforce.env import (NoPreprocessor, AtariRamPreprocessor,
                           AtariPixelPreprocessor)
from reinforce.agent import Agent, RemoteAgent
from reinforce.rollout import collect_samples
from reinforce.utils import shuffle


config = {"kl_coeff": 0.2,
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
          "full_trace_data_load": False}


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Run the policy gradient "
                                               "algorithm.")
  parser.add_argument("--environment", default="Pong-v0", type=str,
                      help="The gym environment to use.")
  parser.add_argument("--redis-address", default=None, type=str,
                      help="The Redis address of the cluster.")

  args = parser.parse_args()

  ray.init(redis_address=args.redis_address)

  mdp_name = args.environment
  if args.environment == "Pong-v0":
    preprocessor = AtariPixelPreprocessor()
  elif mdp_name == "Pong-ram-v3":
    preprocessor = AtariRamPreprocessor()
  elif mdp_name == "CartPole-v0":
    preprocessor = NoPreprocessor()
  elif mdp_name == "Walker2d-v1":
    preprocessor = NoPreprocessor()
  else:
    print("No environment was chosen, so defaulting to Pong-v0.")
    mdp_name = "Pong-v0"
    preprocessor = AtariPixelPreprocessor()

  print("Using the environment {}.".format(mdp_name))
  agents = [RemoteAgent.remote(mdp_name, 1, preprocessor, config, True)
            for _ in range(config["num_agents"])]
  agent = Agent(mdp_name, 1, preprocessor, config, False)

  kl_coeff = config["kl_coeff"]

  file_writer = tf.summary.FileWriter(
      "{}/trpo_{}_{}".format(
          config["tensorboard_log_dir"], mdp_name,
          str(datetime.today()).replace(" ", "_")),
      agent.sess.graph)
  global_step = 0
  for j in range(config["max_iterations"]):
    iter_start = time.time()
    print("== iteration", j)
    weights = ray.put(agent.get_weights())
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
    file_writer.add_summary(traj_stats, global_step)
    global_step += 1
    trajectory["advantages"] = ((trajectory["advantages"] -
                                 trajectory["advantages"].mean()) /
                                trajectory["advantages"].std())
    rollouts_end = time.time()
    print("Computing policy (iterations=" + str(config["num_sgd_iter"]) +
          ", stepsize=" + str(config["sgd_stepsize"]) + "):")
    names = ["iter", "loss", "kl", "entropy"]
    print(("{:>15}" * len(names)).format(*names))
    num_devices = len(config["devices"])
    trajectory = shuffle(trajectory)
    shuffle_end = time.time()
    tuples_per_device = agent.load_data(
        trajectory, j == 0 and config["full_trace_data_load"])
    load_end = time.time()
    rollouts_time = rollouts_end - iter_start
    shuffle_time = shuffle_end - rollouts_end
    load_time = load_end - shuffle_end
    sgd_time = 0
    for i in range(config["num_sgd_iter"]):
      sgd_start = time.time()
      batch_index = 0
      num_batches = int(tuples_per_device) // int(agent.per_device_batch_size)
      loss, kl, entropy = [], [], []
      permutation = np.random.permutation(num_batches)
      while batch_index < num_batches:
        full_trace = (
            i == 0 and j == 0 and
            batch_index == config["full_trace_nth_sgd_batch"])
        batch_loss, batch_kl, batch_entropy = agent.run_sgd_minibatch(
            permutation[batch_index] * agent.per_device_batch_size,
            kl_coeff, full_trace, file_writer)
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
            simple_value=kl_coeff))
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
      file_writer.add_summary(sgd_stats, global_step)
      global_step += 1
      sgd_time += sgd_end - sgd_start
    if kl > 2.0 * config["kl_target"]:
      kl_coeff *= 1.5
    elif kl < 0.5 * config["kl_target"]:
      kl_coeff *= 0.5
    print("kl div:", kl)
    print("kl coeff:", kl_coeff)
    print("rollouts time:", rollouts_time)
    print("shuffle time:", shuffle_time)
    print("load time:", load_time)
    print("sgd time:", sgd_time)
    print("sgd examples/s:", len(trajectory["observations"]) / sgd_time)
