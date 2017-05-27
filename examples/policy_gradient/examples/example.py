from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from datetime import datetime

import argparse
import time

import ray
import tensorflow as tf

from reinforce.env import (NoPreprocessor, AtariRamPreprocessor,
                           AtariPixelPreprocessor)
from reinforce.agent import Agent, RemoteAgent
from reinforce.rollout import collect_samples
from reinforce.utils import iterate, shuffle
from tensorflow.python.client import timeline


config = {"kl_coeff": 0.2,
          "num_sgd_iter": 15,
          "max_iterations": 1000,
          "sgd_stepsize": 5e-5,
          "devices": ["/cpu:1", "/cpu:2", "/cpu:3"],
          "tf_session_args": {
              "device_count": {"CPU": 4},
              "log_device_placement": True,
          },
          "sgd_batchsize": 128,
          "entropy_coeff": 0.0,
          "clip_param": 0.3,
          "kl_target": 0.01,
          "timesteps_per_batch": 4000,
          "num_agents": 5,
          "tensorboard_log_dir": "/tmp/ray",
          "full_trace_nth_batch": 5}


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Run the policy gradient "
                                               "algorithm.")
  parser.add_argument("--environment", default="Pong-ram-v3", type=str,
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
  else:
    print("No environment was chosen, so defaulting to Pong-v0.")
    mdp_name = "Pong-v0"
    preprocessor = AtariPixelPreprocessor()

  print("Using the environment {}.".format(mdp_name))
  agents = [RemoteAgent.remote(mdp_name, 1, preprocessor, config, False)
            for _ in range(config["num_agents"])]
  agent = Agent(mdp_name, 1, preprocessor, config, True)

  kl_coeff = config["kl_coeff"]

  file_writer = tf.summary.FileWriter(
      '{}/trpo_{}_{}'.format(
          config["tensorboard_log_dir"], mdp_name, datetime.today()),
      agent.sess.graph)
  global_step = 0
  for j in range(config["max_iterations"]):
    start = time.time()
    print("== iteration", j)
    weights = ray.put(agent.get_weights())
    [a.load_weights.remote(weights) for a in agents]
    trajectory, total_reward, traj_len_mean = collect_samples(
        agents, config["timesteps_per_batch"], 0.995, 1.0, 2000)
    print("total reward is ", total_reward)
    print("trajectory length mean is ", traj_len_mean)
    print("timesteps: ", trajectory["dones"].shape[0])
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
    print("Computing policy (iterations=" + str(config["num_sgd_iter"]) +
          ", stepsize=" + str(config["sgd_stepsize"]) + "):")
    names = ["iter", "loss", "kl", "entropy"]
    print(("{:>15}" * len(names)).format(*names))
    trajectory = shuffle(trajectory)
    num_devices = len(config["devices"])
    for i in range(config["num_sgd_iter"]):
      # Test on current set of rollouts.
      agent.sess.run(
          [agent.stage_trajectory_data_op],
          feed_dict=agent.make_feed_dict(trajectory, kl_coeff))
      loss, kl, entropy = agent.sess.run(
          [agent.mean_loss, agent.mean_kl, agent.mean_entropy],
          feed_dict=agent.make_feed_dict(None, kl_coeff))
      print("{:>15}{:15.5e}{:15.5e}{:15.5e}".format(i, loss, kl, entropy))

      # Run pipelined SGD training on current set of rollouts.
      for n, batch in enumerate(iterate(trajectory, config["sgd_batchsize"])):
        if n == 0:
          # Warmup: just push data into the pipeline
          agent.sess.run(
              [agent.stage_trajectory_data_op],
              feed_dict=agent.make_feed_dict(batch, kl_coeff))
        else:
          # Steady state: train on prev batch and push in new data
          full_trace = i == 0 and n == config["full_trace_nth_batch"]
          if full_trace:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.FULL_TRACE)
          else:
            run_options = tf.RunOptions(trace_level=tf.RunOptions.NO_TRACE)
          run_metadata = tf.RunMetadata()
          agent.sess.run(
              [agent.train_op, agent.stage_trajectory_data_op],
              feed_dict=agent.make_feed_dict(batch, kl_coeff),
              options=run_options, run_metadata=run_metadata)
          if full_trace:
            trace = timeline.Timeline(step_stats=run_metadata.step_stats)
            trace_file = open('/tmp/ray/timeline.json', 'w')
            trace_file.write(trace.generate_chrome_trace_format())
            file_writer.add_run_metadata(
                run_metadata, "sgd_train_{}".format(j))
      # End pipeline: Finish training on the last staged batch.
      agent.sess.run(
          [agent.train_op], feed_dict=agent.make_feed_dict(None, kl_coeff))

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
    if kl > 2.0 * config["kl_target"]:
      kl_coeff *= 1.5
    elif kl < 0.5 * config["kl_target"]:
      kl_coeff *= 0.5
    print("kl div = ", kl)
    print("kl coeff = ", kl_coeff)
    print("examples per second: {}".format(
        len(trajectory["observations"]) / (time.time() - start)))
