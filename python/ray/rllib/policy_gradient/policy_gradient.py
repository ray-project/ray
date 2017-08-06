from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
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
    # Discount factor of the MDP
    "gamma": 0.995,
    # Number of steps after which the rollout gets cut
    "horizon": 2000,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.2,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # Number of outer loop iterations
    "max_iterations": 1000,
    # Stepsize of SGD
    "sgd_stepsize": 5e-5,
    # TODO(pcm): Expose the choice between gpus and cpus
    # as a command line argument.
    "devices": ["/cpu:%d" % i for i in range(4)],
    "tf_session_args": {
        "device_count": {"CPU": 4},
        "log_device_placement": False,
        "allow_soft_placement": True,
    },
    # Batch size for policy evaluations for rollouts
    "rollout_batchsize": 1,
    # Total SGD batch size across all devices for SGD
    "sgd_batchsize": 128,
    # Coefficient of the entropy regularizer
    "entropy_coeff": 0.0,
    # PPO clip parameter
    "clip_param": 0.3,
    # Target value for KL divergence
    "kl_target": 0.01,
    "model": {"free_logstd": False},
    # Number of timesteps collected in each outer loop
    "timesteps_per_batch": 40000,
    # Each tasks performs rollouts until at least this
    # number of steps is obtained
    "min_steps_per_task": 1000,
    # Number of actors used to collect the rollouts
    "num_agents": 5,
    # Dump TensorFlow timeline after this many SGD minibatches
    "full_trace_nth_sgd_batch": -1,
    # Whether to profile data loading
    "full_trace_data_load": False,
    # If this is True, the TensorFlow debugger is invoked if an Inf or NaN
    # is detected
    "use_tf_debugger": False,
    # If True, we write checkpoints and tensorflow logging
    "write_logs": True, 
    # Name of the model checkpoint file
    "model_checkpoint_file": "iteration-%s.ckpt"}


class PolicyGradient(Algorithm):
    def __init__(self, env_name, config, upload_dir=None):
        config.update({"alg": "PolicyGradient"})

        Algorithm.__init__(self, env_name, config, upload_dir=upload_dir)

        # TODO(ekl): preprocessor should be associated with the env elsewhere
        if self.env_name == "Pong-v0":
            preprocessor = AtariPixelPreprocessor()
        elif self.env_name == "Pong-ram-v3":
            preprocessor = AtariRamPreprocessor()
        elif self.env_name == "CartPole-v0" or self.env_name == "CartPole-v1":
            preprocessor = NoPreprocessor()
        elif self.env_name == "Hopper-v1":
            preprocessor = NoPreprocessor()
        elif self.env_name == "Walker2d-v1":
            preprocessor = NoPreprocessor()
        elif self.env_name == "Humanoid-v1":
            preprocessor = NoPreprocessor()
        else:
            preprocessor = AtariPixelPreprocessor()

        self.preprocessor = preprocessor
        self.global_step = 0
        self.j = 0
        self.kl_coeff = config["kl_coeff"]
        self.model = Agent(
            self.env_name, 1, self.preprocessor, self.config, self.logdir,
            False)
        self.agents = [
            RemoteAgent.remote(
                self.env_name, 1, self.preprocessor, self.config,
                self.logdir, True)
            for _ in range(config["num_agents"])]
        self.start_time = time.time()

    def train(self):
        agents = self.agents
        config = self.config
        model = self.model
        j = self.j
        self.j += 1

        print("===> iteration", self.j)

        saver = tf.train.Saver(max_to_keep=None)
        if "load_checkpoint" in config:
            saver.restore(model.sess, config["load_checkpoint"])

        # TF does not support to write logs to S3 at the moment
        write_tf_logs = config["write_logs"] and self.logdir.startswith("file")
        iter_start = time.time()
        if write_tf_logs:
            file_writer = tf.summary.FileWriter(self.logdir, model.sess.graph)
            if config["model_checkpoint_file"]:
                checkpoint_path = saver.save(
                    model.sess,
                    os.path.join(
                        self.logdir, config["model_checkpoint_file"] % j))
                print("Checkpoint saved in file: %s" % checkpoint_path)
        checkpointing_end = time.time()
        weights = ray.put(model.get_weights())
        [a.load_weights.remote(weights) for a in agents]
        trajectory, total_reward, traj_len_mean = collect_samples(
            agents, config)
        print("total reward is ", total_reward)
        print("trajectory length mean is ", traj_len_mean)
        print("timesteps:", trajectory["dones"].shape[0])
        if write_tf_logs:
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
            num_batches = (
                int(tuples_per_device) // int(model.per_device_batch_size))
            loss, kl, entropy = [], [], []
            permutation = np.random.permutation(num_batches)
            while batch_index < num_batches:
                full_trace = (
                    i == 0 and j == 0 and
                    batch_index == config["full_trace_nth_sgd_batch"])
                batch_loss, batch_kl, batch_entropy = model.run_sgd_minibatch(
                    permutation[batch_index] * model.per_device_batch_size,
                    self.kl_coeff, full_trace,
                    file_writer if write_tf_logs else None)
                loss.append(batch_loss)
                kl.append(batch_kl)
                entropy.append(batch_entropy)
                batch_index += 1
            loss = np.mean(loss)
            kl = np.mean(kl)
            entropy = np.mean(entropy)
            sgd_end = time.time()
            print(
                "{:>15}{:15.5e}{:15.5e}{:15.5e}".format(i, loss, kl, entropy))

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
            if write_tf_logs:
                sgd_stats = tf.Summary(value=values)
                file_writer.add_summary(sgd_stats, self.global_step)
            self.global_step += 1
            sgd_time += sgd_end - sgd_start
        if kl > 2.0 * config["kl_target"]:
            self.kl_coeff *= 1.5
        elif kl < 0.5 * config["kl_target"]:
            self.kl_coeff *= 0.5

        info = {
            "kl_divergence": kl,
            "kl_coefficient": self.kl_coeff,
            "checkpointing_time": checkpointing_time,
            "rollouts_time": rollouts_time,
            "shuffle_time": shuffle_time,
            "load_time": load_time,
            "sgd_time": sgd_time,
            "sample_throughput": len(trajectory["observations"]) / sgd_time
        }

        print("kl div:", kl)
        print("kl coeff:", self.kl_coeff)
        print("checkpointing time:", checkpointing_time)
        print("rollouts time:", rollouts_time)
        print("shuffle time:", shuffle_time)
        print("load time:", load_time)
        print("sgd time:", sgd_time)
        print("sgd examples/s:", len(trajectory["observations"]) / sgd_time)
        print("total time so far:", time.time() - self.start_time)

        result = TrainingResult(
            self.experiment_id.hex, j, total_reward, traj_len_mean, info)

        return result
