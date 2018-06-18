from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

import numpy as np
import pickle
import tensorflow as tf
from tensorflow.python import debug as tf_debug

import ray
from ray.tune.result import TrainingResult
from ray.tune.trial import Resources
from ray.rllib.agent import Agent
from ray.rllib.utils import FilterManager
from ray.rllib.ppo.ppo_evaluator import PPOEvaluator
from ray.rllib.optimizers.multi_gpu import LocalMultiGPUOptimizer
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator, \
    collect_metrics
from ray.rllib.ppo.rollout import collect_samples


DEFAULT_CONFIG = {
    # Discount factor of the MDP
    "gamma": 0.995,
    # Number of steps after which the rollout gets cut
    "horizon": 2000,
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.2,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # Stepsize of SGD
    "sgd_stepsize": 5e-5,
    # TODO(pcm): Expose the choice between gpus and cpus
    # as a command line argument.
    "devices": ["/cpu:%d" % i for i in range(4)],
    "tf_session_args": {
        "device_count": {"CPU": 4},
        "log_device_placement": False,
        "allow_soft_placement": True,
        "intra_op_parallelism_threads": 1,
        "inter_op_parallelism_threads": 1,
    },
    # Batch size for policy evaluations for rollouts
    "rollout_batchsize": 1,
    # Total SGD batch size across all devices for SGD
    "sgd_batchsize": 128,
    # Coefficient of the value function loss
    "vf_loss_coeff": 1.0,
    # Coefficient of the entropy regularizer
    "entropy_coeff": 0.0,
    # PPO clip parameter
    "clip_param": 0.3,
    # Target value for KL divergence
    "kl_target": 0.01,
    # Config params to pass to the model
    "model": {"free_log_std": False},
    # Which observation filter to apply to the observation
    "observation_filter": "MeanStdFilter",
    # If >1, adds frameskip
    "extra_frameskip": 1,
    # Number of timesteps collected in each outer loop
    "timesteps_per_batch": 4000,
    # Each tasks performs rollouts until at least this
    # number of steps is obtained
    "min_steps_per_task": 200,
    # Number of actors used to collect the rollouts
    "num_workers": 5,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Dump TensorFlow timeline after this many SGD minibatches
    "full_trace_nth_sgd_batch": -1,
    # Whether to profile data loading
    "full_trace_data_load": False,
    # Outer loop iteration index when we drop into the TensorFlow debugger
    "tf_debug_iteration": -1,
    # If this is True, the TensorFlow debugger is invoked if an Inf or NaN
    # is detected
    "tf_debug_inf_or_nan": False,
    # If True, we write tensorflow logs and checkpoints
    "write_logs": True,
    # Arguments to pass to the env creator
    "env_config": {},
}


class PPOAgent(Agent):
    _agent_name = "PPO"
    _allow_unknown_subkeys = ["model", "tf_session_args", "env_config"]
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=len([d for d in cf["devices"] if "gpu" in d.lower()]),
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def _init(self):
        self.global_step = 0
        self.kl_coeff_val = self.config["kl_coeff"]
        self.local_evaluator = PPOEvaluator(
            self.registry, self.env_creator, self.config, self.logdir, False)
        RemotePPOEvaluator = ray.remote(
            num_cpus=self.config["num_cpus_per_worker"],
            num_gpus=self.config["num_gpus_per_worker"])(PPOEvaluator)
        self.remote_evaluators = [
            RemotePPOEvaluator.remote(
                self.registry, self.env_creator, self.config, self.logdir,
                True)
            for _ in range(self.config["num_workers"])]

        self.optimizer = LocalMultiGPUOptimizer(
            {"sgd_batch_size": self.config["sgd_batchsize"],
             "sgd_stepsize": self.config["sgd_stepsize"],
             "num_sgd_iter": self.config["num_sgd_iter"]},
            self.local_evaluator, self.remote_evaluators,)

        self.saver = tf.train.Saver(max_to_keep=None)

    def _train(self):
        agents = self.remote_evaluators
        config = self.config
        model = self.local_evaluator

        if (config["num_workers"] * config["min_steps_per_task"] >
                config["timesteps_per_batch"]):
            print(
                "WARNING: num_workers * min_steps_per_task > "
                "timesteps_per_batch. This means that the output of some "
                "tasks will be wasted. Consider decreasing "
                "min_steps_per_task or increasing timesteps_per_batch.")

        weights = ray.put(model.get_weights())
        [a.set_weights.remote(weights) for a in agents]
        samples = collect_samples(agents, config, self.local_evaluator)

        def standardized(value):
            # Divide by the maximum of value.std() and 1e-4
            # to guard against the case where all values are equal
            return (value - value.mean()) / max(1e-4, value.std())

        samples.data["advantages"] = standardized(samples["advantages"])
        print("Computing policy (iterations=" + str(config["num_sgd_iter"]) +
              ", stepsize=" + str(config["sgd_stepsize"]) + "):")
        names = [
            "iter", "total loss", "policy loss", "vf loss", "kl", "entropy"]
        print(("{:>15}" * len(names)).format(*names))
        samples.shuffle()

        use_gae = self.config["use_gae"]
        dummy = np.zeros_like(samples["advantages"])
        tuples_per_device = self.par_opt.load_data(
            self.local_evaluator.sess,
            [samples["obs"],
             samples["value_targets"] if use_gae else dummy,
             samples["advantages"],
             samples["actions"],
             samples["logprobs"],
             samples["vf_preds"] if use_gae else dummy])

        # tuples_per_device = model.load_data(
        #     samples, self.iteration == 0 and config["full_trace_data_load"])
        for i in range(config["num_sgd_iter"]):
            num_batches = (
                int(tuples_per_device) // int(model.per_device_batch_size))
            loss, policy_graph, vf_loss, kl, entropy = [], [], [], [], []
            permutation = np.random.permutation(num_batches)
            # Prepare to drop into the debugger
            for batch_index in range(num_batches):
                batch_loss, batch_policy_graph, batch_vf_loss, batch_kl, \
                    batch_entropy = self.optimizer.par_opt.optimize(
                        self.local_evaluator.sess,
                        permutation[batch_index] * model.per_device_batch_size,
                        extra_ops=list(self.local_evaluator.extra_ops.values()),
                        extra_feed_dict={self.local_evaluator.kl_coeff: self.kl_coeff_val})
                loss.append(batch_loss)
                policy_graph.append(batch_policy_graph)
                vf_loss.append(batch_vf_loss)
                kl.append(batch_kl)
                entropy.append(batch_entropy)
            loss = np.mean(loss)
            policy_graph = np.mean(policy_graph)
            vf_loss = np.mean(vf_loss)
            kl = np.mean(kl)
            entropy = np.mean(entropy)
            print(
                "{:>15}{:15.5e}{:15.5e}{:15.5e}{:15.5e}{:15.5e}".format(
                    i, loss, policy_graph, vf_loss, kl, entropy))
        if kl > 2.0 * config["kl_target"]:
            self.kl_coeff_val *= 1.5
        elif kl < 0.5 * config["kl_target"]:
            self.kl_coeff_val *= 0.5

        info = {
            "kl_divergence": kl,
            "kl_coefficient": self.kl_coeff_val,
        }

        FilterManager.synchronize(
            self.local_evaluator.filters, self.remote_evaluators)
        res = self._fetch_metrics_from_remote_evaluators()
        res = res._replace(info=info)
        return res

    def _fetch_metrics_from_remote_evaluators(self):
        episode_rewards = []
        episode_lengths = []
        metric_lists = [a.get_completed_rollout_metrics.remote()
                        for a in self.remote_evaluators]
        for metrics in metric_lists:
            for episode in ray.get(metrics):
                episode_lengths.append(episode.episode_length)
                episode_rewards.append(episode.episode_reward)
        avg_reward = (
            np.mean(episode_rewards) if episode_rewards else float('nan'))
        avg_length = (
            np.mean(episode_lengths) if episode_lengths else float('nan'))
        timesteps = np.sum(episode_lengths) if episode_lengths else 0

        result = TrainingResult(
            episode_reward_mean=avg_reward,
            episode_len_mean=avg_length,
            timesteps_this_iter=timesteps)

        return result

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = self.saver.save(
            self.local_evaluator.sess,
            os.path.join(checkpoint_dir, "checkpoint"),
            global_step=self.iteration)
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = [
            self.local_evaluator.save(),
            self.global_step,
            self.kl_coeff_val,
            agent_state]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.saver.restore(self.local_evaluator.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.local_evaluator.restore(extra_data[0])
        self.global_step = extra_data[1]
        self.kl_coeff_val = extra_data[2]
        ray.get([
            a.restore.remote(o)
                for (a, o) in zip(self.remote_evaluators, extra_data[3])])

    def compute_action(self, observation):
        observation = self.local_evaluator.obs_filter(
            observation, update=False)
        return self.local_evaluator.common_policy.compute_single_action(
            observation, [], False)[0]
