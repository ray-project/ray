from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import pickle
import tensorflow as tf

import ray
from ray.tune.trial import Resources
from ray.rllib.agent import Agent
from ray.rllib.utils.common_policy_evaluator import (
    CommonPolicyEvaluator, collect_metrics)
from ray.rllib.utils import FilterManager
from ray.rllib.ppo.ppo_tf_policy import PPOTFPolicyGraph
from ray.rllib.optimizers.multi_gpu import LocalMultiGPUOptimizer

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
    "kl_coeff_val": 0.2,
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
    "num_workers": 2,
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
    _default_policy_graph = PPOTFPolicyGraph

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=len([d for d in cf["devices"] if "gpu" in d.lower()]),
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def _init(self):
        def session_creator():
            return tf.Session(
                config=tf.ConfigProto(**self.config["tf_session_args"]))
        self.local_evaluator = CommonPolicyEvaluator(
            self.env_creator,
            self._default_policy_graph,
            tf_session_creator=session_creator,
            batch_mode="truncate_episodes",
            observation_filter=self.config["observation_filter"],
            env_config=self.config["env_config"],
            model_config=self.config["model"],
            policy_config=self.config
            )
        RemoteEvaluator = CommonPolicyEvaluator.as_remote(
            num_cpus=self.config["num_cpus_per_worker"],
            num_gpus=self.config["num_gpus_per_worker"])
        self.remote_evaluators = [
            RemoteEvaluator.remote(
                self.env_creator,
                self._default_policy_graph,
                batch_mode="truncate_episodes",
                observation_filter=self.config["observation_filter"],
                env_config=self.config["env_config"],
                model_config=self.config["model"],
                policy_config=self.config
            )
            for _ in range(self.config["num_workers"])]

        self.optimizer = LocalMultiGPUOptimizer(
            {"sgd_batch_size": self.config["sgd_batchsize"],
             "sgd_stepsize": self.config["sgd_stepsize"],
             "num_sgd_iter": self.config["num_sgd_iter"],
             "timesteps_per_batch": self.config["timesteps_per_batch"]},
            self.local_evaluator, self.remote_evaluators,)


    def _train(self):
        def postprocess_samples(batch):
            # Divide by the maximum of value.std() and 1e-4
            # to guard against the case where all values are equal
            value = batch["advantages"]
            standardized = (value - value.mean()) / max(1e-4, value.std())
            batch.data["advantages"] = standardized
            batch.shuffle()
            dummy = np.zeros_like(batch["advantages"])
            if not self.config["use_gae"]:
                batch.data["value_targets"] = dummy
                batch.data["vf_preds"] = dummy
        extra_fetches = self.optimizer.step(postprocess_fn=postprocess_samples)
        kl = np.array(extra_fetches["kl"]).mean(axis=1)[-1]
        newkl = self.local_evaluator.for_policy(lambda pi: pi.update_kl(kl))

        info = {
            "kl_divergence": kl,
            "kl_coefficient": newkl,
        }

        FilterManager.synchronize(
            self.local_evaluator.filters, self.remote_evaluators)
        res = collect_metrics(self.local_evaluator, self.remote_evaluators)
        res = res._replace(info=info)
        return res


    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        # checkpoint_path = self.saver.save(
        #     self.local_evaluator.sess,
        #     os.path.join(checkpoint_dir, "checkpoint"),
        #     global_step=self.iteration)
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = [
            self.local_evaluator.save(),
            agent_state]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        # self.saver.restore(self.local_evaluator.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.local_evaluator.restore(extra_data[0])
        ray.get([
            a.restore.remote(o)
                for (a, o) in zip(self.remote_evaluators, extra_data[2])])

    def compute_action(self, observation):
        observation = self.local_evaluator.obs_filter(
            observation, update=False)
        return self.local_evaluator.common_policy.compute_actions(
            [observation], [], False)[0][0]

if __name__ == '__main__':
    import gym
    from collections import defaultdict
    # from ray.rllib.ppo import DEFAULT_CONFIG, PPOAgent
    cfg = DEFAULT_CONFIG.copy()
    cfg["use_gae"] = False
    cfg["num_workers"] = 1
    ray.init()
    # env = gym.make()
    agent = PPOAgent(cfg, "CartPole-v0", )
    agent.train()
