from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import numpy as np
import pickle

import ray
from ray.rllib.agents import Agent, with_common_config
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicyGraph
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.utils import FilterManager
from ray.rllib.optimizers import SyncSamplesOptimizer, LocalMultiGPUOptimizer
from ray.tune.trial import Resources

DEFAULT_CONFIG = with_common_config({
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,
    # Initial coefficient for KL divergence
    "kl_coeff": 0.2,
    # Number of timesteps collected for each SGD round
    "timesteps_per_batch": 4000,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 30,
    # Stepsize of SGD
    "sgd_stepsize": 5e-5,
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
    # Number of GPUs to use for SGD
    "num_gpus": 0,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Which observation filter to apply to the observation
    "observation_filter": "MeanStdFilter",
    # Debug only: use the sync samples optimizer instead of the multi-gpu one
    "debug_use_simple_optimizer": False,
})


class PPOAgent(Agent):
    """Multi-GPU optimized implementation of PPO in TensorFlow."""

    _agent_name = "PPO"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=cf["num_gpus"],
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def _init(self):
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, PPOTFPolicyGraph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, PPOTFPolicyGraph, self.config["num_workers"],
            {"num_cpus": self.config["num_cpus_per_worker"],
             "num_gpus": self.config["num_gpus_per_worker"]})
        if self.config["debug_use_simple_optimizer"]:
            self.optimizer = SyncSamplesOptimizer(
                self.config["optimizer"],
                self.local_evaluator, self.remote_evaluators)
        else:
            self.optimizer = LocalMultiGPUOptimizer(
                {"sgd_batch_size": self.config["sgd_batchsize"],
                 "sgd_stepsize": self.config["sgd_stepsize"],
                 "num_sgd_iter": self.config["num_sgd_iter"],
                 "timesteps_per_batch": self.config["timesteps_per_batch"],
                 "standardize_fields": ["advantages"]},
                self.local_evaluator, self.remote_evaluators)

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

        fetches = self.optimizer.step()
        newkl = self.local_evaluator.for_policy(
            lambda pi: pi.update_kl(fetches["kl"]))
        FilterManager.synchronize(
            self.local_evaluator.filters, self.remote_evaluators)

        res = collect_metrics(self.local_evaluator, self.remote_evaluators)
        res = res._replace(info=fetches)
        return res

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = [
            self.local_evaluator.save(),
            agent_state]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.local_evaluator.restore(extra_data[0])
        ray.get([
            a.restore.remote(o)
                for (a, o) in zip(self.remote_evaluators, extra_data[1])])
