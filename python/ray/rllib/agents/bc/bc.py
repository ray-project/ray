from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.agents.agent import Agent
from ray.rllib.agents.bc.bc_evaluator import BCEvaluator, \
    GPURemoteBCEvaluator, RemoteBCEvaluator
from ray.rllib.optimizers import AsyncGradientsOptimizer
from ray.rllib.utils import merge_dicts
from ray.tune.trial import Resources

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 1,
    # Size of rollout batch
    "batch_size": 100,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Whether to use a GPU for local optimization.
    "gpu": False,
    # Whether to place workers on GPUs
    "use_gpu_for_workers": False,
    # Model and preprocessor options
    "model": {
        # (Image statespace) - Converts image to Channels = 1
        "grayscale": True,
        # (Image statespace) - Each pixel
        "zero_mean": False,
        # (Image statespace) - Converts image to (dim, dim, C)
        "dim": 80,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        "channel_major": False
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 100,
    },
    # Arguments to pass to the env creator
    "env_config": {},
}


class BCAgent(Agent):
    _agent_name = "BC"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_configs = True

    @classmethod
    def default_resource_request(cls, config):
        cf = merge_dicts(cls._default_config, config)
        if cf["use_gpu_for_workers"]:
            num_gpus_per_worker = 1
        else:
            num_gpus_per_worker = 0
        return Resources(
            cpu=1,
            gpu=cf["gpu"] and 1 or 0,
            extra_cpu=cf["num_workers"],
            extra_gpu=num_gpus_per_worker * cf["num_workers"])

    def _init(self):
        self.local_evaluator = BCEvaluator(self.env_creator, self.config,
                                           self.logdir)
        if self.config["use_gpu_for_workers"]:
            remote_cls = GPURemoteBCEvaluator
        else:
            remote_cls = RemoteBCEvaluator
        self.remote_evaluators = [
            remote_cls.remote(self.env_creator, self.config, self.logdir)
            for _ in range(self.config["num_workers"])
        ]
        self.optimizer = AsyncGradientsOptimizer(self.local_evaluator,
                                                 self.remote_evaluators,
                                                 self.config["optimizer"])

    def _train(self):
        self.optimizer.step()
        metric_lists = [
            re.get_metrics.remote() for re in self.remote_evaluators
        ]
        total_samples = 0
        total_loss = 0
        for metrics in metric_lists:
            for m in ray.get(metrics):
                total_samples += m["num_samples"]
                total_loss += m["loss"]
        result = dict(
            mean_loss=total_loss / total_samples,
            timesteps_this_iter=total_samples,
        )
        return result

    def compute_action(self, observation):
        action, info = self.local_evaluator.policy.compute(observation)
        return action
