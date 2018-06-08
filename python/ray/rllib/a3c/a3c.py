from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os

import ray
from ray.rllib.agent import Agent
from ray.rllib.optimizers import AsyncOptimizer
from ray.rllib.utils import FilterManager
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator, \
    collect_metrics
from ray.rllib.a3c.common import get_policy_cls
from ray.tune.trial import Resources

DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 2,
    # Size of rollout batch
    "batch_size": 10,
    # Use LSTM model - only applicable for image states
    "use_lstm": False,
    # Use PyTorch as backend - no LSTM support
    "use_pytorch": False,
    # Which observation filter to apply to the observation
    "observation_filter": "NoFilter",
    # Which reward filter to apply to the reward
    "reward_filter": "NoFilter",
    # Discount factor of MDP
    "gamma": 0.99,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Entropy coefficient
    "entropy_coeff": -0.01,
    # Whether to place workers on GPUs
    "use_gpu_for_workers": False,
    # Whether to emit extra summary stats
    "summarize": False,
    # Model and preprocessor options
    "model": {
        # (Image statespace) - Converts image to Channels = 1
        "grayscale": True,
        # (Image statespace) - Each pixel
        "zero_mean": False,
        # (Image statespace) - Converts image to (dim, dim, C)
        "dim": 80,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        "channel_major": False,
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 100,
    },
    # Arguments to pass to the env creator
    "env_config": {},
}


class A3CAgent(Agent):
    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG
    _allow_unknown_subkeys = ["model", "optimizer", "env_config"]

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=0,
            extra_cpu=cf["num_workers"],
            extra_gpu=cf["use_gpu_for_workers"] and cf["num_workers"] or 0)

    def _init(self):
        self.policy_cls = get_policy_cls(self.config)

        if self.config["use_pytorch"]:
            session_creator = None
        else:
            import tensorflow as tf

            def session_creator():
                return tf.Session(
                    config=tf.ConfigProto(
                        intra_op_parallelism_threads=1,
                        inter_op_parallelism_threads=1,
                        gpu_options=tf.GPUOptions(allow_growth=True)))

        remote_cls = CommonPolicyEvaluator.as_remote(
            num_gpus=1 if self.config["use_gpu_for_workers"] else 0)
        self.local_evaluator = CommonPolicyEvaluator(
            self.env_creator, self.policy_cls,
            batch_steps=self.config["batch_size"],
            batch_mode="truncate_episodes",
            tf_session_creator=session_creator,
            registry=self.registry, env_config=self.config["env_config"],
            model_config=self.config["model"], policy_config=self.config)
        self.remote_evaluators = [
            remote_cls.remote(
                self.env_creator, self.policy_cls,
                batch_steps=self.config["batch_size"],
                batch_mode="truncate_episodes", sample_async=True,
                tf_session_creator=session_creator,
                registry=self.registry, env_config=self.config["env_config"],
                model_config=self.config["model"], policy_config=self.config)
            for i in range(self.config["num_workers"])]

        self.optimizer = AsyncOptimizer(
            self.config["optimizer"], self.local_evaluator,
            self.remote_evaluators)

    def _train(self):
        self.optimizer.step()
        FilterManager.synchronize(
            self.local_evaluator.filters, self.remote_evaluators)
        return collect_metrics(self.local_evaluator, self.remote_evaluators)

    def _stop(self):
        # workaround for https://github.com/ray-project/ray/issues/1516
        for ev in self.remote_evaluators:
            ev.__ray_terminate__.remote()

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir,
                                       "checkpoint-{}".format(self.iteration))
        agent_state = ray.get(
            [a.save.remote() for a in self.remote_evaluators])
        extra_data = {
            "remote_state": agent_state,
            "local_state": self.local_evaluator.save()
        }
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        ray.get([
            a.restore.remote(o)
            for a, o in zip(self.remote_evaluators, extra_data["remote_state"])
        ])
        self.local_evaluator.restore(extra_data["local_state"])

    def compute_action(self, observation, state=None):
        if state is None:
            state = []
        obs = self.local_evaluator.obs_filter(observation, update=False)
        return self.local_evaluator.for_policy(
            lambda p: p.compute_single_action(
                obs, state, is_training=False)[0])
