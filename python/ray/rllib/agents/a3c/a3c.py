from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os

import ray
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.optimizers import AsyncGradientsOptimizer
from ray.rllib.utils import FilterManager, merge_dicts
from ray.tune.trial import Resources

DEFAULT_CONFIG = with_common_config({
    # Size of rollout batch
    "sample_batch_size": 10,
    # Use PyTorch as backend - no LSTM support
    "use_pytorch": False,
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
    # Workers sample async. Note that this increases the effective
    # sample_batch_size by up to 5x due to async buffering of batches.
    "sample_async": True,
    # Model and preprocessor options
    "model": {
        # Use LSTM model. Requires TF.
        "use_lstm": False,
        # Max seq length for LSTM training.
        "max_seq_len": 20,
        # (Image statespace) - Converts image to Channels = 1
        "grayscale": True,
        # (Image statespace) - Each pixel
        "zero_mean": False,
        # (Image statespace) - Converts image to (dim, dim, C)
        "dim": 80,
        # (Image statespace) - Converts image shape to (C, dim, dim)
        "channel_major": False,
    },
    # Configure TF for single-process operation
    "tf_session_args": {
        "intra_op_parallelism_threads": 1,
        "inter_op_parallelism_threads": 1,
        "gpu_options": {
            "allow_growth": True,
        },
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        # Number of gradients applied for each `train` step
        "grads_per_step": 100,
    },
})


class A3CAgent(Agent):
    """A3C implementations in TensorFlow and PyTorch."""

    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = merge_dicts(cls._default_config, config)
        return Resources(
            cpu=1,
            gpu=0,
            extra_cpu=cf["num_workers"],
            extra_gpu=cf["use_gpu_for_workers"] and cf["num_workers"] or 0)

    def _init(self):
        if self.config["use_pytorch"]:
            from ray.rllib.agents.a3c.a3c_torch_policy_graph import \
                A3CTorchPolicyGraph
            policy_cls = A3CTorchPolicyGraph
        else:
            from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
            policy_cls = A3CPolicyGraph

        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"],
            {"num_gpus": 1 if self.config["use_gpu_for_workers"] else 0})
        self.optimizer = AsyncGradientsOptimizer(self.local_evaluator,
                                                 self.remote_evaluators,
                                                 self.config["optimizer"])

    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        self.optimizer.step()
        FilterManager.synchronize(self.local_evaluator.filters,
                                  self.remote_evaluators)
        result = self.optimizer.collect_metrics()
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result

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
