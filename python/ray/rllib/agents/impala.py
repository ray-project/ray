from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os

import ray
from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.optimizers import AsyncSamplesOptimizer
from ray.rllib.utils import FilterManager
from ray.tune.trial import Resources

OPTIMIZER_SHARED_CONFIGS = [
    "sample_batch_size",
    "train_batch_size",
]

DEFAULT_CONFIG = with_common_config({
    # Size of rollout batch
    "sample_batch_size": 50,
    # Size of batch to train on.
    "train_batch_size": 512,
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
    },
    # Arguments to pass to the rllib optimizer
    "optimizer": {
        "max_weight_sync_delay": 400,
        "debug": False
    },
})


class ImpalaAgent(Agent):
    """IMPALA implementation using DeepMind's v-trace."""

    _agent_name = "IMPALA"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=0,
            extra_cpu=cf["num_workers"],
            extra_gpu=cf["use_gpu_for_workers"] and cf["num_workers"] or 0)

    def _init(self):
        for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]
        policy_cls = A3CPolicyGraph
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"],
            {"num_cpus": 1})
        self.optimizer = AsyncSamplesOptimizer(self.local_evaluator,
                                               self.remote_evaluators,
                                               self.config["optimizer"])

    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        self.optimizer.step()
        FilterManager.synchronize(self.local_evaluator.filters,
                                  self.remote_evaluators)
        result = self.optimizer.collect_metrics()
        result = result._replace(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps)
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
