from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import os
import time

import ray
from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
from ray.rllib.agents.impala.vtrace_policy_graph import VTracePolicyGraph
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.optimizers import AsyncSamplesOptimizer
from ray.rllib.utils import FilterManager
from ray.tune.trial import Resources

OPTIMIZER_SHARED_CONFIGS = [
    "sample_batch_size",
    "train_batch_size",
]

DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace.py).
    "vtrace": True,
    "vtrace_clip_rho_threshold": 1.0,
    "vtrace_clip_pg_rho_threshold": 1.0,

    # System params.
    "sample_batch_size": 50,
    "train_batch_size": 500,
    "min_iter_time_s": 10,
    "summarize": False,
    "gpu": True,
    "num_workers": 2,
    "num_cpus_per_worker": 1,
    "num_gpus_per_worker": 0,

    # Learning params.
    "grad_clip": 40.0,
    "lr": 0.0001,
    "vf_loss_coeff": 0.5,
    "entropy_coeff": -0.01,

    # Model and preprocessor options.
    "clip_rewards": True,
    "preprocessor_pref": "deepmind",
    "model": {
        "use_lstm": False,
        "max_seq_len": 20,
        "dim": 80,
    },
})


class ImpalaAgent(Agent):
    """IMPALA implementation using DeepMind's V-trace."""

    _agent_name = "IMPALA"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1,
            gpu=cf["gpu"] and 1 or 0,
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def _init(self):
        for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]
        if self.config["vtrace"]:
            policy_cls = VTracePolicyGraph
        else:
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
        start = time.time()
        self.optimizer.step()
        while time.time() - start < self.config["min_iter_time_s"]:
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
