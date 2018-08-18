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
from ray.rllib.utils import FilterManager, merge_dicts
from ray.tune.trial import Resources

OPTIMIZER_SHARED_CONFIGS = [
    "lr",
    "num_envs_per_worker",
    "num_gpus",
    "sample_batch_size",
    "train_batch_size",
    "replay_batch_slots",
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
    "num_gpus": 1,
    "num_workers": 2,
    "num_cpus_per_worker": 1,
    "num_gpus_per_worker": 0,
    "replay_batch_slots": 0,  # TODO only applies if num_gpus > 1?

    # Learning params.
    "grad_clip": 40.0,
    # either "adam" or "rmsprop"
    "opt_type": "adam",
    "lr": 0.0005,
    # rmsprop considered
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    # balancing the three losses
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
            gpu=cf["num_gpus"],
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
            self.env_creator, policy_cls,
            # important: increase the number of cpu threads for multi-gpu
            merge_dicts(
                self.config, {
                    "tf_session_args": {
                        "intra_op_parallelism_threads": max(
                            1, self.config["num_gpus"]),
                        "inter_op_parallelism_threads": max(
                            1, self.config["num_gpus"]),
                    },
                }))
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
