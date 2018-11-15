from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
from ray.rllib.agents.impala.vtrace_policy_graph import VTracePolicyGraph
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.optimizers import AsyncSamplesOptimizer

OPTIMIZER_SHARED_CONFIGS = [
    "lr",
    "num_envs_per_worker",
    "num_gpus",
    "sample_batch_size",
    "train_batch_size",
    "replay_buffer_num_slots",
    "replay_proportion",
    "num_parallel_data_loaders",
    "grad_clip",
    "max_sample_requests_in_flight_per_worker",
    "broadcast_interval",
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # V-trace params (see vtrace.py).
    "vtrace": True,
    "vtrace_clip_rho_threshold": 1.0,
    "vtrace_clip_pg_rho_threshold": 1.0,

    # System params.
    "sample_batch_size": 50,
    "train_batch_size": 500,
    "min_iter_time_s": 10,
    "num_workers": 2,
    # number of GPUs the learner should use.
    "num_gpus": 1,
    # set >1 to load data into GPUs in parallel. Increases GPU memory usage
    # proportionally with the number of loaders.
    "num_parallel_data_loaders": 1,
    # level of queuing for sampling.
    "max_sample_requests_in_flight_per_worker": 2,
    # max number of workers to broadcast one set of weights to
    "broadcast_interval": 5,
    # set >0 to enable experience replay. Saved samples will be replayed with
    # a p:1 proportion to new data samples.
    "replay_proportion": 0.0,
    # number of sample batches to store for replay. The number of transitions
    # saved total will be (replay_buffer_num_slots * sample_batch_size).
    "replay_buffer_num_slots": 100,

    # Learning params.
    "grad_clip": 40.0,
    # either "adam" or "rmsprop"
    "opt_type": "adam",
    "lr": 0.0005,
    "lr_schedule": None,
    # rmsprop considered
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    # balancing the three losses
    "vf_loss_coeff": 0.5,
    "entropy_coeff": -0.01,
})
# __sphinx_doc_end__
# yapf: enable


class ImpalaAgent(Agent):
    """IMPALA implementation using DeepMind's V-trace."""

    _agent_name = "IMPALA"
    _default_config = DEFAULT_CONFIG
    _policy_graph = VTracePolicyGraph

    def _init(self):
        for k in OPTIMIZER_SHARED_CONFIGS:
            if k not in self.config["optimizer"]:
                self.config["optimizer"][k] = self.config[k]
        if self.config["vtrace"]:
            policy_cls = self._policy_graph
        else:
            policy_cls = A3CPolicyGraph
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"])
        self.optimizer = AsyncSamplesOptimizer(self.local_evaluator,
                                               self.remote_evaluators,
                                               self.config["optimizer"])

    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        start = time.time()
        self.optimizer.step()
        while time.time() - start < self.config["min_iter_time_s"]:
            self.optimizer.step()
        result = self.optimizer.collect_metrics(
            self.config["collect_metrics_timeout"])
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result
