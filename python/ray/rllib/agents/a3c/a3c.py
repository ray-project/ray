from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib.agents.a3c.a3c_tf_policy_graph import A3CPolicyGraph
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.optimizers import AsyncGradientsOptimizer
from ray.rllib.utils.annotations import override

# yapf: disable
# __sphinx_doc_begin__
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
    # Learning rate schedule
    "lr_schedule": None,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Entropy coefficient
    "entropy_coeff": 0.01,
    # Min time per iteration
    "min_iter_time_s": 5,
    # Workers sample async. Note that this increases the effective
    # sample_batch_size by up to 5x due to async buffering of batches.
    "sample_async": True,
})
# __sphinx_doc_end__
# yapf: enable


class A3CAgent(Agent):
    """A3C implementations in TensorFlow and PyTorch."""

    _agent_name = "A3C"
    _default_config = DEFAULT_CONFIG
    _policy_graph = A3CPolicyGraph

    @override(Agent)
    def _init(self):
        if self.config["use_pytorch"]:
            from ray.rllib.agents.a3c.a3c_torch_policy_graph import \
                A3CTorchPolicyGraph
            policy_cls = A3CTorchPolicyGraph
        else:
            policy_cls = self._policy_graph

        if self.config["entropy_coeff"] < 0:
            raise DeprecationWarning("entropy_coeff must be >= 0")

        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"])
        self.optimizer = self._make_optimizer()

    @override(Agent)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        start = time.time()
        while time.time() - start < self.config["min_iter_time_s"]:
            self.optimizer.step()
        result = self.collect_metrics()
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result

    def _make_optimizer(self):
        return AsyncGradientsOptimizer(self.local_evaluator,
                                       self.remote_evaluators,
                                       self.config["optimizer"])
