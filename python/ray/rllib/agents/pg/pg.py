from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.pg.pg_policy_graph import PGPolicyGraph

from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils.annotations import override

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default
    "num_workers": 0,
    # Learning rate
    "lr": 0.0004,
    # Use PyTorch as backend
    "use_pytorch": False,
})
# __sphinx_doc_end__
# yapf: enable


class PGAgent(Agent):
    """Simple policy gradient agent.

    This is an example agent to show how to implement algorithms in RLlib.
    In most cases, you will probably want to use the PPO agent instead.
    """

    _agent_name = "PG"
    _default_config = DEFAULT_CONFIG
    _policy_graph = PGPolicyGraph

    @override(Agent)
    def _init(self):
        if self.config["use_pytorch"]:
            from ray.rllib.agents.pg.torch_pg_policy_graph import \
                PGTorchPolicyGraph
            policy_cls = PGTorchPolicyGraph
        else:
            policy_cls = self._policy_graph
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, policy_cls)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, policy_cls, self.config["num_workers"])
        optimizer_config = dict(
            self.config["optimizer"],
            **{"train_batch_size": self.config["train_batch_size"]})
        self.optimizer = SyncSamplesOptimizer(
            self.local_evaluator, self.remote_evaluators, optimizer_config)

    @override(Agent)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        self.optimizer.step()
        result = self.collect_metrics()
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result
