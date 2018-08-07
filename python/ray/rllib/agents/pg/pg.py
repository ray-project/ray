from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils import merge_dicts
from ray.tune.trial import Resources

DEFAULT_CONFIG = with_common_config({
    # No remote workers by default
    "num_workers": 0,
    # Learning rate
    "lr": 0.0004,
    # Override model config
    "model": {
        # Use LSTM model.
        "use_lstm": False,
        # Max seq length for LSTM training.
        "max_seq_len": 20,
    },
})


class PGAgent(Agent):
    """Simple policy gradient agent.

    This is an example agent to show how to implement algorithms in RLlib.
    In most cases, you will probably want to use the PPO agent instead.
    """

    _agent_name = "PG"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = merge_dicts(cls._default_config, config)
        return Resources(cpu=1, gpu=0, extra_cpu=cf["num_workers"])

    def _init(self):
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, PGPolicyGraph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, PGPolicyGraph, self.config["num_workers"], {})
        self.optimizer = SyncSamplesOptimizer(self.local_evaluator,
                                              self.remote_evaluators,
                                              self.config["optimizer"])

    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        self.optimizer.step()
        result = self.optimizer.collect_metrics()
        result.update(timesteps_this_iter=self.optimizer.num_steps_sampled -
                      prev_steps)
        return result
