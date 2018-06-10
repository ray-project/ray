from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agent import Agent
from ray.rllib.optimizers import LocalSyncOptimizer
from ray.rllib.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator, \
    collect_metrics
from ray.tune.trial import Resources


DEFAULT_CONFIG = {
    # Number of workers (excluding master)
    "num_workers": 4,
    # Size of rollout batch
    "batch_size": 512,
    # Discount factor of MDP
    "gamma": 0.99,
    # Number of steps after which the rollout gets cut
    "horizon": 500,
    # Learning rate
    "lr": 0.0004,
    # Arguments to pass to the rllib optimizer
    "optimizer": {},
    # Model parameters
    "model": {"fcnet_hiddens": [128, 128]},
    # Arguments to pass to the env creator
    "env_config": {},
}


class PGAgent(Agent):
    """Simple policy gradient agent.

    This is an example agent to show how to implement algorithms in RLlib.
    In most cases, you will probably want to use the PPO agent instead.
    """

    _agent_name = "PG"
    _default_config = DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(cpu=1, gpu=0, extra_cpu=cf["num_workers"])

    def _init(self):
        self.optimizer = LocalSyncOptimizer.make(
            evaluator_cls=CommonPolicyEvaluator,
            evaluator_args={
                "env_creator": self.env_creator,
                "policy_graph": PGPolicyGraph,
                "batch_steps": self.config["batch_size"],
                "batch_mode": "truncate_episodes",
                "registry": self.registry,
                "model_config": self.config["model"],
                "env_config": self.config["env_config"],
                "policy_config": self.config,
            },
            num_workers=self.config["num_workers"],
            optimizer_config=self.config["optimizer"])

    def _train(self):
        self.optimizer.step()
        return collect_metrics(
            self.optimizer.local_evaluator, self.optimizer.remote_evaluators)

    def compute_action(self, observation, state=None):
        if state is None:
            state = []
        return self.local_evaluator.for_policy(
            lambda p: p.compute_single_action(
                observation, state, is_training=False)[0])
