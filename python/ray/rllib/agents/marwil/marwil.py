from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.marwil.marwil_policy_graph import MARWILPolicyGraph
from ray.rllib.optimizers import SyncBatchReplayOptimizer
from ray.rllib.utils.annotations import override

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # You should override this to point to an offline dataset (see agent.py).
    "input": "sampler",
    # Use importance sampling estimators for reward
    "input_evaluation": ["is", "wis"],

    # Scaling of advantages in exponential terms
    # When beta is 0, MARWIL is reduced to imitation learning
    "beta": 1.0,
    # Balancing value estimation loss and policy optimization loss
    "vf_coeff": 1.0,
    # Whether to calculate cumulative rewards
    "postprocess_inputs": True,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Learning rate for adam optimizer
    "lr": 1e-4,
    # Number of timesteps collected for each SGD round
    "train_batch_size": 2000,
    # Number of steps max to keep in the batch replay buffer
    "replay_buffer_size": 100000,
    # Number of steps to read before learning starts
    "learning_starts": 0,
    # === Parallelism ===
    "num_workers": 0,
})
# __sphinx_doc_end__
# yapf: enable


class MARWILAgent(Agent):
    """MARWIL implementation in TensorFlow."""

    _agent_name = "MARWIL"
    _default_config = DEFAULT_CONFIG
    _policy_graph = MARWILPolicyGraph

    @override(Agent)
    def _init(self):
        self.local_evaluator = self.make_local_evaluator(
            self.env_creator, self._policy_graph)
        self.remote_evaluators = self.make_remote_evaluators(
            self.env_creator, self._policy_graph, self.config["num_workers"])
        self.optimizer = SyncBatchReplayOptimizer(
            self.local_evaluator, self.remote_evaluators, {
                "learning_starts": self.config["learning_starts"],
                "buffer_size": self.config["replay_buffer_size"],
                "train_batch_size": self.config["train_batch_size"],
            })

    @override(Agent)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        fetches = self.optimizer.step()
        res = self.collect_metrics()
        res.update(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
            info=dict(fetches, **res.get("info", {})))
        return res
