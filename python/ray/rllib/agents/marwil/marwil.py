from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

from ray.rllib import optimizers
from ray.rllib.agents.agent import Agent, with_common_config
from ray.rllib.agents.marwil.marwil_policy_graph import MARWILPolicyGraph
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.schedules import ConstantSchedule, LinearSchedule

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Scaling of advantages in exponential terms
    # When beta is 0, MARWIL is reduced to imitation learning
    "beta": 1.0,
    # Balancing value estimation loss and policy optimization loss
    "c": 1.0,
    # Whether to calculate cumulative rewards
    "postprocess_inputs": True,
    # Whether to rollout "complete_episodes" or "truncate_episodes"
    "batch_mode": "complete_episodes",
    # Read data from historic data and evaluate by a sampler
    "input_evaluation": "simulation",
    # Learning rate for adam optimizer
    "lr": 5e-5,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": 40,
    # Size of batches collected from each worker
    "sample_batch_size": 200,
    # Number of timesteps collected for each SGD round
    "train_batch_size": 4000,
    # Total SGD batch size across all devices for SGD
    "sgd_minibatch_size": 128,
    # Number of SGD iterations in each outer loop
    "num_sgd_iter": 2,
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
        self.optimizer = SyncSamplesOptimizer(
            self.local_evaluator, self.remote_evaluators, {
                "num_sgd_iter": self.config["num_sgd_iter"],
                "train_batch_size": self.config["train_batch_size"],
            })

    @override(Agent)
    def _train(self):
        prev_steps = self.optimizer.num_steps_sampled
        fetches = self.optimizer.step()
        res = self.optimizer.collect_metrics(
            self.config["collect_metrics_timeout"])
        res.update(
            timesteps_this_iter=self.optimizer.num_steps_sampled - prev_steps,
            info=dict(fetches, **res.get("info", {})))
        return res
