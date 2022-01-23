"""
SlateQ (Reinforcement Learning for Recommendation)
==================================================

This file defines the trainer class for the SlateQ algorithm from the
`"Reinforcement Learning for Slate-based Recommender Systems: A Tractable
Decomposition and Practical Methodology" <https://arxiv.org/abs/1905.12767>`_
paper.

See `slateq_torch_policy.py` for the definition of the policy. Currently, only
PyTorch is supported. The algorithm is written and tested for Google's RecSim
environment (https://github.com/google-research/recsim).
"""

import logging
from typing import List, Type

from ray.rllib.agents.slateq.slateq_torch_policy import SlateQTorchPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainOneStep
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# Defines all SlateQ strategies implemented.
ALL_SLATEQ_STRATEGIES = [
    # RANDOM: Randomly select documents for slates.
    "RANDOM",
    # MYOP: Select documents that maximize user click probabilities. This is
    # a myopic strategy and ignores long term rewards. This is equivalent to
    # setting a zero discount rate for future rewards.
    "MYOP",
    # SARSA: Use the SlateQ SARSA learning algorithm.
    "SARSA",
    # QL: Use the SlateQ Q-learning algorithm.
    "QL",
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    # Dense-layer setup for each the advantage branch and the value branch
    # in a dueling architecture.
    "hiddens": [256, 64, 16],

    # set batchmode
    "batch_mode": "complete_episodes",

    # === Deep Learning Framework Settings ===
    # Currently, only PyTorch is supported
    "framework": "torch",

    # === Exploration Settings ===
    "exploration_config": {
        # The Exploration class to use.
        "type": "EpsilonGreedy",
        # Config for the Exploration class' constructor:
        "initial_epsilon": 1.0,
        "final_epsilon": 0.02,
        "epsilon_timesteps": 10000,  # Timesteps over which to anneal epsilon.
    },
    # Switch to greedy actions in evaluation workers.
    "evaluation_config": {
        "explore": False,
    },

    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 1000,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": DEPRECATED_VALUE,
    "replay_buffer_config": {
        "type": "MultiAgentReplayBuffer",
        "capacity": 50000,
    },
    # The number of contiguous environment steps to replay at once. This may
    # be set to greater than 1 to support recurrent models.
    "replay_sequence_length": 1,
    # Whether to LZ4 compress observations
    "compress_observations": False,
    # If set, this will fix the ratio of replayed from a buffer and learned on
    # timesteps to sampled from an environment and stored in the replay buffer
    # timesteps. Otherwise, the replay will proceed at the native ratio
    # determined by (train_batch_size / rollout_fragment_length).
    "training_intensity": None,

    # === Optimization ===
    # Learning rate for adam optimizer for the user choice model
    "lr_choice_model": 1e-2,
    # Learning rate for adam optimizer for the q model
    "lr_q_model": 1e-2,
    # Adam epsilon hyper parameter
    "adam_epsilon": 1e-8,
    # If not None, clip gradients during optimization at this value
    "grad_clip": 40,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 1000,
    # Size of a batch sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,

    # === SlateQ specific options ===
    # Learning method used by the slateq policy. Choose from: RANDOM,
    # MYOP (myopic), SARSA, QL (Q-Learning),
    "slateq_strategy": "QL",
    # user/doc embedding size for the recsim environment
    "recsim_embedding_size": 20,
})
# __sphinx_doc_end__
# yapf: enable


def calculate_round_robin_weights(config: TrainerConfigDict) -> List[float]:
    """Calculate the round robin weights for the rollout and train steps"""
    if not config["training_intensity"]:
        return [1, 1]
    # e.g., 32 / 4 -> native ratio of 8.0
    native_ratio = (
        config["train_batch_size"] / config["rollout_fragment_length"])
    # Training intensity is specified in terms of
    # (steps_replayed / steps_sampled), so adjust for the native ratio.
    weights = [1, config["training_intensity"] / native_ratio]
    return weights


class SlateQTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for SlateQ!")

        if config["framework"] != "torch":
            raise ValueError("SlateQ only runs on PyTorch")

        if config["slateq_strategy"] not in ALL_SLATEQ_STRATEGIES:
            raise ValueError("Unknown slateq_strategy: "
                             f"{config['slateq_strategy']}.")

        if config["slateq_strategy"] == "SARSA":
            if config["batch_mode"] != "complete_episodes":
                raise ValueError("For SARSA strategy, batch_mode must be "
                                 "'complete_episodes'")

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> \
            Type[Policy]:
        if config["slateq_strategy"] == "RANDOM":
            return RandomPolicy
        else:
            return SlateQTorchPolicy

    @staticmethod
    @override(Trainer)
    def execution_plan(workers: WorkerSet, config: TrainerConfigDict,
                       **kwargs) -> LocalIterator[dict]:
        assert "local_replay_buffer" in kwargs, (
            "SlateQ execution plan requires a local replay buffer.")

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        # We execute the following steps concurrently:
        # (1) Generate rollouts and store them in our local replay buffer.
        # Calling next() on store_op drives this.
        store_op = rollouts.for_each(
            StoreToReplayBuffer(local_buffer=kwargs["local_replay_buffer"]))

        # (2) Read and train on experiences from the replay buffer. Every batch
        # returned from the LocalReplay() iterator is passed to TrainOneStep to
        # take a SGD step.
        replay_op = Replay(local_buffer=kwargs["local_replay_buffer"]) \
            .for_each(TrainOneStep(workers))

        if config["slateq_strategy"] != "RANDOM":
            # Alternate deterministically between (1) and (2). Only return the
            # output of (2) since training metrics are not available until (2)
            # runs.
            train_op = Concurrently(
                [store_op, replay_op],
                mode="round_robin",
                output_indexes=[1],
                round_robin_weights=calculate_round_robin_weights(config))
        else:
            # No training is needed for the RANDOM strategy.
            train_op = rollouts

        return StandardMetricsReporting(train_op, workers, config)
