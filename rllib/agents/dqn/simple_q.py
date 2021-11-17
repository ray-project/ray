"""
Simple Q-Learning
=================

This module provides a basic implementation of the DQN algorithm without any
optimizations.

This file defines the distributed Trainer class for the Simple Q algorithm.
See `simple_q_[tf|torch]_policy.py` for the definition of the policy loss.
"""

import logging
from typing import Optional, Type

from ray.rllib.agents.dqn.simple_q_tf_policy import SimpleQTFPolicy
from ray.rllib.agents.dqn.simple_q_torch_policy import SimpleQTorchPolicy
from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import MultiGPUTrainOneStep, TrainOneStep, \
    UpdateTargetNetwork
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.deprecation import DEPRECATED_VALUE
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Exploration Settings ===
    "exploration_config": {
        # The Exploration class to use.
        "type": "EpsilonGreedy",
        # Config for the Exploration class' constructor:
        "initial_epsilon": 1.0,
        "final_epsilon": 0.02,
        "epsilon_timesteps": 10000,  # Timesteps over which to anneal epsilon.

        # For soft_q, use:
        # "exploration_config" = {
        #   "type": "SoftQ"
        #   "temperature": [float, e.g. 1.0]
        # }
    },
    # Switch to greedy actions in evaluation workers.
    "evaluation_config": {
        "explore": False,
    },

    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 500,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": DEPRECATED_VALUE,
    "replay_buffer_config": {
        "type": "LocalReplayBuffer",
        "capacity": 50000,
    },
    # Set this to True, if you want the contents of your buffer(s) to be
    # stored in any saved checkpoints as well.
    # Warnings will be created if:
    # - This is True AND restoring from a checkpoint that contains no buffer
    #   data.
    # - This is False AND restoring from a checkpoint that does contain
    #   buffer data.
    "store_buffer_in_checkpoints": False,
    # The number of contiguous environment steps to replay at once. This may
    # be set to greater than 1 to support recurrent models.
    "replay_sequence_length": 1,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
    # Learning rate schedule
    # In the format of [[timestep, value], [timestep, value], ...]
    # A schedule should normally start from timestep 0.
    "lr_schedule": None,
    # Adam epsilon hyper parameter
    "adam_epsilon": 1e-8,
    # If not None, clip gradients during optimization at this value
    "grad_clip": 40,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 4,
    # Size of a batch sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Prevent iterations from going lower than this time span.
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with SimpleQTrainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return SimpleQTorchPolicy


def execution_plan(workers: WorkerSet, config: TrainerConfigDict,
                   **kwargs) -> LocalIterator[dict]:
    """Execution plan of the Simple Q algorithm. Defines the distributed dataflow.

    Args:
        trainer (Trainer): The Trainer object creating the execution plan.
        workers (WorkerSet): The WorkerSet for training the Polic(y/ies)
            of the Trainer.
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        LocalIterator[dict]: A local iterator over training metrics.
    """
    assert "local_replay_buffer" in kwargs, (
        "SimpleQ execution plan requires a local replay buffer.")

    local_replay_buffer = kwargs["local_replay_buffer"]

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # (1) Generate rollouts and store them in our local replay buffer.
    store_op = rollouts.for_each(
        StoreToReplayBuffer(local_buffer=local_replay_buffer))

    if config["simple_optimizer"]:
        train_step_op = TrainOneStep(workers)
    else:
        train_step_op = MultiGPUTrainOneStep(
            workers=workers,
            sgd_minibatch_size=config["train_batch_size"],
            num_sgd_iter=1,
            num_gpus=config["num_gpus"],
            shuffle_sequences=True,
            _fake_gpus=config["_fake_gpus"],
            framework=config.get("framework"))

    # (2) Read and train on experiences from the replay buffer.
    replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(train_step_op) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"]))

    # Alternate deterministically between (1) and (2).
    train_op = Concurrently(
        [store_op, replay_op], mode="round_robin", output_indexes=[1])

    return StandardMetricsReporting(train_op, workers, config)


# Build a child class of `Trainer`, which uses the framework specific Policy
# determined in `get_policy_class()` above.
SimpleQTrainer = build_trainer(
    name="SimpleQTrainer",
    default_policy=SimpleQTFPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    default_config=DEFAULT_CONFIG,
)
