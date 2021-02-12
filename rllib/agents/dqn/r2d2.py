"""
Recurrent Experience Replay in Distributed Reinforcement Learning (R2D2)
========================================================================

[1] Recurrent Experience Replay in Distributed Reinforcement Learning -
    S Kapturowski, G Ostrovski, J Quan, R Munos, W Dabney - 2019, DeepMind

This file defines the distributed Trainer class for the R2D2
algorithm. See `r2d2_[tf|torch]_policy.py` for the definition of the policies.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#deep-q-networks-dqn-rainbow-parametric-dqn
"""  # noqa: E501
#TODO: ^^ update documentation link

import logging
from typing import List, Optional, Type

from ray.rllib.agents import dqn
from ray.rllib.agents.dqn.r2d2_tf_policy import R2D2TFPolicy
from ray.rllib.agents.dqn.r2d2_torch_policy import R2D2TorchPolicy
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainOneStep, UpdateTargetNetwork
from ray.rllib.policy.policy import LEARNER_STATS_KEY, Policy
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = dqn.DQNTrainer.merge_trainer_configs(
    dqn.DEFAULT_CONFIG,  # See keys in impala.py, which are also supported.
    {
        # If True, assume a zero-initialized state input (no matter where in
        # the episode the sequence is located).
        # If False, store the initial states along with each SampleBatch, use
        # it (as initial state when running through the network for training),
        # and update that initial state during training (from the internal
        # state outputs of the immediately preceding sequence).
        "zero_init_states": True,
        # If > 0, use the `burn_in` first steps of each replay-sampled sequence
        # (starting either from all 0.0-values if `zero_init_state=True` or
        # from the already stored values) to calculate an even more accurate
        # initial states for the actual sequence (starting after this burn-in
        # window). In the burn-in case, the actual length of the sequence
        # used for loss calculation is `n - burn_in` time steps
        # (n=LSTM’s/attention net’s max_seq_len).
        "burn_in": 0,

        # === Hyperparameters from the paper [1] ===
        # Size of the replay buffer (in sequences, not timesteps).
        "buffer_size": 100000,
        # If True prioritized replay buffer will be used.
        # Note: Not supported yet by R2D2!
        "prioritized_replay": False,
        # Set automatically: The number of contiguous environment steps to
        # replay at once. Will be calculated via
        # model->max_seq_len + burn_in.
        # Do not set this to any valid value!
        "replay_sequence_length": -1,
        # Update the target network every `target_network_update_freq` steps.
        "target_network_update_freq": 2500,
        # Discount factor.
        "gamma": 0.997,
        "rollout_fragment_length": 200,
        # Train batch size (in number of single timesteps).
        "train_batch_size": 64 * 20, #TODO: Make this sequences, not timesteps
        # Learning rate for adam optimizer
        "lr": 1e-4,
        # Adam epsilon hyper parameter
        "adam_epsilon": 1e-3,
        # The epsilon parameter from the R2D2 loss function.
        "epsilon": 1e-3,
        # Run in parallel by default.
        "num_workers": 2,

        # Batch mode must be complete_episodes.
        "batch_mode": "complete_episodes",
    },
    _allow_unknown_configs=True,
)
# __sphinx_doc_end__
# yapf: enable


def validate_config(config: TrainerConfigDict) -> None:
    """Checks and updates the config based on settings.

    Rewrites rollout_fragment_length to take into account n_step truncation.
    """
    # Update effective batch size to include n-step
    adjusted_batch_size = max(config["rollout_fragment_length"],
                              config.get("n_step", 1))
    config["rollout_fragment_length"] = adjusted_batch_size

    if config["replay_sequence_length"] != -1:
        raise ValueError(
            "`replay_sequence_length` is calculated automatically to be "
            "model->max_seq_len + burn_in!")
    # Add the `burn_in` to the Model's max_seq_len.
    config["model"]["max_seq_len"] += config["burn_in"]
    # Set the replay sequence length to the max_seq_len of the model.
    config["replay_sequence_length"] = config["model"]["max_seq_len"]

    if config.get("prioritized_replay"):
        raise ValueError("Prioritized replay is not supported for R2D2 yet!")

    if config.get("batch_mode") != "complete_episodes":
        raise ValueError("`batch_mode` must be 'complete_episodes'!")


def execution_plan(workers: WorkerSet,
                   config: TrainerConfigDict) -> LocalIterator[dict]:
    """Execution plan of the DQN algorithm. Defines the distributed dataflow.

    Args:
        workers (WorkerSet): The WorkerSet for training the Polic(y/ies)
            of the Trainer.
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        LocalIterator[dict]: A local iterator over training metrics.
    """
    if config.get("prioritized_replay"):
        prio_args = {
            "prioritized_replay_alpha": config["prioritized_replay_alpha"],
            "prioritized_replay_beta": config["prioritized_replay_beta"],
            "prioritized_replay_eps": config["prioritized_replay_eps"],
        }
    else:
        prio_args = {}

    local_replay_buffer = LocalReplayBuffer(
        num_shards=1,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        replay_batch_size=config["train_batch_size"],
        replay_mode=config["multiagent"]["replay_mode"],
        replay_sequence_length=config["replay_sequence_length"],
        **prio_args)

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # We execute the following steps concurrently:
    # (1) Generate rollouts and store them in our local replay buffer. Calling
    # next() on store_op drives this.
    store_op = rollouts.for_each(
        StoreToReplayBuffer(local_buffer=local_replay_buffer))

    def update_prio(item):
        samples, info_dict = item
        if config.get("prioritized_replay"):
            prio_dict = {}
            for policy_id, info in info_dict.items():
                # TODO(sven): This is currently structured differently for
                #  torch/tf. Clean up these results/info dicts across
                #  policies (note: fixing this in torch_policy.py will
                #  break e.g. DDPPO!).
                td_error = info.get("td_error",
                                    info[LEARNER_STATS_KEY].get("td_error"))
                prio_dict[policy_id] = (samples.policy_batches[policy_id]
                                        .data.get("batch_indexes"), td_error)
            local_replay_buffer.update_priorities(prio_dict)
        return info_dict

    # (2) Read and train on experiences from the replay buffer. Every batch
    # returned from the LocalReplay() iterator is passed to TrainOneStep to
    # take a SGD step, and then we decide whether to update the target network.
    post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)
    replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(lambda x: post_fn(x, workers, config)) \
        .for_each(TrainOneStep(workers)) \
        .for_each(update_prio) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"]))

    # Alternate deterministically between (1) and (2). Only return the output
    # of (2) since training metrics are not available until (2) runs.
    train_op = Concurrently(
        [store_op, replay_op],
        mode="round_robin",
        output_indexes=[1],
        round_robin_weights=calculate_rr_weights(config))

    return StandardMetricsReporting(train_op, workers, config)


def calculate_rr_weights(config: TrainerConfigDict) -> List[float]:
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


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with R2D2Trainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return R2D2TorchPolicy


# Build an R2D2 trainer, which uses the framework specific Policy
# determined in `get_policy_class()` above.
R2D2Trainer = dqn.DQNTrainer.with_updates(
    name="R2D2",
    default_policy=R2D2TFPolicy,
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
    get_policy_class=get_policy_class,
)
