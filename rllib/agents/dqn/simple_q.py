import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.simple_q_tf_policy import SimpleQTFPolicy
from ray.rllib.agents.dqn.dqn import DQNTrainer
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.replay_ops import StoreToReplayBuffer, Replay
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import TrainOneStep, UpdateTargetNetwork
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import LocalReplayBuffer

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Exploration Settings (Experimental) ===
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
    "buffer_size": 50000,
    # Whether to LZ4 compress observations
    "compress_observations": True,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
    # Learning rate schedule
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
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.dqn.simple_q_torch_policy import \
            SimpleQTorchPolicy
        return SimpleQTorchPolicy
    else:
        return SimpleQTFPolicy


def execution_plan(workers, config):
    local_replay_buffer = LocalReplayBuffer(
        num_shards=1,
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        replay_batch_size=config["train_batch_size"])

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # (1) Generate rollouts and store them in our local replay buffer.
    store_op = rollouts.for_each(
        StoreToReplayBuffer(local_buffer=local_replay_buffer))

    # (2) Read and train on experiences from the replay buffer.
    replay_op = Replay(local_buffer=local_replay_buffer) \
        .for_each(TrainOneStep(workers)) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"]))

    # Alternate deterministically between (1) and (2).
    train_op = Concurrently(
        [store_op, replay_op], mode="round_robin", output_indexes=[1])

    return StandardMetricsReporting(train_op, workers, config)


SimpleQTrainer = DQNTrainer.with_updates(
    default_policy=SimpleQTFPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan,
    default_config=DEFAULT_CONFIG)
