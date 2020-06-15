from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.marwil.marwil_tf_policy import MARWILTFPolicy
from ray.rllib.execution.replay_ops import SimpleReplayBuffer, Replay, \
    StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.train_ops import TrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting

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


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.marwil.marwil_torch_policy import \
            MARWILTorchPolicy
        return MARWILTorchPolicy
    else:
        return MARWILTFPolicy


def execution_plan(workers, config):
    rollouts = ParallelRollouts(workers, mode="bulk_sync")
    replay_buffer = SimpleReplayBuffer(config["replay_buffer_size"])

    store_op = rollouts \
        .for_each(StoreToReplayBuffer(local_buffer=replay_buffer))

    replay_op = Replay(local_buffer=replay_buffer) \
        .combine(
            ConcatBatches(min_batch_size=config["train_batch_size"])) \
        .for_each(TrainOneStep(workers))

    train_op = Concurrently(
        [store_op, replay_op], mode="round_robin", output_indexes=[1])

    return StandardMetricsReporting(train_op, workers, config)


MARWILTrainer = build_trainer(
    name="MARWIL",
    default_config=DEFAULT_CONFIG,
    default_policy=MARWILTFPolicy,
    get_policy_class=get_policy_class,
    execution_plan=execution_plan)
