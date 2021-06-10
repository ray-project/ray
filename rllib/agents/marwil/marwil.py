from typing import Optional, Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.marwil.marwil_tf_policy import MARWILTFPolicy
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.replay_buffer import LocalReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.concurrency_ops import Concurrently
from ray.rllib.execution.train_ops import TrainOneStep
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.util.iter import LocalIterator
from ray.rllib.policy.policy import Policy

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Input settings ===
    # You should override this to point to an offline dataset
    # (see trainer.py).
    # The dataset may have an arbitrary number of timesteps
    # (and even episodes) per line.
    # However, each line must only contain consecutive timesteps in
    # order for MARWIL to be able to calculate accumulated
    # discounted returns. It is ok, though, to have multiple episodes in
    # the same line.
    "input": "sampler",
    # Use importance sampling estimators for reward.
    "input_evaluation": ["is", "wis"],

    # === Postprocessing/accum., discounted return calculation ===
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf in
    # case an input line ends with a non-terminal timestep.
    "use_gae": True,
    # Whether to calculate cumulative rewards. Must be True.
    "postprocess_inputs": True,

    # === Training ===
    # Scaling of advantages in exponential terms.
    # When beta is 0.0, MARWIL is reduced to behavior cloning
    # (imitation learning); see bc.py algorithm in this same directory.
    "beta": 1.0,
    # Balancing value estimation loss and policy optimization loss.
    "vf_coeff": 1.0,
    # If specified, clip the global norm of gradients by this amount.
    "grad_clip": None,
    # Learning rate for Adam optimizer.
    "lr": 1e-4,
    # The squared moving avg. advantage norm (c^2) update rate
    # (1e-8 in the paper).
    "moving_average_sqd_adv_norm_update_rate": 1e-8,
    # Starting value for the squared moving avg. advantage norm (c^2).
    "moving_average_sqd_adv_norm_start": 100.0,
    # Number of (independent) timesteps pushed through the loss
    # each SGD round.
    "train_batch_size": 2000,
    # Size of the replay buffer in (single and independent) timesteps.
    # The buffer gets filled by reading from the input files line-by-line
    # and adding all timesteps on one line at once. We then sample
    # uniformly from the buffer (`train_batch_size` samples) for
    # each training step.
    "replay_buffer_size": 10000,
    # Number of steps to read before learning starts.
    "learning_starts": 0,

    # === Parallelism ===
    "num_workers": 0,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.
    MARWIL/BC have both TF and Torch policy support.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with DQNTrainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        from ray.rllib.agents.marwil.marwil_torch_policy import \
            MARWILTorchPolicy
        return MARWILTorchPolicy


def execution_plan(workers: WorkerSet,
                   config: TrainerConfigDict) -> LocalIterator[dict]:
    """Execution plan of the MARWIL/BC algorithm. Defines the distributed
    dataflow.

    Args:
        workers (WorkerSet): The WorkerSet for training the Polic(y/ies)
            of the Trainer.
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        LocalIterator[dict]: A local iterator over training metrics.
    """
    rollouts = ParallelRollouts(workers, mode="bulk_sync")
    replay_buffer = LocalReplayBuffer(
        learning_starts=config["learning_starts"],
        buffer_size=config["replay_buffer_size"],
        replay_batch_size=config["train_batch_size"],
        replay_sequence_length=1,
    )

    store_op = rollouts \
        .for_each(StoreToReplayBuffer(local_buffer=replay_buffer))

    replay_op = Replay(local_buffer=replay_buffer) \
        .combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"]["count_steps_by"],
            )) \
        .for_each(TrainOneStep(workers))

    train_op = Concurrently(
        [store_op, replay_op], mode="round_robin", output_indexes=[1])

    return StandardMetricsReporting(train_op, workers, config)


def validate_config(config: TrainerConfigDict) -> None:
    """Checks and updates the config based on settings."""
    if config["num_gpus"] > 1:
        raise ValueError("`num_gpus` > 1 not yet supported for MARWIL!")

    if config["postprocess_inputs"] is False and config["beta"] > 0.0:
        raise ValueError("`postprocess_inputs` must be True for MARWIL (to "
                         "calculate accum., discounted returns)!")


MARWILTrainer = build_trainer(
    name="MARWIL",
    default_config=DEFAULT_CONFIG,
    default_policy=MARWILTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config,
    execution_plan=execution_plan)
