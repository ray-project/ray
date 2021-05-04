from typing import Optional, Type

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.marwil.marwil_tf_policy import MARWILTFPolicy
from ray.rllib.execution.replay_ops import SimpleReplayBuffer, Replay, \
    StoreToReplayBuffer
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
    # You should override this to point to an offline dataset (see agent.py).
    "input": "sampler",
    # Use importance sampling estimators for reward
    "input_evaluation": ["is", "wis"],

    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,

    # Scaling of advantages in exponential terms.
    # When beta is 0.0, MARWIL is reduced to imitation learning.
    "beta": 1.0,
    # Balancing value estimation loss and policy optimization loss.
    "vf_coeff": 1.0,
    # If specified, clip the global norm of gradients by this amount.
    "grad_clip": None,
    # Whether to calculate cumulative rewards.
    "postprocess_inputs": True,
    # Whether to rollout "complete_episodes" or "truncate_episodes".
    "batch_mode": "complete_episodes",
    # Learning rate for adam optimizer.
    "lr": 1e-4,
    # Number of timesteps collected for each SGD round.
    "train_batch_size": 2000,
    # Size of the replay buffer in batches (not timesteps!).
    "replay_buffer_size": 1000,
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
    replay_buffer = SimpleReplayBuffer(config["replay_buffer_size"])

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
    """Checks and updates the config based on settings.

    Rewrites rollout_fragment_length to take into account n_step truncation.
    """
    if config["num_gpus"] > 1:
        raise ValueError("`num_gpus` > 1 not yet supported for MARWIL!")


MARWILTrainer = build_trainer(
    name="MARWIL",
    default_config=DEFAULT_CONFIG,
    default_policy=MARWILTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config,
    execution_plan=execution_plan)
