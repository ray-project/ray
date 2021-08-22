import logging
from typing import Optional, Type

from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.agents.trainer_template import build_trainer_class
from ray.rllib.execution.rollout_ops import AsyncGradients
from ray.rllib.execution.train_ops import ApplyGradients
from ray.rllib.execution.metric_ops import StandardMetricsReporting
import ray.rllib.utils.events.events as events
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.util.iter import LocalIterator
from ray.rllib.policy.policy import Policy

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Should use a critic as a baseline (otherwise don't use value baseline;
    # required for using GAE).
    "use_critic": True,
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # Size of rollout batch
    "rollout_fragment_length": 10,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Learning rate schedule
    "lr_schedule": None,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Entropy coefficient
    "entropy_coeff": 0.01,
    # Min time per iteration
    "min_iter_time_s": 5,
    # Workers sample async. Note that this increases the effective
    # rollout_fragment_length by up to 5x due to async buffering of batches.
    "sample_async": True,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(trainer: Trainer, config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function.
    
    If None is returned, Trainer will use its built-in default policy class
    (which may be None as well, in which case, an error is raised).

    Args:
        trainer (Trainer): The Trainer object for which to determine the
            (default) policy class.
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with DQNTrainer.
            If None, use `default_policy` provided in build_trainer_class().
    """
    # Class is chosen based on DL-framework.
    if config["framework"] == "torch":
        from ray.rllib.agents.a3c.a3c_torch_policy import \
            A3CTorchPolicy
        return A3CTorchPolicy


def validate_config(trainer: Trainer, config: TrainerConfigDict) -> None:
    """Checks and updates the config based on settings.

    Rewrites rollout_fragment_length to take into account n_step truncation.
    """
    if config["entropy_coeff"] < 0:
        raise ValueError("`entropy_coeff` must be >= 0.0!")
    if config["num_workers"] <= 0 and config["sample_async"]:
        raise ValueError("`num_workers` for A3C must be >= 1!")


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
    # For A3C, compute policy gradients remotely on the rollout workers.
    grads = AsyncGradients(workers)

    # Apply the gradients as they arrive. We set update_all to False so that
    # only the worker sending the gradient is updated with new weights.
    train_op = grads.for_each(ApplyGradients(workers, update_all=False))

    return StandardMetricsReporting(train_op, workers, config)


A3CTrainer = build_trainer_class(
    name="A3C",
    default_config=DEFAULT_CONFIG,
    default_policy=A3CTFPolicy,
    execution_plan=execution_plan,
    # Customizations (event subscriptions).
    event_subscriptions={
        events.AFTER_VALIDATE_CONFIG: validate_config,
        events.SUGGEST_DEFAULT_POLICY_CLASS: get_policy_class,
        #events.SUGGEST_EXECUTION_PLAN: execution_plan,
    }
)
