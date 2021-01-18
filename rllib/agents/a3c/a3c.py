import logging

from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.execution.rollout_ops import AsyncGradients
from ray.rllib.execution.train_ops import ApplyGradients
from ray.rllib.execution.metric_ops import StandardMetricsReporting

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


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.a3c.a3c_torch_policy import \
            A3CTorchPolicy
        return A3CTorchPolicy
    else:
        return A3CTFPolicy


def validate_config(config):
    if config["entropy_coeff"] < 0:
        raise ValueError("`entropy_coeff` must be >= 0.0!")
    if config["num_workers"] <= 0 and config["sample_async"]:
        raise ValueError("`num_workers` for A3C must be >= 1!")


def execution_plan(workers, config):
    # For A3C, compute policy gradients remotely on the rollout workers.
    grads = AsyncGradients(workers)

    # Apply the gradients as they arrive. We set update_all to False so that
    # only the worker sending the gradient is updated with new weights.
    train_op = grads.for_each(ApplyGradients(workers, update_all=False))

    return StandardMetricsReporting(train_op, workers, config)


A3CTrainer = build_trainer(
    name="A3C",
    default_config=DEFAULT_CONFIG,
    default_policy=A3CTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config,
    execution_plan=execution_plan)
