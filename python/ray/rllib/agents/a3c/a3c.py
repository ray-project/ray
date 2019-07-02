from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.optimizers import AsyncGradientsOptimizer

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Size of rollout batch
    "sample_batch_size": 10,
    # Use PyTorch as backend - no LSTM support
    "use_pytorch": False,
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
    # sample_batch_size by up to 5x due to async buffering of batches.
    "sample_async": True,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["use_pytorch"]:
        from ray.rllib.agents.a3c.a3c_torch_policy import \
            A3CTorchPolicy
        return A3CTorchPolicy
    else:
        return A3CTFPolicy


def validate_config(config):
    if config["entropy_coeff"] < 0:
        raise DeprecationWarning("entropy_coeff must be >= 0")
    if config["sample_async"] and config["use_pytorch"]:
        raise ValueError(
            "The sample_async option is not supported with use_pytorch: "
            "Multithreading can be lead to crashes if used with pytorch.")


def make_async_optimizer(workers, config):
    return AsyncGradientsOptimizer(workers, **config["optimizer"])


A3CTrainer = build_trainer(
    name="A3C",
    default_config=DEFAULT_CONFIG,
    default_policy=A3CTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config,
    make_policy_optimizer=make_async_optimizer)
