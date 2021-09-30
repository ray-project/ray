"""
Policy Gradient (PG)
====================

This file defines the distributed Trainer class for policy gradients.
See `pg_[tf|torch]_policy.py` for the definition of the policy loss.

Detailed documentation: https://docs.ray.io/en/master/rllib-algorithms.html#pg
"""

import logging
import time
from typing import Optional, Type

from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
from ray.rllib.execution import synchronous_parallel_sample, train_multi_gpu #train_one_step, \
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__

# Adds the following updates to the (base) `Trainer` config in
# rllib/agents/trainer.py (`COMMON_CONFIG` dict).
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    # Learning rate.
    "lr": 0.0004,
    # POC: PG by default works without the distributed execution API.
    "_disable_distributed_execution_api": True,
})

# __sphinx_doc_end__
# yapf: enable


def training_iteration_fn(trainer: Trainer) -> ResultDict:
    """Execution plan of the PG algorithm representing one training iteration.
    
    - Collect on-policy samples (SampleBatches) in parallel using the
      Trainer's RolloutWorkers (@ray.remote).
    - Concatenate collected SampleBatches into one train batch.
    - Note that we may have more than one policy in the multi-agent case:
      Call the different policies' `learn_on_batch` (simple optimizer) OR
      `load_batch_into_buffer` + `learn_on_loaded_batch` (multi-GPU optimizer)
      methods to calculate loss and update the model(s).
    - Return all collected metrics for the iteration.

    Args:
        trainer: The Trainer object that performs the training iteration.

    Returns:
        The results dict from executing the training iteration.
    """
    # Some shortcuts.
    config = trainer.config
    workers = trainer.workers

    # Collects SampleBatches in parallel and synchronously
    # from the Trainer's RolloutWorkers until we hit the
    # configured `train_batch_size`.
    sample_batches = []
    num_samples = 0
    while num_samples < config["train_batch_size"]:
        new_sample_batches = synchronous_parallel_sample(workers)
        sample_batches.extend(new_sample_batches)
        num_samples += sum(len(s) for s in new_sample_batches)

    # Combine all batches at once
    train_batch = SampleBatch.concat_samples(sample_batches)

    # Use simple optimizer (only for multi-agent or tf-eager; all other cases
    # should use the multi-GPU optimizer, even if only using 1 GPU).
    # TODO: (sven) rename MultiGPUOptimizer into something more meaningful.
    if config.get("simple_optimizer") is True:
        train_results = train(trainer, train_batch)
    else:
        train_results = train_multi_gpu(trainer, train_batch)

    #if trainer.
    #report = False

    #time_now = time.time()
    #if time_now - config["min_iter_time_s"] > trainer._last_time_reported:
    #    trainer._last_time_reported = time_now
    #    report = True
    #elif trainer._last_timestep + len(train_batch) > config["timesteps_per_iteration"]:

    #if report:
    #    return collect_metrics(
    #        workers.remote_workers() or [workers.local_worker()],
    #        min_history=config["metrics_smoothing_episodes"],
    #        timeout_seconds=config["collect_metrics_timeout"],
    #    )
    #return {}

    return train_results


def get_policy_class(config: TrainerConfigDict) -> Optional[Type[Policy]]:
    """Policy class picker function. Class is chosen based on DL-framework.

    Args:
        config (TrainerConfigDict): The trainer's configuration dict.

    Returns:
        Optional[Type[Policy]]: The Policy class to use with PGTrainer.
            If None, use `default_policy` provided in build_trainer().
    """
    if config["framework"] == "torch":
        return PGTorchPolicy


# Build a child class of `Trainer`, which uses the framework specific Policy
# determined in `get_policy_class()` above.
PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class,
    training_iteration_fn=training_iteration_fn,
)
