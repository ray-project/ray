"""Experimental pipeline-based impl; run this with --run='A3C_pl'"""

from ray.rllib.agents.a3c.a3c import A3CTrainer
from ray.rllib.utils.experimental_dsl import (AsyncGradients, ApplyGradients,
                                              StandardMetricsReporting)


def training_pipeline(workers, config):
    # For A3C, compute policy gradients remotely on the rollout workers.
    grads = AsyncGradients(workers)

    # Apply the gradients as they arrive. We set update_all to False so that
    # only the worker sending the gradient is updated with new weights.
    train_op = grads.for_each(ApplyGradients(workers, update_all=False))

    return StandardMetricsReporting(train_op, workers, config)


A3CPipeline = A3CTrainer.with_updates(training_pipeline=training_pipeline)
