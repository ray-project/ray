"""Experimental pipeline-based impl; run this with --run='A2C_pl'"""

import math

from ray.rllib.agents.a3c.a2c import A2CTrainer
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, ConcatBatches, ComputeGradients, AverageGradients,
    ApplyGradients, TrainOneStep, StandardMetricsReporting)


def training_pipeline(workers, config):
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    if config["microbatch_size"]:
        num_microbatches = math.ceil(
            config["train_batch_size"] / config["microbatch_size"])
        # In microbatch mode, we want to compute gradients on experience
        # microbatches, average a number of these microbatches, and then apply
        # the averaged gradient in one SGD step. This conserves GPU memory,
        # allowing for extremely large experience batches to be used.
        train_op = (
            rollouts.combine(
                ConcatBatches(min_batch_size=config["microbatch_size"]))
            .for_each(ComputeGradients(workers))  # (grads, info)
            .batch(num_microbatches)  # List[(grads, info)]
            .for_each(AverageGradients())  # (avg_grads, info)
            .for_each(ApplyGradients(workers)))
    else:
        # In normal mode, we execute one SGD step per each train batch.
        train_op = rollouts \
            .combine(ConcatBatches(
                min_batch_size=config["train_batch_size"])) \
            .for_each(TrainOneStep(workers))

    return StandardMetricsReporting(train_op, workers, config)


A2CPipeline = A2CTrainer.with_updates(training_pipeline=training_pipeline)
