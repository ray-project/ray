"""Experimental workflow-based impl; run this with --run='A2C_wf'"""

import math

from ray.rllib.agents.a3c.a2c import A2CTrainer
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, ConcatBatches, ComputeGradients, AverageGradients,
    ApplyGradients, TrainOneStep, StandardMetricsReporting)


def training_workflow(workers, config):
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    if config["microbatch_size"]:
        num_microbatches = math.ceil(
            config["train_batch_size"] / config["microbatch_size"])

        train_op = (
            rollouts.combine(
                ConcatBatches(min_batch_size=config["microbatch_size"]))
            .for_each(ComputeGradients(workers))  # (grads, info)
            .batch(num_microbatches)  # List[(grads, info)]
            .for_each(AverageGradients())  # (avg_grads, info)
            .for_each(ApplyGradients(workers)))
    else:
        train_op = rollouts \
            .combine(ConcatBatches(
                min_batch_size=config["train_batch_size"])) \
            .for_each(TrainOneStep(workers))

    return StandardMetricsReporting(train_op, workers, config)


A2CWorkflow = A2CTrainer.with_updates(training_workflow=training_workflow)
