"""Experimental workflow-based impl; run this with --run='PG_wf'"""

from ray.rllib.agents.pg.pg import PGTrainer
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, ConcatBatches, TrainOneStep, StandardMetricsReporting)


def training_workflow(workers, config):

    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    train_op = rollouts \
        .combine(ConcatBatches(
            min_batch_size=config["train_batch_size"])) \
        .for_each(TrainOneStep(workers))

    return StandardMetricsReporting(train_op, workers, config)


PGWorkflow = PGTrainer.with_updates(training_workflow=training_workflow)
