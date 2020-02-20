"""Experimental pipeline-based impl; run this with --run='PG_pl'"""

from ray.rllib.agents.pg.pg import PGTrainer
from ray.rllib.utils.experimental_dsl import (
    ParallelRollouts, ConcatBatches, TrainOneStep, StandardMetricsReporting)


def training_pipeline(workers, config):
    # Collects experiences in parallel from multiple RolloutWorker actors.
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    # Combine experiences batches until we hit `train_batch_size` in size.
    # Then, train the policy on those experiences and update the workers.
    train_op = rollouts \
        .combine(ConcatBatches(
            min_batch_size=config["train_batch_size"])) \
        .for_each(TrainOneStep(workers))

    # Add on the standard episode reward, etc. metrics reporting. This returns
    # a LocalIterator[metrics_dict] representing metrics for each train step.
    return StandardMetricsReporting(train_op, workers, config)


PGPipeline = PGTrainer.with_updates(training_pipeline=training_pipeline)
