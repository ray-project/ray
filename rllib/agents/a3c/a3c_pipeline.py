"""Experimental pipeline-based impl; run this with --run='A3C_pl'"""

from ray.rllib.agents.a3c.a3c import A3CTrainer
from ray.util.iter import from_actors, LocalIterator
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.utils.experimental_dsl import (ApplyGradients,
                                              StandardMetricsReporting)
from ray.rllib.policy.policy import LEARNER_STATS_KEY


def training_pipeline(workers, config):
    """Async gradients training pipeline.

    This pipeline asynchronously pulls and applies gradients from remote
    workers, sending updated weights back as needed. This pipelines the
    gradient computations on the remote workers.
    """

    # We use the lower level from_actors() API instead of ParallelRollouts,
    # since we want to compute gradients remotely on workers.
    grads = (
        from_actors(workers.remote_workers())  # SampleBatches
        .for_each(  # This lambda will be applied remotely on the workers.
            lambda samples: ( \
                get_global_worker().compute_gradients(samples), \
                samples.count))
        .gather_async())  # -> (grads, info), count

    # Record learner metrics and pass through (grads, count).
    def record_metrics(item):
        (grads, info), count = item
        ctx = LocalIterator.get_context()
        ctx.counters["num_steps_sampled"] += count
        ctx.info["learner"] = info[LEARNER_STATS_KEY]
        return grads, count

    train_op = (
        grads  # (grad, info), count
        .for_each(record_metrics)  # (grad, count)
        .for_each(ApplyGradients(workers, update_all=False)))

    return StandardMetricsReporting(train_op, workers, config)


A3CPipeline = A3CTrainer.with_updates(training_pipeline=training_pipeline)
