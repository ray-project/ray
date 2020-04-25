import logging
from typing import List

import ray
from ray.util.iter import LocalIterator
from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import SampleBatchType, \
    STEPS_SAMPLED_COUNTER, STEPS_TRAINED_COUNTER, LEARNER_INFO, \
    APPLY_GRADS_TIMER, COMPUTE_GRADS_TIMER, WORKER_UPDATE_TIMER, \
    LEARN_ON_BATCH_TIMER, LAST_TARGET_UPDATE_TS, NUM_TARGET_UPDATES, \
    _get_global_vars, _check_sample_batch_type

logger = logging.getLogger(__name__)


class TrainOneStep:
    """Callable that improves the policy and updates workers.

    This should be used with the .for_each() operator. A tuple of the input
    and learner stats will be returned.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> train_op = rollouts.for_each(TrainOneStep(workers))
        >>> print(next(train_op))  # This trains the policy on one batch.
        SampleBatch(...), {"learner_stats": ...}

    Updates the STEPS_TRAINED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    def __init__(self, workers: WorkerSet):
        self.workers = workers

    def __call__(self,
                 batch: SampleBatchType) -> (SampleBatchType, List[dict]):
        _check_sample_batch_type(batch)
        metrics = LocalIterator.get_metrics()
        learn_timer = metrics.timers[LEARN_ON_BATCH_TIMER]
        with learn_timer:
            info = self.workers.local_worker().learn_on_batch(batch)
            learn_timer.push_units_processed(batch.count)
        metrics.counters[STEPS_TRAINED_COUNTER] += batch.count
        metrics.info[LEARNER_INFO] = get_learner_stats(info)
        if self.workers.remote_workers():
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())
        return batch, info


class ComputeGradients:
    """Callable that computes gradients with respect to the policy loss.

    This should be used with the .for_each() operator.

    Examples:
        >>> grads_op = rollouts.for_each(ComputeGradients(workers))
        >>> print(next(grads_op))
        {"var_0": ..., ...}, 50  # grads, batch count

    Updates the LEARNER_INFO info field in the local iterator context.
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, samples: SampleBatchType):
        _check_sample_batch_type(samples)
        metrics = LocalIterator.get_metrics()
        with metrics.timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(samples)
        metrics.info[LEARNER_INFO] = get_learner_stats(info)
        return grad, samples.count


class ApplyGradients:
    """Callable that applies gradients and updates workers.

    This should be used with the .for_each() operator.

    Examples:
        >>> apply_op = grads_op.for_each(ApplyGradients(workers))
        >>> print(next(apply_op))
        None

    Updates the STEPS_TRAINED_COUNTER counter in the local iterator context.
    """

    def __init__(self, workers, update_all=True):
        """Creates an ApplyGradients instance.

        Arguments:
            workers (WorkerSet): workers to apply gradients to.
            update_all (bool): If true, updates all workers. Otherwise, only
                update the worker that produced the sample batch we are
                currently processing (i.e., A3C style).
        """
        self.workers = workers
        self.update_all = update_all

    def __call__(self, item):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(
                "Input must be a tuple of (grad_dict, count), got {}".format(
                    item))
        gradients, count = item
        metrics = LocalIterator.get_metrics()
        metrics.counters[STEPS_TRAINED_COUNTER] += count

        apply_timer = metrics.timers[APPLY_GRADS_TIMER]
        with apply_timer:
            self.workers.local_worker().apply_gradients(gradients)
            apply_timer.push_units_processed(count)

        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())

        if self.update_all:
            if self.workers.remote_workers():
                with metrics.timers[WORKER_UPDATE_TIMER]:
                    weights = ray.put(
                        self.workers.local_worker().get_weights())
                    for e in self.workers.remote_workers():
                        e.set_weights.remote(weights, _get_global_vars())
        else:
            if metrics.current_actor is None:
                raise ValueError(
                    "Could not find actor to update. When "
                    "update_all=False, `current_actor` must be set "
                    "in the iterator context.")
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = self.workers.local_worker().get_weights()
                metrics.current_actor.set_weights.remote(
                    weights, _get_global_vars())


class AverageGradients:
    """Callable that averages the gradients in a batch.

    This should be used with the .for_each() operator after a set of gradients
    have been batched with .batch().

    Examples:
        >>> batched_grads = grads_op.batch(32)
        >>> avg_grads = batched_grads.for_each(AverageGradients())
        >>> print(next(avg_grads))
        {"var_0": ..., ...}, 1600  # averaged grads, summed batch count
    """

    def __call__(self, gradients):
        acc = None
        sum_count = 0
        for grad, count in gradients:
            if acc is None:
                acc = grad
            else:
                acc = [a + b for a, b in zip(acc, grad)]
            sum_count += count
        logger.info("Computing average of {} microbatch gradients "
                    "({} samples total)".format(len(gradients), sum_count))
        return acc, sum_count


class UpdateTargetNetwork:
    """Periodically call policy.update_target() on all trainable policies.

    This should be used with the .for_each() operator after training step
    has been taken.

    Examples:
        >>> train_op = ParallelRollouts(...).for_each(TrainOneStep(...))
        >>> update_op = train_op.for_each(
        ...     UpdateTargetIfNeeded(workers, target_update_freq=500))
        >>> print(next(update_op))
        None

    Updates the LAST_TARGET_UPDATE_TS and NUM_TARGET_UPDATES counters in the
    local iterator context. The value of the last update counter is used to
    track when we should update the target next.
    """

    def __init__(self, workers, target_update_freq, by_steps_trained=False):
        self.workers = workers
        self.target_update_freq = target_update_freq
        if by_steps_trained:
            self.metric = STEPS_TRAINED_COUNTER
        else:
            self.metric = STEPS_SAMPLED_COUNTER

    def __call__(self, _):
        metrics = LocalIterator.get_metrics()
        cur_ts = metrics.counters[self.metric]
        last_update = metrics.counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update > self.target_update_freq:
            self.workers.local_worker().foreach_trainable_policy(
                lambda p, _: p.update_target())
            metrics.counters[NUM_TARGET_UPDATES] += 1
            metrics.counters[LAST_TARGET_UPDATE_TS] = cur_ts
