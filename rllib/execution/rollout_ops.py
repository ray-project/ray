import logging
from typing import List, Tuple
import time

from ray.util.iter import from_actors, LocalIterator
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import AGENT_STEPS_SAMPLED_COUNTER, \
    STEPS_SAMPLED_COUNTER, SAMPLE_TIMER, GRAD_WAIT_TIMER, \
    _check_sample_batch_type, _get_shared_metrics
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, \
    LEARNER_STATS_KEY
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.typing import PolicyID, SampleBatchType, ModelGradients

logger = logging.getLogger(__name__)


def ParallelRollouts(workers: WorkerSet, *, mode="bulk_sync",
                     num_async=1) -> LocalIterator[SampleBatch]:
    """Operator to collect experiences in parallel from rollout workers.

    If there are no remote workers, experiences will be collected serially from
    the local worker instance instead.

    Args:
        workers (WorkerSet): set of rollout workers to use.
        mode (str): One of 'async', 'bulk_sync', 'raw'. In 'async' mode,
            batches are returned as soon as they are computed by rollout
            workers with no order guarantees. In 'bulk_sync' mode, we collect
            one batch from each worker and concatenate them together into a
            large batch to return. In 'raw' mode, the ParallelIterator object
            is returned directly and the caller is responsible for implementing
            gather and updating the timesteps counter.
        num_async (int): In async mode, the max number of async
            requests in flight per actor.

    Returns:
        A local iterator over experiences collected in parallel.

    Examples:
        >>> rollouts = ParallelRollouts(workers, mode="async")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        50  # config.rollout_fragment_length

        >>> rollouts = ParallelRollouts(workers, mode="bulk_sync")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        200  # config.rollout_fragment_length * config.num_workers

    Updates the STEPS_SAMPLED_COUNTER counter in the local iterator context.
    """

    # Ensure workers are initially in sync.
    workers.sync_weights()

    def report_timesteps(batch):
        metrics = _get_shared_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += batch.count
        if isinstance(batch, MultiAgentBatch):
            metrics.counters[AGENT_STEPS_SAMPLED_COUNTER] += \
                batch.agent_steps()
        else:
            metrics.counters[AGENT_STEPS_SAMPLED_COUNTER] += batch.count
        return batch

    if not workers.remote_workers():
        # Handle the `num_workers=0` case, in which the local worker
        # has to do sampling as well.
        def sampler(_):
            while True:
                yield workers.local_worker().sample()

        return (LocalIterator(sampler,
                              SharedMetrics()).for_each(report_timesteps))

    # Create a parallel iterator over generated experiences.
    rollouts = from_actors(workers.remote_workers())

    if mode == "bulk_sync":
        return rollouts \
            .batch_across_shards() \
            .for_each(lambda batches: SampleBatch.concat_samples(batches)) \
            .for_each(report_timesteps)
    elif mode == "async":
        return rollouts.gather_async(
            num_async=num_async).for_each(report_timesteps)
    elif mode == "raw":
        return rollouts
    else:
        raise ValueError("mode must be one of 'bulk_sync', 'async', 'raw', "
                         "got '{}'".format(mode))


def AsyncGradients(
        workers: WorkerSet) -> LocalIterator[Tuple[ModelGradients, int]]:
    """Operator to compute gradients in parallel from rollout workers.

    Args:
        workers (WorkerSet): set of rollout workers to use.

    Returns:
        A local iterator over policy gradients computed on rollout workers.

    Examples:
        >>> grads_op = AsyncGradients(workers)
        >>> print(next(grads_op))
        {"var_0": ..., ...}, 50  # grads, batch count

    Updates the STEPS_SAMPLED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    # Ensure workers are initially in sync.
    workers.sync_weights()

    # This function will be applied remotely on the workers.
    def samples_to_grads(samples):
        return get_global_worker().compute_gradients(samples), samples.count

    # Record learner metrics and pass through (grads, count).
    class record_metrics:
        def _on_fetch_start(self):
            self.fetch_start_time = time.perf_counter()

        def __call__(self, item):
            (grads, info), count = item
            metrics = _get_shared_metrics()
            metrics.counters[STEPS_SAMPLED_COUNTER] += count
            metrics.info[LEARNER_INFO] = {
                DEFAULT_POLICY_ID: info
            } if LEARNER_STATS_KEY in info else info
            metrics.timers[GRAD_WAIT_TIMER].push(time.perf_counter() -
                                                 self.fetch_start_time)
            return grads, count

    rollouts = from_actors(workers.remote_workers())
    grads = rollouts.for_each(samples_to_grads)
    return grads.gather_async().for_each(record_metrics())


class ConcatBatches:
    """Callable used to merge batches into larger batches for training.

    This should be used with the .combine() operator.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> rollouts = rollouts.combine(ConcatBatches(
        ...    min_batch_size=10000, count_steps_by="env_steps"))
        >>> print(next(rollouts).count)
        10000
    """

    def __init__(self, min_batch_size: int, count_steps_by: str = "env_steps"):
        self.min_batch_size = min_batch_size
        self.count_steps_by = count_steps_by
        self.buffer = []
        self.count = 0
        self.last_batch_time = time.perf_counter()

    def __call__(self, batch: SampleBatchType) -> List[SampleBatchType]:
        _check_sample_batch_type(batch)

        if self.count_steps_by == "env_steps":
            size = batch.count
        else:
            assert isinstance(batch, MultiAgentBatch), \
                "`count_steps_by=agent_steps` only allowed in multi-agent " \
                "environments!"
            size = batch.agent_steps()

        # Incoming batch is an empty dummy batch -> Ignore.
        # Possibly produced automatically by a PolicyServer to unblock
        # an external env waiting for inputs from unresponsive/disconnected
        # client(s).
        if size == 0:
            return []

        self.count += size
        self.buffer.append(batch)

        if self.count >= self.min_batch_size:
            if self.count > self.min_batch_size * 2:
                logger.info("Collected more training samples than expected "
                            "(actual={}, expected={}). ".format(
                                self.count, self.min_batch_size) +
                            "This may be because you have many workers or "
                            "long episodes in 'complete_episodes' batch mode.")
            out = SampleBatch.concat_samples(self.buffer)

            perf_counter = time.perf_counter()
            timer = _get_shared_metrics().timers[SAMPLE_TIMER]
            timer.push(perf_counter - self.last_batch_time)
            timer.push_units_processed(self.count)

            self.last_batch_time = perf_counter
            self.buffer = []
            self.count = 0
            return [out]
        return []


class SelectExperiences:
    """Callable used to select experiences from a MultiAgentBatch.

    This should be used with the .for_each() operator.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> rollouts = rollouts.for_each(SelectExperiences(["pol1", "pol2"]))
        >>> print(next(rollouts).policy_batches.keys())
        {"pol1", "pol2"}
    """

    def __init__(self, policy_ids: List[PolicyID]):
        assert isinstance(policy_ids, list), policy_ids
        self.policy_ids = policy_ids

    def __call__(self, samples: SampleBatchType) -> SampleBatchType:
        _check_sample_batch_type(samples)

        if isinstance(samples, MultiAgentBatch):
            samples = MultiAgentBatch({
                k: v
                for k, v in samples.policy_batches.items()
                if k in self.policy_ids
            }, samples.count)

        return samples


class StandardizeFields:
    """Callable used to standardize fields of batches.

    This should be used with the .for_each() operator. Note that the input
    may be mutated by this operator for efficiency.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> rollouts = rollouts.for_each(StandardizeFields(["advantages"]))
        >>> print(np.std(next(rollouts)["advantages"]))
        1.0
    """

    def __init__(self, fields: List[str]):
        self.fields = fields

    def __call__(self, samples: SampleBatchType) -> SampleBatchType:
        _check_sample_batch_type(samples)
        wrapped = False

        if isinstance(samples, SampleBatch):
            samples = MultiAgentBatch({
                DEFAULT_POLICY_ID: samples
            }, samples.count)
            wrapped = True

        for policy_id in samples.policy_batches:
            batch = samples.policy_batches[policy_id]
            for field in self.fields:
                batch[field] = standardized(batch[field])

        if wrapped:
            samples = samples.policy_batches[DEFAULT_POLICY_ID]

        return samples
