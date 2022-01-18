import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, \
    TYPE_CHECKING

import ray
from ray.actor import ActorHandle
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import AGENT_STEPS_SAMPLED_COUNTER, \
    STEPS_SAMPLED_COUNTER, SAMPLE_TIMER, GRAD_WAIT_TIMER, \
    _check_sample_batch_type, _get_shared_metrics
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.metrics.learner_info import LEARNER_INFO, \
    LEARNER_STATS_KEY
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.typing import PolicyID, SampleBatchType, ModelGradients
from ray.util.iter import from_actors, LocalIterator
from ray.util.iter_metrics import SharedMetrics

if TYPE_CHECKING:
    from ray.rllib.agents.trainer import Trainer
    from ray.rllib.evaluation.rollout_worker import RolloutWorker

logger = logging.getLogger(__name__)


@ExperimentalAPI
def synchronous_parallel_sample(
        worker_set: WorkerSet,
        remote_fn: Optional[Callable[["RolloutWorker"], None]] = None,
) -> List[SampleBatch]:
    """Runs parallel and synchronous rollouts on all remote workers.

    Waits for all workers to return from the remote calls.

    If no remote workers exist (num_workers == 0), use the local worker
    for sampling.

    Alternatively to calling `worker.sample.remote()`, the user can provide a
    `remote_fn()`, which will be applied to the worker(s) instead.

    Args:
        worker_set: The WorkerSet to use for sampling.
        remote_fn: If provided, use `worker.apply.remote(remote_fn)` instead
            of `worker.sample.remote()` to generate the requests.

    Returns:
        The list of collected sample batch types (one for each parallel
        rollout worker in the given `worker_set`).

    Examples:
        >>> # 2 remote workers (num_workers=2):
        >>> batches = synchronous_parallel_sample(trainer.workers)
        >>> print(len(batches))
        ... 2
        >>> print(batches[0])
        ... SampleBatch(16: ['obs', 'actions', 'rewards', 'dones'])

        >>> # 0 remote workers (num_workers=0): Using the local worker.
        >>> batches = synchronous_parallel_sample(trainer.workers)
        >>> print(len(batches))
        ... 1
    """
    # No remote workers in the set -> Use local worker for collecting
    # samples.
    if not worker_set.remote_workers():
        return [worker_set.local_worker().sample()]

    # Loop over remote workers' `sample()` method in parallel.
    sample_batches = ray.get(
        [r.sample.remote() for r in worker_set.remote_workers()])

    # Return all collected batches.
    return sample_batches


# TODO: Move to generic parallel ops module and rename to
#  `asynchronous_parallel_requests`:
@ExperimentalAPI
def asynchronous_parallel_sample(
        trainer: "Trainer",
        actors: List[ActorHandle],
        ray_wait_timeout_s: Optional[float] = None,
        max_remote_requests_in_flight_per_actor: int = 2,
        remote_fn: Optional[Callable[["RolloutWorker"], None]] = None,
        remote_args: Optional[List[List[Any]]] = None,
        remote_kwargs: Optional[List[Dict[str, Any]]] = None,
) -> Optional[List[SampleBatch]]:
    """Runs parallel and asynchronous rollouts on all remote workers.

    May use a timeout (if provided) on `ray.wait()` and returns only those
    samples that could be gathered in the timeout window. Allows a maximum
    of `max_remote_requests_in_flight_per_actor` remote calls to be in-flight
    per remote actor.

    Alternatively to calling `actor.sample.remote()`, the user can provide a
    `remote_fn()`, which will be applied to the actor(s) instead.

    Args:
        trainer: The Trainer object that we run the sampling for.
        actors: The List of ActorHandles to perform the remote requests on.
        ray_wait_timeout_s: Timeout (in sec) to be used for the underlying
            `ray.wait()` calls. If None (default), never time out (block
            until at least one actor returns something).
        max_remote_requests_in_flight_per_actor: Maximum number of remote
            requests sent to each actor. 2 (default) is probably
            sufficient to avoid idle times between two requests.
        remote_fn: If provided, use `actor.apply.remote(remote_fn)` instead of
            `actor.sample.remote()` to generate the requests.
        remote_args: If provided, use this list (per-actor) of lists (call
            args) as *args to be passed to the `remote_fn`.
            E.g.: actors=[A, B],
            remote_args=[[...] <- *args for A, [...] <- *args for B].
        remote_kwargs: If provided, use this list (per-actor) of dicts
            (kwargs) as **kwargs to be passed to the `remote_fn`.
            E.g.: actors=[A, B],
            remote_kwargs=[{...} <- **kwargs for A, {...} <- **kwargs for B].

    Returns:
        The list of asynchronously collected sample batch types. None, if no
        samples are ready.

    Examples:
        >>> # 2 remote rollout workers (num_workers=2):
        >>> batches = asynchronous_parallel_sample(
        ...     trainer,
        ...     actors=trainer.workers.remote_workers(),
        ...     ray_wait_timeout_s=0.1,
        ...     remote_fn=lambda w: time.sleep(1)  # sleep 1sec
        ... )
        >>> print(len(batches))
        ... 2
        >>> # Expect a timeout to have happened.
        >>> batches[0] is None and batches[1] is None
        ... True
    """

    if remote_args is not None:
        assert len(remote_args) == len(actors)
    if remote_kwargs is not None:
        assert len(remote_kwargs) == len(actors)

    # Collect all currently pending remote requests into a single set of
    # object refs.
    pending_remotes = set()
    # Also build a map to get the associated actor for each remote request.
    remote_to_actor = {}
    for actor, set_ in trainer.remote_requests_in_flight.items():
        pending_remotes |= set_
        for r in set_:
            remote_to_actor[r] = actor

    # Add new requests, if possible (if
    # `max_remote_requests_in_flight_per_actor` setting allows it).
    for actor_idx, actor in enumerate(actors):
        # Still room for another request to this actor.
        if len(trainer.remote_requests_in_flight[actor]) < \
                max_remote_requests_in_flight_per_actor:
            if remote_fn is None:
                req = actor.sample.remote()
            else:
                args = remote_args[actor_idx] if remote_args else []
                kwargs = remote_kwargs[actor_idx] if remote_kwargs else {}
                req = actor.apply.remote(remote_fn, *args, **kwargs)
            # Add to our set to send to ray.wait().
            pending_remotes.add(req)
            # Keep our mappings properly updated.
            trainer.remote_requests_in_flight[actor].add(req)
            remote_to_actor[req] = actor

    # There must always be pending remote requests.
    assert len(pending_remotes) > 0
    pending_remote_list = list(pending_remotes)

    # No timeout: Block until at least one result is returned.
    if ray_wait_timeout_s is None:
        # First try to do a `ray.wait` w/o timeout for efficiency.
        ready, _ = ray.wait(
            pending_remote_list, num_returns=len(pending_remotes), timeout=0)
        # Nothing returned and `timeout` is None -> Fall back to a
        # blocking wait to make sure we can return something.
        if not ready:
            ready, _ = ray.wait(pending_remote_list, num_returns=1)
    # Timeout: Do a `ray.wait() call` w/ timeout.
    else:
        ready, _ = ray.wait(
            pending_remote_list,
            num_returns=len(pending_remotes),
            timeout=ray_wait_timeout_s)

        # Return None if nothing ready after the timeout.
        if not ready:
            return None

    for obj_ref in ready:
        # Remove in-flight record for this ref.
        trainer.remote_requests_in_flight[remote_to_actor[obj_ref]].remove(
            obj_ref)
        remote_to_actor.pop(obj_ref)

    results = ray.get(ready)

    return results


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
            samples = samples.as_multi_agent()
            wrapped = True

        for policy_id in samples.policy_batches:
            batch = samples.policy_batches[policy_id]
            for field in self.fields:
                if field not in batch:
                    raise KeyError(
                        f"`{field}` not found in SampleBatch for policy "
                        f"`{policy_id}`! Maybe this policy fails to add "
                        f"{field} in its `postprocess_trajectory` method? Or "
                        "this policy is not meant to learn at all and you "
                        "forgot to add it to the list under `config."
                        "multiagent.policies_to_train`.")
                batch[field] = standardized(batch[field])

        if wrapped:
            samples = samples.policy_batches[DEFAULT_POLICY_ID]

        return samples
