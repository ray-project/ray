import logging
from typing import List, Optional, Union

from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    _check_sample_batch_type,
)
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    DEFAULT_POLICY_ID,
    concat_samples,
)
from ray.rllib.utils.annotations import ExperimentalAPI
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


@ExperimentalAPI
def synchronous_parallel_sample(
    *,
    worker_set: WorkerSet,
    max_agent_steps: Optional[int] = None,
    max_env_steps: Optional[int] = None,
    concat: bool = True,
) -> Union[List[SampleBatchType], SampleBatchType]:
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
        max_agent_steps: Optional number of agent steps to be included in the
            final batch.
        max_env_steps: Optional number of environment steps to be included in the
            final batch.
        concat: Whether to concat all resulting batches at the end and return the
            concat'd batch.

    Returns:
        The list of collected sample batch types (one for each parallel
        rollout worker in the given `worker_set`).

    Examples:
        >>> # Define an RLlib Algorithm.
        >>> algorithm = ... # doctest: +SKIP
        >>> # 2 remote workers (num_workers=2):
        >>> batches = synchronous_parallel_sample(algorithm.workers) # doctest: +SKIP
        >>> print(len(batches)) # doctest: +SKIP
        2
        >>> print(batches[0]) # doctest: +SKIP
        SampleBatch(16: ['obs', 'actions', 'rewards', 'dones'])
        >>> # 0 remote workers (num_workers=0): Using the local worker.
        >>> batches = synchronous_parallel_sample(algorithm.workers) # doctest: +SKIP
        >>> print(len(batches)) # doctest: +SKIP
        1
    """
    # Only allow one of `max_agent_steps` or `max_env_steps` to be defined.
    assert not (max_agent_steps is not None and max_env_steps is not None)

    agent_or_env_steps = 0
    max_agent_or_env_steps = max_agent_steps or max_env_steps or None
    all_sample_batches = []

    # Stop collecting batches as soon as one criterium is met.
    while (max_agent_or_env_steps is None and agent_or_env_steps == 0) or (
        max_agent_or_env_steps is not None
        and agent_or_env_steps < max_agent_or_env_steps
    ):
        # No remote workers in the set -> Use local worker for collecting
        # samples.
        if worker_set.num_remote_workers() <= 0:
            sample_batches = [worker_set.local_worker().sample()]
        # Loop over remote workers' `sample()` method in parallel.
        else:
            sample_batches = worker_set.foreach_worker(
                lambda w: w.sample(), local_worker=False, healthy_only=True
            )
            if worker_set.num_healthy_remote_workers() <= 0:
                # There is no point staying in this loop, since we will not be able to
                # get any new samples if we don't have any healthy remote workers left.
                break
        # Update our counters for the stopping criterion of the while loop.
        for b in sample_batches:
            if max_agent_steps:
                agent_or_env_steps += b.agent_steps()
            else:
                agent_or_env_steps += b.env_steps()
        all_sample_batches.extend(sample_batches)

    if concat is True:
        full_batch = concat_samples(all_sample_batches)
        # Discard collected incomplete episodes in episode mode.
        # if max_episodes is not None and episodes >= max_episodes:
        #    last_complete_ep_idx = len(full_batch) - full_batch[
        #        SampleBatch.DONES
        #    ].reverse().index(1)
        #    full_batch = full_batch.slice(0, last_complete_ep_idx)
        return full_batch
    else:
        return all_sample_batches


def standardize_fields(samples: SampleBatchType, fields: List[str]) -> SampleBatchType:
    """Standardize fields of the given SampleBatch"""
    _check_sample_batch_type(samples)
    wrapped = False

    if isinstance(samples, SampleBatch):
        samples = samples.as_multi_agent()
        wrapped = True

    for policy_id in samples.policy_batches:
        batch = samples.policy_batches[policy_id]
        for field in fields:
            if field in batch:
                batch[field] = standardized(batch[field])

    if wrapped:
        samples = samples.policy_batches[DEFAULT_POLICY_ID]

    return samples
