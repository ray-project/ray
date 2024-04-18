import logging
from typing import List, Optional, Union
import tree

from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    DEFAULT_POLICY_ID,
    concat_samples,
)
from ray.rllib.utils.annotations import ExperimentalAPI, OldAPIStack
from ray.rllib.utils.sgd import standardized
from ray.rllib.utils.typing import EpisodeType, SampleBatchType

logger = logging.getLogger(__name__)


@ExperimentalAPI
def synchronous_parallel_sample(
    *,
    worker_set: WorkerSet,
    max_agent_steps: Optional[int] = None,
    max_env_steps: Optional[int] = None,
    concat: bool = True,
    sample_timeout_s: Optional[float] = 60.0,
    _uses_new_env_runners: bool = False,
) -> Union[List[SampleBatchType], SampleBatchType, List[EpisodeType], EpisodeType]:
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
            final batch or list of episodes.
        max_env_steps: Optional number of environment steps to be included in the
            final batch or list of episodes.
        concat: Whether to aggregate all resulting batches or episodes. in case of
            batches the list of batches is concatinated at the end. in case of
            episodes all episode lists from workers are flattened into a single list.
        sample_timeout_s: The timeout in sec to use on the `foreach_worker` call.
            After this time, the call will return with a result (or not if all workers
            are stalling).
        _uses_new_env_runners: Whether the new `EnvRunner API` is used. In this case
            episodes instead of `SampleBatch` objects are returned.

    Returns:
        The list of collected sample batch types or episode types (one for each parallel
        rollout worker in the given `worker_set`).

    .. testcode::

        # Define an RLlib Algorithm.
        from ray.rllib.algorithms.ppo import PPO, PPOConfig
        config = PPOConfig().environment("CartPole-v1")
        algorithm = PPO(config=config)
        # 2 remote workers (num_workers=2):
        batches = synchronous_parallel_sample(worker_set=algorithm.workers,
            concat=False)
        print(len(batches))

    .. testoutput::

        2
    """
    # Only allow one of `max_agent_steps` or `max_env_steps` to be defined.
    assert not (max_agent_steps is not None and max_env_steps is not None)

    agent_or_env_steps = 0
    max_agent_or_env_steps = max_agent_steps or max_env_steps or None
    sample_batches_or_episodes = []

    # Stop collecting batches as soon as one criterium is met.
    while (max_agent_or_env_steps is None and agent_or_env_steps == 0) or (
        max_agent_or_env_steps is not None
        and agent_or_env_steps < max_agent_or_env_steps
    ):
        # No remote workers in the set -> Use local worker for collecting
        # samples.
        if worker_set.num_remote_workers() <= 0:
            sampled_data = [worker_set.local_worker().sample()]
        # Loop over remote workers' `sample()` method in parallel.
        else:
            sampled_data = worker_set.foreach_worker(
                lambda w: w.sample(),
                local_worker=False,
                healthy_only=True,
                timeout_seconds=sample_timeout_s,
            )
            # Nothing was returned (maybe all workers are stalling) or no healthy
            # remote workers left: Break.
            # There is no point staying in this loop, since we will not be able to
            # get any new samples if we don't have any healthy remote workers left.
            if not sampled_data or worker_set.num_healthy_remote_workers() <= 0:
                if not sampled_data:
                    logger.warning(
                        "No samples returned from remote workers. If you have a "
                        "slow environment or model, consider increasing the "
                        "`sample_timeout_s` or decreasing the "
                        "`rollout_fragment_length` in `AlgorithmConfig.rollouts()."
                    )
                elif worker_set.num_healthy_remote_workers() <= 0:
                    logger.warning(
                        "No healthy remote workers left. Trying to restore workers ..."
                    )
                break
        # Update our counters for the stopping criterion of the while loop.
        for batch_or_episode in sampled_data:
            if max_agent_steps:
                agent_or_env_steps += (
                    sum(e.agent_steps() for e in batch_or_episode)
                    if _uses_new_env_runners
                    else batch_or_episode.agent_steps()
                )
            else:
                agent_or_env_steps += (
                    sum(e.env_steps() for e in batch_or_episode)
                    if _uses_new_env_runners
                    else batch_or_episode.env_steps()
                )
        sample_batches_or_episodes.extend(sampled_data)

    if concat is True:
        # If we have episodes flatten the episode list.
        if _uses_new_env_runners:
            return tree.flatten(sample_batches_or_episodes)
        # Otherwise we concatenate the `SampleBatch` objects
        else:
            return concat_samples(sample_batches_or_episodes)
    else:
        return sample_batches_or_episodes


@OldAPIStack
def standardize_fields(samples: SampleBatchType, fields: List[str]) -> SampleBatchType:
    """Standardize fields of the given SampleBatch"""
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
