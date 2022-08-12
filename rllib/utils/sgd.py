"""Utils for minibatch SGD across multiple RLlib policies."""

import logging
import numpy as np
import random

from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder

logger = logging.getLogger(__name__)


@DeveloperAPI
def standardized(array: np.ndarray):
    """Normalize the values in an array.

    Args:
        array (np.ndarray): Array of values to normalize.

    Returns:
        array with zero mean and unit standard deviation.
    """
    return (array - array.mean()) / max(1e-4, array.std())


@DeveloperAPI
def minibatches(samples: SampleBatch, sgd_minibatch_size: int, shuffle: bool = True):
    """Return a generator yielding minibatches from a sample batch.

    Args:
        samples: SampleBatch to split up.
        sgd_minibatch_size: Size of minibatches to return.
        shuffle: Whether to shuffle the order of the generated minibatches.
            Note that in case of a non-recurrent policy, the incoming batch
            is globally shuffled first regardless of this setting, before
            the minibatches are generated from it!

    Yields:
        SampleBatch: Each of size `sgd_minibatch_size`.
    """
    if not sgd_minibatch_size:
        yield samples
        return

    if isinstance(samples, MultiAgentBatch):
        raise NotImplementedError(
            "Minibatching not implemented for multi-agent in simple mode"
        )

    if "state_in_0" not in samples and "state_out_0" not in samples:
        samples.shuffle()

    all_slices = samples._get_slice_indices(sgd_minibatch_size)
    data_slices, state_slices = all_slices

    if len(state_slices) == 0:
        if shuffle:
            random.shuffle(data_slices)
        for i, j in data_slices:
            yield samples[i:j]
    else:
        all_slices = list(zip(data_slices, state_slices))
        if shuffle:
            # Make sure to shuffle data and states while linked together.
            random.shuffle(all_slices)
        for (i, j), (si, sj) in all_slices:
            yield samples.slice(i, j, si, sj)


@DeveloperAPI
def do_minibatch_sgd(
    samples,
    policies,
    local_worker,
    num_sgd_iter,
    sgd_minibatch_size,
    standardize_fields,
):
    """Execute minibatch SGD.

    Args:
        samples: Batch of samples to optimize.
        policies: Dictionary of policies to optimize.
        local_worker: Master rollout worker instance.
        num_sgd_iter: Number of epochs of optimization to take.
        sgd_minibatch_size: Size of minibatches to use for optimization.
        standardize_fields: List of sample field names that should be
            normalized prior to optimization.

    Returns:
        averaged info fetches over the last SGD epoch taken.
    """

    # Handle everything as if multi-agent.
    samples = samples.as_multi_agent()

    # Use LearnerInfoBuilder as a unified way to build the final
    # results dict from `learn_on_loaded_batch` call(s).
    # This makes sure results dicts always have the same structure
    # no matter the setup (multi-GPU, multi-agent, minibatch SGD,
    # tf vs torch).
    learner_info_builder = LearnerInfoBuilder(num_devices=1)
    for policy_id, policy in policies.items():
        if policy_id not in samples.policy_batches:
            continue

        batch = samples.policy_batches[policy_id]
        for field in standardize_fields:
            batch[field] = standardized(batch[field])

        # Check to make sure that the sgd_minibatch_size is not smaller
        # than max_seq_len otherwise this will cause indexing errors while
        # performing sgd when using a RNN or Attention model
        if (
            policy.is_recurrent()
            and policy.config["model"]["max_seq_len"] > sgd_minibatch_size
        ):
            raise ValueError(
                "`sgd_minibatch_size` ({}) cannot be smaller than"
                "`max_seq_len` ({}).".format(
                    sgd_minibatch_size, policy.config["model"]["max_seq_len"]
                )
            )

        for i in range(num_sgd_iter):
            for minibatch in minibatches(batch, sgd_minibatch_size):
                results = (
                    local_worker.learn_on_batch(
                        MultiAgentBatch({policy_id: minibatch}, minibatch.count)
                    )
                )[policy_id]
                learner_info_builder.add_learn_on_batch_results(results, policy_id)

    learner_info = learner_info_builder.finalize()
    return learner_info
