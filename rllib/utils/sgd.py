"""Utils for minibatch SGD across multiple RLlib policies."""

import numpy as np
import logging
from collections import defaultdict
import random

from ray.util import log_once
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch

logger = logging.getLogger(__name__)


def averaged(kv, axis=None):
    """Average the value lists of a dictionary.

    For non-scalar values, we simply pick the first value.

    Args:
        kv (dict): dictionary with values that are lists of floats.

    Returns:
        dictionary with single averaged float as values.
    """
    out = {}
    for k, v in kv.items():
        if v[0] is not None and not isinstance(v[0], dict):
            out[k] = np.mean(v, axis=axis)
        else:
            out[k] = v[0]
    return out


def standardized(array):
    """Normalize the values in an array.

    Args:
        array (np.ndarray): Array of values to normalize.

    Returns:
        array with zero mean and unit standard deviation.
    """
    return (array - array.mean()) / max(1e-4, array.std())


def minibatches(samples, sgd_minibatch_size):
    """Return a generator yielding minibatches from a sample batch.

    Args:
        samples (SampleBatch): batch of samples to split up.
        sgd_minibatch_size (int): size of minibatches to return.

    Returns:
        generator that returns mini-SampleBatches of size sgd_minibatch_size.
    """
    if not sgd_minibatch_size:
        yield samples
        return

    if isinstance(samples, MultiAgentBatch):
        raise NotImplementedError(
            "Minibatching not implemented for multi-agent in simple mode")

    # Replace with `if samples.seq_lens` check.
    if "state_in_0" in samples.data or "state_out_0" in samples.data:
        if log_once("not_shuffling_rnn_data_in_simple_mode"):
            logger.warning("Not time-shuffling RNN data for SGD.")
    else:
        samples.shuffle()

    slices = samples._get_slice_indices(sgd_minibatch_size)
    random.shuffle(slices)

    for i, j in slices:
        yield samples.slice(i, j)


def do_minibatch_sgd(samples, policies, local_worker, num_sgd_iter,
                     sgd_minibatch_size, standardize_fields):
    """Execute minibatch SGD.

    Args:
        samples (SampleBatch): Batch of samples to optimize.
        policies (dict): Dictionary of policies to optimize.
        local_worker (RolloutWorker): Master rollout worker instance.
        num_sgd_iter (int): Number of epochs of optimization to take.
        sgd_minibatch_size (int): Size of minibatches to use for optimization.
        standardize_fields (list): List of sample field names that should be
            normalized prior to optimization.

    Returns:
        averaged info fetches over the last SGD epoch taken.
    """
    if isinstance(samples, SampleBatch):
        samples = MultiAgentBatch({DEFAULT_POLICY_ID: samples}, samples.count)

    fetches = defaultdict(dict)
    for policy_id in policies.keys():
        if policy_id not in samples.policy_batches:
            continue

        batch = samples.policy_batches[policy_id]
        for field in standardize_fields:
            batch[field] = standardized(batch[field])

        for i in range(num_sgd_iter):
            learner_stats = defaultdict(list)
            for minibatch in minibatches(batch, sgd_minibatch_size):
                batch_fetches = (local_worker.learn_on_batch(
                    MultiAgentBatch({
                        policy_id: minibatch
                    }, minibatch.count)))[policy_id]
                for k, v in batch_fetches.get(LEARNER_STATS_KEY, {}).items():
                    learner_stats[k].append(v)
        fetches[policy_id][LEARNER_STATS_KEY] = averaged(learner_stats)
    return fetches
