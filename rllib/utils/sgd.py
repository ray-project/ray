"""Utils for minibatch SGD across multiple RLlib policies."""

import numpy as np
import logging
from collections import defaultdict
import random

from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch

logger = logging.getLogger(__name__)


def averaged(kv):
    """Average the value lists of a dictionary."""
    out = {}
    for k, v in kv.items():
        if v[0] is not None and not isinstance(v[0], dict):
            out[k] = np.mean(v)
    return out


def minibatches(samples, sgd_minibatch_size):
    """Return a generator yielding minibatches from a sample batch."""
    if not sgd_minibatch_size:
        yield samples
        return

    if isinstance(samples, MultiAgentBatch):
        raise NotImplementedError(
            "Minibatching not implemented for multi-agent in simple mode")

    if "state_in_0" in samples.data:
        logger.warning("Not shuffling RNN data for SGD in simple mode")
    else:
        samples.shuffle()

    i = 0
    slices = []
    while i < samples.count:
        slices.append((i, i + sgd_minibatch_size))
        i += sgd_minibatch_size
    random.shuffle(slices)

    for i, j in slices:
        yield samples.slice(i, j)


def do_minibatch_sgd(samples, policies, local_worker, num_sgd_iter,
                     sgd_minibatch_size, standardize_fields):
    """Execute minibatch SGD with the given parameters."""
    if isinstance(samples, SampleBatch):
        samples = MultiAgentBatch({DEFAULT_POLICY_ID: samples}, samples.count)

    fetches = {}
    for policy_id, policy in policies.items():
        if policy_id not in samples.policy_batches:
            continue

        batch = samples.policy_batches[policy_id]
        for field in standardize_fields:
            value = batch[field]
            standardized = (value - value.mean()) / max(1e-4, value.std())
            batch[field] = standardized

        for i in range(num_sgd_iter):
            iter_extra_fetches = defaultdict(list)
            for minibatch in minibatches(batch, sgd_minibatch_size):
                batch_fetches = (local_worker.learn_on_batch(
                    MultiAgentBatch({
                        policy_id: minibatch
                    }, minibatch.count)))[policy_id]
                for k, v in batch_fetches[LEARNER_STATS_KEY].items():
                    iter_extra_fetches[k].append(v)
            logger.debug("{} {}".format(i, averaged(iter_extra_fetches)))
        fetches[policy_id] = averaged(iter_extra_fetches)
    return fetches
