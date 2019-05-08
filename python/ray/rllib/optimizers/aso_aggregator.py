"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import random

import ray
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.annotations import override
from ray.rllib.utils.memory import ray_get_and_free


class Aggregator(object):
    """An aggregator collects and processes samples from evaluators.

    This class is used to abstract away the strategy for sample collection.
    For example, you may want to use a tree of actors to collect samples. The
    use of multiple actors can be necessary to offload expensive work such
    as concatenating and decompressing sample batches.

    Attributes:
        local_evaluator: local PolicyEvaluator copy
    """

    def iter_train_batches(self):
        """Returns a generator over batches ready to learn on.

        Iterating through this generator will also send out weight updates to
        remote evaluators as needed.

        This call may block until results are available.
        """
        raise NotImplementedError

    def broadcast_new_weights(self):
        """Broadcast a new set of weights from the local evaluator."""
        raise NotImplementedError

    def should_broadcast(self):
        """Returns whether broadcast() should be called to update weights."""
        raise NotImplementedError

    def stats(self):
        """Returns runtime statistics for debugging."""
        raise NotImplementedError

    def reset(self, remote_evaluators):
        """Called to change the set of remote evaluators being used."""
        raise NotImplementedError


class AggregationWorkerBase(object):
    """Aggregators should extend from this class."""

    def __init__(self, initial_weights_obj_id, remote_evaluators,
                 max_sample_requests_in_flight_per_worker, replay_proportion,
                 replay_buffer_num_slots, train_batch_size, sample_batch_size):
        self.broadcasted_weights = initial_weights_obj_id
        self.remote_evaluators = remote_evaluators
        self.sample_batch_size = sample_batch_size
        self.train_batch_size = train_batch_size

        if replay_proportion:
            if replay_buffer_num_slots * sample_batch_size <= train_batch_size:
                raise ValueError(
                    "Replay buffer size is too small to produce train, "
                    "please increase replay_buffer_num_slots.",
                    replay_buffer_num_slots, sample_batch_size,
                    train_batch_size)

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(self.broadcasted_weights)
            for _ in range(max_sample_requests_in_flight_per_worker):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

        self.replay_proportion = replay_proportion
        self.replay_buffer_num_slots = replay_buffer_num_slots
        self.replay_batches = []
        self.replay_index = 0
        self.num_sent_since_broadcast = 0
        self.num_weight_syncs = 0
        self.num_replayed = 0

    @override(Aggregator)
    def iter_train_batches(self, max_yield=999):
        """Iterate over train batches.

        Arguments:
            max_yield (int): Max number of batches to iterate over in this
                cycle. Setting this avoids iter_train_batches returning too
                much data at once.
        """

        for ev, sample_batch in self._augment_with_replay(
                self.sample_tasks.completed_prefetch(
                    blocking_wait=True, max_yield=max_yield)):
            sample_batch.decompress_if_needed()
            self.batch_buffer.append(sample_batch)
            if sum(b.count
                   for b in self.batch_buffer) >= self.train_batch_size:
                train_batch = self.batch_buffer[0].concat_samples(
                    self.batch_buffer)
                yield train_batch
                self.batch_buffer = []

            # If the batch was replayed, skip the update below.
            if ev is None:
                continue

            # Put in replay buffer if enabled
            if self.replay_buffer_num_slots > 0:
                if len(self.replay_batches) < self.replay_buffer_num_slots:
                    self.replay_batches.append(sample_batch)
                else:
                    self.replay_batches[self.replay_index] = sample_batch
                    self.replay_index += 1
                    self.replay_index %= self.replay_buffer_num_slots

            ev.set_weights.remote(self.broadcasted_weights)
            self.num_weight_syncs += 1
            self.num_sent_since_broadcast += 1

            # Kick off another sample request
            self.sample_tasks.add(ev, ev.sample.remote())

    @override(Aggregator)
    def stats(self):
        return {
            "num_weight_syncs": self.num_weight_syncs,
            "num_steps_replayed": self.num_replayed,
        }

    @override(Aggregator)
    def reset(self, remote_evaluators):
        self.sample_tasks.reset_evaluators(remote_evaluators)

    def _augment_with_replay(self, sample_futures):
        def can_replay():
            num_needed = int(
                np.ceil(self.train_batch_size / self.sample_batch_size))
            return len(self.replay_batches) > num_needed

        for ev, sample_batch in sample_futures:
            sample_batch = ray_get_and_free(sample_batch)
            yield ev, sample_batch

            if can_replay():
                f = self.replay_proportion
                while random.random() < f:
                    f -= 1
                    replay_batch = random.choice(self.replay_batches)
                    self.num_replayed += replay_batch.count
                    yield None, replay_batch


class SimpleAggregator(AggregationWorkerBase, Aggregator):
    """Simple single-threaded implementation of an Aggregator."""

    def __init__(self,
                 local_evaluator,
                 remote_evaluators,
                 max_sample_requests_in_flight_per_worker=2,
                 replay_proportion=0.0,
                 replay_buffer_num_slots=0,
                 train_batch_size=500,
                 sample_batch_size=50,
                 broadcast_interval=5):
        self.local_evaluator = local_evaluator
        self.broadcast_interval = broadcast_interval
        self.broadcast_new_weights()
        AggregationWorkerBase.__init__(
            self, self.broadcasted_weights, remote_evaluators,
            max_sample_requests_in_flight_per_worker, replay_proportion,
            replay_buffer_num_slots, train_batch_size, sample_batch_size)

    @override(Aggregator)
    def broadcast_new_weights(self):
        self.broadcasted_weights = ray.put(self.local_evaluator.get_weights())
        self.num_sent_since_broadcast = 0

    @override(Aggregator)
    def should_broadcast(self):
        return self.num_sent_since_broadcast >= self.broadcast_interval
