"""Helper class for AsyncSamplesOptimizer."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np

import ray
from ray.rllib.utils.actors import TaskPool


class Aggregator(object):
    """Handles sample collection, collation, and broadcasting of new weights.
    
    For performance, aggregators may implement a tree of Ray actors.
    """

    def __init__(self, local_evaluator, remote_evaluators):
        self.local_evaluator = local_evaluator
        self.remote_evaluators = remote_evaluators
        self.broadcast_new_weights()

    def ready_batches(self):
        """Returns a generator over batches ready to learn on.
        
        Iterating through this generator will also sent out weight updates to
        remote evaluators as needed.
        """
        raise NotImplementedError

    def broadcast_new_weights(self):
        """Broadcast a new set of weights from the local evaluator."""
        self.broadcasted_weights = ray.put(self.local_evaluator.get_weights())

    def should_broadcast(self):
        """Returns whether broadcast() should be called to update weights."""
        raise NotImplementedError


class SimpleAggregator(Aggregator):
    def __init__(
            self, local_evaluator, remote_evaluators,
            max_sample_requests_in_flight_per_worker=2,
            replay_proportion=0.0,
            replay_buffer_num_slots=0,
            train_batch_size=500,
            sample_batch_size=50,
            broadcast_interval=5):
        Aggregator.__init__(self, local_evaluator, remote_evaluators)
        self.broadcast_interval = broadcast_interval
        self.num_weight_syncs = 0
        self.sample_batch_size = sample_batch_size
        self.train_batch_size = train_batch_size

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(self.broadcasted_weights)
            for _ in range(max_sample_requests_in_flight_per_worker):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

        if replay_proportion:
            if replay_buffer_num_slots * sample_batch_size <= train_batch_size:
                raise ValueError(
                    "Replay buffer size is too small to produce train, "
                    "please increase replay_buffer_num_slots.",
                    replay_buffer_num_slots, sample_batch_size,
                    train_batch_size)

        self.replay_proportion = replay_proportion
        self.replay_buffer_num_slots = replay_buffer_num_slots
        self.replay_batches = []
        self.num_sent = 0
        self.num_weight_syncs = 0
        self.num_replayed = 0

    def ready_batches(self):
        for ev, sample_batch in self._augment_with_replay(
                self.sample_tasks.completed_prefetch()):
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
                self.replay_batches.append(sample_batch)
                if len(self.replay_batches) > self.replay_buffer_num_slots:
                    self.replay_batches.pop(0)

            ev.set_weights.remote(self.broadcasted_weights)
            self.num_weight_syncs += 1
            self.num_sent += 1

            # Kick off another sample request
            self.sample_tasks.add(ev, ev.sample.remote())

    def should_broadcast(self):
        return self.num_sent > self.broadcast_interval

    def stats(self):
        return {
            "num_weight_syncs": self.num_weight_syncs,
            "num_steps_replayed": self.num_replayed,
        } 

    def _augment_with_replay(self, sample_futures):
        def can_replay():
            num_needed = int(
                np.ceil(self.train_batch_size / self.sample_batch_size))
            return len(self.replay_batches) > num_needed

        for ev, sample_batch in sample_futures:
            sample_batch = ray.get(sample_batch)
            yield ev, sample_batch

            if can_replay():
                f = self.replay_proportion
                while random.random() < f:
                    f -= 1
                    replay_batch = random.choice(self.replay_batches)
                    self.num_replayed += replay_batch.count
                    yield None, replay_batch
