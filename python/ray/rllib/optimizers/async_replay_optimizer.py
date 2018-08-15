"""Implements Distributed Prioritized Experience Replay.

https://arxiv.org/abs/1803.00933"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import time
import threading

import numpy as np
from six.moves import queue

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.replay_buffer import PrioritizedReplayBuffer
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.actors import TaskPool, create_colocated
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

SAMPLE_QUEUE_DEPTH = 2
REPLAY_QUEUE_DEPTH = 4
LEARNER_QUEUE_MAX_SIZE = 16


@ray.remote(num_cpus=0)
class ReplayActor(object):
    """A replay buffer shard.

    Ray actors are single-threaded, so for scalability multiple replay actors
    may be created to increase parallelism."""

    def __init__(self, num_shards, learning_starts, buffer_size,
                 train_batch_size, prioritized_replay_alpha,
                 prioritized_replay_beta, prioritized_replay_eps,
                 clip_rewards):
        self.replay_starts = learning_starts // num_shards
        self.buffer_size = buffer_size // num_shards
        self.train_batch_size = train_batch_size
        self.prioritized_replay_beta = prioritized_replay_beta
        self.prioritized_replay_eps = prioritized_replay_eps

        self.replay_buffer = PrioritizedReplayBuffer(
            self.buffer_size,
            alpha=prioritized_replay_alpha,
            clip_rewards=clip_rewards)

        # Metrics
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.update_priorities_timer = TimerStat()

    def get_host(self):
        return os.uname()[1]

    def add_batch(self, batch):
        PolicyOptimizer._check_not_multiagent(batch)
        with self.add_batch_timer:
            for row in batch.rows():
                self.replay_buffer.add(row["obs"], row["actions"],
                                       row["rewards"], row["new_obs"],
                                       row["dones"], row["weights"])

    def replay(self):
        with self.replay_timer:
            if len(self.replay_buffer) < self.replay_starts:
                return None

            (obses_t, actions, rewards, obses_tp1, dones, weights,
             batch_indexes) = self.replay_buffer.sample(
                 self.train_batch_size, beta=self.prioritized_replay_beta)

            batch = SampleBatch({
                "obs": obses_t,
                "actions": actions,
                "rewards": rewards,
                "new_obs": obses_tp1,
                "dones": dones,
                "weights": weights,
                "batch_indexes": batch_indexes
            })
            return batch

    def update_priorities(self, batch_indexes, td_errors):
        with self.update_priorities_timer:
            new_priorities = (np.abs(td_errors) + self.prioritized_replay_eps)
            self.replay_buffer.update_priorities(batch_indexes, new_priorities)

    def stats(self):
        stat = {
            "add_batch_time_ms": round(1000 * self.add_batch_timer.mean, 3),
            "replay_time_ms": round(1000 * self.replay_timer.mean, 3),
            "update_priorities_time_ms": round(
                1000 * self.update_priorities_timer.mean, 3),
        }
        stat.update(self.replay_buffer.stats())
        return stat


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from replay data.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_evaluator):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False

    def run(self):
        while True:
            self.step()

    def step(self):
        with self.queue_timer:
            ra, replay = self.inqueue.get()
        if replay is not None:
            with self.grad_timer:
                td_error = self.local_evaluator.compute_apply(replay)[
                    "td_error"]
            self.outqueue.put((ra, replay, td_error, replay.count))
        self.learner_queue_size.push(self.inqueue.qsize())
        self.weights_updated = True


class AsyncReplayOptimizer(PolicyOptimizer):
    """Main event loop of the Ape-X optimizer (async sampling with replay).

    This class coordinates the data transfers between the learner thread,
    remote evaluators (Ape-X actors), and replay buffer actors.

    This optimizer requires that policy evaluators return an additional
    "td_error" array in the info return of compute_gradients(). This error
    term will be used for sample prioritization."""

    def _init(self,
              learning_starts=1000,
              buffer_size=10000,
              prioritized_replay=True,
              prioritized_replay_alpha=0.6,
              prioritized_replay_beta=0.4,
              prioritized_replay_eps=1e-6,
              train_batch_size=512,
              sample_batch_size=50,
              num_replay_buffer_shards=1,
              max_weight_sync_delay=400,
              clip_rewards=True,
              debug=False):

        self.debug = debug
        self.replay_starts = learning_starts
        self.prioritized_replay_beta = prioritized_replay_beta
        self.prioritized_replay_eps = prioritized_replay_eps
        self.max_weight_sync_delay = max_weight_sync_delay

        self.learner = LearnerThread(self.local_evaluator)
        self.learner.start()

        self.replay_actors = create_colocated(ReplayActor, [
            num_replay_buffer_shards, learning_starts, buffer_size,
            train_batch_size, prioritized_replay_alpha,
            prioritized_replay_beta, prioritized_replay_eps, clip_rewards
        ], num_replay_buffer_shards)

        # Stats
        self.timers = {
            k: TimerStat()
            for k in [
                "put_weights", "get_samples", "enqueue", "sample_processing",
                "replay_processing", "update_priorities", "train", "sample"
            ]
        }
        self.num_weight_syncs = 0
        self.learning_started = False

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Otherwise kick of replay tasks for local gradient updates
        self.replay_tasks = TaskPool()
        for ra in self.replay_actors:
            for _ in range(REPLAY_QUEUE_DEPTH):
                self.replay_tasks.add(ra, ra.replay.remote())

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        if self.remote_evaluators:
            self.set_evaluators(self.remote_evaluators)

    # For https://github.com/ray-project/ray/issues/2541 only
    def set_evaluators(self, remote_evaluators):
        self.remote_evaluators = remote_evaluators
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            self.steps_since_update[ev] = 0
            for _ in range(SAMPLE_QUEUE_DEPTH):
                self.sample_tasks.add(ev, ev.sample_with_count.remote())

    def step(self):
        assert len(self.remote_evaluators) > 0
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.timers["sample"].push(time_delta)
        self.timers["sample"].push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.learning_started = True
        if self.learning_started:
            self.timers["train"].push(time_delta)
            self.timers["train"].push_units_processed(train_timesteps)
        self.num_steps_sampled += sample_timesteps
        self.num_steps_trained += train_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        weights = None

        with self.timers["sample_processing"]:
            completed = list(self.sample_tasks.completed())
            counts = ray.get([c[1][1] for c in completed])
            for i, (ev, (sample_batch, count)) in enumerate(completed):
                sample_timesteps += counts[i]

                # Send the data to the replay buffer
                random.choice(
                    self.replay_actors).add_batch.remote(sample_batch)

                # Update weights if needed
                self.steps_since_update[ev] += counts[i]
                if self.steps_since_update[ev] >= self.max_weight_sync_delay:
                    # Note that it's important to pull new weights once
                    # updated to avoid excessive correlation between actors
                    if weights is None or self.learner.weights_updated:
                        self.learner.weights_updated = False
                        with self.timers["put_weights"]:
                            weights = ray.put(
                                self.local_evaluator.get_weights())
                    ev.set_weights.remote(weights)
                    self.num_weight_syncs += 1
                    self.steps_since_update[ev] = 0

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample_with_count.remote())

        with self.timers["replay_processing"]:
            for ra, replay in self.replay_tasks.completed():
                self.replay_tasks.add(ra, ra.replay.remote())
                with self.timers["get_samples"]:
                    samples = ray.get(replay)
                with self.timers["enqueue"]:
                    self.learner.inqueue.put((ra, samples))

        with self.timers["update_priorities"]:
            while not self.learner.outqueue.empty():
                ra, replay, td_error, count = self.learner.outqueue.get()
                ra.update_priorities.remote(replay["batch_indexes"], td_error)
                train_timesteps += count

        return sample_timesteps, train_timesteps

    def stats(self):
        replay_stats = ray.get(self.replay_actors[0].stats.remote())
        timing = {
            "{}_time_ms".format(k): round(1000 * self.timers[k].mean, 3)
            for k in self.timers
        }
        timing["learner_grad_time_ms"] = round(
            1000 * self.learner.grad_timer.mean, 3)
        timing["learner_dequeue_time_ms"] = round(
            1000 * self.learner.queue_timer.mean, 3)
        stats = {
            "sample_throughput": round(self.timers["sample"].mean_throughput,
                                       3),
            "train_throughput": round(self.timers["train"].mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
        }
        debug_stats = {
            "replay_shard_0": replay_stats,
            "timing_breakdown": timing,
            "pending_sample_tasks": self.sample_tasks.count,
            "pending_replay_tasks": self.replay_tasks.count,
            "learner_queue": self.learner.learner_queue_size.stats(),
        }
        if self.debug:
            stats.update(debug_stats)
        return dict(PolicyOptimizer.stats(self), **stats)
