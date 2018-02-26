from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import queue
import random
import time
import threading

import numpy as np

import ray
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.actors import TaskPool, create_colocated
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStats

SAMPLE_QUEUE_DEPTH = 2
REPLAY_QUEUE_DEPTH = 4
LEARNER_QUEUE_MAX_SIZE = 16


@ray.remote
class ReplayActor(object):
    def __init__(self, config, num_shards):
        self.config = config
        self.replay_starts = self.config["learning_starts"] // num_shards
        self.buffer_size = self.config["buffer_size"] // num_shards
        if self.config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                self.buffer_size,
                alpha=self.config["prioritized_replay_alpha"])
        else:
            self.replay_buffer = ReplayBuffer(self.buffer_size)

        # Metrics
        self.add_batch_timer = TimerStat()
        self.replay_timer = TimerStat()
        self.update_priorities_timer = TimerStat()

    def get_host(self):
        return os.uname()[1]

    def add_batch(self, batch):
        with self.add_batch_timer:
            for row in batch.rows():
                self.replay_buffer.add(
                    row["obs"], row["actions"], row["rewards"], row["new_obs"],
                    row["dones"], row["weights"])

    def replay(self):
        with self.replay_timer:
            if len(self.replay_buffer) < self.replay_starts:
                return None

            if self.config["prioritized_replay"]:
                (obses_t, actions, rewards, obses_tp1,
                    dones, weights, batch_indexes) = self.replay_buffer.sample(
                        self.config["train_batch_size"],
                        beta=self.config["prioritized_replay_beta"])
            else:
                (obses_t, actions, rewards, obses_tp1,
                    dones) = self.replay_buffer.sample(
                        self.config["train_batch_size"])
                weights = np.ones_like(rewards)
                batch_indexes = - np.ones_like(rewards)

            batch = SampleBatch({
                "obs": obses_t, "actions": actions, "rewards": rewards,
                "new_obs": obses_tp1, "dones": dones, "weights": weights,
                "batch_indexes": batch_indexes})
            return batch

    def update_priorities(self, batch, td_errors):
        with self.update_priorities_timer:
            if self.config["prioritized_replay"]:
                new_priorities = (
                    np.abs(td_errors) + self.config["prioritized_replay_eps"])
                self.replay_buffer.update_priorities(
                    batch["batch_indexes"], new_priorities)

    def stats(self):
        stat = {
            "add_batch_time_ms": round(
                1000 * self.add_batch_timer.mean, 3),
            "replay_time_ms": round(
                1000 * self.replay_timer.mean, 3),
            "update_priorities_time_ms": round(
                1000 * self.update_priorities_timer.mean, 3),
        }
        stat.update(self.replay_buffer.stats())
        return stat


class GenericLearner(threading.Thread):
    def __init__(self, local_evaluator):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStats("size", 50)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.daemon = True

    def run(self):
        while True:
            self.step()

    def step(self):
        with self.queue_timer:
            ra, replay = self.inqueue.get()
        if replay is not None:
            with self.grad_timer:
                td_error = self.local_evaluator.compute_apply(replay)
            self.outqueue.put((ra, replay, td_error))
        self.learner_queue_size.push(self.inqueue.qsize())


class ApexOptimizer(Optimizer):

    def _init(self):
        self.learner = GenericLearner(self.local_evaluator)
        self.learner.start()

        num_replay_actors = self.config["num_replay_buffer_shards"]
        self.replay_actors = create_colocated(
            ReplayActor, [self.config, num_replay_actors], num_replay_actors)
        assert len(self.remote_evaluators) > 0

        # Stats
        self.put_weights_timer = TimerStat()
        self.get_samples_timer = TimerStat()
        self.enqueue_timer = TimerStat()
        self.remote_call_timer = TimerStat()
        self.sample_processing = TimerStat()
        self.replay_processing_timer = TimerStat()
        self.update_priorities_timer = TimerStat()
        self.samples_per_loop = WindowStats("samples_per_loop", 10)
        self.replays_per_loop = WindowStats("replays_per_loop", 10)
        self.reprios_per_loop = WindowStats("reprios_per_loop", 10)
        self.reweights_per_loop = WindowStats("reweights_per_loop", 10)
        self.train_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_samples_added = 0
        self.num_samples_trained = 0
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
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            self.steps_since_update[ev] = 0
            for _ in range(SAMPLE_QUEUE_DEPTH):
                self.sample_tasks.add(ev, ev.sample.remote())

    def step(self):
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.sample_timer.push(time_delta)
        self.sample_timer.push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.learning_started = True
        if self.learning_started:
            self.train_timer.push(time_delta)
            self.train_timer.push_units_processed(train_timesteps)
        self.num_samples_added += sample_timesteps
        self.num_samples_trained += train_timesteps
        return sample_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        weights = None

        with self.sample_processing:
            i = 0
            num_weight_syncs = 0
            for ev, sample_batch in self.sample_tasks.completed():
                i += 1
                sample_timesteps += self.config["sample_batch_size"]

                # Send the data to the replay buffer
                random.choice(self.replay_actors).add_batch.remote(
                    sample_batch)

                # Update weights if needed
                self.steps_since_update[ev] += self.config["sample_batch_size"]
                if (self.steps_since_update[ev] >=
                        self.config["max_weight_sync_delay"]):
                    if weights is None:
                        with self.put_weights_timer:
                            weights = ray.put(
                                self.local_evaluator.get_weights())
                    ev.set_weights.remote(weights)
                    self.num_weight_syncs += 1
                    num_weight_syncs += 1
                    self.steps_since_update[ev] = 0

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample.remote())
            self.samples_per_loop.push(i)
            self.reweights_per_loop.push(num_weight_syncs)

        with self.replay_processing_timer:
            i = 0
            for ra, replay in self.replay_tasks.completed():
                i += 1
                with self.remote_call_timer:
                    self.replay_tasks.add(ra, ra.replay.remote())
                with self.get_samples_timer:
                    samples = ray.get(replay)
                with self.enqueue_timer:
                    self.learner.inqueue.put((ra, samples))
            self.replays_per_loop.push(i)

        with self.update_priorities_timer:
            i = 0
            while not self.learner.outqueue.empty():
                i += 1
                ra, replay, td_error = self.learner.outqueue.get()
                ra.update_priorities.remote(replay, td_error)
                train_timesteps += self.config["train_batch_size"]
            self.reprios_per_loop.push(i)

        return sample_timesteps, train_timesteps

    def stats(self):
        replay_stats = ray.get(self.replay_actors[0].stats.remote())
        stats = {
            "replay_shard_0": replay_stats,
            "timing_breakdown": {
                "put_weights_time_ms": round(
                    1000 * self.put_weights_timer.mean, 3),
                "remote_call_time_ms": round(
                    1000 * self.remote_call_timer.mean, 3),
                "get_samples_time_ms": round(
                    1000 * self.get_samples_timer.mean, 3),
                "enqueue_time_ms": round(
                    1000 * self.enqueue_timer.mean, 3),
                "grad_queue_time_ms": round(
                    1000 * self.learner.queue_timer.mean, 3),
                "grad_time_ms": round(
                    1000 * self.learner.grad_timer.mean, 3),
                "1_sample_processing_time_ms": round(
                    1000 * self.sample_processing.mean, 3),
                "2_replay_processing_time_ms": round(
                    1000 * self.replay_processing_timer.mean, 3),
                "3_update_priorities_time_ms": round(
                    1000 * self.update_priorities_timer.mean, 3),
            },
            "sample_throughput": round(self.sample_timer.mean_throughput, 3),
            "train_throughput": round(self.train_timer.mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
            "num_samples_trained": self.num_samples_trained,
            "pending_sample_tasks": self.sample_tasks.count,
            "pending_replay_tasks": self.replay_tasks.count,
            "learner_queue": self.learner.learner_queue_size.stats(),
            "samples": self.samples_per_loop.stats(),
            "replays": self.replays_per_loop.stats(),
            "reprios": self.reprios_per_loop.stats(),
            "reweights": self.reweights_per_loop.stats(),
        }
        return stats
