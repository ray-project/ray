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
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStats

SAMPLE_QUEUE_DEPTH = 2
REPLAY_QUEUE_DEPTH = 4
LEARNER_QUEUE_MAX_SIZE = 16


class TaskQueue(object):
    def __init__(self, work_fn):
        self._tasks = {}
        self._completed = []
        self._work_fn = work_fn

    def add_worker(self, worker):
        obj_id = self._work_fn(worker)
        self._tasks[obj_id] = worker

    def completed(self, max_yield):
        pending = list(self._tasks)
        if pending:
            ready, _ = ray.wait(pending, num_returns=len(pending), timeout=10)
            for obj_id in ready:
                worker = self._tasks.pop(obj_id)
                self._completed.append((worker, obj_id))
                new_obj_id = self._work_fn(worker)
                self._tasks[new_obj_id] = worker
        i = 0
        while self._completed and i < max_yield:
            i += 1
            yield self._completed.pop(0)

    @property
    def count(self):
        return len(self._tasks)


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


class Learner(threading.Thread):
    def __init__(self, local_evaluator):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStats("size", 50)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=LEARNER_QUEUE_MAX_SIZE)
        self.outqueue = queue.Queue()
        self.daemon = True

    def run(self):
        while True:
            self.step()

    def step(self):
        ra, replay = self.inqueue.get()
        td_error = self.local_evaluator.compute_apply(replay)
        if td_error is not None:
            self.outqueue.put((ra, replay, td_error))
        self.learner_queue_size.push(self.inqueue.qsize())


def split_colocated(actors):
    localhost = os.uname()[1]
    hosts = ray.get([a.get_host.remote() for a in actors])
    local = []
    non_local = []
    for host, a in zip(hosts, actors):
        if host == localhost:
            local.append(a)
        else:
            non_local.append(a)
    return local, non_local


def try_create_colocated(cls, args, count):
    actors = [cls.remote(*args) for _ in range(count)]
    local, _ = split_colocated(actors)
    print("Got {} colocated actors of {}".format(len(local), count))
    return local


def create_colocated(cls, args, count):
    ok = []
    i = 1
    while len(ok) < count and i < 10:
        attempt = try_create_colocated(cls, args, count * i)
        ok.extend(attempt)
        i += 1
    if len(ok) < count:
        raise Exception("Unable to create enough colocated actors, abort.")
    return ok[:count]


class ApexOptimizer(Optimizer):

    def _init(self):
        self.learner = Learner(self.local_evaluator)
        self.learner.start()

        num_replay_actors = self.config["num_replay_buffer_shards"]
        self.replay_actors = create_colocated(
            ReplayActor, [self.config, num_replay_actors], num_replay_actors)
        assert len(self.remote_evaluators) > 0

        # Stats
        self.put_weights_timer = TimerStat()
        self.get_samples_timer = TimerStat()
        self.sample_processing = TimerStat()
        self.replay_processing_timer = TimerStat()
        self.update_priorities_timer = TimerStat()
        self.train_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_samples_added = 0
        self.num_samples_trained = 0

        # Number of worker steps since the last weight update
        self.steps_since_update = {}

        # Otherwise kick of replay tasks for local gradient updates
        self.replay_tasks = TaskQueue(lambda ev: ev.replay.remote())
        for ra in self.replay_actors:
            for _ in range(REPLAY_QUEUE_DEPTH):
                self.replay_tasks.add_worker(ra)

        # Kick off async background sampling
        self.sample_tasks = TaskQueue(lambda ev: ev.sample.remote())
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            self.steps_since_update[ev] = 0
            for _ in range(SAMPLE_QUEUE_DEPTH):
                self.sample_tasks.add_worker(ev)

    def step(self):
        start = time.time()
        sample_timesteps, train_timesteps = self._step()
        time_delta = time.time() - start
        self.sample_timer.push(time_delta)
        self.sample_timer.push_units_processed(sample_timesteps)
        if train_timesteps > 0:
            self.train_timer.push(time_delta)
            self.train_timer.push_units_processed(train_timesteps)
        self.num_samples_added += sample_timesteps
        self.num_samples_trained += train_timesteps
        return sample_timesteps

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        weights = None

        with self.sample_processing:
            for ev, sample_batch in self.sample_tasks.completed(max_yield=10):
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
                    self.steps_since_update[ev] = 0

        with self.replay_processing_timer:
            for ra, replay in self.replay_tasks.completed(max_yield=10):
                with self.get_samples_timer:
                    samples = ray.get(replay)
                self.learner.inqueue.put((ra, samples))

        with self.update_priorities_timer:
            while not self.learner.outqueue.empty():
                ra, replay, td_error = self.learner.outqueue.get()
                ra.update_priorities.remote(replay, td_error)
                train_timesteps += self.config["train_batch_size"]

        return sample_timesteps, train_timesteps

    def stats(self):
        replay_stats = ray.get(self.replay_actors[0].stats.remote())
        return {
            "replay_shard_0": replay_stats,
            "timing_breakdown": {
                "put_weights_time_ms": round(
                    1000 * self.put_weights_timer.mean, 3),
                "get_samples_time_ms": round(
                    1000 * self.get_samples_timer.mean, 3),
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
        }
