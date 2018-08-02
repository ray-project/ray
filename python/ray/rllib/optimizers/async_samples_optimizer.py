"""Implements the IMPALA architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time
import threading

from six.moves import queue

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

SAMPLE_QUEUE_DEPTH = 2
LEARNER_QUEUE_MAX_SIZE = 16


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

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
        self.weights_updated = 0
        self.stats = {}

    def run(self):
        while True:
            self.step()

    def step(self):
        with self.queue_timer:
            ra, batch = self.inqueue.get()

        if batch is not None:
            with self.grad_timer:
                fetches = self.local_evaluator.compute_apply(batch)
                self.weights_updated += 1
                if "stats" in fetches:
                    self.stats = fetches["stats"]
            self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class AsyncSamplesOptimizer(PolicyOptimizer):
    """Main event loop of the IMPALA architecture.

    This class coordinates the data transfers between the learner thread
    and remote evaluators (IMPALA actors).
    """

    def _init(self, train_batch_size=512, sample_batch_size=50, debug=False):

        self.debug = debug
        self.learning_started = False
        self.train_batch_size = train_batch_size

        self.learner = LearnerThread(self.local_evaluator)
        self.learner.start()

        assert len(self.remote_evaluators) > 0

        # Stats
        self.timers = {
            k: TimerStat()
            for k in
            ["put_weights", "enqueue", "sample_processing", "train", "sample"]
        }
        self.num_weight_syncs = 0
        self.learning_started = False

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            for _ in range(SAMPLE_QUEUE_DEPTH):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

    def step(self):
        assert self.learner.is_alive()
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
            for ev, sample_batch in self.sample_tasks.completed_prefetch():
                sample_batch = ray.get(sample_batch)
                sample_timesteps += sample_batch.count
                self.batch_buffer.append(sample_batch)
                if sum(b.count
                       for b in self.batch_buffer) >= self.train_batch_size:
                    train_batch = self.batch_buffer[0].concat_samples(
                        self.batch_buffer)
                    with self.timers["enqueue"]:
                        self.learner.inqueue.put((ev, train_batch))
                    self.batch_buffer = []

                # Note that it's important to pull new weights once
                # updated to avoid excessive correlation between actors
                if weights is None or self.learner.weights_updated:
                    self.learner.weights_updated = False
                    with self.timers["put_weights"]:
                        weights = ray.put(self.local_evaluator.get_weights())
                ev.set_weights.remote(weights)
                self.num_weight_syncs += 1

                # Kick off another sample request
                self.sample_tasks.add(ev, ev.sample.remote())

        while not self.learner.outqueue.empty():
            count = self.learner.outqueue.get()
            train_timesteps += count

        return sample_timesteps, train_timesteps

    def stats(self):
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
            "timing_breakdown": timing,
            "pending_sample_tasks": self.sample_tasks.count,
            "learner_queue": self.learner.learner_queue_size.stats(),
        }
        if self.debug:
            stats.update(debug_stats)
        if self.learner.stats:
            stats["learner"] = self.learner.stats
        return dict(PolicyOptimizer.stats(self), **stats)
