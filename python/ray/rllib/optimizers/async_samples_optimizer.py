"""Implements the IMPALA architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import random
import time
import threading

from six.moves import queue

import ray
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

LEARNER_QUEUE_MAX_SIZE = 16
NUM_DATA_LOAD_THREADS = 16


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
        self.load_timer = TimerStat()
        self.load_wait_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stats = {}

    def run(self):
        while True:
            self.step()

    def step(self):
        with self.queue_timer:
            batch = self.inqueue.get()

        with self.grad_timer:
            fetches = self.local_evaluator.compute_apply(batch)
            self.weights_updated = True
            if "stats" in fetches:
                self.stats = fetches["stats"]

        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class TFMultiGPULearner(LearnerThread):
    def __init__(self,
                 local_evaluator,
                 num_gpus=2,
                 lr=0.0005,
                 train_batch_size=500,
                 grad_clip=40,
                 gpu_queue_size=1):
        import tensorflow as tf

        LearnerThread.__init__(self, local_evaluator)
        self.lr = lr
        self.train_batch_size = train_batch_size
        if not num_gpus:
            self.devices = ["/cpu:0"]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(num_gpus)]
            print("TFMultiGPULearner devices", self.devices)
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"
        self.per_device_batch_size = int(
            self.train_batch_size / len(self.devices))
        self.policy = self.local_evaluator.policy_map["default"]

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        self.par_opt = []
        with self.local_evaluator.tf_sess.graph.as_default():
            with self.local_evaluator.tf_sess.as_default():
                with tf.variable_scope("default", reuse=tf.AUTO_REUSE):
                    if self.policy._state_inputs:
                        rnn_inputs = self.policy._state_inputs + [
                            self.policy._seq_lens
                        ]
                    else:
                        rnn_inputs = []
                    adam = tf.train.AdamOptimizer(self.lr)
                    for _ in range(gpu_queue_size):
                        self.par_opt.append(
                            LocalSyncParallelOptimizer(
                                adam,
                                self.devices,
                                [v for _, v in self.policy.loss_inputs()],
                                rnn_inputs,
                                self.per_device_batch_size,
                                self.policy.copy,
                                os.getcwd(),
                                grad_norm_clipping=grad_clip))

                self.sess = self.local_evaluator.tf_sess
                self.sess.run(tf.global_variables_initializer())

        self.idle_optimizers = queue.Queue()
        self.ready_optimizers = queue.Queue()
        for opt in self.par_opt:
            self.idle_optimizers.put(opt)
        for i in range(NUM_DATA_LOAD_THREADS):
            self.loader_thread = _LoaderThread(self, share_stats=(i == 0))
            self.loader_thread.start()

    def step(self):
        assert self.loader_thread.is_alive()
        with self.load_wait_timer:
            opt = self.ready_optimizers.get()

        with self.grad_timer:
            fetches = opt.optimize(self.sess, 0)
            self.weights_updated = True
            if "stats" in fetches:
                self.stats = fetches["stats"]

        self.idle_optimizers.put(opt)
        self.outqueue.put(self.train_batch_size)
        self.learner_queue_size.push(self.inqueue.qsize())


class _LoaderThread(threading.Thread):
    def __init__(self, learner, share_stats):
        threading.Thread.__init__(self)
        self.learner = learner
        self.daemon = True
        if share_stats:
            self.queue_timer = learner.queue_timer
            self.load_timer = learner.load_timer
        else:
            self.queue_timer = TimerStat()
            self.load_timer = TimerStat()

    def run(self):
        while True:
            self.step()

    def step(self):
        l = self.learner
        with self.queue_timer:
            batch = l.inqueue.get()
            assert batch.count == l.train_batch_size

        opt = l.idle_optimizers.get()

        with self.load_timer:
            tuples = l.policy._get_loss_inputs_dict(batch)
            data_keys = [ph for _, ph in l.policy.loss_inputs()]
            if l.policy._state_inputs:
                state_keys = l.policy._state_inputs + [l.policy._seq_lens]
            else:
                state_keys = []
            tuples_per_device = opt.load_data(l.sess,
                                              [tuples[k] for k in data_keys],
                                              [tuples[k] for k in state_keys])
            assert int(tuples_per_device) == int(l.per_device_batch_size)

        l.ready_optimizers.put(opt)


class AsyncSamplesOptimizer(PolicyOptimizer):
    """Main event loop of the IMPALA architecture.

    This class coordinates the data transfers between the learner thread
    and remote evaluators (IMPALA actors).
    """

    def _init(self,
              train_batch_size=500,
              sample_batch_size=50,
              num_envs_per_worker=1,
              num_gpus=0,
              lr=0.0005,
              debug=False,
              grad_clip=40,
              replay_batch_slots=0,
              replay_proportion=0.0,
              gpu_queue_size=1,
              sample_queue_depth=2):
        self.debug = debug
        self.learning_started = False
        self.train_batch_size = train_batch_size
        self.sample_batch_size = sample_batch_size

        if num_gpus > 1 or gpu_queue_size > 1:
            if train_batch_size // num_gpus % (
                    sample_batch_size // num_envs_per_worker) != 0:
                raise ValueError(
                    "Sample batches must evenly divide across GPUs.")
            self.learner = TFMultiGPULearner(
                self.local_evaluator,
                lr=lr,
                num_gpus=num_gpus,
                train_batch_size=train_batch_size,
                grad_clip=grad_clip,
                gpu_queue_size=gpu_queue_size)
        else:
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
        self.num_replayed = 0
        self.learning_started = False

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
            for _ in range(sample_queue_depth):
                self.sample_tasks.add(ev, ev.sample.remote())

        self.batch_buffer = []

        assert not replay_proportion or replay_batch_slots > 0
        assert not replay_batch_slots or \
            replay_batch_slots * sample_batch_size > train_batch_size
        self.replay_proportion = replay_proportion
        self.replay_batch_slots = replay_batch_slots
        self.replay_batches = []

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

    def _replay_as_needed(self):
        num_needed = self.train_batch_size // self.sample_batch_size + 1
        if len(self.replay_batches) <= num_needed:
            return
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            samples = np.random.choice(
                self.replay_batches, num_needed, replace=False)
            train_batch = samples[0].concat_samples(samples)
            self.learner.inqueue.put(train_batch)
            self.num_replayed += train_batch.count

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
                        self.learner.inqueue.put(train_batch)
                    self.batch_buffer = []

                    # Replay with configured probability
                    self._replay_as_needed()

                # Put in replay buffer if enabled
                if self.replay_batch_slots > 0:
                    self.replay_batches.append(sample_batch)
                    if len(self.replay_batches) > self.replay_batch_slots:
                        self.replay_batches.pop(0)

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
        timing["learner_load_time_ms"] = round(
            1000 * self.learner.load_timer.mean, 3)
        timing["learner_load_wait_time_ms"] = round(
            1000 * self.learner.load_wait_timer.mean, 3)
        timing["learner_dequeue_time_ms"] = round(
            1000 * self.learner.queue_timer.mean, 3)
        stats = {
            "sample_throughput": round(self.timers["sample"].mean_throughput,
                                       3),
            "train_throughput": round(self.timers["train"].mean_throughput, 3),
            "num_weight_syncs": self.num_weight_syncs,
            "num_replays": self.num_replayed,
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
