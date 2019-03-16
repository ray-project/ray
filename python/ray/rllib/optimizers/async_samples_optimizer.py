"""Implements the IMPALA architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import random
import time
import threading

from six.moves import queue

import ray
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.window_stat import WindowStat

logger = logging.getLogger(__name__)

NUM_DATA_LOAD_THREADS = 16


class AsyncSamplesOptimizer(PolicyOptimizer):
    """Main event loop of the IMPALA architecture.

    This class coordinates the data transfers between the learner thread
    and remote evaluators (IMPALA actors).
    """

    @override(PolicyOptimizer)
    def _init(self,
              train_batch_size=500,
              sample_batch_size=50,
              num_envs_per_worker=1,
              num_gpus=0,
              lr=0.0005,
              replay_buffer_num_slots=0,
              replay_proportion=0.0,
              num_data_loader_buffers=1,
              max_sample_requests_in_flight_per_worker=2,
              broadcast_interval=1,
              num_sgd_iter=1,
              minibatch_buffer_size=1,
              learner_queue_size=16,
              _fake_gpus=False):
        self.train_batch_size = train_batch_size
        self.sample_batch_size = sample_batch_size
        self.broadcast_interval = broadcast_interval

        self._stats_start_time = time.time()
        self._last_stats_time = {}
        self._last_stats_sum = {}

        if num_gpus > 1 or num_data_loader_buffers > 1:
            logger.info(
                "Enabling multi-GPU mode, {} GPUs, {} parallel loaders".format(
                    num_gpus, num_data_loader_buffers))
            if num_data_loader_buffers < minibatch_buffer_size:
                raise ValueError(
                    "In multi-gpu mode you must have at least as many "
                    "parallel data loader buffers as minibatch buffers: "
                    "{} vs {}".format(num_data_loader_buffers,
                                      minibatch_buffer_size))
            self.learner = TFMultiGPULearner(
                self.local_evaluator,
                lr=lr,
                num_gpus=num_gpus,
                train_batch_size=train_batch_size,
                num_data_loader_buffers=num_data_loader_buffers,
                minibatch_buffer_size=minibatch_buffer_size,
                num_sgd_iter=num_sgd_iter,
                learner_queue_size=learner_queue_size,
                _fake_gpus=_fake_gpus)
        else:
            self.learner = LearnerThread(self.local_evaluator,
                                         minibatch_buffer_size, num_sgd_iter,
                                         learner_queue_size)
        self.learner.start()

        # Stats
        self._optimizer_step_timer = TimerStat()
        self.num_weight_syncs = 0
        self.num_replayed = 0
        self._stats_start_time = time.time()
        self._last_stats_time = {}
        self._last_stats_val = {}

        # Kick off async background sampling
        self.sample_tasks = TaskPool()
        weights = self.local_evaluator.get_weights()
        for ev in self.remote_evaluators:
            ev.set_weights.remote(weights)
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

    def add_stat_val(self, key, val):
        if key not in self._last_stats_sum:
            self._last_stats_sum[key] = 0
            self._last_stats_time[key] = self._stats_start_time
        self._last_stats_sum[key] += val

    def get_mean_stats_and_reset(self):
        now = time.time()
        mean_stats = {
            key: round(val / (now - self._last_stats_time[key]), 3)
            for key, val in self._last_stats_sum.items()
        }

        for key in self._last_stats_sum.keys():
            self._last_stats_sum[key] = 0
            self._last_stats_time[key] = time.time()

        return mean_stats

    @override(PolicyOptimizer)
    def step(self):
        if len(self.remote_evaluators) == 0:
            raise ValueError("Config num_workers=0 means training will hang!")
        assert self.learner.is_alive()
        with self._optimizer_step_timer:
            sample_timesteps, train_timesteps = self._step()

        if sample_timesteps > 0:
            self.add_stat_val("sample_throughput", sample_timesteps)
        if train_timesteps > 0:
            self.add_stat_val("train_throughput", train_timesteps)

        self.num_steps_sampled += sample_timesteps
        self.num_steps_trained += train_timesteps

    @override(PolicyOptimizer)
    def stop(self):
        self.learner.stopped = True

    @override(PolicyOptimizer)
    def reset(self, remote_evaluators):
        self.remote_evaluators = remote_evaluators
        self.sample_tasks.reset_evaluators(remote_evaluators)

    @override(PolicyOptimizer)
    def stats(self):
        def timer_to_ms(timer):
            return round(1000 * timer.mean, 3)

        timing = {
            "optimizer_step_time_ms": timer_to_ms(self._optimizer_step_timer),
            "learner_grad_time_ms": timer_to_ms(self.learner.grad_timer),
            "learner_load_time_ms": timer_to_ms(self.learner.load_timer),
            "learner_load_wait_time_ms": timer_to_ms(
                self.learner.load_wait_timer),
            "learner_dequeue_time_ms": timer_to_ms(self.learner.queue_timer),
        }
        stats = dict({
            "num_weight_syncs": self.num_weight_syncs,
            "num_steps_replayed": self.num_replayed,
            "timing_breakdown": timing,
            "learner_queue": self.learner.learner_queue_size.stats(),
        }, **self.get_mean_stats_and_reset())
        self._last_stats_val.clear()
        if self.learner.stats:
            stats["learner"] = self.learner.stats
        return dict(PolicyOptimizer.stats(self), **stats)

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0
        num_sent = 0
        weights = None

        for ev, sample_batch in self._augment_with_replay(
                self.sample_tasks.completed_prefetch()):
            self.batch_buffer.append(sample_batch)
            if sum(b.count
                   for b in self.batch_buffer) >= self.train_batch_size:
                train_batch = self.batch_buffer[0].concat_samples(
                    self.batch_buffer)
                self.learner.inqueue.put(train_batch)
                self.batch_buffer = []

            # If the batch was replayed, skip the update below.
            if ev is None:
                continue

            sample_timesteps += sample_batch.count

            # Put in replay buffer if enabled
            if self.replay_buffer_num_slots > 0:
                self.replay_batches.append(sample_batch)
                if len(self.replay_batches) > self.replay_buffer_num_slots:
                    self.replay_batches.pop(0)

            # Note that it's important to pull new weights once
            # updated to avoid excessive correlation between actors
            if weights is None or (self.learner.weights_updated
                                   and num_sent >= self.broadcast_interval):
                self.learner.weights_updated = False
                weights = ray.put(self.local_evaluator.get_weights())
                num_sent = 0
            ev.set_weights.remote(weights)
            self.num_weight_syncs += 1
            num_sent += 1

            # Kick off another sample request
            self.sample_tasks.add(ev, ev.sample.remote())

        while not self.learner.outqueue.empty():
            count = self.learner.outqueue.get()
            train_timesteps += count

        return sample_timesteps, train_timesteps

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


class LearnerThread(threading.Thread):
    """Background thread that updates the local model from sample trajectories.

    The learner thread communicates with the main thread through Queues. This
    is needed since Ray operations can only be run on the main thread. In
    addition, moving heavyweight gradient ops session runs off the main thread
    improves overall throughput.
    """

    def __init__(self, local_evaluator, minibatch_buffer_size, num_sgd_iter,
                 learner_queue_size):
        threading.Thread.__init__(self)
        self.learner_queue_size = WindowStat("size", 50)
        self.local_evaluator = local_evaluator
        self.inqueue = queue.Queue(maxsize=learner_queue_size)
        self.outqueue = queue.Queue()
        self.minibatch_buffer = MinibatchBuffer(
            self.inqueue, minibatch_buffer_size, num_sgd_iter)
        self.queue_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.load_timer = TimerStat()
        self.load_wait_timer = TimerStat()
        self.daemon = True
        self.weights_updated = False
        self.stats = {}
        self.stopped = False

    def run(self):
        while not self.stopped:
            self.step()

    def step(self):
        with self.queue_timer:
            batch, _ = self.minibatch_buffer.get()

        with self.grad_timer:
            fetches = self.local_evaluator.learn_on_batch(batch)
            self.weights_updated = True
            self.stats = fetches.get("stats", {})

        self.outqueue.put(batch.count)
        self.learner_queue_size.push(self.inqueue.qsize())


class TFMultiGPULearner(LearnerThread):
    """Learner that can use multiple GPUs and parallel loading."""

    def __init__(self,
                 local_evaluator,
                 num_gpus=1,
                 lr=0.0005,
                 train_batch_size=500,
                 num_data_loader_buffers=1,
                 minibatch_buffer_size=1,
                 num_sgd_iter=1,
                 learner_queue_size=16,
                 _fake_gpus=False):
        # Multi-GPU requires TensorFlow to function.
        import tensorflow as tf

        LearnerThread.__init__(self, local_evaluator, minibatch_buffer_size,
                               num_sgd_iter, learner_queue_size)
        self.lr = lr
        self.train_batch_size = train_batch_size
        if not num_gpus:
            self.devices = ["/cpu:0"]
        elif _fake_gpus:
            self.devices = ["/cpu:{}".format(i) for i in range(num_gpus)]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(num_gpus)]
        logger.info("TFMultiGPULearner devices {}".format(self.devices))
        assert self.train_batch_size % len(self.devices) == 0
        assert self.train_batch_size >= len(self.devices), "batch too small"

        if set(self.local_evaluator.policy_map.keys()) != {"default"}:
            raise NotImplementedError("Multi-gpu mode for multi-agent")
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
                    for _ in range(num_data_loader_buffers):
                        self.par_opt.append(
                            LocalSyncParallelOptimizer(
                                adam,
                                self.devices,
                                [v for _, v in self.policy._loss_inputs],
                                rnn_inputs,
                                999999,  # it will get rounded down
                                self.policy.copy))

                self.sess = self.local_evaluator.tf_sess
                self.sess.run(tf.global_variables_initializer())

        self.idle_optimizers = queue.Queue()
        self.ready_optimizers = queue.Queue()
        for opt in self.par_opt:
            self.idle_optimizers.put(opt)
        for i in range(NUM_DATA_LOAD_THREADS):
            self.loader_thread = _LoaderThread(self, share_stats=(i == 0))
            self.loader_thread.start()

        self.minibatch_buffer = MinibatchBuffer(
            self.ready_optimizers, minibatch_buffer_size, num_sgd_iter)

    @override(LearnerThread)
    def step(self):
        assert self.loader_thread.is_alive()
        with self.load_wait_timer:
            opt, released = self.minibatch_buffer.get()
            if released:
                self.idle_optimizers.put(opt)

        with self.grad_timer:
            fetches = opt.optimize(self.sess, 0)
            self.weights_updated = True
            self.stats = fetches.get("stats", {})

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
            self._step()

    def _step(self):
        s = self.learner
        with self.queue_timer:
            batch = s.inqueue.get()

        opt = s.idle_optimizers.get()

        with self.load_timer:
            tuples = s.policy._get_loss_inputs_dict(batch)
            data_keys = [ph for _, ph in s.policy._loss_inputs]
            if s.policy._state_inputs:
                state_keys = s.policy._state_inputs + [s.policy._seq_lens]
            else:
                state_keys = []
            opt.load_data(s.sess, [tuples[k] for k in data_keys],
                          [tuples[k] for k in state_keys])

        s.ready_optimizers.put(opt)


class MinibatchBuffer(object):
    """Ring buffer of recent data batches for minibatch SGD."""

    def __init__(self, inqueue, size, num_passes):
        """Initialize a minibatch buffer.

        Arguments:
           inqueue: Queue to populate the internal ring buffer from.
           size: Max number of data items to buffer.
           num_passes: Max num times each data item should be emitted.
       """
        self.inqueue = inqueue
        self.size = size
        self.max_ttl = num_passes
        self.cur_max_ttl = 1  # ramp up slowly to better mix the input data
        self.buffers = [None] * size
        self.ttl = [0] * size
        self.idx = 0

    def get(self):
        """Get a new batch from the internal ring buffer.

        Returns:
           buf: Data item saved from inqueue.
           released: True if the item is now removed from the ring buffer.
       """
        if self.ttl[self.idx] <= 0:
            self.buffers[self.idx] = self.inqueue.get()
            self.ttl[self.idx] = self.cur_max_ttl
            if self.cur_max_ttl < self.max_ttl:
                self.cur_max_ttl += 1
        buf = self.buffers[self.idx]
        self.ttl[self.idx] -= 1
        released = self.ttl[self.idx] <= 0
        if released:
            self.buffers[self.idx] = None
        self.idx = (self.idx + 1) % len(self.buffers)
        return buf, released
