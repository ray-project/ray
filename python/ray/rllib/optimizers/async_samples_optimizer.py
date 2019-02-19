"""Implements the IMPALA asynchronous sampling architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import numpy as np
import random
import time

import ray
from ray.rllib.optimizers.aso_learner import LearnerThread
from ray.rllib.optimizers.aso_multi_gpu_learner import TFMultiGPULearner
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.actors import TaskPool
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat

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

        assert len(self.remote_evaluators) > 0

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
