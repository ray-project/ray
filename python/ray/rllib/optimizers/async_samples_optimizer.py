"""Implements the IMPALA asynchronous sampling architecture.

https://arxiv.org/abs/1802.01561"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time

from ray.rllib.optimizers.aso_aggregator import SimpleAggregator
from ray.rllib.optimizers.aso_tree_aggregator import TreeAggregator
from ray.rllib.optimizers.aso_learner import LearnerThread
from ray.rllib.optimizers.aso_multi_gpu_learner import TFMultiGPULearner
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat

logger = logging.getLogger(__name__)


class AsyncSamplesOptimizer(PolicyOptimizer):
    """Main event loop of the IMPALA architecture.

    This class coordinates the data transfers between the learner thread
    and remote workers (IMPALA actors).
    """

    def __init__(self,
                 workers,
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
                 learner_queue_timeout=300,
                 num_aggregation_workers=0,
                 _fake_gpus=False):
        PolicyOptimizer.__init__(self, workers)

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
                self.workers.local_worker(),
                lr=lr,
                num_gpus=num_gpus,
                train_batch_size=train_batch_size,
                num_data_loader_buffers=num_data_loader_buffers,
                minibatch_buffer_size=minibatch_buffer_size,
                num_sgd_iter=num_sgd_iter,
                learner_queue_size=learner_queue_size,
                learner_queue_timeout=learner_queue_timeout,
                _fake_gpus=_fake_gpus)
        else:
            self.learner = LearnerThread(
                self.workers.local_worker(),
                minibatch_buffer_size=minibatch_buffer_size,
                num_sgd_iter=num_sgd_iter,
                learner_queue_size=learner_queue_size,
                learner_queue_timeout=learner_queue_timeout)
        self.learner.start()

        # Stats
        self._optimizer_step_timer = TimerStat()
        self._stats_start_time = time.time()
        self._last_stats_time = {}

        if num_aggregation_workers > 0:
            self.aggregator = TreeAggregator(
                workers,
                num_aggregation_workers,
                replay_proportion=replay_proportion,
                max_sample_requests_in_flight_per_worker=(
                    max_sample_requests_in_flight_per_worker),
                replay_buffer_num_slots=replay_buffer_num_slots,
                train_batch_size=train_batch_size,
                sample_batch_size=sample_batch_size,
                broadcast_interval=broadcast_interval)
        else:
            self.aggregator = SimpleAggregator(
                workers,
                replay_proportion=replay_proportion,
                max_sample_requests_in_flight_per_worker=(
                    max_sample_requests_in_flight_per_worker),
                replay_buffer_num_slots=replay_buffer_num_slots,
                train_batch_size=train_batch_size,
                sample_batch_size=sample_batch_size,
                broadcast_interval=broadcast_interval)

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
        if len(self.workers.remote_workers()) == 0:
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
    def reset(self, remote_workers):
        self.workers.reset(remote_workers)
        self.aggregator.reset(remote_workers)

    @override(PolicyOptimizer)
    def stats(self):
        def timer_to_ms(timer):
            return round(1000 * timer.mean, 3)

        stats = self.aggregator.stats()
        stats.update(self.get_mean_stats_and_reset())
        stats["timing_breakdown"] = {
            "optimizer_step_time_ms": timer_to_ms(self._optimizer_step_timer),
            "learner_grad_time_ms": timer_to_ms(self.learner.grad_timer),
            "learner_load_time_ms": timer_to_ms(self.learner.load_timer),
            "learner_load_wait_time_ms": timer_to_ms(
                self.learner.load_wait_timer),
            "learner_dequeue_time_ms": timer_to_ms(self.learner.queue_timer),
        }
        stats["learner_queue"] = self.learner.learner_queue_size.stats()
        if self.learner.stats:
            stats["learner"] = self.learner.stats
        return dict(PolicyOptimizer.stats(self), **stats)

    def _step(self):
        sample_timesteps, train_timesteps = 0, 0

        for train_batch in self.aggregator.iter_train_batches():
            sample_timesteps += train_batch.count
            self.learner.inqueue.put(train_batch)
            if (self.learner.weights_updated
                    and self.aggregator.should_broadcast()):
                self.aggregator.broadcast_new_weights()

        while not self.learner.outqueue.empty():
            count = self.learner.outqueue.get()
            train_timesteps += count

        return sample_timesteps, train_timesteps
