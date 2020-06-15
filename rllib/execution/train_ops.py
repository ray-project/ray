from collections import defaultdict
import logging
import numpy as np
import math
from typing import List

import ray
from ray.rllib.evaluation.metrics import get_learner_stats, LEARNER_STATS_KEY
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import SampleBatchType, \
    STEPS_SAMPLED_COUNTER, STEPS_TRAINED_COUNTER, LEARNER_INFO, \
    APPLY_GRADS_TIMER, COMPUTE_GRADS_TIMER, WORKER_UPDATE_TIMER, \
    LEARN_ON_BATCH_TIMER, LOAD_BATCH_TIMER, LAST_TARGET_UPDATE_TS, \
    NUM_TARGET_UPDATES, _get_global_vars, _check_sample_batch_type, \
    _get_shared_metrics
from ray.rllib.execution.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.policy.policy import PolicyID
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.sgd import do_minibatch_sgd, averaged

tf = try_import_tf()

logger = logging.getLogger(__name__)


class TrainOneStep:
    """Callable that improves the policy and updates workers.

    This should be used with the .for_each() operator. A tuple of the input
    and learner stats will be returned.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> train_op = rollouts.for_each(TrainOneStep(workers))
        >>> print(next(train_op))  # This trains the policy on one batch.
        SampleBatch(...), {"learner_stats": ...}

    Updates the STEPS_TRAINED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    def __init__(self,
                 workers: WorkerSet,
                 policies: List[PolicyID] = frozenset([]),
                 num_sgd_iter: int = 1,
                 sgd_minibatch_size: int = 0):
        self.workers = workers
        self.policies = policies or workers.local_worker().policies_to_train
        self.num_sgd_iter = num_sgd_iter
        self.sgd_minibatch_size = sgd_minibatch_size

    def __call__(self,
                 batch: SampleBatchType) -> (SampleBatchType, List[dict]):
        _check_sample_batch_type(batch)
        metrics = _get_shared_metrics()
        learn_timer = metrics.timers[LEARN_ON_BATCH_TIMER]
        with learn_timer:
            if self.num_sgd_iter > 1 or self.sgd_minibatch_size > 0:
                w = self.workers.local_worker()
                info = do_minibatch_sgd(
                    batch, {p: w.get_policy(p)
                            for p in self.policies}, w, self.num_sgd_iter,
                    self.sgd_minibatch_size, [])
                # TODO(ekl) shouldn't be returning learner stats directly here
                metrics.info[LEARNER_INFO] = info
            else:
                info = self.workers.local_worker().learn_on_batch(batch)
                metrics.info[LEARNER_INFO] = get_learner_stats(info)
            learn_timer.push_units_processed(batch.count)
        metrics.counters[STEPS_TRAINED_COUNTER] += batch.count
        if self.workers.remote_workers():
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = ray.put(self.workers.local_worker().get_weights(
                    self.policies))
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())
        return batch, info


class TrainTFMultiGPU:
    """TF Multi-GPU version of TrainOneStep.

    This should be used with the .for_each() operator. A tuple of the input
    and learner stats will be returned.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> train_op = rollouts.for_each(TrainMultiGPU(workers, ...))
        >>> print(next(train_op))  # This trains the policy on one batch.
        SampleBatch(...), {"learner_stats": ...}

    Updates the STEPS_TRAINED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    def __init__(self,
                 workers: WorkerSet,
                 sgd_minibatch_size: int,
                 num_sgd_iter: int,
                 num_gpus: int,
                 rollout_fragment_length: int,
                 num_envs_per_worker: int,
                 train_batch_size: int,
                 shuffle_sequences: bool,
                 policies: List[PolicyID] = frozenset([]),
                 _fake_gpus: bool = False):
        self.workers = workers
        self.policies = policies or workers.local_worker().policies_to_train
        self.num_sgd_iter = num_sgd_iter
        self.sgd_minibatch_size = sgd_minibatch_size
        self.shuffle_sequences = shuffle_sequences

        # Collect actual devices to use.
        if not num_gpus:
            _fake_gpus = True
            num_gpus = 1
        type_ = "cpu" if _fake_gpus else "gpu"
        self.devices = [
            "/{}:{}".format(type_, i) for i in range(int(math.ceil(num_gpus)))
        ]

        self.batch_size = int(sgd_minibatch_size / len(self.devices)) * len(
            self.devices)
        assert self.batch_size % len(self.devices) == 0
        assert self.batch_size >= len(self.devices), "batch size too small"
        self.per_device_batch_size = int(self.batch_size / len(self.devices))

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        self.optimizers = {}
        with self.workers.local_worker().tf_sess.graph.as_default():
            with self.workers.local_worker().tf_sess.as_default():
                for policy_id in self.policies:
                    policy = self.workers.local_worker().get_policy(policy_id)
                    with tf.variable_scope(policy_id, reuse=tf.AUTO_REUSE):
                        if policy._state_inputs:
                            rnn_inputs = policy._state_inputs + [
                                policy._seq_lens
                            ]
                        else:
                            rnn_inputs = []
                        self.optimizers[policy_id] = (
                            LocalSyncParallelOptimizer(
                                policy._optimizer, self.devices,
                                [v
                                 for _, v in policy._loss_inputs], rnn_inputs,
                                self.per_device_batch_size, policy.copy))

                self.sess = self.workers.local_worker().tf_sess
                self.sess.run(tf.global_variables_initializer())

    def __call__(self,
                 samples: SampleBatchType) -> (SampleBatchType, List[dict]):
        _check_sample_batch_type(samples)

        # Handle everything as if multiagent
        if isinstance(samples, SampleBatch):
            samples = MultiAgentBatch({
                DEFAULT_POLICY_ID: samples
            }, samples.count)

        metrics = _get_shared_metrics()
        load_timer = metrics.timers[LOAD_BATCH_TIMER]
        learn_timer = metrics.timers[LEARN_ON_BATCH_TIMER]
        with load_timer:
            # (1) Load data into GPUs.
            num_loaded_tuples = {}
            for policy_id, batch in samples.policy_batches.items():
                if policy_id not in self.policies:
                    continue

                policy = self.workers.local_worker().get_policy(policy_id)
                policy._debug_vars()
                tuples = policy._get_loss_inputs_dict(
                    batch, shuffle=self.shuffle_sequences)
                data_keys = [ph for _, ph in policy._loss_inputs]
                if policy._state_inputs:
                    state_keys = policy._state_inputs + [policy._seq_lens]
                else:
                    state_keys = []
                num_loaded_tuples[policy_id] = (
                    self.optimizers[policy_id].load_data(
                        self.sess, [tuples[k] for k in data_keys],
                        [tuples[k] for k in state_keys]))

        with learn_timer:
            # (2) Execute minibatch SGD on loaded data.
            fetches = {}
            for policy_id, tuples_per_device in num_loaded_tuples.items():
                optimizer = self.optimizers[policy_id]
                num_batches = max(
                    1,
                    int(tuples_per_device) // int(self.per_device_batch_size))
                logger.debug("== sgd epochs for {} ==".format(policy_id))
                for i in range(self.num_sgd_iter):
                    iter_extra_fetches = defaultdict(list)
                    permutation = np.random.permutation(num_batches)
                    for batch_index in range(num_batches):
                        batch_fetches = optimizer.optimize(
                            self.sess, permutation[batch_index] *
                            self.per_device_batch_size)
                        for k, v in batch_fetches[LEARNER_STATS_KEY].items():
                            iter_extra_fetches[k].append(v)
                    if logger.getEffectiveLevel() <= logging.DEBUG:
                        avg = averaged(iter_extra_fetches)
                        logger.debug("{} {}".format(i, avg))
                fetches[policy_id] = averaged(iter_extra_fetches, axis=0)

        load_timer.push_units_processed(samples.count)
        learn_timer.push_units_processed(samples.count)

        metrics.counters[STEPS_TRAINED_COUNTER] += samples.count
        metrics.info[LEARNER_INFO] = fetches
        if self.workers.remote_workers():
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = ray.put(self.workers.local_worker().get_weights(
                    self.policies))
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())
        return samples, fetches


class ComputeGradients:
    """Callable that computes gradients with respect to the policy loss.

    This should be used with the .for_each() operator.

    Examples:
        >>> grads_op = rollouts.for_each(ComputeGradients(workers))
        >>> print(next(grads_op))
        {"var_0": ..., ...}, 50  # grads, batch count

    Updates the LEARNER_INFO info field in the local iterator context.
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, samples: SampleBatchType):
        _check_sample_batch_type(samples)
        metrics = _get_shared_metrics()
        with metrics.timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(samples)
        metrics.info[LEARNER_INFO] = get_learner_stats(info)
        return grad, samples.count


class ApplyGradients:
    """Callable that applies gradients and updates workers.

    This should be used with the .for_each() operator.

    Examples:
        >>> apply_op = grads_op.for_each(ApplyGradients(workers))
        >>> print(next(apply_op))
        None

    Updates the STEPS_TRAINED_COUNTER counter in the local iterator context.
    """

    def __init__(self,
                 workers,
                 policies: List[PolicyID] = frozenset([]),
                 update_all=True):
        """Creates an ApplyGradients instance.

        Arguments:
            workers (WorkerSet): workers to apply gradients to.
            update_all (bool): If true, updates all workers. Otherwise, only
                update the worker that produced the sample batch we are
                currently processing (i.e., A3C style).
        """
        self.workers = workers
        self.policies = policies or workers.local_worker().policies_to_train
        self.update_all = update_all

    def __call__(self, item):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(
                "Input must be a tuple of (grad_dict, count), got {}".format(
                    item))
        gradients, count = item
        metrics = _get_shared_metrics()
        metrics.counters[STEPS_TRAINED_COUNTER] += count

        apply_timer = metrics.timers[APPLY_GRADS_TIMER]
        with apply_timer:
            self.workers.local_worker().apply_gradients(gradients)
            apply_timer.push_units_processed(count)

        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())

        if self.update_all:
            if self.workers.remote_workers():
                with metrics.timers[WORKER_UPDATE_TIMER]:
                    weights = ray.put(self.workers.local_worker().get_weights(
                        self.policies))
                    for e in self.workers.remote_workers():
                        e.set_weights.remote(weights, _get_global_vars())
        else:
            if metrics.current_actor is None:
                raise ValueError(
                    "Could not find actor to update. When "
                    "update_all=False, `current_actor` must be set "
                    "in the iterator context.")
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = self.workers.local_worker().get_weights(
                    self.policies)
                metrics.current_actor.set_weights.remote(
                    weights, _get_global_vars())


class AverageGradients:
    """Callable that averages the gradients in a batch.

    This should be used with the .for_each() operator after a set of gradients
    have been batched with .batch().

    Examples:
        >>> batched_grads = grads_op.batch(32)
        >>> avg_grads = batched_grads.for_each(AverageGradients())
        >>> print(next(avg_grads))
        {"var_0": ..., ...}, 1600  # averaged grads, summed batch count
    """

    def __call__(self, gradients):
        acc = None
        sum_count = 0
        for grad, count in gradients:
            if acc is None:
                acc = grad
            else:
                acc = [a + b for a, b in zip(acc, grad)]
            sum_count += count
        logger.info("Computing average of {} microbatch gradients "
                    "({} samples total)".format(len(gradients), sum_count))
        return acc, sum_count


class UpdateTargetNetwork:
    """Periodically call policy.update_target() on all trainable policies.

    This should be used with the .for_each() operator after training step
    has been taken.

    Examples:
        >>> train_op = ParallelRollouts(...).for_each(TrainOneStep(...))
        >>> update_op = train_op.for_each(
        ...     UpdateTargetIfNeeded(workers, target_update_freq=500))
        >>> print(next(update_op))
        None

    Updates the LAST_TARGET_UPDATE_TS and NUM_TARGET_UPDATES counters in the
    local iterator context. The value of the last update counter is used to
    track when we should update the target next.
    """

    def __init__(self,
                 workers,
                 target_update_freq,
                 by_steps_trained=False,
                 policies=frozenset([])):
        self.workers = workers
        self.target_update_freq = target_update_freq
        self.policies = (policies or workers.local_worker().policies_to_train)
        if by_steps_trained:
            self.metric = STEPS_TRAINED_COUNTER
        else:
            self.metric = STEPS_SAMPLED_COUNTER

    def __call__(self, _):
        metrics = _get_shared_metrics()
        cur_ts = metrics.counters[self.metric]
        last_update = metrics.counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update > self.target_update_freq:
            to_update = self.policies
            self.workers.local_worker().foreach_trainable_policy(
                lambda p, p_id: p_id in to_update and p.update_target())
            metrics.counters[NUM_TARGET_UPDATES] += 1
            metrics.counters[LAST_TARGET_UPDATE_TS] = cur_ts
