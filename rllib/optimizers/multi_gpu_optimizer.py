import logging
import math
import numpy as np
from collections import defaultdict

import ray
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.optimizers.rollout import collect_samples
from ray.rllib.utils.annotations import override
from ray.rllib.utils.sgd import averaged
from ray.rllib.utils.timer import TimerStat
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)


class LocalMultiGPUOptimizer(PolicyOptimizer):
    """A synchronous optimizer that uses multiple local GPUs.

    Samples are pulled synchronously from multiple remote workers,
    concatenated, and then split across the memory of multiple local GPUs.
    A number of SGD passes are then taken over the in-memory data. For more
    details, see `multi_gpu_impl.LocalSyncParallelOptimizer`.

    This optimizer is Tensorflow-specific and requires the underlying
    Policy to be a TFPolicy instance that implements the `copy()` method
    for multi-GPU tower generation.

    Note that all replicas of the TFPolicy will merge their
    extra_compute_grad and apply_grad feed_dicts and fetches. This
    may result in unexpected behavior.
    """

    def __init__(self,
                 workers,
                 sgd_batch_size=128,
                 num_sgd_iter=10,
                 rollout_fragment_length=200,
                 num_envs_per_worker=1,
                 train_batch_size=1024,
                 num_gpus=0,
                 standardize_fields=[],
                 shuffle_sequences=True,
                 _fake_gpus=False):
        """Initialize a synchronous multi-gpu optimizer.

        Arguments:
            workers (WorkerSet): all workers
            sgd_batch_size (int): SGD minibatch size within train batch size
            num_sgd_iter (int): number of passes to learn on per train batch
            rollout_fragment_length (int): size of batches to sample from
                workers.
            num_envs_per_worker (int): num envs in each rollout worker
            train_batch_size (int): size of batches to learn on
            num_gpus (int): number of GPUs to use for data-parallel SGD
            standardize_fields (list): list of fields in the training batch
                to normalize
            shuffle_sequences (bool): whether to shuffle the train batch prior
                to SGD to break up correlations
            _fake_gpus (bool): Whether to use fake-GPUs (CPUs) instead of
                actual GPUs (should only be used for testing on non-GPU
                machines).
        """
        PolicyOptimizer.__init__(self, workers)

        self.batch_size = sgd_batch_size
        self.num_sgd_iter = num_sgd_iter
        self.num_envs_per_worker = num_envs_per_worker
        self.rollout_fragment_length = rollout_fragment_length
        self.train_batch_size = train_batch_size
        self.shuffle_sequences = shuffle_sequences

        # Collect actual devices to use.
        if not num_gpus:
            _fake_gpus = True
            num_gpus = 1
        type_ = "cpu" if _fake_gpus else "gpu"
        self.devices = [
            "/{}:{}".format(type_, i) for i in range(int(math.ceil(num_gpus)))
        ]

        self.batch_size = int(sgd_batch_size / len(self.devices)) * len(
            self.devices)
        assert self.batch_size % len(self.devices) == 0
        assert self.batch_size >= len(self.devices), "batch size too small"
        self.per_device_batch_size = int(self.batch_size / len(self.devices))
        self.sample_timer = TimerStat()
        self.load_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.update_weights_timer = TimerStat()
        self.standardize_fields = standardize_fields

        logger.info("LocalMultiGPUOptimizer devices {}".format(self.devices))

        self.policies = dict(self.workers.local_worker()
                             .foreach_trainable_policy(lambda p, i: (i, p)))
        logger.debug("Policies to train: {}".format(self.policies))
        for policy_id, policy in self.policies.items():
            if not isinstance(policy, TFPolicy):
                raise ValueError(
                    "Only TF graph policies are supported with multi-GPU. "
                    "Try setting `simple_optimizer=True` instead.")

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        self.optimizers = {}
        with self.workers.local_worker().tf_sess.graph.as_default():
            with self.workers.local_worker().tf_sess.as_default():
                for policy_id, policy in self.policies.items():
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

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.workers.remote_workers():
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.workers.remote_workers():
                samples = collect_samples(self.workers.remote_workers(),
                                          self.rollout_fragment_length,
                                          self.num_envs_per_worker,
                                          self.train_batch_size)
                if samples.count > self.train_batch_size * 2:
                    logger.info(
                        "Collected more training samples than expected "
                        "(actual={}, train_batch_size={}). ".format(
                            samples.count, self.train_batch_size) +
                        "This may be because you have many workers or "
                        "long episodes in 'complete_episodes' batch mode.")
            else:
                samples = []
                while sum(s.count for s in samples) < self.train_batch_size:
                    samples.append(self.workers.local_worker().sample())
                samples = SampleBatch.concat_samples(samples)

            # Handle everything as if multiagent
            if isinstance(samples, SampleBatch):
                samples = MultiAgentBatch({
                    DEFAULT_POLICY_ID: samples
                }, samples.count)

        for policy_id, policy in self.policies.items():
            if policy_id not in samples.policy_batches:
                continue

            batch = samples.policy_batches[policy_id]
            for field in self.standardize_fields:
                value = batch[field]
                standardized = (value - value.mean()) / max(1e-4, value.std())
                batch[field] = standardized

        num_loaded_tuples = {}
        with self.load_timer:
            for policy_id, batch in samples.policy_batches.items():
                if policy_id not in self.policies:
                    continue

                policy = self.policies[policy_id]
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

        fetches = {}
        with self.grad_timer:
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
                    logger.debug("{} {}".format(i,
                                                averaged(iter_extra_fetches)))
                fetches[policy_id] = averaged(iter_extra_fetches)

        self.num_steps_sampled += samples.count
        self.num_steps_trained += tuples_per_device * len(self.devices)
        self.learner_stats = fetches
        return fetches

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "load_time_ms": round(1000 * self.load_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "learner": self.learner_stats,
            })
