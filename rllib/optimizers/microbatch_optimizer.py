import logging

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat

logger = logging.getLogger(__name__)


class MicrobatchOptimizer(PolicyOptimizer):
    """A microbatching synchronous RL optimizer.

    This optimizer pulls sample batches from workers until the target
    microbatch size is reached. Then, it computes and accumulates the policy
    gradient in a local buffer. This process is repeated until the number of
    samples collected equals the train batch size. Then, an accumulated
    gradient update is made.

    This allows for training with effective batch sizes much larger than can
    fit in GPU or host memory.
    """

    def __init__(self, workers, train_batch_size=10000, microbatch_size=1000):
        PolicyOptimizer.__init__(self, workers)

        if train_batch_size <= microbatch_size:
            raise ValueError(
                "The microbatch size must be smaller than the train batch "
                "size, got {} vs {}".format(microbatch_size, train_batch_size))

        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()
        self.train_batch_size = train_batch_size
        self.microbatch_size = microbatch_size
        self.learner_stats = {}
        self.policies = dict(self.workers.local_worker()
                             .foreach_trainable_policy(lambda p, i: (i, p)))
        logger.debug("Policies to train: {}".format(self.policies))

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.workers.remote_workers():
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights)

        fetches = {}
        accumulated_gradients = {}
        samples_so_far = 0

        # Accumulate minibatches.
        i = 0
        while samples_so_far < self.train_batch_size:
            i += 1
            with self.sample_timer:
                samples = []
                while sum(s.count for s in samples) < self.microbatch_size:
                    if self.workers.remote_workers():
                        samples.extend(
                            ray.get([
                                e.sample.remote()
                                for e in self.workers.remote_workers()
                            ]))
                    else:
                        samples.append(self.workers.local_worker().sample())
                samples = SampleBatch.concat_samples(samples)
                self.sample_timer.push_units_processed(samples.count)
                samples_so_far += samples.count

            logger.info(
                "Computing gradients for microbatch {} ({}/{} samples)".format(
                    i, samples_so_far, self.train_batch_size))

            # Handle everything as if multiagent
            if isinstance(samples, SampleBatch):
                samples = MultiAgentBatch({
                    DEFAULT_POLICY_ID: samples
                }, samples.count)

            with self.grad_timer:
                for policy_id, policy in self.policies.items():
                    if policy_id not in samples.policy_batches:
                        continue
                    batch = samples.policy_batches[policy_id]
                    grad_out, info_out = (
                        self.workers.local_worker().compute_gradients(
                            MultiAgentBatch({
                                policy_id: batch
                            }, batch.count)))
                    grad = grad_out[policy_id]
                    fetches.update(info_out)
                    if policy_id not in accumulated_gradients:
                        accumulated_gradients[policy_id] = grad
                    else:
                        grad_size = len(accumulated_gradients[policy_id])
                        assert grad_size == len(grad), (grad_size, len(grad))
                        c = []
                        for a, b in zip(accumulated_gradients[policy_id],
                                        grad):
                            c.append(a + b)
                        accumulated_gradients[policy_id] = c
            self.grad_timer.push_units_processed(samples.count)

        # Apply the accumulated gradient
        logger.info("Applying accumulated gradients ({} samples)".format(
            samples_so_far))
        self.workers.local_worker().apply_gradients(accumulated_gradients)

        if len(fetches) == 1 and DEFAULT_POLICY_ID in fetches:
            self.learner_stats = fetches[DEFAULT_POLICY_ID]
        else:
            self.learner_stats = fetches
        self.num_steps_sampled += samples_so_far
        self.num_steps_trained += samples_so_far
        return self.learner_stats

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "opt_peak_throughput": round(self.grad_timer.mean_throughput,
                                             3),
                "sample_peak_throughput": round(
                    self.sample_timer.mean_throughput, 3),
                "opt_samples": round(self.grad_timer.mean_units_processed, 3),
                "learner": self.learner_stats,
            })
