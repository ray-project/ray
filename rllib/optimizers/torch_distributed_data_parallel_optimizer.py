import logging

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.timer import TimerStat

logger = logging.getLogger(__name__)


class TorchDistributedDataParallelOptimizer(PolicyOptimizer):
    """EXPERIMENTAL: torch distributed multi-node SGD."""

    def __init__(self,
                 workers,
                 expected_batch_size,
                 num_sgd_iter=1,
                 sgd_minibatch_size=0,
                 standardize_fields=frozenset([]),
                 keep_local_weights_in_sync=True,
                 backend="gloo"):
        PolicyOptimizer.__init__(self, workers)
        self.learner_stats = {}
        self.num_sgd_iter = num_sgd_iter
        self.expected_batch_size = expected_batch_size
        self.sgd_minibatch_size = sgd_minibatch_size
        self.standardize_fields = standardize_fields
        self.keep_local_weights_in_sync = keep_local_weights_in_sync
        self.sync_down_timer = TimerStat()
        self.sync_up_timer = TimerStat()
        self.learn_timer = TimerStat()

        # Setup the distributed processes.
        if not self.workers.remote_workers():
            raise ValueError("This optimizer requires >0 remote workers.")
        ip = ray.get(workers.remote_workers()[0].get_node_ip.remote())
        port = ray.get(workers.remote_workers()[0].find_free_port.remote())
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        logger.info(
            "Creating torch process group with leader {}".format(address))

        # Get setup tasks in order to throw errors on failure.
        ray.get([
            worker.setup_torch_data_parallel.remote(
                address, i, len(workers.remote_workers()), backend)
            for i, worker in enumerate(workers.remote_workers())
        ])
        logger.info("Torch process group init completed")

    @override(PolicyOptimizer)
    def step(self):
        # Sync up the weights. In principle we don't need this, but it doesn't
        # add too much overhead and handles the case where the user manually
        # updates the local weights.
        if self.keep_local_weights_in_sync:
            with self.sync_up_timer:
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights)

        with self.learn_timer:
            results = ray.get([
                w.sample_and_learn.remote(
                    self.expected_batch_size, self.num_sgd_iter,
                    self.sgd_minibatch_size, self.standardize_fields)
                for w in self.workers.remote_workers()
            ])
        for info, count in results:
            self.num_steps_sampled += count
            self.num_steps_trained += count
        self.learner_stats = results[0][0]

        # In debug mode, check the allreduce successfully synced the weights.
        if logger.isEnabledFor(logging.DEBUG):
            weights = ray.get([
                w.get_weights.remote() for w in self.workers.remote_workers()
            ])
            sums = []
            for w in weights:
                acc = 0
                for p in w.values():
                    for k, v in p.items():
                        acc += v.sum()
                sums.append(float(acc))
            logger.debug("The worker weight sums are {}".format(sums))
            assert len(set(sums)) == 1, sums

        # Sync down the weights. As with the sync up, this is not really
        # needed unless the user is reading the local weights.
        if self.keep_local_weights_in_sync:
            with self.sync_down_timer:
                self.workers.local_worker().set_weights(
                    ray.get(
                        self.workers.remote_workers()[0].get_weights.remote()))

        return self.learner_stats

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sync_weights_up_time": round(1000 * self.sync_up_timer.mean,
                                              3),
                "sync_weights_down_time": round(
                    1000 * self.sync_down_timer.mean, 3),
                "learn_time_ms": round(1000 * self.learn_timer.mean, 3),
                "learner": self.learner_stats,
            })
