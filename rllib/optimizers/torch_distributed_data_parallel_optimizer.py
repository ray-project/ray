import logging
import random
from collections import defaultdict

import ray
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.optimizers.multi_gpu_optimizer import _averaged
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.memory import ray_get_and_free

logger = logging.getLogger(__name__)


class TorchDistributedDataParallelOptimizer(PolicyOptimizer):
    """EXPERIMENTAL: torch distributed multi-node SGD."""

    def __init__(self,
                 workers,
                 num_sgd_iter=1,
                 train_batch_size=1,
                 sgd_minibatch_size=0,
                 standardize_fields=frozenset([])):
        PolicyOptimizer.__init__(self, workers)
        self.learner_stats = {}
        assert self.workers.remote_workers()

        # Setup the distributed processes.
        ip = ray.get(workers.remote_workers()[0].get_node_ip.remote())
        port = ray.get(workers.remote_workers()[0].find_free_port.remote())
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)
        logger.info(
            "Creating torch process group with leader {}".format(address))

        # Get setup tasks in order to throw errors on failure.
        ray.get([
            worker.setup_torch_data_parallel.remote(
                address, i, len(workers.remote_workers()))
            for i, worker in enumerate(workers.remote_workers())
        ])
        logger.info("Torch process group init completed")

    @override(PolicyOptimizer)
    def step(self):
        results = ray.get([
            w.sample_and_learn.remote() for w in self.workers.remote_workers()
        ])
        self.learner_stats = results[0][LEARNER_STATS_KEY]
        return self.learner_stats

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "learner": self.learner_stats,
            })
