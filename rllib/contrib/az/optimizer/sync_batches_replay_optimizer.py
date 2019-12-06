import random

from ray.rllib.evaluation.metrics import get_learner_stats
from ray.rllib.optimizers.sync_batch_replay_optimizer import \
    SyncBatchReplayOptimizer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override


class SyncBatchesReplayOptimizer(SyncBatchReplayOptimizer):
    def __init__(self,
                 workers,
                 learning_starts=1000,
                 buffer_size=10000,
                 train_batch_size=32,
                 num_gradient_descents=10):
        super(SyncBatchesReplayOptimizer, self).__init__(
            workers, learning_starts, buffer_size, train_batch_size)
        self.num_sgds = num_gradient_descents

    @override(SyncBatchReplayOptimizer)
    def _optimize(self):
        for _ in range(self.num_sgds):
            samples = [random.choice(self.replay_buffer)]
            while sum(s.count for s in samples) < self.train_batch_size:
                samples.append(random.choice(self.replay_buffer))
            samples = SampleBatch.concat_samples(samples)
            with self.grad_timer:
                info_dict = self.workers.local_worker().learn_on_batch(samples)
                for policy_id, info in info_dict.items():
                    self.learner_stats[policy_id] = get_learner_stats(info)
                self.grad_timer.push_units_processed(samples.count)
            self.num_steps_trained += samples.count
        return info_dict
