import numpy as np

import ray
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.filter import MeanStdFilter


class _MockWorker:
    def __init__(self, sample_count=10):
        self._weights = np.array([-10, -10, -10, -10])
        self._grad = np.array([1, 1, 1, 1])
        self._sample_count = sample_count
        self.obs_filter = MeanStdFilter(())
        self.rew_filter = MeanStdFilter(())
        self.filters = {"obs_filter": self.obs_filter, "rew_filter": self.rew_filter}

    def sample(self):
        samples_dict = {"observations": [], "rewards": []}
        for i in range(self._sample_count):
            samples_dict["observations"].append(self.obs_filter(np.random.randn()))
            samples_dict["rewards"].append(self.rew_filter(np.random.randn()))
        return SampleBatch(samples_dict)

    def compute_gradients(self, samples):
        return self._grad * samples.count, {"batch_count": samples.count}

    def apply_gradients(self, grads):
        self._weights += self._grad

    def get_weights(self):
        return self._weights

    def set_weights(self, weights):
        self._weights = weights

    def get_filters(self, flush_after=False):
        obs_filter = self.obs_filter.copy()
        rew_filter = self.rew_filter.copy()
        if flush_after:
            self.obs_filter.reset_buffer(), self.rew_filter.reset_buffer()

        return {"obs_filter": obs_filter, "rew_filter": rew_filter}

    def sync_filters(self, new_filters):
        assert all(k in new_filters for k in self.filters)
        for k in self.filters:
            self.filters[k].sync(new_filters[k])

    def apply(self, fn):
        return fn(self)


class _MockWorkerSet(WorkerSet):
    def __init__(self, num_mock_workers):
        super().__init__(local_worker=False, _setup=False)
        self.add_workers(num_workers=num_mock_workers, validate=False)

    def _make_worker(self, *args, **kwargs):
        RemoteWorker = ray.remote(_MockWorker)
        return RemoteWorker.remote(sample_count=10)
