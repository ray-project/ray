import ray
from ray.util.iter import ParallelIterator


class Dataset():
    def __init__(self,
                 iterable,
                 batch_size=2,
                 download_func=None,
                 max_concur=0,
                 transform=None):
        par_iter = None
        if isinstance(iterable, ParallelIterator):
            par_iter = iterable
        else:
            par_iter = ParallelIterator.from_items(iterable)
        if download_func:
            # par_iter = par_iter.for_each_concur(download_func, max_concur=max_concur)
            par_iter = par_iter.for_each(download_func)
        self.iter = par_iter.batch(batch_size)

        self.batch_size = batch_size
        self.max_concur = max_concur
        self.transform = transform

        # TODO: Remove after Parallel Iterator bug is fixed
        self._materialized = {}

    def set_num_shards(self, num_shards):
        if num_shards != self.iter.num_shards():
            print("Setting num shards", num_shards)
            self.iter = self.iter.repartition(num_shards)

            # TODO: Remove after Parallel Iterator bug is fixed
            self._materialized = {}

    def get_shard(self, i):
        assert i < self.iter.num_shards(
        ), "Trying to get shard {} but there are only {} shards. Are you sure you called set_num_shards already".format(
            i, self.iter.num_shards())
        # iter = self.iter.get_shard(i, max_async_requests=self.max_concur)

        # TODO: Remove after Parallel Iterator bug is fixed
        if i not in self._materialized:
            it = self.iter.get_shard(i)
            if self.transform:
                it = it.for_each(lambda x: (self.transform(x[0]), x[1]))
            self._materialized[i] = list(it)

        return iter(self._materialized[i])
