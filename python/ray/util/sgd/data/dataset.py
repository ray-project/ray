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
            par_iter = par_iter.for_each_concur(download_func,
                max_concur=max_concur)
            # par_iter = par_iter.for_each(download_func)
        self.iter = par_iter.batch(batch_size)

        self.batch_size = batch_size
        self.max_concur = max_concur
        self.transform = transform

    def set_num_shards(self, num_shards):
        if num_shards != self.iter.num_shards():
            print("Setting num shards", num_shards)
            self.iter = self.iter.repartition(num_shards)

    def get_shard(self, i):
        assert i < self.iter.num_shards(), \
            "Trying to get shard {} but there are only {} shards." + \
            "Are you sure you called set_num_shards already?".format(
                i, self.iter.num_shards()
            )

        return self.iter.get_shard(i)
