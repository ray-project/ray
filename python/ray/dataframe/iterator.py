from collections import Iterator


class PartitionIterator(Iterator):
    def __init__(self, partitions, func):
        """PartitionIterator class to define a generator on partitioned data

        Args:
            partitions ([ObjectID]): Partitions to iterate over
            func (callable): The function to get inner iterables from
                each partition
        """
        self.partitions = iter(partitions)
        self.func = func
        self.iter_cache = iter([])

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        try:
            return next(self.iter_cache)
        except StopIteration:
            next_partition = next(self.partitions)
            self.iter_cache = self.func(next_partition)
            return self.next()
