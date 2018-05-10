class PartitionIterator(object):
    def __init__(self, partitions, func):
        """PartitionIterator class to define a generator on partitioned data

        Args:
            partitions ([ObjectID]): Partitions to iterate over
            func (callable): The function to get inner iterables from
                each partition
        """
        self.partitions = partitions
        self.curr_partition = -1
        self.func = func
        self.iter_cache = iter([])

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        try:
            n = next(self.iter_cache)
            return n
        except StopIteration:
            self.curr_partition += 1
            if self.curr_partition < len(self.partitions):
                next_partition = self.partitions[self.curr_partition]
            else:
                raise StopIteration()

            self.iter_cache = self.func(next_partition, self.curr_partition)
            return self.next()
