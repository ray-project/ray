from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import ray
from multiprocessing.pool import ThreadPool


@ray.remote(num_cpus=2)
class ShuffleActor(object):

    def __init__(self, partition_data):
        self.incoming = []
        self.partition_data = partition_data
        self.index_of_self = None

    def shuffle(self, index, partition_assignments, index_of_self, *list_of_partitions):
        assert(len(partition_assignments) == len(list(list_of_partitions)))
        self.index_of_self = index_of_self
        self.assign_and_send_data(index, partition_assignments, list(list_of_partitions))

    def assign_and_send_data(self, index, partition_assignments, list_of_partitions):
        num_partitions = len(partition_assignments)
        self.partition_data.index = index.index

        for i in range(num_partitions):
            if i == self.index_of_self:
                continue
            try:
                indices_to_send = [idx
                                   for idx in partition_assignments[i]
                                   if idx in index.index]
                data_to_send = \
                    self.partition_data.loc[indices_to_send]

                # raise ValueError(str(self.partition_data))
                self.partition_data = \
                    self.partition_data.drop(data_to_send.index)
                list_of_partitions[i].add_to_incoming.remote(data_to_send)
            except KeyError:
                pass

    def add_to_incoming(self, data):
        self.incoming.append(data)

    def apply_func(self, func, *args):
        self.incoming.append(self.partition_data)
        data = pd.concat(self.incoming)

        if len(args) == 0:
            return func(data)
        else:
            return func(data, *args)