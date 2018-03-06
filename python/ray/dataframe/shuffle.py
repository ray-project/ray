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

    def shuffle(self, partition_assignments, index_of_self, *list_of_partitions):
        assert(len(partition_assignments) == len(list(list_of_partitions)))
        self.index_of_self = index_of_self
        self.assign_and_send_data(partition_assignments, list(list_of_partitions))

    def assign_and_send_data(self, partition_assignments, list_of_partitions):
        num_partitions = len(partition_assignments)

        for i in range(num_partitions):
            if i == self.index_of_self:
                continue

            data_to_send = \
                self.partition_data[partition_assignments[i][partition_assignments[i]['partition'] == self.index_of_self]['index_within_partition']]
            self.partition_data.drop(
                data_to_send)
            list_of_partitions[i].add_to_incoming.remote(data_to_send)

    def add_to_incoming(self, data):
        self.incoming.append(data)

    def groupby(self):
        self.incoming.append(self.partition_data)
        data = pd.concat(self.incoming)
        return data.groupby(by=data.index)