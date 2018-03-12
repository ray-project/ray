from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import ray
from threading import Thread


@ray.remote(num_cpus=2)
class ShuffleActor(object):

    def __init__(self, partition_data):
        self.incoming = []
        self.partition_data = partition_data
        self.index_of_self = None

    def shuffle(self, index, partition_assignments, index_of_self,
                *list_of_partitions):
        """Shuffle the data between ShuffleActors given partition assignments.

        Args:

            index: The pd.Index object being used to shuffle the data.
            partition_assignments: A pd.DataFrame object of assignments.
            index_of_self: The integer index of this ShuffleActor in the
                list_of_partitions object.
            list_of_partitions: The list of all ShuffleActor objects being
                used.

        Returns:
            A value to be used with ray.get() to ensure proper scheduling
            order.
        """
        self.index_of_self = index_of_self
        return self.assign_and_send_data(index,
                                         partition_assignments,
                                         list(list_of_partitions))

    def assign_and_send_data(self, index, partition_assignments,
                             list_of_partitions):
        """Sends data to the necessary ShuffleActors based on assignments.

        Args:
            index:
            partition_assignments:
            list_of_partitions:

        Returns:
             A value to be used with ray.get() to ensure proper scheduling
             order.
        """

        def calc_send(i, indices_to_send, data_to_send):
            """Separates the data to send into 

            :param i:
            :param indices_to_send:
            :param data_to_send:
            :return:
            """
            indices_to_send[i] = [idx
                                  for idx in partition_assignments[i]
                                  if idx in index.index]
            data_to_send[i] = self.partition_data.loc[indices_to_send[i]]

        num_partitions = len(partition_assignments)
        self.partition_data.index = index.index

        indices_to_send = [None] * num_partitions
        data_to_send = [None] * num_partitions

        threads = [Thread(target=calc_send, args=(i, indices_to_send,
                                                  data_to_send))
                   for i in range(num_partitions)]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

        for i in range(num_partitions):
            if i == self.index_of_self:
                continue
            try:
                list_of_partitions[i].add_to_incoming.remote((
                    self.index_of_self, data_to_send[i]))

                self.partition_data.drop(indices_to_send[i], inplace=True)
            except KeyError:
                pass

        # Returning here to guarantee scheduling consistency.
        return None

    def add_to_incoming(self, data):
        self.incoming.append(data)

    def apply_func(self, func, *args):
        self.incoming.append((self.index_of_self, self.partition_data))
        self.incoming.sort(key=lambda x: x[0])
        self.incoming = [x[1] for x in self.incoming]
        data = pd.concat(self.incoming, axis=1)

        if len(args) == 0:
            return func(data)
        else:
            return func(data, *args)
