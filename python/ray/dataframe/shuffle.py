from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import ray
from threading import Thread


@ray.remote(num_cpus=2)
class ShuffleActor(object):

    def __init__(self, partition_data, axis=1):
        self.incoming = []
        self.partition_data = partition_data
        self.index_of_self = None
        self.axis = axis

    def shuffle(self, index, partition_assignments, index_of_self,
                *list_of_partitions):
        """Shuffle the data between ShuffleActors given partition assignments.

        Args:

            index: The pd.Index object being used to shuffle the data.
            partition_assignments: A list of pd.Index objects of assignments.
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
            """Separates the data to send into a list based on assignments.

            Args:
                i: The index within the list of Threads generated.
                indices_to_send: The indices containing data to send.
                data_to_send: An empty list to fill with the destination data
                    for a given index (i).
            """

            indices_to_send[i] = [idx
                                  for idx in partition_assignments[i]
                                  if idx in index.index]
            data_to_send[i] = self.partition_data.loc[indices_to_send[i]]

        num_partitions = len(partition_assignments)
        # Reindexing here to properly drop the data.
        self.partition_data.index = index.index

        indices_to_send = [None] * num_partitions
        data_to_send = [None] * num_partitions

        # Fill the above two lists with relevant data.
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
                # Drop the data once it's been sent.
                self.partition_data.drop(indices_to_send[i], inplace=True)
            except KeyError:
                pass

        # Returning here to guarantee scheduling consistency.
        return None

    def add_to_incoming(self, data):
        self.incoming.append(data)

    def apply_func(self, func, *args):
        """Perform some post-processing on the result of the shuffle.

        Note: This function guarantees the order of data is preserved as it was
        sent i.e. Data sent from partition 0 will come before partition 1..n,
        etc.

        Args:
            func: A function to perform on the data.
            args: The args to the function

        Returns:
            The result of the function on the data.
        """
        self.incoming.append((self.index_of_self, self.partition_data))
        # x[0] in the lambda is the partition index, which was sent in the call
        # to shuffle.
        self.incoming.sort(key=lambda x: x[0])
        # After we drop the partition indices we use pd.concat with the axis
        # provided in the constructor.
        self.incoming = [x[1] for x in self.incoming]
        data = pd.concat(self.incoming, axis=self.axis)

        if len(args) == 0:
            return func(data)
        else:
            return func(data, *args)
