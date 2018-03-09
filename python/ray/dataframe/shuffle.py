from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd
import ray
from threading import Thread


@ray.remote(num_cpus=2)
class ShuffleActor(object):

    def __init__(self, partition_data, partition_axis=0, shuffle_axis=0):
        """Actor for facilitating distributed dataframe shuffle
        operations. Each partition in a Ray DataFrame will have be wrapped
        by a ShuffleActor, and during a shuffle, a collection of
        ShuffleActors will shuffle data onto each other together.

        Args:
            partition_data (ObjectID): The ObjectID of the partition this
                ShuffleActor is wrapped around.
            partition_axis (int): The axis on which the data was partitioned.
            shuffle_axis (int): The axis to index on for the shuffle.
        """
        if partition_axis != 0 and partition_axis != 1:
            raise TypeError('partition_axis must be 0 or 1. Got %s' % str(self.partition_axis))
        if shuffle_axis != 0 and shuffle_axis != 1:
            raise TypeError('shuffle_axis must be 0 or 1. Got %s' % str(self.shuffle_axis))
        self.incoming = []
        self.partition_data = partition_data
        self.index_of_self = None
        self.partition_axis = partition_axis
        self.shuffle_axis = shuffle_axis

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
                i: 
                    The index within the list of Threads generated.
                indices_to_send: 
                    The indices containing data to send.
                data_to_send: 
                    An empty list to fill with the destination data for a given index (i).
            """

            indices_to_send[i] = [idx
                                  for idx in partition_assignments[i]
                                  if idx in index]

            if self.shuffle_axis:
                data_to_send[i] = \
                    self.partition_data.loc[:, indices_to_send[i]]
            else:
                data_to_send[i] = \
                    self.partition_data.loc[indices_to_send[i], :]

        # Support both RangeIndex and pandas.Index
        if isinstance(index, pd.DataFrame):
            index = index.index

        num_partitions = len(partition_assignments)
        # Reindexing here to properly drop the data.
        if self.partition_axis:
            self.partition_data.index = index
        else:
            self.partition_data.columns = index

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

        # Append data to other new partitions' ShuffleActor's `add_to_incoming` lists
        for i in range(num_partitions):
            if i == self.index_of_self:
                continue
            try:
                list_of_partitions[i].add_to_incoming.remote((
                    self.index_of_self, data_to_send[i]))
                # Drop the data once it's been sent.
                self.partition_data.drop(indices_to_send[i], inplace=True,
                                         axis=self.shuffle_axis)
            except KeyError:
                pass

        # Returning here to guarantee scheduling consistency.
        return None

    def add_to_incoming(self, data):
        """Add data to the list of data to be coalesced into. Note that `self.incoming` is a 
        list of Pandas DataFrames, which will eventually all be concatenated together.

        Args:
            data (pd.DataFrame): A DataFrame containing the rows to be coalesced.
        """

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
        _, self.incoming = zip(*self.incoming)
        data = pd.concat(self.incoming, axis=(self.partition_axis))

        if len(args) == 0:
            return func(data)
        else:
            return func(data, *args)
