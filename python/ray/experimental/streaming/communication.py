from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import hashlib

from ray.experimental.slib.operator import PStrategy
from ray.experimental.slib.batched_queue import BatchedQueue


def _hash(value):
    return int(hashlib.sha1(value.encode('ascii')).hexdigest(), 16)


# A data channel is a batched queue between two operator instances in a streaming environment
class DataChannel(object):
    """A data channel for actor-to-actor communication.

     Attributes:
         env (Environment): The environment the channel belongs to.
         src_operator_id (UUID): The id of the source operator of the channel.
         dst_operator_id (UUID): The id of the destination operator of the channel.
         src_instance_id (int): The id of the source instance.
         dst_instance_id (int): The id of the destination instance.
         queue (BatchedQueue): The batched queue used for data movement.
    """

    def __init__(self, env, src_operator_id, dst_operator_id, src_instance_id,
                 dst_instance_id):
        self.env = env
        self.src_operator_id = src_operator_id
        self.dst_operator_id = dst_operator_id
        self.src_instance_id = src_instance_id
        self.dst_instance_id = dst_instance_id
        self.queue = BatchedQueue(
            max_size=self.env.config.queue_config.max_size,
            max_batch_size=self.env.config.queue_config.max_batch_size,
            max_batch_time=self.env.config.queue_config.max_batch_time,
            prefetch_depth=self.env.config.queue_config.prefetch_depth,
            background_flush=self.env.config.queue_config.background_flush)

    def __repr__(self):
        return "({},{},{},{})".format(
            self.src_operator_id, self.dst_operator_id, self.src_instance_id,
            self.dst_instance_id)


# Pulls and merges data from multiple input channels
class DataInput(object):
    """An input gate of an operator instance.

     The input gate pulls records from all input channels in a round-robin fashion.

     Attributes:
         input_channels (list): The list of input channels.
         channel_index (int): The index of the next channel to pull from.
         max_index (int): The number of input channels.
         closed (list): A list of flags indicating whether an input channel has been marked as 'closed'.
         all_closed (bool): Denotes whether all input channels have been closed (True) or not (False).
    """

    def __init__(self, channels):
        self.input_channels = channels
        self.channel_index = 0
        self.max_index = len(channels)
        self.closed = [False] * len(
            self.input_channels)  # Tracks the channels that have been closed
        self.all_closed = False

    # Fetches records from input channels in a round-robin fashion
    # TODO (john): Make sure the instance is not blocked on any of its input channels
    # TODO (john): In case of input skew, it might be better to pull from the largest queue more often
    def _pull(self):
        while True:
            c = self.input_channels[self.channel_index]  # Channel to pull from
            self.channel_index += 1
            if self.channel_index == self.max_index:  # Reset channel index
                self.channel_index = 0
            if self.closed[self.channel_index -
                           1]:  # Channel has been 'closed', check next
                continue
            record = c.queue.read_next()
            #print("Actor ({},{}) pulled '{}'.".format(c.src_operator_id,c.src_instance_id,record))
            if record == None:  # Mark channel as 'closed' and pull from the next open one
                self.closed[self.channel_index - 1] = True
                self.all_closed = True
                for s in self.closed:
                    if not s:
                        self.all_closed = False
                        break
                if not self.all_closed:
                    continue
            # Returns 'None' iff all input channels are 'closed'
            return record


# Selects output channel(s) and pushes data
class DataOutput(object):
    """An output gate of an operator instance.

     The output gate pushes records to output channels according to the user-defined partitioning scheme.

     Attributes:
         partitioning_schemes (dict): A mapping from destination operator ids to partitioning schemes (see: PScheme in operator.py).
         forward_channels (list): A list of channels to forward records.
         shuffle_channels (list(list)): A list of output channels to shuffle records grouped by destination operator.
         shuffle_key_channels (list(list)): A list of output channels to shuffle records by a key grouped by destination operator.
         shuffle_exists (bool): A flag indicating that there exists at least one shuffle_channel.
         shuffle_key_exists (bool): A flag indicating that there exists at least one shuffle_key_channel.
    """

    def __init__(self, channels, partitioning_schemes):
        self.partitioning_schemes = partitioning_schemes
        # Prepare output -- collect channels by type
        self.forward_channels = []  # Forward and broadcast channels
        slots = sum(1 for scheme in self.partitioning_schemes.values()
                    if scheme.strategy == PStrategy.Shuffle)
        self.shuffle_exists = slots > 0  # Flag used to avoid hashing when there is no shuffling
        self.shuffle_channels = [[]] * slots  # Shuffle channels
        slots = sum(1 for scheme in self.partitioning_schemes.values()
                    if scheme.strategy == PStrategy.ShuffleByKey)
        self.shuffle_key_exists = slots > 0  # Flag used to avoid hashing when there is no shuffling by key
        self.shuffle_key_channels = [[]] * slots  # Shuffle by key channels
        shuffle_destinations = {}  # Distinct shuffle destinations
        shuffle_by_key_destinations = {
        }  # Distinct shuffle by key destinations
        index_1 = 0
        index_2 = 0
        for channel in channels:
            strategy = self.partitioning_schemes[
                channel.dst_operator_id].strategy
            if strategy == PStrategy.Forward or strategy == PStrategy.Broadcast:
                self.forward_channels.append(channel)
            elif strategy == PStrategy.Shuffle:
                slot = shuffle_destinations.setdefault(channel.dst_operator_id,
                                                       index_1)
                self.shuffle_channels[slot].append(channel)
                if slot == index_1:
                    index_1 += 1
            elif strategy == PStrategy.ShuffleByKey:
                slot = shuffle_by_key_destinations.setdefault(
                    channel.dst_operator_id, index_2)
                self.shuffle_key_channels[slot].append(channel)
                if slot == index_2:
                    index_2 += 1
            else:  # TODO (john): Add support for other partitioting strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        # A KeyedDataStream can only be shuffled by key
        assert (not (self.shuffle_exists and self.shuffle_key_exists))

    # Flushes any remaining records in the output channels
    # 'close' indicates whether we should also 'close' the channel (True) by propagating 'None'
    # or just flush the remaining records to plasma (False)
    def _flush(self, close=False):
        """Flushes remaining output records in the output queues to plasma.

         None is used as special type of record that is propagated from sources
         to sink to notify that the end of data in a stream.

         Attributes:
             close (bool): A flag denoting whether the channel should be also marked
             as 'closed' (True) or not (False) after flushing.
        """
        for channel in self.forward_channels:
            if close == True:
                channel.queue.put_next(None)
            channel.queue._flush_writes()
        for channels in self.shuffle_channels:
            for channel in channels:
                if close == True:
                    channel.queue.put_next(None)
                channel.queue._flush_writes()
        for channels in self.shuffle_key_channels:
            for channel in channels:
                if close == True:
                    channel.queue.put_next(None)
                channel.queue._flush_writes()
        # TODO (john): Add more channel types

    # Returns all destination actor ids
    def _destination_actor_ids(self):
        destinations = []
        for channel in self.forward_channels:
            destinations.append((channel.dst_operator_id,
                                 channel.dst_instance_id))
        for channels in self.shuffle_channels:
            for channel in channels:
                destinations.append((channel.dst_operator_id,
                                     channel.dst_instance_id))
        for channels in self.shuffle_key_channels:
            for channel in channels:
                destinations.append((channel.dst_operator_id,
                                     channel.dst_instance_id))
        # TODO (john): Add more channel types
        return destinations

    # Pushes the record to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def _push(self, record):
        # Forward record
        for channel in self.forward_channels:
            # print("[writer] Push record '{}' to channel {}".format(record,c))
            channel.queue.put_next(record)
        # Hash-based shuffling by key
        if self.shuffle_key_exists:
            key, _ = record
            h = _hash(key)
            for channels in self.shuffle_key_channels:
                l = len(channels)  # Number of downstream instances
                channel = channels[h % l]
                #print("[writer] Push record '{}' to channel {}".format(record,c))
                channel.queue.put_next(record)
        elif self.shuffle_exists:  # Hash-based shuffling per destination
            h = _hash(record)
            for channels in self.shuffle_channels:
                l = len(channels)  # Number of downstream instances
                channel = channels[h % l]
                # print("[writer] Push record '{}' to channel {}".format(record,c))
                channel.queue.put_next(record)
        else:  # TODO (john): Handle rescaling
            pass

    # Pushes a list of records to the output
    # Each individual output queue flushes batches to plasma periodically based on 'batch_max_size' and 'batch_max_time'
    def _push_all(self, records):
        # Forward records
        for record in records:
            for channel in self.forward_channels:
                # print("[writer] Push record '{}' to channel {}".format(r,c))
                channel.queue.put_next(record)
        # Hash-based shuffling by key per destination
        if self.shuffle_key_exists:
            for record in records:
                key, _ = record
                h = _hash(key)
                for channels in self.shuffle_channels:
                    l = len(channels)  # Number of downstream instances
                    channel = channels[h % l]
                    # print("[writer] Push record '{}' to channel {}".format(r,c))
                    channel.queue.put_next(record)
        elif self.shuffle_exists:  # Hash-based shuffling per destination
            for record in records:
                h = _hash(record)
                for channels in self.shuffle_channels:
                    l = len(channels)  # Number of downstream instances
                    channel = channels[h % l]
                    # print("[writer] Push record '{}' to channel {}".format(r,c))
                    channel.queue.put_next(record)
        else:  # TODO (john): Handle rescaling
            pass


# Batched queue configuration
class QueueConfig(object):
    """The configuration of a batched queue.

     Attributes:
         max_size (int): The maximum size of the queue in number of batches (if exceeded, backpressure kicks in).
         max_batch_size (int): The size of each batch in number of records.
         max_batch_time (float): The flush timeout per batch.
         prefetch_depth (int): The  number of batches to prefetch from plasma.
         background_flush (bool): Denotes whether a daemon flush thread should be used (True) to flush batches to plasma.
    """

    def __init__(self):
        self.max_size = 999999
        self.max_batch_size = 99999
        self.max_batch_time = 0.01
        self.prefetch_depth = 10
        self.background_flush = False
