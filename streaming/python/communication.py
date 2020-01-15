import hashlib
import logging
import pickle
import sys
import time

import ray
import ray.streaming.runtime.transfer as transfer
from ray.streaming.config import Config
from ray.streaming.operator import PStrategy
from ray.streaming.runtime.transfer import ChannelID

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Forward and broadcast stream partitioning strategies
forward_broadcast_strategies = [PStrategy.Forward, PStrategy.Broadcast]


# Used to choose output channel in case of hash-based shuffling
def _hash(value):
    if isinstance(value, int):
        return value
    try:
        return int(hashlib.sha1(value.encode("utf-8")).hexdigest(), 16)
    except AttributeError:
        return int(hashlib.sha1(value).hexdigest(), 16)


class DataChannel:
    """A data channel for actor-to-actor communication.

    Attributes:
         env (Environment): The environment the channel belongs to.
         src_operator_id (UUID): The id of the source operator of the channel.
         src_instance_index (int): The id of the source instance.
         dst_operator_id (UUID): The id of the destination operator of the
         channel.
         dst_instance_index (int): The id of the destination instance.
    """

    def __init__(self, src_operator_id, src_instance_index, dst_operator_id,
                 dst_instance_index, str_qid):
        self.src_operator_id = src_operator_id
        self.src_instance_index = src_instance_index
        self.dst_operator_id = dst_operator_id
        self.dst_instance_index = dst_instance_index
        self.str_qid = str_qid
        self.qid = ChannelID(str_qid)

    def __repr__(self):
        return "(src({},{}),dst({},{}), qid({}))".format(
            self.src_operator_id, self.src_instance_index,
            self.dst_operator_id, self.dst_instance_index, self.str_qid)


_CLOSE_FLAG = b" "


# Pulls and merges data from multiple input channels
class DataInput:
    """An input gate of an operator instance.

    The input gate pulls records from all input channels in a round-robin
    fashion.

    Attributes:
         input_channels (list): The list of input channels.
         channel_index (int): The index of the next channel to pull from.
         max_index (int): The number of input channels.
         closed (list): A list of flags indicating whether an input channel
         has been marked as 'closed'.
         all_closed (bool): Denotes whether all input channels have been
         closed (True) or not (False).
    """

    def __init__(self, env, channels):
        assert len(channels) > 0
        self.env = env
        self.reader = None  # created in `init` method
        self.input_channels = channels
        self.channel_index = 0
        self.max_index = len(channels)
        # Tracks the channels that have been closed. qid: close status
        self.closed = {}

    def init(self):
        channels = [c.str_qid for c in self.input_channels]
        input_actors = []
        for c in self.input_channels:
            actor = self.env.execution_graph.get_actor(c.src_operator_id,
                                                       c.src_instance_index)
            input_actors.append(actor)
        logger.info("DataInput input_actors %s", input_actors)
        conf = {
            Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context()
            .current_driver_id,
            Config.CHANNEL_TYPE: self.env.config.channel_type
        }
        self.reader = transfer.DataReader(channels, input_actors, conf)

    def pull(self):
        # pull from channel
        item = self.reader.read(100)
        while item is None:
            time.sleep(0.001)
            item = self.reader.read(100)
        msg_data = item.body()
        if msg_data == _CLOSE_FLAG:
            self.closed[item.channel_id] = True
            if len(self.closed) == len(self.input_channels):
                return None
            else:
                return self.pull()
        else:
            return pickle.loads(msg_data)

    def close(self):
        self.reader.stop()


# Selects output channel(s) and pushes data
class DataOutput:
    """An output gate of an operator instance.

    The output gate pushes records to output channels according to the
    user-defined partitioning scheme.

    Attributes:
         partitioning_schemes (dict): A mapping from destination operator ids
         to partitioning schemes (see: PScheme in operator.py).
         forward_channels (list): A list of channels to forward records.
         shuffle_channels (list(list)): A list of output channels to shuffle
         records grouped by destination operator.
         shuffle_key_channels (list(list)): A list of output channels to
         shuffle records by a key grouped by destination operator.
         shuffle_exists (bool): A flag indicating that there exists at least
         one shuffle_channel.
         shuffle_key_exists (bool): A flag indicating that there exists at
         least one shuffle_key_channel.
    """

    def __init__(self, env, channels, partitioning_schemes):
        assert len(channels) > 0
        self.env = env
        self.writer = None  # created in `init` method
        self.channels = channels
        self.key_selector = None
        self.round_robin_indexes = [0]
        self.partitioning_schemes = partitioning_schemes
        # Prepare output -- collect channels by type
        self.forward_channels = []  # Forward and broadcast channels
        slots = sum(1 for scheme in self.partitioning_schemes.values()
                    if scheme.strategy == PStrategy.RoundRobin)
        self.round_robin_channels = [[]] * slots  # RoundRobin channels
        self.round_robin_indexes = [-1] * slots
        slots = sum(1 for scheme in self.partitioning_schemes.values()
                    if scheme.strategy == PStrategy.Shuffle)
        # Flag used to avoid hashing when there is no shuffling
        self.shuffle_exists = slots > 0
        self.shuffle_channels = [[]] * slots  # Shuffle channels
        slots = sum(1 for scheme in self.partitioning_schemes.values()
                    if scheme.strategy == PStrategy.ShuffleByKey)
        # Flag used to avoid hashing when there is no shuffling by key
        self.shuffle_key_exists = slots > 0
        self.shuffle_key_channels = [[]] * slots  # Shuffle by key channels
        # Distinct shuffle destinations
        shuffle_destinations = {}
        # Distinct shuffle by key destinations
        shuffle_by_key_destinations = {}
        # Distinct round robin destinations
        round_robin_destinations = {}
        index_1 = 0
        index_2 = 0
        index_3 = 0
        for channel in channels:
            p_scheme = self.partitioning_schemes[channel.dst_operator_id]
            strategy = p_scheme.strategy
            if strategy in forward_broadcast_strategies:
                self.forward_channels.append(channel)
            elif strategy == PStrategy.Shuffle:
                pos = shuffle_destinations.setdefault(channel.dst_operator_id,
                                                      index_1)
                self.shuffle_channels[pos].append(channel)
                if pos == index_1:
                    index_1 += 1
            elif strategy == PStrategy.ShuffleByKey:
                pos = shuffle_by_key_destinations.setdefault(
                    channel.dst_operator_id, index_2)
                self.shuffle_key_channels[pos].append(channel)
                if pos == index_2:
                    index_2 += 1
            elif strategy == PStrategy.RoundRobin:
                pos = round_robin_destinations.setdefault(
                    channel.dst_operator_id, index_3)
                self.round_robin_channels[pos].append(channel)
                if pos == index_3:
                    index_3 += 1
            else:  # TODO (john): Add support for other strategies
                sys.exit("Unrecognized or unsupported partitioning strategy.")
        # A KeyedDataStream can only be shuffled by key
        assert not (self.shuffle_exists and self.shuffle_key_exists)

    def init(self):
        """init DataOutput which creates DataWriter"""
        channel_ids = [c.str_qid for c in self.channels]
        to_actors = []
        for c in self.channels:
            actor = self.env.execution_graph.get_actor(c.dst_operator_id,
                                                       c.dst_instance_index)
            to_actors.append(actor)
        logger.info("DataOutput output_actors %s", to_actors)

        conf = {
            Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context()
            .current_driver_id,
            Config.CHANNEL_TYPE: self.env.config.channel_type
        }
        self.writer = transfer.DataWriter(channel_ids, to_actors, conf)

    def close(self):
        """Close the channel (True) by propagating _CLOSE_FLAG

        _CLOSE_FLAG is used as special type of record that is propagated from
         sources to sink to notify that the end of data in a stream.
        """
        for c in self.channels:
            self.writer.write(c.qid, _CLOSE_FLAG)
        # must ensure DataWriter send None flag to peer actor
        self.writer.stop()

    def push(self, record):
        target_channels = []
        # Forward record
        for c in self.forward_channels:
            logger.debug("[writer] Push record '{}' to channel {}".format(
                record, c))
            target_channels.append(c)
        # Forward record
        index = 0
        for channels in self.round_robin_channels:
            self.round_robin_indexes[index] += 1
            if self.round_robin_indexes[index] == len(channels):
                self.round_robin_indexes[index] = 0  # Reset index
            c = channels[self.round_robin_indexes[index]]
            logger.debug("[writer] Push record '{}' to channel {}".format(
                record, c))
            target_channels.append(c)
            index += 1
        # Hash-based shuffling by key
        if self.shuffle_key_exists:
            key, _ = record
            h = _hash(key)
            for channels in self.shuffle_key_channels:
                num_instances = len(channels)  # Downstream instances
                c = channels[h % num_instances]
                logger.debug(
                    "[key_shuffle] Push record '{}' to channel {}".format(
                        record, c))
                target_channels.append(c)
        elif self.shuffle_exists:  # Hash-based shuffling per destination
            h = _hash(record)
            for channels in self.shuffle_channels:
                num_instances = len(channels)  # Downstream instances
                c = channels[h % num_instances]
                logger.debug("[shuffle] Push record '{}' to channel {}".format(
                    record, c))
                target_channels.append(c)
        else:  # TODO (john): Handle rescaling
            pass

        msg_data = pickle.dumps(record)
        for c in target_channels:
            # send data to channel
            self.writer.write(c.qid, msg_data)

    def push_all(self, records):
        for record in records:
            self.push(record)
