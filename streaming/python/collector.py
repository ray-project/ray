import logging
import pickle
import typing
from abc import ABC, abstractmethod

from ray.streaming import message
from ray.streaming import partition
from ray.streaming.runtime.transfer import ChannelID, DataWriter

logger = logging.getLogger(__name__)


class Collector(ABC):
    """
    The collector that collects data from an upstream operator,
     and emits data to downstream operators.
    """

    @abstractmethod
    def collect(self, record):
        pass


class CollectionCollector(Collector):
    def __init__(self, collector_list):
        self._collector_list = collector_list

    def collect(self, value):
        for collector in self._collector_list:
            collector.collect(message.Record(value))


class OutputCollector(Collector):
    def __init__(self, channel_ids: typing.List[str], writer: DataWriter,
                 partition_func: partition.Partition):
        self._channel_ids = [ChannelID(id_str) for id_str in channel_ids]
        self._writer = writer
        self._partition_func = partition_func
        logger.info(
            "Create OutputCollector, channel_ids {}, partition_func {}".format(
                channel_ids, partition_func))

    def collect(self, record):
        partitions = self._partition_func.partition(record,
                                                    len(self._channel_ids))
        serialized_message = pickle.dumps(record)
        for partition_index in partitions:
            self._writer.write(self._channel_ids[partition_index],
                               serialized_message)
