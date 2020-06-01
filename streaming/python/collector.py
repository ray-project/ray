import logging
import typing
from abc import ABC, abstractmethod

from ray import Language
from ray.actor import ActorHandle
from ray.streaming import function
from ray.streaming import message
from ray.streaming import partition
from ray.streaming.runtime import serialization
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
    def __init__(self, writer: DataWriter, channel_ids: typing.List[str],
                 target_actors: typing.List[ActorHandle],
                 partition_func: partition.Partition):
        self._writer = writer
        self._channel_ids = [ChannelID(id_str) for id_str in channel_ids]
        self._target_languages = []
        for actor in target_actors:
            if actor._ray_actor_language == Language.PYTHON:
                self._target_languages.append(function.Language.PYTHON)
            elif actor._ray_actor_language == Language.JAVA:
                self._target_languages.append(function.Language.JAVA)
            else:
                raise Exception("Unsupported language {}"
                                .format(actor._ray_actor_language))
        self._partition_func = partition_func
        self.python_serializer = serialization.PythonSerializer()
        self.cross_lang_serializer = serialization.CrossLangSerializer()
        logger.info(
            "Create OutputCollector, channel_ids {}, partition_func {}".format(
                channel_ids, partition_func))

    def collect(self, record):
        partitions = self._partition_func \
            .partition(record, len(self._channel_ids))
        python_buffer = None
        cross_lang_buffer = None
        for partition_index in partitions:
            if self._target_languages[partition_index] == \
                    function.Language.PYTHON:
                # avoid repeated serialization
                if python_buffer is None:
                    python_buffer = self.python_serializer.serialize(record)
                self._writer.write(
                    self._channel_ids[partition_index],
                    serialization._PYTHON_TYPE_ID + python_buffer)
            else:
                # avoid repeated serialization
                if cross_lang_buffer is None:
                    cross_lang_buffer = self.cross_lang_serializer.serialize(
                        record)
                self._writer.write(
                    self._channel_ids[partition_index],
                    serialization._CROSS_LANG_TYPE_ID + cross_lang_buffer)
