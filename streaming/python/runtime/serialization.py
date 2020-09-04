from abc import ABC, abstractmethod
import pickle
import msgpack
from ray.streaming import message

RECORD_TYPE_ID = 0
KEY_RECORD_TYPE_ID = 1
CROSS_LANG_TYPE_ID = 0
JAVA_TYPE_ID = 1
PYTHON_TYPE_ID = 2


class Serializer(ABC):
    @abstractmethod
    def serialize(self, obj):
        pass

    @abstractmethod
    def deserialize(self, serialized_bytes):
        pass


class PythonSerializer(Serializer):
    def serialize(self, obj):
        return pickle.dumps(obj)

    def deserialize(self, serialized_bytes):
        return pickle.loads(serialized_bytes)


class CrossLangSerializer(Serializer):
    """Serialize stream element between java/python"""

    def serialize(self, obj):
        if type(obj) is message.Record:
            fields = [RECORD_TYPE_ID, obj.stream, obj.value]
        elif type(obj) is message.KeyRecord:
            fields = [KEY_RECORD_TYPE_ID, obj.stream, obj.key, obj.value]
        else:
            raise Exception("Unsupported value {}".format(obj))
        return msgpack.packb(fields, use_bin_type=True)

    def deserialize(self, data):
        fields = msgpack.unpackb(data, raw=False)
        if fields[0] == RECORD_TYPE_ID:
            stream, value = fields[1:]
            record = message.Record(value)
            record.stream = stream
            return record
        elif fields[0] == KEY_RECORD_TYPE_ID:
            stream, key, value = fields[1:]
            key_record = message.KeyRecord(key, value)
            key_record.stream = stream
            return key_record
        else:
            raise Exception("Unsupported type id {}, type {}".format(
                fields[0], type(fields[0])))
