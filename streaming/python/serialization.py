import msgpack
from ray.streaming import message


_RECORD_TYPE_ID = 0
_KEY_RECORD_TYPE_ID = 1


class StreamElementSerializer:
    """Serialize stream element between java/python"""

    def serialize(self, record):
        if type(record) is message.Record:
            assert type(record.value) is bytes, "Only bytes value is supported"
            fields = [_RECORD_TYPE_ID,
                      record.stream, record.value]
        elif type(record) is message.KeyRecord:
            assert type(record.key) is bytes, "Only bytes key is supported"
            assert type(record.value) is bytes, "Only bytes value is supported"
            fields = [_KEY_RECORD_TYPE_ID,
                      record.stream, record.key, record.value]
        else:
            raise Exception("Unsupported value {}".format(record))
        return msgpack.packb(fields, use_bin_type=True)

    def deserialize(self, data):
        fields = msgpack.unpackb(data, raw=False)
        if fields[0] == _RECORD_TYPE_ID:
            stream, value = fields[1:]
            record = message.Record(value)
            record.stream = stream
            return record
        elif fields[0] == _KEY_RECORD_TYPE_ID:
            stream, key, value = fields[1:]
            key_record = message.KeyRecord(key, value)
            key_record.stream = stream
            return key_record
        else:
            raise Exception("Unsupported type id {}".format(fields[0]))
