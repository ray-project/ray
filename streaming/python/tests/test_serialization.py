from ray.streaming.serialization import StreamElementSerializer
from ray.streaming.message import Record, KeyRecord


def test_serialize():
    serializer = StreamElementSerializer()
    record = Record(b"value")
    record.stream = "stream1"
    key_record = KeyRecord(b"key", b"value")
    key_record.stream = "stream2"
    assert record == serializer.deserialize(serializer.serialize(record))
    assert key_record == serializer.\
        deserialize(serializer.serialize(key_record))


test_serialize()