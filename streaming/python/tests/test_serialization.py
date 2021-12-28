from ray.streaming.runtime.serialization import CrossLangSerializer
from ray.streaming.message import Record, KeyRecord


def test_serialize():
    serializer = CrossLangSerializer()
    record = Record("value")
    record.stream = "stream1"
    key_record = KeyRecord("key", "value")
    key_record.stream = "stream2"
    assert record == serializer.deserialize(serializer.serialize(record))
    assert key_record == serializer.deserialize(serializer.serialize(key_record))
