# coding: utf-8
import logging
import sys
import ray
import ray._private.serialization_new as ser_new
from ray._private.serialization_new import RaySerializer, RaySerializationResult
from ray import ObjectRef

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def test_bytes():
    b = b"10086"
    res = ser_new._serialize(b)
    deserialized = ser_new._deserialize(res)
    assert deserialized == b


def test_bytes_task_arg(ray_start_regular):
    @ray.remote
    def fun(data: bytes) -> bytes:
        return data

    b = b"966091"
    res = ray.get(fun.remote(b))
    assert res == b


def test_out_of_band():
    class MemoryviewOutOfBandSerializer(RaySerializer):
        TYPE_ID = b"ray_serde_memoryview"

        def serialize(self, instance: memoryview) -> RaySerializationResult:
            random_id = ObjectRef.from_random().binary()
            oob_buffers = {random_id: instance}
            return RaySerializationResult(
                MemoryviewOutOfBandSerializer.TYPE_ID, random_id, oob_buffers
            )

        def deserialize(
            self, ray_serialization_result: RaySerializationResult
        ) -> bytes:
            memoryview_id = ray_serialization_result.in_band_buffer
            return ray_serialization_result.out_of_band_buffers[memoryview_id]

    ser_new._register_serializer(
        MemoryviewOutOfBandSerializer.TYPE_ID,
        type(memoryview(b"")),
        MemoryviewOutOfBandSerializer(),
    )

    byte_arr = bytearray(b"1314521")
    view = memoryview(byte_arr)
    res = ser_new._serialize(view)
    deserialized = ser_new._deserialize(res)
    assert deserialized.obj == byte_arr
    # change the underlying buffer
    byte_arr[0] = 9
    # the deserialized object should also be changed
    # since the underlying buffer is the same in OOB serialization
    assert deserialized.obj[0] == 9


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-sv", __file__]))
