from abc import ABC, abstractmethod
from typing import Mapping, Tuple
from ray._raylet import RaySerializationResult
from ray import ObjectRef
import logging

logger = logging.getLogger(__name__)


class RaySerializer(ABC):
    @abstractmethod
    def serialize(self, instance) -> RaySerializationResult:
        pass

    @abstractmethod
    def deserialize(
        self,
        in_band_buffer: bytes,
        oob_buffers: Mapping[bytes, Mapping[int, memoryview]],
    ):
        pass


# python type -> (type ID, serializer)
_ray_serializer_map: Mapping[type, Tuple[bytes, RaySerializer]] = {}
# type ID -> python type
_ray_type_id_to_type: Mapping[bytes, type] = {}


def _register_serializer(type_id: bytes, class_type: type, serializer: RaySerializer):
    logger.info(f"registering serializer, type_id={type_id}, class_type={class_type}")
    _ray_serializer_map[class_type] = (type_id, serializer)
    _ray_type_id_to_type[type_id] = class_type


def _get_serializer(serializer_indicator) -> RaySerializer:
    try:
        if type(serializer_indicator) is type:
            return _ray_serializer_map[serializer_indicator][1]
        elif type(serializer_indicator) is bytes:
            class_type = _ray_type_id_to_type[serializer_indicator]
            return _ray_serializer_map[class_type][1]
        else:
            raise TypeError(
                f"Can't get serializer by {str(type(serializer_indicator))}"
                ", a type or str should be provided!"
            )
    except KeyError:
        logger.exception(f"Can't find a serializer by {serializer_indicator}!")


def _serialize(instance) -> RaySerializationResult:
    class_type = type(instance)
    return _get_serializer(class_type).serialize(instance)


def _deserialize(ray_serialization_result: RaySerializationResult):
    serializer = _get_serializer(ray_serialization_result.type_id)
    return serializer.deserialize(
        ray_serialization_result.in_band_buffer,
        ray_serialization_result.out_of_band_buffers,
    )


# ------------------------Build-in serializers-----------------------------


class BytesOutOfBandSerializer(RaySerializer):
    TYPE_ID = b"ray_serde_bytes"

    def serialize(self, instance: bytes) -> RaySerializationResult:
        random_id = ObjectRef.from_random().binary()
        oob_buffers = {
            BytesOutOfBandSerializer.TYPE_ID: {random_id: memoryview(instance)}
        }
        return RaySerializationResult(
            BytesOutOfBandSerializer.TYPE_ID, random_id, oob_buffers
        )

    def deserialize(
        self, in_band_buffer: bytes, oob_buffers: Mapping[str, Mapping[int, memoryview]]
    ) -> bytes:
        byte_id = in_band_buffer
        return bytes(oob_buffers[BytesOutOfBandSerializer.TYPE_ID][byte_id])


_register_serializer(
    BytesOutOfBandSerializer.TYPE_ID, type(b""), BytesOutOfBandSerializer()
)
