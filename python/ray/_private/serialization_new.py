from abc import ABC, abstractmethod
from typing import Mapping, Tuple
from ray._raylet import RaySerializationResult, SerializedObject
import logging
import ray
import msgpack

logger = logging.getLogger(__name__)


class RaySerializer(ABC):
    @abstractmethod
    def serialize(self, instance) -> RaySerializationResult:
        pass

    @abstractmethod
    def deserialize(
        self,
        ray_serialization_result: RaySerializationResult,
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
    return serializer.deserialize(ray_serialization_result)


# ------------------------Build-in serializers-----------------------------


class OldDefaultSerializer(RaySerializer):
    TYPE_ID = b"ray_serde_old_default"

    def serialize(self, instance) -> RaySerializationResult:
        old_serialized_obj: SerializedObject = (
            ray._private.worker.global_worker.get_serialization_context().serialize(
                instance
            )
        )
        """
        msgpack.packb(old.write_to(), old.metadata)
        -> new.in_band_data
        ------------------------
        old.contained_object_refs -> new.contained_object_refs
        """
        return RaySerializationResult.from_old_serialized_obj(
            OldDefaultSerializer.TYPE_ID, old_serialized_obj
        )

    def deserialize(self, ray_serialization_result: RaySerializationResult):
        old_bytes, old_metadata = msgpack.unpackb(
            ray_serialization_result.in_band_buffer
        )
        context = ray._private.worker.global_worker.get_serialization_context()
        return context.deserialize_objects(
            data_metadata_pairs=[(old_bytes, old_metadata)],
            object_refs=ray_serialization_result.contained_object_refs,
        )[0]


class BytesInBandSerializer(RaySerializer):
    TYPE_ID = b"ray_serde_bytes"

    def serialize(self, instance: bytes) -> RaySerializationResult:
        return RaySerializationResult(BytesInBandSerializer.TYPE_ID, instance)

    def deserialize(self, ray_serialization_result: RaySerializationResult) -> bytes:
        return ray_serialization_result.in_band_buffer


_register_serializer(BytesInBandSerializer.TYPE_ID, type(b""), BytesInBandSerializer())
