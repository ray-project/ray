from abc import ABC, abstractmethod

# python type -> (type ID, serializer)
_ray_serializer_map: Map[type, Tuple[str, RaySerializer]] = {}
# type ID -> python type
_ray_type_id_to_type: Map[str, type] = {}

class RaySerializer(ABC):
    @abstractmethod
    def serialize(self, instance) -> RaySerializationResult:
        pass

    @abstractmethod
    def deserialize(
        self, in_band_buffer: bytes, oob_buffers: Map[str, Map[int, memoryview]]
    ):
        pass


def _register_serializer(type_id: str, class_type: type, serializer: RaySerializer):
    _ray_serializer_map[class_type] = (serializer_id, serializer)
    _ray_type_id_to_type[serializer_id] = class_type


def _get_serializer(serializer_indicator) -> RaySerializer:
    try:
        if type(serializer_indicator) is type:
            return _ray_serializer_map[serializer_indicator][1]
        elif type(serializer_indicator) is str:
            class_type = _ray_type_id_to_type[serializer_indicator]
            return _ray_serializer_map[class_type][1]
        else:
            raise TypeError(
                f"Can't get serializer by {str(type(str))}, a type or str should be provided!"
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

class BytesOOBSerializer(RaySerializer):
    def serialize(self, instance: bytes) -> RaySerializationResult:
        pass

    def deserialize(
        self, in_band_buffer: bytes, oob_buffers: Map[str, Map[int, memoryview]]
    ) -> bytes:
        return in_band_buffer
