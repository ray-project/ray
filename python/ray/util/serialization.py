import ray


def register_serializer(cls, *, serializer, deserializer):
    """Use the given serializer to serialize instances of type ``cls``,
    and use the deserializer to deserialize the serialized object.

    Args:
        cls: A Python class/type.
        serializer (callable): A function that converts an instances of
            type ``cls`` into a serializable object (e.g. python dict
            of basic objects).
        deserializer (callable): A function that constructs the
            instance of type ``cls`` from the serialized object.
            This function itself must be serializable.
    """
    context = ray.worker.global_worker.get_serialization_context()
    context._register_cloudpickle_serializer(cls, serializer, deserializer)
