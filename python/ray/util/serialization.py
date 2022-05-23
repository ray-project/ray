import ray
import ray.cloudpickle as pickle


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


def deregister_serializer(cls):
    """Deregister the serializer associated with the type ``cls``.
    There is no effect if the serializer is unavailable.

    Args:
        cls: A Python class/type.
    """
    context = ray.worker.global_worker.get_serialization_context()
    context._unregister_cloudpickle_reducer(cls)


class StandaloneSerializationContext:
    # NOTE(simon): Used for registering custom serializers. We cannot directly
    # use the SerializationContext because it requires Ray workers. Please
    # make sure to keep the API consistent.

    def _unregister_cloudpickle_reducer(self, cls):
        pickle.CloudPickler.dispatch.pop(cls, None)

    def _register_cloudpickle_serializer(
        self, cls, custom_serializer, custom_deserializer
    ):
        def _CloudPicklerReducer(obj):
            return custom_deserializer, (custom_serializer(obj),)

        # construct a reducer
        pickle.CloudPickler.dispatch[cls] = _CloudPicklerReducer
