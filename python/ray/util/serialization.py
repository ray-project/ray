import ray
import ray.cloudpickle as pickle
from ray.util.annotations import DeveloperAPI, PublicAPI


@PublicAPI
def register_serializer(cls: type, *, serializer: callable, deserializer: callable):
    """Use the given serializer to serialize instances of type ``cls``,
    and use the deserializer to deserialize the serialized object.

    Args:
        cls: A Python class/type.
        serializer: A function that converts an instances of
            type ``cls`` into a serializable object (e.g. python dict
            of basic objects).
        deserializer: A function that constructs the
            instance of type ``cls`` from the serialized object.
            This function itself must be serializable.
    """
    context = ray._private.worker.global_worker.get_serialization_context()
    context._register_cloudpickle_serializer(cls, serializer, deserializer)


@PublicAPI
def deregister_serializer(cls: type):
    """Deregister the serializer associated with the type ``cls``.
    There is no effect if the serializer is unavailable.

    Args:
        cls: A Python class/type.
    """
    context = ray._private.worker.global_worker.get_serialization_context()
    context._unregister_cloudpickle_reducer(cls)


@DeveloperAPI
class StandaloneSerializationContext:
    # NOTE(simon): Used for registering custom serializers. We cannot directly
    # use the SerializationContext because it requires Ray workers. Please
    # make sure to keep the API consistent.

    def _register_cloudpickle_reducer(self, cls, reducer):
        pickle.CloudPickler.dispatch[cls] = reducer

    def _unregister_cloudpickle_reducer(self, cls):
        pickle.CloudPickler.dispatch.pop(cls, None)

    def _register_cloudpickle_serializer(
        self, cls, custom_serializer, custom_deserializer
    ):
        def _CloudPicklerReducer(obj):
            return custom_deserializer, (custom_serializer(obj),)

        # construct a reducer
        pickle.CloudPickler.dispatch[cls] = _CloudPicklerReducer
