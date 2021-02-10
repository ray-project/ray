import logging
import threading

import ray.cloudpickle as pickle
from ray import ray_constants
import ray.utils
from ray.gcs_utils import ErrorType
from ray.exceptions import (
    RayError,
    PlasmaObjectNotAvailable,
    RayTaskError,
    RayActorError,
    TaskCancelledError,
    WorkerCrashedError,
    ObjectLostError,
)
from ray._raylet import (
    split_buffer,
    unpack_pickle5_buffers,
    Pickle5Writer,
    Pickle5SerializedObject,
    MessagePackSerializer,
    MessagePackSerializedObject,
    RawSerializedObject,
)

logger = logging.getLogger(__name__)


class DeserializationError(Exception):
    pass


def _object_ref_deserializer(binary, owner_address):
    # NOTE(suquark): This function should be a global function so
    # cloudpickle can access it directly. Otherwise couldpickle
    # has to dump the whole function definition, which is inefficient.

    # NOTE(swang): Must deserialize the object first before asking
    # the core worker to resolve the value. This is to make sure
    # that the ref count for the ObjectRef is greater than 0 by the
    # time the core worker resolves the value of the object.
    obj_ref = ray.ObjectRef(binary)

    # TODO(edoakes): we should be able to just capture a reference
    # to 'self' here instead, but this function is itself pickled
    # somewhere, which causes an error.
    if owner_address:
        worker = ray.worker.global_worker
        worker.check_connected()
        context = worker.get_serialization_context()
        outer_id = context.get_outer_object_ref()
        # outer_id is None in the case that this ObjectRef was closed
        # over in a function or pickled directly using pickle.dumps().
        if outer_id is None:
            outer_id = ray.ObjectRef.nil()
        worker.core_worker.deserialize_and_register_object_ref(
            obj_ref.binary(), outer_id, owner_address)
    return obj_ref


def _actor_handle_deserializer(serialized_obj):
    # If this actor handle was stored in another object, then tell the
    # core worker.
    context = ray.worker.global_worker.get_serialization_context()
    outer_id = context.get_outer_object_ref()
    return ray.actor.ActorHandle._deserialization_helper(
        serialized_obj, outer_id)


class SerializationContext:
    """Initialize the serialization library.

    This defines a custom serializer for object refs and also tells ray to
    serialize several exception classes that we define for error handling.
    """

    def __init__(self, worker):
        self.worker = worker
        self._thread_local = threading.local()

        def actor_handle_reducer(obj):
            serialized, actor_handle_id = obj._serialization_helper()
            # Update ref counting for the actor handle
            self.add_contained_object_ref(actor_handle_id)
            return _actor_handle_deserializer, (serialized, )

        self._register_cloudpickle_reducer(ray.actor.ActorHandle,
                                           actor_handle_reducer)

        def object_ref_reducer(obj):
            self.add_contained_object_ref(obj)
            worker = ray.worker.global_worker
            worker.check_connected()
            obj, owner_address = (
                worker.core_worker.serialize_and_promote_object_ref(obj))
            return _object_ref_deserializer, (obj.binary(), owner_address)

        self._register_cloudpickle_reducer(ray.ObjectRef, object_ref_reducer)

    def _register_cloudpickle_reducer(self, cls, reducer):
        pickle.CloudPickler.dispatch[cls] = reducer

    def _unregister_cloudpickle_reducer(self, cls):
        pickle.CloudPickler.dispatch.pop(cls, None)

    def _register_cloudpickle_serializer(self, cls, custom_serializer,
                                         custom_deserializer):
        def _CloudPicklerReducer(obj):
            return custom_deserializer, (custom_serializer(obj), )

        # construct a reducer
        pickle.CloudPickler.dispatch[cls] = _CloudPicklerReducer

    def is_in_band_serialization(self):
        return getattr(self._thread_local, "in_band", False)

    def set_in_band_serialization(self):
        self._thread_local.in_band = True

    def set_out_of_band_serialization(self):
        self._thread_local.in_band = False

    def set_outer_object_ref(self, outer_object_ref):
        self._thread_local.outer_object_ref = outer_object_ref

    def get_outer_object_ref(self):
        return getattr(self._thread_local, "outer_object_ref", None)

    def get_and_clear_contained_object_refs(self):
        if not hasattr(self._thread_local, "object_refs"):
            self._thread_local.object_refs = set()
            return set()

        object_refs = self._thread_local.object_refs
        self._thread_local.object_refs = set()
        return object_refs

    def add_contained_object_ref(self, object_ref):
        if self.is_in_band_serialization():
            # This object ref is being stored in an object. Add the ID to the
            # list of IDs contained in the object so that we keep the inner
            # object value alive as long as the outer object is in scope.
            if not hasattr(self._thread_local, "object_refs"):
                self._thread_local.object_refs = set()
            self._thread_local.object_refs.add(object_ref)
        else:
            # If this serialization is out-of-band (e.g., from a call to
            # cloudpickle directly or captured in a remote function/actor),
            # then pin the object for the lifetime of this worker by adding
            # a local reference that won't ever be removed.
            ray.worker.global_worker.core_worker.add_object_ref_reference(
                object_ref)

    def _deserialize_pickle5_data(self, data):
        try:
            in_band, buffers = unpack_pickle5_buffers(data)
            if len(buffers) > 0:
                obj = pickle.loads(in_band, buffers=buffers)
            else:
                obj = pickle.loads(in_band)
        # cloudpickle does not provide error types
        except pickle.pickle.PicklingError:
            raise DeserializationError()
        return obj

    def _deserialize_msgpack_data(self, data, metadata_fields):
        msgpack_data, pickle5_data = split_buffer(data)

        if metadata_fields[0] == ray_constants.OBJECT_METADATA_TYPE_PYTHON:
            python_objects = self._deserialize_pickle5_data(pickle5_data)
        else:
            python_objects = []

        try:

            def _python_deserializer(index):
                return python_objects[index]

            obj = MessagePackSerializer.loads(msgpack_data,
                                              _python_deserializer)
        except Exception:
            raise DeserializationError()
        return obj

    def _deserialize_object(self, data, metadata, object_ref):
        if metadata:
            metadata_fields = metadata.split(b",")
            if metadata_fields[0] in [
                    ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
                    ray_constants.OBJECT_METADATA_TYPE_PYTHON
            ]:
                return self._deserialize_msgpack_data(data, metadata_fields)
            # Check if the object should be returned as raw bytes.
            if metadata_fields[0] == ray_constants.OBJECT_METADATA_TYPE_RAW:
                if data is None:
                    return b""
                return data.to_pybytes()
            elif metadata_fields[
                    0] == ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE:
                obj = self._deserialize_msgpack_data(data, metadata_fields)
                return _actor_handle_deserializer(obj)
            # Otherwise, return an exception object based on
            # the error type.
            try:
                error_type = int(metadata_fields[0])
            except Exception:
                raise Exception(f"Can't deserialize object: {object_ref}, "
                                f"metadata: {metadata}")

            # RayTaskError is serialized with pickle5 in the data field.
            # TODO (kfstorm): exception serialization should be language
            # independent.
            if error_type == ErrorType.Value("TASK_EXECUTION_EXCEPTION"):
                obj = self._deserialize_msgpack_data(data, metadata_fields)
                return RayError.from_bytes(obj)
            elif error_type == ErrorType.Value("WORKER_DIED"):
                return WorkerCrashedError()
            elif error_type == ErrorType.Value("ACTOR_DIED"):
                return RayActorError()
            elif error_type == ErrorType.Value("TASK_CANCELLED"):
                return TaskCancelledError()
            elif error_type == ErrorType.Value("OBJECT_UNRECONSTRUCTABLE"):
                return ObjectLostError(ray.ObjectRef(object_ref.binary()))
            else:
                assert error_type != ErrorType.Value("OBJECT_IN_PLASMA"), \
                    "Tried to get object that has been promoted to plasma."
                assert False, "Unrecognized error type " + str(error_type)
        elif data:
            raise ValueError("non-null object should always have metadata")
        else:
            # Object isn't available in plasma. This should never be returned
            # to the user. We should only reach this line if this object was
            # deserialized as part of a list, and another object in the list
            # throws an exception.
            return PlasmaObjectNotAvailable

    def deserialize_objects(self, data_metadata_pairs, object_refs):
        assert len(data_metadata_pairs) == len(object_refs)
        results = []
        for object_ref, (data, metadata) in zip(object_refs,
                                                data_metadata_pairs):
            assert self.get_outer_object_ref() is None
            self.set_outer_object_ref(object_ref)
            results.append(
                self._deserialize_object(data, metadata, object_ref))
            # Must clear ObjectRef to not hold a reference.
            self.set_outer_object_ref(None)
        return results

    def _serialize_to_pickle5(self, metadata, value):
        writer = Pickle5Writer()
        # TODO(swang): Check that contained_object_refs is empty.
        try:
            self.set_in_band_serialization()
            inband = pickle.dumps(
                value, protocol=5, buffer_callback=writer.buffer_callback)
        except Exception as e:
            self.get_and_clear_contained_object_refs()
            raise e
        finally:
            self.set_out_of_band_serialization()

        return Pickle5SerializedObject(
            metadata, inband, writer,
            self.get_and_clear_contained_object_refs())

    def _serialize_to_msgpack(self, value):
        # Only RayTaskError is possible to be serialized here. We don't
        # need to deal with other exception types here.
        contained_object_refs = []

        if isinstance(value, RayTaskError):
            metadata = str(
                ErrorType.Value("TASK_EXECUTION_EXCEPTION")).encode("ascii")
            value = value.to_bytes()
        elif isinstance(value, ray.actor.ActorHandle):
            # TODO(fyresone): ActorHandle should be serialized via the
            # custom type feature of cross-language.
            serialized, actor_handle_id = value._serialization_helper()
            contained_object_refs.append(actor_handle_id)
            # Update ref counting for the actor handle
            metadata = ray_constants.OBJECT_METADATA_TYPE_ACTOR_HANDLE
            value = serialized
        else:
            metadata = ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE

        python_objects = []

        def _python_serializer(o):
            index = len(python_objects)
            python_objects.append(o)
            return index

        msgpack_data = MessagePackSerializer.dumps(value, _python_serializer)

        if python_objects:
            metadata = ray_constants.OBJECT_METADATA_TYPE_PYTHON
            pickle5_serialized_object = \
                self._serialize_to_pickle5(metadata, python_objects)
        else:
            pickle5_serialized_object = None

        return MessagePackSerializedObject(metadata, msgpack_data,
                                           contained_object_refs,
                                           pickle5_serialized_object)

    def serialize(self, value):
        """Serialize an object.

        Args:
            value: The value to serialize.
        """
        if isinstance(value, bytes):
            # If the object is a byte array, skip serializing it and
            # use a special metadata to indicate it's raw binary. So
            # that this object can also be read by Java.
            return RawSerializedObject(value)
        else:
            return self._serialize_to_msgpack(value)
