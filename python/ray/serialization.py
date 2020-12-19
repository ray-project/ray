import hashlib
import logging
import time
import threading

import ray.cloudpickle as pickle
from ray import ray_constants, JobID
import ray.utils
from ray.utils import _random_string
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


class RayNotDictionarySerializable(Exception):
    pass


# This exception is used to represent situations where cloudpickle fails to
# pickle an object (cloudpickle can fail in many different ways).
class CloudPickleError(Exception):
    pass


class DeserializationError(Exception):
    pass


def _try_to_compute_deterministic_class_id(cls, depth=5):
    """Attempt to produce a deterministic class ID for a given class.

    The goal here is for the class ID to be the same when this is run on
    different worker processes. Pickling, loading, and pickling again seems to
    produce more consistent results than simply pickling. This is a bit crazy
    and could cause problems, in which case we should revert it and figure out
    something better.

    Args:
        cls: The class to produce an ID for.
        depth: The number of times to repeatedly try to load and dump the
            string while trying to reach a fixed point.

    Returns:
        A class ID for this class. We attempt to make the class ID the same
            when this function is run on different workers, but that is not
            guaranteed.

    Raises:
        Exception: This could raise an exception if cloudpickle raises an
            exception.
    """
    # Pickling, loading, and pickling again seems to produce more consistent
    # results than simply pickling. This is a bit
    class_id = pickle.dumps(cls)
    for _ in range(depth):
        new_class_id = pickle.dumps(pickle.loads(class_id))
        if new_class_id == class_id:
            # We appear to have reached a fix point, so use this as the ID.
            return hashlib.sha1(new_class_id).digest()
        class_id = new_class_id

    # We have not reached a fixed point, so we may end up with a different
    # class ID for this custom class on each worker, which could lead to the
    # same class definition being exported many many times.
    logger.warning(
        f"WARNING: Could not produce a deterministic class ID for class {cls}")
    return hashlib.sha1(new_class_id).digest()


def object_ref_deserializer(reduced_obj_ref, owner_address):
    # NOTE(suquark): This function should be a global function so
    # cloudpickle can access it directly. Otherwise couldpickle
    # has to dump the whole function definition, which is inefficient.

    # NOTE(swang): Must deserialize the object first before asking
    # the core worker to resolve the value. This is to make sure
    # that the ref count for the ObjectRef is greater than 0 by the
    # time the core worker resolves the value of the object.

    # UniqueIDs are serialized as (class name, (unique bytes,)).
    obj_ref = reduced_obj_ref[0](*reduced_obj_ref[1])

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


def actor_handle_deserializer(serialized_obj):
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
            return actor_handle_deserializer, (serialized, )

        self._register_cloudpickle_reducer(ray.actor.ActorHandle,
                                           actor_handle_reducer)

        def object_ref_reducer(obj):
            self.add_contained_object_ref(obj)
            worker = ray.worker.global_worker
            worker.check_connected()
            obj, owner_address = (
                worker.core_worker.serialize_and_promote_object_ref(obj))
            return object_ref_deserializer, (obj.__reduce__(), owner_address)

        # Because objects have default __reduce__ method, we only need to
        # treat ObjectRef specifically.
        self._register_cloudpickle_reducer(ray.ObjectRef, object_ref_reducer)

    def _register_cloudpickle_reducer(self, cls, reducer):
        pickle.CloudPickler.dispatch[cls] = reducer

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
                return actor_handle_deserializer(obj)
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

    def deserialize_objects(self,
                            data_metadata_pairs,
                            object_refs,
                            error_timeout=10):
        assert len(data_metadata_pairs) == len(object_refs)

        start_time = time.time()
        results = []
        warning_sent = False
        i = 0
        while i < len(object_refs):
            object_ref = object_refs[i]
            data, metadata = data_metadata_pairs[i]
            assert self.get_outer_object_ref() is None
            self.set_outer_object_ref(object_ref)
            try:
                results.append(
                    self._deserialize_object(data, metadata, object_ref))
                i += 1
            except DeserializationError:
                # Wait a little bit for the import thread to import the class.
                # If we currently have the worker lock, we need to release it
                # so that the import thread can acquire it.
                time.sleep(0.01)

                if time.time() - start_time > error_timeout:
                    warning_message = ("This worker or driver is waiting to "
                                       "receive a class definition so that it "
                                       "can deserialize an object from the "
                                       "object store. This may be fine, or it "
                                       "may be a bug.")
                    if not warning_sent:
                        ray.utils.push_error_to_driver(
                            self,
                            ray_constants.WAIT_FOR_CLASS_PUSH_ERROR,
                            warning_message,
                            job_id=self.worker.current_job_id)
                    warning_sent = True
            finally:
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

    def register_custom_serializer(self,
                                   cls,
                                   serializer,
                                   deserializer,
                                   local=False,
                                   job_id=None,
                                   class_id=None):
        """Enable serialization and deserialization for a particular class.

        This method runs the register_class function defined below on
        every worker, which will enable ray to properly serialize and
        deserialize objects of this class.

        Args:
            cls (type): The class that ray should use this custom serializer
                for.
            serializer: The custom serializer to use.
            deserializer: The custom deserializer to use.
            local: True if the serializers should only be registered on the
                current worker. This should usually be False.
            job_id: ID of the job that we want to register the class for.
            class_id (str): Unique ID of the class. Autogenerated if None.

        Raises:
            RayNotDictionarySerializable: Raised if use_dict is true and cls
                cannot be efficiently serialized by Ray.
            ValueError: Raised if ray could not autogenerate a class_id.
        """
        assert serializer is not None and deserializer is not None, (
            "Must provide serializer and deserializer.")

        if class_id is None:
            if not local:
                # In this case, the class ID will be used to deduplicate the
                # class across workers. Note that cloudpickle unfortunately
                # does not produce deterministic strings, so these IDs could
                # be different on different workers. We could use something
                # weaker like cls.__name__, however that would run the risk
                # of having collisions.
                # TODO(rkn): We should improve this.
                try:
                    # Attempt to produce a class ID that will be the same on
                    # each worker. However, determinism is not guaranteed,
                    # and the result may be different on different workers.
                    class_id = _try_to_compute_deterministic_class_id(cls)
                except Exception:
                    raise ValueError(
                        "Failed to use pickle in generating a unique id"
                        f"for '{cls}'. Provide a unique class_id.")
            else:
                # In this case, the class ID only needs to be meaningful on
                # this worker and not across workers.
                class_id = _random_string()

            # Make sure class_id is a string.
            class_id = ray.utils.binary_to_hex(class_id)

        if job_id is None:
            job_id = self.worker.current_job_id
        assert isinstance(job_id, JobID)

        def register_class_for_serialization(worker_info):
            context = worker_info["worker"].get_serialization_context(job_id)
            context._register_cloudpickle_serializer(cls, serializer,
                                                     deserializer)

        if not local:
            self.worker.run_function_on_all_workers(
                register_class_for_serialization)
        else:
            # Since we are pickling objects of this class, we don't actually
            # need to ship the class definition.
            register_class_for_serialization({"worker": self.worker})
