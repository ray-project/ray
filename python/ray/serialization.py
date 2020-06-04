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
    PlasmaObjectNotAvailable,
    RayTaskError,
    RayActorError,
    RayCancellationError,
    RayWorkerError,
    UnreconstructableError,
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
        "WARNING: Could not produce a deterministic class ID for class "
        "{}".format(cls))
    return hashlib.sha1(new_class_id).digest()


class SerializationContext:
    """Initialize the serialization library.

    This defines a custom serializer for object IDs and also tells ray to
    serialize several exception classes that we define for error handling.
    """

    def __init__(self, worker):
        self.worker = worker
        self._thread_local = threading.local()

        def actor_handle_serializer(obj):
            serialized, actor_handle_id = obj._serialization_helper()
            # Update ref counting for the actor handle
            self.add_contained_object_id(actor_handle_id)
            return serialized

        def actor_handle_deserializer(serialized_obj):
            # If this actor handle was stored in another object, then tell the
            # core worker.
            context = ray.worker.global_worker.get_serialization_context()
            outer_id = context.get_outer_object_id()
            return ray.actor.ActorHandle._deserialization_helper(
                serialized_obj, outer_id)

        self._register_cloudpickle_serializer(
            ray.actor.ActorHandle,
            custom_serializer=actor_handle_serializer,
            custom_deserializer=actor_handle_deserializer)

        def id_serializer(obj):
            return obj.__reduce__()

        def id_deserializer(serialized_obj):
            return serialized_obj[0](*serialized_obj[1])

        def object_id_serializer(obj):
            self.add_contained_object_id(obj)
            worker = ray.worker.global_worker
            worker.check_connected()
            obj, owner_id, owner_address = (
                worker.core_worker.serialize_and_promote_object_id(obj))
            obj = id_serializer(obj)
            owner_id = id_serializer(owner_id) if owner_id else owner_id
            return obj, owner_id, owner_address

        def object_id_deserializer(serialized_obj):
            obj_id, owner_id, owner_address = serialized_obj
            # NOTE(swang): Must deserialize the object first before asking
            # the core worker to resolve the value. This is to make sure
            # that the ref count for the ObjectID is greater than 0 by the
            # time the core worker resolves the value of the object.
            deserialized_object_id = id_deserializer(obj_id)
            # TODO(edoakes): we should be able to just capture a reference
            # to 'self' here instead, but this function is itself pickled
            # somewhere, which causes an error.
            context = ray.worker.global_worker.get_serialization_context()
            if owner_id:
                worker = ray.worker.global_worker
                worker.check_connected()
                # UniqueIDs are serialized as
                # (class name, (unique bytes,)).
                outer_id = context.get_outer_object_id()
                # outer_id is None in the case that this ObjectID was closed
                # over in a function or pickled directly using pickle.dumps().
                if outer_id is None:
                    outer_id = ray.ObjectID.nil()
                worker.core_worker.deserialize_and_register_object_id(
                    obj_id[1][0], outer_id, owner_id[1][0], owner_address)
            return deserialized_object_id

        for id_type in ray._raylet._ID_TYPES:
            if id_type == ray._raylet.ObjectID:
                self._register_cloudpickle_serializer(
                    id_type, object_id_serializer, object_id_deserializer)
            else:
                self._register_cloudpickle_serializer(id_type, id_serializer,
                                                      id_deserializer)

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

    def set_outer_object_id(self, outer_object_id):
        self._thread_local.outer_object_id = outer_object_id

    def get_outer_object_id(self):
        return getattr(self._thread_local, "outer_object_id", None)

    def get_and_clear_contained_object_ids(self):
        if not hasattr(self._thread_local, "object_ids"):
            self._thread_local.object_ids = set()
            return set()

        object_ids = self._thread_local.object_ids
        self._thread_local.object_ids = set()
        return object_ids

    def add_contained_object_id(self, object_id):
        if self.is_in_band_serialization():
            # This object ID is being stored in an object. Add the ID to the
            # list of IDs contained in the object so that we keep the inner
            # object value alive as long as the outer object is in scope.
            if not hasattr(self._thread_local, "object_ids"):
                self._thread_local.object_ids = set()
            self._thread_local.object_ids.add(object_id)
        else:
            # If this serialization is out-of-band (e.g., from a call to
            # cloudpickle directly or captured in a remote function/actor),
            # then pin the object for the lifetime of this worker by adding
            # a local reference that won't ever be removed.
            ray.worker.global_worker.core_worker.add_object_id_reference(
                object_id)

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

    def _deserialize_msgpack_data(self, data, metadata):
        msgpack_data, pickle5_data = split_buffer(data)

        if metadata == ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE:
            python_objects = []
        else:
            python_objects = self._deserialize_pickle5_data(pickle5_data)

        try:

            def _python_deserializer(index):
                return python_objects[index]

            obj = MessagePackSerializer.loads(msgpack_data,
                                              _python_deserializer)
        except Exception:
            raise DeserializationError()
        return obj

    def _deserialize_object(self, data, metadata, object_id):
        if metadata:
            if metadata in [
                    ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE,
                    ray_constants.OBJECT_METADATA_TYPE_PYTHON
            ]:
                return self._deserialize_msgpack_data(data, metadata)
            # Check if the object should be returned as raw bytes.
            if metadata == ray_constants.OBJECT_METADATA_TYPE_RAW:
                if data is None:
                    return b""
                return data.to_pybytes()
            # Otherwise, return an exception object based on
            # the error type.
            try:
                error_type = int(metadata)
            except Exception:
                raise Exception(
                    "Can't deserialize object: {}, metadata: {}".format(
                        object_id, metadata))

            # RayTaskError is serialized with pickle5 in the data field.
            # TODO (kfstorm): exception serialization should be language
            # independent.
            if error_type == ErrorType.Value("TASK_EXECUTION_EXCEPTION"):
                obj = self._deserialize_msgpack_data(data, metadata)
                assert isinstance(obj, RayTaskError)
                return obj
            elif error_type == ErrorType.Value("WORKER_DIED"):
                return RayWorkerError()
            elif error_type == ErrorType.Value("ACTOR_DIED"):
                return RayActorError()
            elif error_type == ErrorType.Value("TASK_CANCELLED"):
                return RayCancellationError()
            elif error_type == ErrorType.Value("OBJECT_UNRECONSTRUCTABLE"):
                return UnreconstructableError(ray.ObjectID(object_id.binary()))
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
                            object_ids,
                            error_timeout=10):
        assert len(data_metadata_pairs) == len(object_ids)

        start_time = time.time()
        results = []
        warning_sent = False
        i = 0
        while i < len(object_ids):
            object_id = object_ids[i]
            data, metadata = data_metadata_pairs[i]
            assert self.get_outer_object_id() is None
            self.set_outer_object_id(object_id)
            try:
                results.append(
                    self._deserialize_object(data, metadata, object_id))
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
                # Must clear ObjectID to not hold a reference.
                self.set_outer_object_id(None)

        return results

    def _serialize_to_pickle5(self, metadata, value):
        writer = Pickle5Writer()
        # TODO(swang): Check that contained_object_ids is empty.
        try:
            self.set_in_band_serialization()
            inband = pickle.dumps(
                value, protocol=5, buffer_callback=writer.buffer_callback)
        except Exception as e:
            self.get_and_clear_contained_object_ids()
            raise e
        finally:
            self.set_out_of_band_serialization()

        return Pickle5SerializedObject(
            metadata, inband, writer,
            self.get_and_clear_contained_object_ids())

    def _serialize_to_msgpack(self, metadata, value):
        python_objects = []

        def _python_serializer(o):
            index = len(python_objects)
            python_objects.append(o)
            return index

        msgpack_data = MessagePackSerializer.dumps(value, _python_serializer)

        if python_objects:
            pickle5_serialized_object = \
                self._serialize_to_pickle5(metadata, python_objects)
        else:
            metadata = ray_constants.OBJECT_METADATA_TYPE_CROSS_LANGUAGE
            pickle5_serialized_object = None

        return MessagePackSerializedObject(metadata, msgpack_data,
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
            # Only RayTaskError is possible to be serialized here. We don't
            # need to deal with other exception types here.
            if isinstance(value, RayTaskError):
                metadata = str(ErrorType.Value(
                    "TASK_EXECUTION_EXCEPTION")).encode("ascii")
            else:
                metadata = ray_constants.OBJECT_METADATA_TYPE_PYTHON

            return self._serialize_to_msgpack(metadata, value)

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
                        "for '{}'. Provide a unique class_id.".format(cls))
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
