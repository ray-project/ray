from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import hashlib
import pyarrow

import ray.cloudpickle as pickle
import ray.dataflow.signature as signature
from ray.dataflow.exceptions import (
    RayGetError, RayGetArgumentError, RayTaskError)
import ray.plasma
from ray.services import logger
from ray.utils import random_string


class RayNotDictionarySerializable(Exception):
    pass


# This exception is used to represent situations where cloudpickle fails to
# pickle an object (cloudpickle can fail in many different ways).
class CloudPickleError(Exception):
    pass


def check_serializable(cls):
    """Throws an exception if Ray cannot serialize this class efficiently.

    Args:
        cls (type): The class to be serialized.

    Raises:
        Exception: An exception is raised if Ray cannot serialize this class
            efficiently.
    """
    if is_named_tuple(cls):
        # This case works.
        return
    if not hasattr(cls, "__new__"):
        print("The class {} does not have a '__new__' attribute and is "
              "probably an old-stye class. Please make it a new-style class "
              "by inheriting from 'object'.")
        raise RayNotDictionarySerializable("The class {} does not have a "
                                           "'__new__' attribute and is "
                                           "probably an old-style class. We "
                                           "do not support this. Please make "
                                           "it a new-style class by "
                                           "inheriting from 'object'."
                                           .format(cls))
    try:
        obj = cls.__new__(cls)
    except Exception:
        raise RayNotDictionarySerializable("The class {} has overridden "
                                           "'__new__', so Ray may not be able "
                                           "to serialize it efficiently."
                                           .format(cls))
    if not hasattr(obj, "__dict__"):
        raise RayNotDictionarySerializable("Objects of the class {} do not "
                                           "have a '__dict__' attribute, so "
                                           "Ray cannot serialize it "
                                           "efficiently.".format(cls))
    if hasattr(obj, "__slots__"):
        raise RayNotDictionarySerializable("The class {} uses '__slots__', so "
                                           "Ray may not be able to serialize "
                                           "it efficiently.".format(cls))


def is_named_tuple(cls):
    """Return True if cls is a namedtuple and False otherwise."""
    b = cls.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(cls, "_fields", None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


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


class SerializationContextMap(object):
    def __init__(self, worker):
        self.worker = worker
        # A dictionary that maps from driver id to SerializationContext
        # TODO: clean up the SerializationContext once the job finished.
        self.serialization_context_map = {}

    def fallback(self, example_object):
        try:
            self.register_custom_serializer(
                type(example_object), use_dict=True)
            warning_message = ("WARNING: Serializing objects of type "
                               "{} by expanding them as dictionaries "
                               "of their fields. This behavior may "
                               "be incorrect in some cases.".format(
                type(example_object)))
            logger.warning(warning_message)
        except (RayNotDictionarySerializable,
                CloudPickleError,
                pickle.pickle.PicklingError, Exception):
            # We also handle generic exceptions here because
            # cloudpickle can fail with many different types of errors.
            try:
                self.register_custom_serializer(
                    type(example_object), use_pickle=True)
                warning_message = ("WARNING: Falling back to "
                                   "serializing objects of type {} by "
                                   "using pickle. This may be "
                                   "inefficient.".format(
                    type(example_object)))
                logger.warning(warning_message)
            except CloudPickleError:
                self.register_custom_serializer(
                    type(example_object),
                    use_pickle=True,
                    local=True)
                warning_message = ("WARNING: Pickling the class {} "
                                   "failed, so we are using pickle "
                                   "and only registering the class "
                                   "locally.".format(
                    type(example_object)))
                logger.warning(warning_message)

    def get_serialization_context(self, driver_id):
        """Get the SerializationContext of the driver that this worker is processing.

        Args:
            driver_id: The ID of the driver that indicates which driver to get
                the serialization context for.

        Returns:
            The serialization context of the given driver.
        """
        if driver_id not in self.serialization_context_map:
            self._initialize_serialization(driver_id)
        return self.serialization_context_map[driver_id]

    def register_custom_serializer(self, cls,
                                   use_pickle=False,
                                   use_dict=False,
                                   serializer=None,
                                   deserializer=None,
                                   local=False,
                                   driver_id=None,
                                   class_id=None):
        """Enable serialization and deserialization for a particular class.

        This method runs the register_class function defined below on every worker,
        which will enable ray to properly serialize and deserialize objects of
        this class.

        Args:
            cls (type): The class that ray should use this custom serializer for.
            use_pickle (bool): If true, then objects of this class will be
                serialized using pickle.
            use_dict: If true, then objects of this class be serialized turning
                their __dict__ fields into a dictionary. Must be False if
                use_pickle is true.
            serializer: The custom serializer to use. This should be provided if
                and only if use_pickle and use_dict are False.
            deserializer: The custom deserializer to use. This should be provided
                if and only if use_pickle and use_dict are False.
            local: True if the serializers should only be registered on the current
                worker. This should usually be False.
            driver_id: ID of the driver that we want to register the class for.
            class_id: ID of the class that we are registering. If this is not
                specified, we will calculate a new one inside the function.

        Raises:
            Exception: An exception is raised if pickle=False and the class cannot
                be efficiently serialized by Ray. This can also raise an exception
                if use_dict is true and cls is not pickleable.
        """
        assert (serializer is None) == (deserializer is None), (
            "The serializer/deserializer arguments must both be provided or "
            "both not be provided.")
        use_custom_serializer = (serializer is not None)

        assert use_custom_serializer + use_pickle + use_dict == 1, (
            "Exactly one of use_pickle, use_dict, or serializer/deserializer must "
            "be specified.")

        if use_dict:
            # Raise an exception if cls cannot be serialized efficiently by Ray.
            check_serializable(cls)

        if class_id is None:
            if not local:
                # In this case, the class ID will be used to deduplicate the class
                # across workers. Note that cloudpickle unfortunately does not
                # produce deterministic strings, so these IDs could be different
                # on different workers. We could use something weaker like
                # cls.__name__, however that would run the risk of having
                # collisions.
                # TODO(rkn): We should improve this.
                try:
                    # Attempt to produce a class ID that will be the same on each
                    # worker. However, determinism is not guaranteed, and the
                    # result may be different on different workers.
                    class_id = _try_to_compute_deterministic_class_id(cls)
                except Exception as e:
                    raise CloudPickleError("Failed to pickle class "
                                           "'{}'".format(cls))
            else:
                # In this case, the class ID only needs to be meaningful on this
                # worker and not across workers.
                class_id = random_string()

        if driver_id is None:
            driver_id_bytes = self.worker.task_driver_id.id()
        else:
            driver_id_bytes = driver_id.id()

        def register_class_for_serialization(worker_info):
            # TODO(rkn): We need to be more thoughtful about what to do if custom
            # serializers have already been registered for class_id. In some cases,
            # we may want to use the last user-defined serializers and ignore
            # subsequent calls to register_custom_serializer that were made by the
            # system.

            serialization_context = worker_info[
                "worker"].get_serialization_context(
                ray.ObjectID(driver_id_bytes))
            serialization_context.register_type(
                cls,
                class_id,
                pickle=use_pickle,
                custom_serializer=serializer,
                custom_deserializer=deserializer)

        if not local:
            self.worker.run_function_on_all_workers(
                register_class_for_serialization)
        else:
            # Since we are pickling objects of this class, we don't actually need
            # to ship the class definition.
            register_class_for_serialization({"worker": self.worker})

    def _initialize_serialization(self, driver_id):
        """Initialize the serialization library.

        This defines a custom serializer for object IDs and also tells ray to
        serialize several exception classes that we define for error handling.
        """
        serialization_context = pyarrow.default_serialization_context()
        # Tell the serialization context to use the cloudpickle version that we
        # ship with Ray.
        serialization_context.set_pickle(pickle.dumps, pickle.loads)
        pyarrow.register_torch_serialization_handlers(serialization_context)

        # Define a custom serializer and deserializer for handling Object IDs.
        def object_id_custom_serializer(obj):
            return obj.id()

        def object_id_custom_deserializer(serialized_obj):
            return ray.ObjectID(serialized_obj)

        # We register this serializer on each worker instead of calling
        # register_custom_serializer from the driver so that isinstance still
        # works.
        serialization_context.register_type(
            ray.ObjectID,
            "ray.ObjectID",
            pickle=False,
            custom_serializer=object_id_custom_serializer,
            custom_deserializer=object_id_custom_deserializer)

        def actor_handle_serializer(obj):
            return obj._serialization_helper(True)

        def actor_handle_deserializer(serialized_obj):
            new_handle = ray.actor.ActorHandle.__new__(ray.actor.ActorHandle)
            new_handle._deserialization_helper(serialized_obj, True)
            return new_handle

        # We register this serializer on each worker instead of calling
        # register_custom_serializer from the driver so that isinstance still
        # works.
        serialization_context.register_type(
            ray.actor.ActorHandle,
            "ray.ActorHandle",
            pickle=False,
            custom_serializer=actor_handle_serializer,
            custom_deserializer=actor_handle_deserializer)

        self.serialization_context_map[driver_id] = serialization_context

        self.register_custom_serializer(
            RayTaskError,
            use_dict=True,
            local=True,
            driver_id=driver_id,
            class_id="ray.RayTaskError")
        self.register_custom_serializer(
            RayGetError,
            use_dict=True,
            local=True,
            driver_id=driver_id,
            class_id="ray.RayGetError")
        self.register_custom_serializer(
            RayGetArgumentError,
            use_dict=True,
            local=True,
            driver_id=driver_id,
            class_id="ray.RayGetArgumentError")
        # Tell Ray to serialize lambdas with pickle.
        self.register_custom_serializer(
            type(lambda: 0),
            use_pickle=True,
            local=True,
            driver_id=driver_id,
            class_id="lambda")
        # Tell Ray to serialize types with pickle.
        self.register_custom_serializer(
            type(int),
            use_pickle=True,
            local=True,
            driver_id=driver_id,
            class_id="type")
        # Tell Ray to serialize FunctionSignatures as dictionaries. This is
        # used when passing around actor handles.
        self.register_custom_serializer(
            signature.FunctionSignature,
            use_dict=True,
            local=True,
            driver_id=driver_id,
            class_id="ray.signature.FunctionSignature")

    def clear(self):
        self.serialization_context_map.clear()
