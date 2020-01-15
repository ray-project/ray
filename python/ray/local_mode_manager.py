import copy
import traceback

import ray
from ray import ObjectID
from ray.utils import format_error_message
from ray.exceptions import RayTaskError


class LocalModeObjectID(ObjectID):
    """Wrapper class around ray.ObjectID used for local mode.

    Object values are stored directly as a field of the LocalModeObjectID.

    Attributes:
        value: Field that stores object values. If this field does not exist,
            it equates to the object not existing in the object store. This is
            necessary because None is a valid object value.
    """

    def __copy__(self):
        new = LocalModeObjectID(self.binary())
        if hasattr(self, "value"):
            new.value = self.value
        return new

    def __deepcopy__(self, memo=None):
        new = LocalModeObjectID(self.binary())
        if hasattr(self, "value"):
            new.value = self.value
        return new


class LocalModeManager:
    """Used to emulate remote operations when running in local mode."""

    def __init__(self):
        """Initialize a LocalModeManager."""

    def execute(self, function, function_name, args, kwargs, num_return_vals):
        """Synchronously executes a "remote" function or actor method.

        Stores results directly in the generated and returned
        LocalModeObjectIDs. Any exceptions raised during function execution
        will be stored under all returned object IDs and later raised by the
        worker.

        Args:
            function: The function to execute.
            function_name: Name of the function to execute.
            args: Arguments to the function. These will not be modified by
                the function execution.
            kwargs: Keyword arguments to the function.
            num_return_vals: Number of expected return values specified in the
                function's decorator.

        Returns:
            LocalModeObjectIDs corresponding to the function return values.
        """
        return_ids = [
            LocalModeObjectID.from_random() for _ in range(num_return_vals)
        ]
        new_args = []
        for i, arg in enumerate(args):
            if isinstance(arg, ObjectID):
                new_args.append(ray.get(arg))
            else:
                new_args.append(copy.deepcopy(arg))

        new_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, ObjectID):
                new_kwargs[k] = ray.get(v)
            else:
                new_kwargs[k] = copy.deepcopy(v)

        try:
            results = function(*new_args, **new_kwargs)
            if num_return_vals == 1:
                return_ids[0].value = results
            else:
                for object_id, result in zip(return_ids, results):
                    object_id.value = result
        except Exception as e:
            backtrace = format_error_message(traceback.format_exc())
            task_error = RayTaskError(function_name, backtrace, e.__class__)
            for object_id in return_ids:
                object_id.value = task_error

        return return_ids

    def put_object(self, value):
        """Store an object in the emulated object store.

        Implemented by generating a LocalModeObjectID and storing the value
        directly within it.

        Args:
            value: The value to store.

        Returns:
            LocalModeObjectID corresponding to the value.
        """
        object_id = LocalModeObjectID.from_random()
        object_id.value = value
        return object_id

    def get_objects(self, object_ids):
        """Fetch objects from the emulated object store.

        Accepts only LocalModeObjectIDs and reads values directly from them.

        Args:
            object_ids: A list of object IDs to fetch values for.

        Raises:
            TypeError if any of the object IDs are not LocalModeObjectIDs.
            KeyError if any of the object IDs do not contain values.
        """
        results = []
        for object_id in object_ids:
            if not isinstance(object_id, LocalModeObjectID):
                raise TypeError("Only LocalModeObjectIDs are supported "
                                "when running in LOCAL_MODE. Using "
                                "user-generated ObjectIDs will fail.")
            if not hasattr(object_id, "value"):
                raise KeyError("Value for {} not found".format(object_id))

            results.append(object_id.value)

        return results

    def free(self, object_ids):
        """Delete objects from the emulated object store.

        Accepts only LocalModeObjectIDs and deletes their values directly.

        Args:
            object_ids: A list of ObjectIDs to delete.

        Raises:
            TypeError if any of the object IDs are not LocalModeObjectIDs.
        """
        for object_id in object_ids:
            if not isinstance(object_id, LocalModeObjectID):
                raise TypeError("Only LocalModeObjectIDs are supported "
                                "when running in LOCAL_MODE. Using "
                                "user-generated ObjectIDs will fail.")
            try:
                del object_id.value
            except AttributeError:
                pass
