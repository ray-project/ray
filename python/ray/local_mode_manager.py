from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import traceback

from pyarrow import PlasmaObjectExists
from ray import ObjectID, ray_constants
from ray.utils import format_error_message
from ray.exceptions import RayTaskError


class LocalModeManager(object):
    """A class used by the worker process to emulate remote operations when
    running in local mode

    Attributes:
        object_store (dict): Local dictionary used to map ObjectIDs to objects.
    """

    def __init__(self):
        """Initialize a LocalModeManager."""
        self.object_store = {}

    def execute(self, function, function_descriptor, args, num_return_vals):
        """Synchronously executes a "remote" function or actor method.

        Stores results in the local object store and returns ObjectIDs. Any
        exceptions raised during function execution will be stored under all
        ObjectIDs corresponding to the function and later raised by the worker.

        Args:
            function: The function to execute.
            function_descriptor: Metadata about the function.
            args: Arguments to the function. These will not be modified by
                the function execution.
            num_return_vals: Number of expected return values specified in the
                function's decorator.

        Returns:
            The ObjectIDs corresponding to the function return values.
        """
        object_ids = [ObjectID.from_random() for _ in range(num_return_vals)]
        try:
            results = function(*copy.deepcopy(args))
            if num_return_vals == 1:
                self.object_store[object_ids[0]] = results
            else:
                for object_id, result in zip(object_ids, results):
                    self.object_store[object_id] = result
        except Exception:
            function_name = function_descriptor.function_name
            backtrace = format_error_message(traceback.format_exc())
            task_error = RayTaskError(function_name, backtrace)
            for object_id in object_ids:
                self.object_store[object_id] = task_error

        return object_ids

    def put_object(self, object_id, value):
        """Store an object in the local object store.

        Args:
            object_id: The ObjectID to store the value under. An existing value
                for this ID will be overwritten if it exists.

        Raises:
            pyarrow.PlasmaObjectExists if the ObjectID exists in the store.
        """
        if object_id in self.object_store:
            raise PlasmaObjectExists()

        self.object_store[object_id] = value

    def get_object(self, object_ids):
        """Fetch objects from the local object store.

        Args:
            object_ids: A list of ObjectIDs to fetch values for.

        Raises:
            KeyError if any of the ObjectIDs do not exist in the object store.
        """
        return [self.object_store[object_id] for object_id in object_ids]

    def free(self, object_ids):
        """Delete objects from the local object store.

        Args:
            object_ids: A list of ObjectIDs to delete.
        """
        for object_id in object_ids:
            if object_id in self.object_store:
                del self.object_store[object_id]
