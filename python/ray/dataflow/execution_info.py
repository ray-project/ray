from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import ray.plasma

FunctionExecutionInfo = collections.namedtuple(
    "FunctionExecutionInfo", ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""


def _ensure_type(obj):
    if not isinstance(obj, ray.ObjectID):
        return ray.ObjectID(obj)
    else:
        return obj


class ExecutionInfo(object):
    """A class for maintaining execution information.

    function_execution_info (Dict[str, FunctionExecutionInfo]): A
            dictionary mapping the name of a remote function to the remote
            function itself. This is the set of remote functions that can be
            executed by this worker.
    """

    def __init__(self):
        # This field is a dictionary that maps a driver ID to a dictionary of
        # functions (and information about those functions) that have been
        # registered for that driver (this inner dictionary maps function IDs
        # to a FunctionExecutionInfo object. This should only be used on
        # workers that execute remote functions.
        self.function_execution_info = collections.defaultdict(lambda: {})

        # This is a dictionary mapping driver ID to a dictionary that maps
        # remote function IDs for that driver to a counter of the number of
        # times that remote function has been executed on this worker. The
        # counter is incremented every time the function is executed on this
        # worker. When the counter reaches the maximum number of executions
        # allowed for a particular function, the worker is killed.
        self.num_task_executions = collections.defaultdict(lambda: {})

    def add_function_info(self,
                          driver_id,
                          function_id,
                          function,
                          function_name,
                          max_calls,
                          reset_execution_count=True):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)

        self.function_execution_info[driver_id][function_id] = (
            FunctionExecutionInfo(
                function=function,
                function_name=function_name, max_calls=max_calls))

        if reset_execution_count:
            self.num_task_executions[driver_id][function_id] = 0

    def get_function_info(self, driver_id, function_id):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)
        function_info = self.function_execution_info[driver_id][function_id]
        return function_info

    def get_function_name(self, driver_id, function_id):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)
        return self.function_execution_info[driver_id][
            function_id].function_name

    def has_function_id(self, driver_id, function_id):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)
        return function_id in self.function_execution_info[driver_id]

    def increase_function_call_count(self, driver_id, function_id):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)
        self.num_task_executions[driver_id][function_id] += 1

    def has_reached_max_executions(self, driver_id, function_id):
        driver_id = _ensure_type(driver_id)
        function_id = _ensure_type(function_id)
        return (self.num_task_executions[driver_id][function_id] ==
                self.function_execution_info[driver_id][function_id].max_calls)
