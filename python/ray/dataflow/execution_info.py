from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import ray.plasma

FunctionExecutionInfo = collections.namedtuple(
    "FunctionExecutionInfo", ["function", "function_name", "max_calls"])
"""FunctionExecutionInfo: A named tuple storing remote function information."""

CACHED_REMOTE_FUNCTION = 'remote_function'
CACHED_ACTOR = 'actor'


def _ensure_type(obj):
    if not isinstance(obj, ray.ObjectID):
        return ray.ObjectID(obj)
    else:
        return obj


class TasksCache(object):
    """A class that caches tasks.

    Attributes:
        cached_functions_to_run (List): A list of functions to run on all of
            the workers that should be exported as soon as connect is called.
        cached_remote_functions_and_actors: A list of information for exporting
            remote functions and actor classes definitions that were defined
            before the worker called connect. When the worker eventually does
            call connect, if it is a driver, it will export these functions and
            actors. If cached_remote_functions_and_actors is None, that means
            that connect has been called already.
    """

    def __init__(self):
        self._enabled = True
        self.cached_functions_to_run = []
        self.cached_remote_functions_and_actors = []

    @property
    def cache_enabled(self):
        return self._enabled

    @cache_enabled.setter
    def cache_enabled(self, v):
        assert isinstance(v, bool)
        if v == self._enabled:
            return
        if v:
            # Begin caching functions. No works will be done.
            self.cached_functions_to_run = []
            self.cached_remote_functions_and_actors = []
        else:
            # Finish caching functions. Start to work.
            self.cached_functions_to_run = None
            self.cached_remote_functions_and_actors = None
        self._enabled = v

    def append_cached_function_to_run(self, func):
        assert self._enabled
        self.cached_functions_to_run.append(func)

    def append_cached_remote_function(self, remote_function):
        assert self._enabled
        self.cached_remote_functions_and_actors.append((CACHED_REMOTE_FUNCTION,
                                                        remote_function))

    def append_cached_actor(self, key, actor_class_info):
        assert self._enabled
        self.cached_remote_functions_and_actors.append(
            (CACHED_ACTOR, (key, actor_class_info)))

    def visit_caches(self, function_executor, remote_function_executor,
                     actor_executor):
        assert self._enabled
        if function_executor is not None:
            for function in self.cached_functions_to_run:
                function_executor(function)

        if remote_function_executor is None and actor_executor is None:
            return

        if remote_function_executor is None:
            remote_function_executor = lambda _: None
        if actor_executor is None:
            actor_executor = lambda a, b: None

        # Export cached remote functions to the workers.
        for cached_type, info in self.cached_remote_functions_and_actors:
            if cached_type == CACHED_REMOTE_FUNCTION:
                remote_function_executor(info)
            elif cached_type == CACHED_ACTOR:
                (key, actor_class_info) = info
                actor_executor(key, actor_class_info)
            else:
                assert False, "This code should be unreachable."


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

        # A set of all of the actor class keys that have been imported by the
        # import thread. It is safe to convert this worker into an actor of
        # these types.
        self.imported_actor_classes = set()

    def add_actor_class(self, actor_class):
        self.imported_actor_classes.add(actor_class)

    def has_imported_actor(self, actor_class):
        return actor_class in self.imported_actor_classes

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
                function_name=function_name,
                max_calls=max_calls))

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
