from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import ray.plasma
import ray.ray_constants as ray_constants
import ray.utils as utils

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


class ActorInfo(object):
    def __init__(self, worker):
        self.worker = worker
        self.actors = {}
        self.actor_class = None
        self.actor_task_counter = 0
        self.actor_checkpoint_interval = 0

    @property
    def actor_id(self):
        return self.worker.actor_id

    @property
    def task_driver_id(self):
        return self.worker.task_driver_id

    @property
    def redis_client(self):
        return self.worker.redis_client

    @property
    def logger(self):
        return self.worker.logger

    @property
    def checkpoint_enabled(self):
        return self.actor_checkpoint_interval > 0

    def on_checkpoint(self):
        if not self.checkpoint_enabled:
            return False
        return self.actor_task_counter % self.actor_checkpoint_interval == 0

    def get_actor_checkpoint(self, actor_id):
        """Get the most recent checkpoint associated with a given actor ID.

        Args:
            actor_id: The actor ID of the actor to get the checkpoint for.

        Returns:
            If a checkpoint exists, this returns a tuple of the number of tasks
                included in the checkpoint, the saved checkpoint state, and the
                task frontier at the time of the checkpoint. If no checkpoint
                exists, all objects are set to None.  The checkpoint index is the .
                executed on the actor before the checkpoint was made.
        """

        actor_key = b"Actor:" + actor_id
        checkpoint_index, checkpoint, frontier = self.redis_client.hmget(
            actor_key, ["checkpoint_index", "checkpoint", "frontier"])
        if checkpoint_index is not None:
            checkpoint_index = int(checkpoint_index)
        return checkpoint_index, checkpoint, frontier

    def set_actor_checkpoint(self, actor_id, checkpoint_index, checkpoint,
                             frontier):
        """Set the most recent checkpoint associated with a given actor ID.

        Args:
            actor_id: The actor ID of the actor to get the checkpoint for.
            checkpoint_index: The number of tasks included in the checkpoint.
            checkpoint: The state object to save.
            frontier: The task frontier at the time of the checkpoint.
        """

        actor_key = b"Actor:" + actor_id
        self.redis_client.hmset(
            actor_key, {
                "checkpoint_index": checkpoint_index,
                "checkpoint": checkpoint,
                "frontier": frontier,
            })

    def restore_actor_checkpoint(self):
        # Get the most recent checkpoint stored, if any.
        checkpoint_index, checkpoint, frontier = self.get_actor_checkpoint(
            self.actor_id)
        # Try to resume from the checkpoint.
        checkpoint_resumed = False
        if checkpoint_index is not None:
            # Load the actor state from the checkpoint.
            self.actors[self.worker.actor_id] = (
                self.actor_class.__ray_restore_from_checkpoint__(checkpoint))
            # Set the number of tasks executed so far.
            self.actor_task_counter = checkpoint_index
            # Set the actor frontier in the local scheduler.
            self.worker.local_scheduler_client.set_actor_frontier(frontier)
            checkpoint_resumed = True

        return checkpoint_resumed

    def save_actor_checkpoint(self, checkpoint):
        # Get the current task frontier, per actor handle.
        # NOTE(swang): This only includes actor handles that the local
        # scheduler has seen. Handle IDs for which no task has yet reached
        # the local scheduler will not be included, and may not be runnable
        # on checkpoint resumption.
        actor_id = ray.ObjectID(self.actor_id)
        frontier = self.worker.local_scheduler_client.get_actor_frontier(
            actor_id)
        # Save the checkpoint in Redis. TODO(rkn): Checkpoints
        # should not be stored in Redis. Fix this.
        self.set_actor_checkpoint(self.actor_id,
                                  checkpoint_index=self.actor_task_counter,
                                  checkpoint=checkpoint,
                                  frontier=frontier)

    def set_checkpoint_interval(self, interval):
        self.actor_checkpoint_interval = int(interval)

    def save_and_log_checkpoint(self, actor):
        """Save a checkpoint on the actor and log any errors.

        Args:
            actor: The actor to checkpoint.
            checkpoint_index: The number of tasks that have executed so far.
        """
        try:
            actor.__ray_checkpoint__()
        except Exception:
            # Log the error message.
            self.logger.push_exception_to_driver(
                ray_constants.CHECKPOINT_PUSH_ERROR,
                driver_id=self.task_driver_id.id(),
                data={
                    "actor_class": actor.__class__.__name__,
                    "function_name": actor.__ray_checkpoint__.__name__
                }, format_exc=True)

    def restore_and_log_checkpoint(self, actor):
        """Restore an actor from a checkpoint and log any errors.

        Args:
            actor: The actor to restore.
        """
        checkpoint_resumed = False
        try:
            checkpoint_resumed = actor.__ray_checkpoint_restore__()
        except Exception:
            # Log the error message.
            self.logger.push_exception_to_driver(
                ray_constants.CHECKPOINT_PUSH_ERROR,
                driver_id=self.task_driver_id.id(),
                data={
                    "actor_class": actor.__class__.__name__,
                    "function_name": actor.__ray_checkpoint_restore__.__name__
                }, format_exc=True)

        return checkpoint_resumed

    def make_actor_method_executor(self, method_name, method,
                                   actor_imported):
        """Make an executor that wraps a user-defined actor method.

        The wrapped method updates the worker's internal state and performs any
        necessary checkpointing operations.

        Args:
            method_name (str): The name of the actor method.
            method (instancemethod): The actor method to wrap. This should be a
                method defined on the actor class and should therefore take an
                instance of the actor as the first argument.
            actor_imported (bool): Whether the actor has been imported.
                Checkpointing operations will not be run if this is set to False.

        Returns:
            A function that executes the given actor method on the worker's stored
                instance of the actor. The function also updates the worker's
                internal state to record the executed method.
        """

        def actor_method_executor(dummy_return_id, actor, *args):
            # Update the actor's task counter to reflect the task we're about to
            # execute.
            self.actor_task_counter += 1

            # If this is the first task to execute on the actor, try to resume from
            # a checkpoint.
            if actor_imported and self.actor_task_counter == 1:
                checkpoint_resumed = self.restore_and_log_checkpoint(actor)
                if checkpoint_resumed:
                    # NOTE(swang): Since we did not actually execute the __init__
                    # method, this will put None as the return value. If the
                    # __init__ method is supposed to return multiple values, an
                    # exception will be logged.
                    return

            # Determine whether we should checkpoint the actor.
            # We should checkpoint the actor if user checkpointing is on, we've
            # executed checkpoint_interval tasks since the last checkpoint, and the
            # method we're about to execute is not a checkpoint.
            save_checkpoint = (actor_imported and
                               self.on_checkpoint() and
                               method_name != "__ray_checkpoint__")

            # Execute the assigned method and save a checkpoint if necessary.
            try:
                if utils.is_classmethod(method):
                    method_returns = method(*args)
                else:
                    method_returns = method(actor, *args)
            except Exception:
                # Save the checkpoint before allowing the method exception to
                # be thrown.
                if save_checkpoint:
                    self.save_and_log_checkpoint(actor)
                raise
            else:
                # Save the checkpoint before returning the method's return values.
                if save_checkpoint:
                    self.save_and_log_checkpoint(actor)
                return method_returns

        return actor_method_executor
