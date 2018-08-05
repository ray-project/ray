import colorama

class RayTaskError(Exception):
    """An object used internally to represent a task that threw an exception.

    If a task throws an exception during execution, a RayTaskError is stored in
    the object store for each of the task's outputs. When an object is
    retrieved from the object store, the Python method that retrieved it checks
    to see if the object is a RayTaskError and if it is then an exception is
    thrown propagating the error message.

    Currently, we either use the exception attribute or the traceback attribute
    but not both.

    Attributes:
        function_name (str): The name of the function that failed and produced
            the RayTaskError.
        exception (Exception): The exception object thrown by the failed task.
        traceback_str (str): The traceback from the exception.
    """

    def __init__(self, function_name, exception, traceback_str):
        """Initialize a RayTaskError."""
        self.function_name = function_name
        if (isinstance(exception, RayGetError)
                or isinstance(exception, RayGetArgumentError)):
            self.exception = exception
        else:
            self.exception = None
        self.traceback_str = traceback_str

    def __str__(self):
        import traceback
        traceback.print_stack()

        """Format a RayTaskError as a string."""
        if self.traceback_str is None:
            # This path is taken if getting the task arguments failed.
            return ("Remote function {}{}{} failed with:\n\n{}".format(
                colorama.Fore.RED, self.function_name, colorama.Fore.RESET,
                self.exception))
        else:
            # This path is taken if the task execution failed.
            return ("Remote function {}{}{} failed with:\n\n{}".format(
                colorama.Fore.RED, self.function_name, colorama.Fore.RESET,
                self.traceback_str))


class RayGetError(Exception):
    """An exception used when get is called on an output of a failed task.

    Attributes:
        objectid (lib.ObjectID): The ObjectID that get was called on.
        task_error (RayTaskError): The RayTaskError object created by the
            failed task.
    """

    def __init__(self, objectid, task_error):
        """Initialize a RayGetError object."""
        self.objectid = objectid
        self.task_error = task_error

    def __str__(self):
        """Format a RayGetError as a string."""
        return ("Could not get objectid {}. It was created by remote function "
                "{}{}{} which failed with:\n\n{}".format(
                    self.objectid, colorama.Fore.RED,
                    self.task_error.function_name, colorama.Fore.RESET,
                    self.task_error))


class RayGetArgumentError(Exception):
    """An exception used when a task's argument was produced by a failed task.

    Attributes:
        argument_index (int): The index (zero indexed) of the failed argument
            in present task's remote function call.
        function_name (str): The name of the function for the current task.
        objectid (lib.ObjectID): The ObjectID that was passed in as the
            argument.
        task_error (RayTaskError): The RayTaskError object created by the
            failed task.
    """

    def __init__(self, function_name, argument_index, objectid, task_error):
        """Initialize a RayGetArgumentError object."""
        self.argument_index = argument_index
        self.function_name = function_name
        self.objectid = objectid
        self.task_error = task_error

    def __str__(self):
        """Format a RayGetArgumentError as a string."""
        return ("Failed to get objectid {} as argument {} for remote function "
                "{}{}{}. It was created by remote function {}{}{} which "
                "failed with:\n{}".format(
                    self.objectid, self.argument_index, colorama.Fore.RED,
                    self.function_name, colorama.Fore.RESET, colorama.Fore.RED,
                    self.task_error.function_name, colorama.Fore.RESET,
                    self.task_error))