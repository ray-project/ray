from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
import hashlib
import inspect

import ray.signature

# Default parameters for remote functions.
DEFAULT_REMOTE_FUNCTION_CPUS = 1
DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS = 1
DEFAULT_REMOTE_FUNCTION_MAX_CALLS = 0


def in_ipython():
    """Return true if we are in an IPython interpreter and false otherwise."""
    try:
        __IPYTHON__
        return True
    except NameError:
        return False


def compute_function_id(function):
    """Compute an function ID for a function.

    Args:
        func: The actual function.

    Returns:
        This returns the function ID.
    """
    function_id_hash = hashlib.sha1()
    # Include the function module and name in the hash.
    function_id_hash.update(function.__module__.encode("ascii"))
    function_id_hash.update(function.__name__.encode("ascii"))
    # If we are running a script or are in IPython, include the source code in
    # the hash. If we are in a regular Python interpreter we skip this part
    # because the source code is not accessible. If the function is a built-in
    # (e.g., Cython), the source code is not accessible.
    import __main__ as main
    if (hasattr(main, "__file__") or in_ipython()) \
            and inspect.isfunction(function):
        function_id_hash.update(inspect.getsource(function).encode("ascii"))
    # Compute the function ID.
    function_id = function_id_hash.digest()
    assert len(function_id) == 20
    function_id = ray.ObjectID(function_id)

    return function_id


class RemoteFunction(object):
    """A remote function.

    This is a decorated function. It can be used to spawn tasks.

    Attributes:
        _function: The original function.
        _function_id: The ID of the function.
        _function_name: The module and function name.
        _num_cpus: The default number of CPUs to use for invocations of this
            remote function.
        _num_gpus: The default number of GPUs to use for invocations of this
            remote function.
        _resources: The default custom resource requirements for invocations of
            this remote function.
        _num_return_vals: The default number of return values for invocations
            of this remote function.
        _max_calls: The number of times a worker can execute this function
            before executing.
        _function_signature: The function signature.
    """

    def __init__(self, function, num_cpus, num_gpus, resources,
                 num_return_vals, max_calls):
        self._function = function
        # TODO(rkn): We store the function ID as a string, so that
        # RemoteFunction objects can be pickled. We should undo this when
        # we allow ObjectIDs to be pickled.
        self._function_id = compute_function_id(self._function).id()
        self._function_name = (
            self._function.__module__ + '.' + self._function.__name__)
        self._num_cpus = (DEFAULT_REMOTE_FUNCTION_CPUS
                          if num_cpus is None else num_cpus)
        self._num_gpus = num_gpus
        self._resources = resources
        self._num_return_vals = (DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS if
                                 num_return_vals is None else num_return_vals)
        self._max_calls = (DEFAULT_REMOTE_FUNCTION_MAX_CALLS
                           if max_calls is None else max_calls)

        ray.signature.check_signature_supported(self._function)
        self._function_signature = ray.signature.extract_signature(
            self._function)

        # # Export the function.
        worker = ray.worker.get_global_worker()
        if worker.mode in [ray.worker.SCRIPT_MODE, ray.worker.SILENT_MODE]:
            self._export()
        elif worker.mode is None:
            worker.cached_remote_functions_and_actors.append(
                ("remote_function", self))

    def __call__(self, *args, **kwargs):
        raise Exception("Remote functions cannot be called directly. Instead "
                        "of running '{}()', try '{}.remote()'.".format(
                            self._function_name, self._function_name))

    def remote(self, *args, **kwargs):
        """This runs immediately when a remote function is called."""
        return self._submit(args=args, kwargs=kwargs)

    def _submit(self,
                args=None,
                kwargs=None,
                num_return_vals=None,
                num_cpus=None,
                num_gpus=None,
                resources=None):
        """An experimental alternate way to submit remote functions."""
        worker = ray.worker.get_global_worker()
        worker.check_connected()
        ray.worker.check_main_thread()
        kwargs = {} if kwargs is None else kwargs
        args = ray.signature.extend_args(self._function_signature, args,
                                         kwargs)

        if num_return_vals is None:
            num_return_vals = self._num_return_vals

        resources = ray.utils.resources_from_resource_arguments(
            self._num_cpus, self._num_gpus, self._resources, num_cpus,
            num_gpus, resources)
        if worker.mode == ray.worker.PYTHON_MODE:
            # In PYTHON_MODE, remote calls simply execute the function.
            # We copy the arguments to prevent the function call from
            # mutating them and to match the usual behavior of
            # immutable remote objects.
            result = self._function(*copy.deepcopy(args))
            return result
        object_ids = worker.submit_task(
            ray.ObjectID(self._function_id),
            args,
            num_return_vals=num_return_vals,
            resources=resources)
        if len(object_ids) == 1:
            return object_ids[0]
        elif len(object_ids) > 1:
            return object_ids

    def _export(self):
        worker = ray.worker.get_global_worker()
        worker.export_remote_function(
            ray.ObjectID(self._function_id), self._function_name,
            self._function, self._max_calls, self)
