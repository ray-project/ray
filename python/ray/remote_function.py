from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from functools import wraps

from ray.function_manager import FunctionDescriptor
import ray.signature

# Default parameters for remote functions.
DEFAULT_REMOTE_FUNCTION_CPUS = 1
DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS = 1
DEFAULT_REMOTE_FUNCTION_MAX_CALLS = 0

logger = logging.getLogger(__name__)


class RemoteFunction(object):
    """A remote function.

    This is a decorated function. It can be used to spawn tasks.

    Attributes:
        _function: The original function.
        _function_descriptor: The function descriptor.
        _function_name: The module and function name.
        _num_cpus: The default number of CPUs to use for invocations of this
            remote function.
        _num_gpus: The default number of GPUs to use for invocations of this
            remote function.
        _memory: The heap memory request for this task.
        _object_store_memory: The object store memory request for this task.
        _resources: The default custom resource requirements for invocations of
            this remote function.
        _num_return_vals: The default number of return values for invocations
            of this remote function.
        _max_calls: The number of times a worker can execute this function
            before executing.
        _decorator: An optional decorator that should be applied to the remote
            function invocation (as opposed to the function execution) before
            invoking the function. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectIDs. For an example, see
            "test_decorated_function" in "python/ray/tests/test_basic.py".
        _function_signature: The function signature.
        _last_export_session_and_job: A pair of the last exported session
            and job to help us to know whether this function was exported.
            This is an imperfect mechanism used to determine if we need to
            export the remote function again. It is imperfect in the sense that
            the actor class definition could be exported multiple times by
            different workers.
    """

    def __init__(self, function, num_cpus, num_gpus, memory,
                 object_store_memory, resources, num_return_vals, max_calls):
        self._function = function
        self._function_descriptor = FunctionDescriptor.from_function(function)
        self._function_name = (
            self._function.__module__ + "." + self._function.__name__)
        self._num_cpus = (DEFAULT_REMOTE_FUNCTION_CPUS
                          if num_cpus is None else num_cpus)
        self._num_gpus = num_gpus
        self._memory = memory
        if object_store_memory is not None:
            raise NotImplementedError(
                "setting object_store_memory is not implemented for tasks")
        self._object_store_memory = None
        self._resources = resources
        self._num_return_vals = (DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS if
                                 num_return_vals is None else num_return_vals)
        self._max_calls = (DEFAULT_REMOTE_FUNCTION_MAX_CALLS
                           if max_calls is None else max_calls)
        self._decorator = getattr(function, "__ray_invocation_decorator__",
                                  None)

        ray.signature.check_signature_supported(self._function)
        self._function_signature = ray.signature.extract_signature(
            self._function)
        self._last_export_session_and_job = None
        # Override task.remote's signature and docstring
        @wraps(function)
        def _remote_proxy(*args, **kwargs):
            return self._remote(args=args, kwargs=kwargs)

        self.remote = _remote_proxy

    def __call__(self, *args, **kwargs):
        raise Exception("Remote functions cannot be called directly. Instead "
                        "of running '{}()', try '{}.remote()'.".format(
                            self._function_name, self._function_name))

    def _submit(self,
                args=None,
                kwargs=None,
                num_return_vals=None,
                num_cpus=None,
                num_gpus=None,
                resources=None):
        logger.warning(
            "WARNING: _submit() is being deprecated. Please use _remote().")
        return self._remote(
            args=args,
            kwargs=kwargs,
            num_return_vals=num_return_vals,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            resources=resources)

    def _remote(self,
                args=None,
                kwargs=None,
                num_return_vals=None,
                num_cpus=None,
                num_gpus=None,
                memory=None,
                object_store_memory=None,
                resources=None):
        """An experimental alternate way to submit remote functions."""
        worker = ray.worker.get_global_worker()
        worker.check_connected()

        if self._last_export_session_and_job != worker.current_session_and_job:
            # If this function was not exported in this session and job,
            # we need to export this function again, because current GCS
            # doesn't have it.
            self._last_export_session_and_job = worker.current_session_and_job
            worker.function_actor_manager.export(self)

        kwargs = {} if kwargs is None else kwargs
        args = [] if args is None else args

        if num_return_vals is None:
            num_return_vals = self._num_return_vals

        resources = ray.utils.resources_from_resource_arguments(
            self._num_cpus, self._num_gpus, self._memory,
            self._object_store_memory, self._resources, num_cpus, num_gpus,
            memory, object_store_memory, resources)

        def invocation(args, kwargs):
            args = ray.signature.extend_args(self._function_signature, args,
                                             kwargs)

            if worker.mode == ray.worker.LOCAL_MODE:
                object_ids = worker.local_mode_manager.execute(
                    self._function, self._function_descriptor, args,
                    num_return_vals)
            else:
                object_ids = worker.submit_task(
                    self._function_descriptor,
                    args,
                    num_return_vals=num_return_vals,
                    resources=resources)

            if len(object_ids) == 1:
                return object_ids[0]
            elif len(object_ids) > 1:
                return object_ids

        if self._decorator is not None:
            invocation = self._decorator(invocation)

        return invocation(args, kwargs)
