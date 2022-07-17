import inspect
import logging
import os
import uuid
from functools import wraps

import ray._private.signature
from ray import Language
from ray import cloudpickle as pickle
from ray import cross_language
from ray._private import ray_option_utils
from ray._private.client_mode_hook import (
    client_mode_convert_function,
    client_mode_should_convert,
)
from ray._private.utils import get_runtime_env_info, parse_runtime_env
from ray._raylet import PythonFunctionDescriptor
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray.util.placement_group import _configure_placement_group_based_on_context
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ray.util.tracing.tracing_helper import (
    _inject_tracing_into_function,
    _tracing_task_invocation,
)

logger = logging.getLogger(__name__)


# Hook to call with (fn, resources, strategy) on each local task submission.
_task_launch_hook = None


@PublicAPI
class RemoteFunction:
    """A remote function.

    This is a decorated function. It can be used to spawn tasks.

    Attributes:
        _language: The target language.
        _function: The original function.
        _function_descriptor: The function descriptor. This is not defined
            until the remote function is first invoked because that is when the
            function is pickled, and the pickled function is used to compute
            the function descriptor.
        _function_name: The module and function name.
        _num_cpus: The default number of CPUs to use for invocations of this
            remote function.
        _num_gpus: The default number of GPUs to use for invocations of this
            remote function.
        _memory: The heap memory request for this task.
        _object_store_memory: The object store memory request for this task.
        _resources: The default custom resource requirements for invocations of
            this remote function.
        _num_returns: The default number of return values for invocations
            of this remote function.
        _max_calls: The number of times a worker can execute this function
            before exiting.
        _max_retries: The number of times this task may be retried
            on worker failure.
        _retry_exceptions: Whether application-level errors should be retried.
        _runtime_env: The runtime environment for this task.
        _decorator: An optional decorator that should be applied to the remote
            function invocation (as opposed to the function execution) before
            invoking the function. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectRefs. For an example, see
            "test_decorated_function" in "python/ray/tests/test_basic.py".
        _function_signature: The function signature.
        _last_export_session_and_job: A pair of the last exported session
            and job to help us to know whether this function was exported.
            This is an imperfect mechanism used to determine if we need to
            export the remote function again. It is imperfect in the sense that
            the actor class definition could be exported multiple times by
            different workers.
        _scheduling_strategy: Strategy about how to schedule
            this remote function.
    """

    def __init__(
        self,
        language,
        function,
        function_descriptor,
        task_options,
    ):
        if inspect.iscoroutinefunction(function):
            raise ValueError(
                "'async def' should not be used for remote tasks. You can wrap the "
                "async function with `asyncio.get_event_loop.run_until(f())`. "
                "See more at https://docs.ray.io/en/latest/ray-core/async_api.html#asyncio-for-remote-tasks"  # noqa
            )
        self._default_options = task_options

        # TODO(suquark): This is a workaround for class attributes of options.
        # They are being used in some other places, mostly tests. Need cleanup later.
        # E.g., actors uses "__ray_metadata__" to collect options, we can so something
        # similar for remote functions.
        for k, v in ray_option_utils.task_options.items():
            setattr(self, "_" + k, task_options.get(k, v.default_value))
        self._runtime_env = parse_runtime_env(self._runtime_env)
        if "runtime_env" in self._default_options:
            self._default_options["runtime_env"] = self._runtime_env

        self._language = language
        self._function = _inject_tracing_into_function(function)
        self._function_name = function.__module__ + "." + function.__name__
        self._function_descriptor = function_descriptor
        self._is_cross_language = language != Language.PYTHON
        self._decorator = getattr(function, "__ray_invocation_decorator__", None)
        self._function_signature = ray._private.signature.extract_signature(
            self._function
        )
        self._last_export_session_and_job = None
        self._uuid = uuid.uuid4()

        # Override task.remote's signature and docstring
        @wraps(function)
        def _remote_proxy(*args, **kwargs):
            return self._remote(args=args, kwargs=kwargs, **self._default_options)

        self.remote = _remote_proxy

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote functions cannot be called directly. Instead "
            f"of running '{self._function_name}()', "
            f"try '{self._function_name}.remote()'."
        )

    def options(self, **task_options):
        """Configures and overrides the task invocation parameters.

        The arguments are the same as those that can be passed to :obj:`ray.remote`.
        Overriding `max_calls` is not supported.

        Examples:

        .. code-block:: python

            @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
            def f():
               return 1, 2
            # Task f will require 2 gpus instead of 1.
            g = f.options(num_gpus=2)
        """

        func_cls = self

        # override original options
        default_options = self._default_options.copy()
        # max_calls could not be used in ".options()", we should remove it before
        # merging options from '@ray.remote'.
        default_options.pop("max_calls", None)
        updated_options = ray_option_utils.update_options(default_options, task_options)
        ray_option_utils.validate_task_options(updated_options, in_options=True)

        # only update runtime_env when ".options()" specifies new runtime_env
        if "runtime_env" in task_options:
            updated_options["runtime_env"] = parse_runtime_env(
                updated_options["runtime_env"]
            )

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                return func_cls._remote(args=args, kwargs=kwargs, **updated_options)

            @DeveloperAPI
            def bind(self, *args, **kwargs):
                """
                For Ray DAG building that creates static graph from decorated
                class or functions.
                """
                from ray.dag.function_node import FunctionNode

                return FunctionNode(func_cls._function, args, kwargs, updated_options)

        return FuncWrapper()

    @_tracing_task_invocation
    def _remote(self, args=None, kwargs=None, **task_options):
        """Submit the remote function for execution."""
        # We pop the "max_calls" coming from "@ray.remote" here. We no longer need
        # it in "_remote()".
        task_options.pop("max_calls", None)
        if client_mode_should_convert(auto_init=True):
            return client_mode_convert_function(self, args, kwargs, **task_options)

        worker = ray._private.worker.global_worker
        worker.check_connected()

        # If this function was not exported in this session and job, we need to
        # export this function again, because the current GCS doesn't have it.
        if (
            not self._is_cross_language
            and self._last_export_session_and_job != worker.current_session_and_job
        ):
            self._function_descriptor = PythonFunctionDescriptor.from_function(
                self._function, self._uuid
            )
            # There is an interesting question here. If the remote function is
            # used by a subsequent driver (in the same script), should the
            # second driver pickle the function again? If yes, then the remote
            # function definition can differ in the second driver (e.g., if
            # variables in its closure have changed). We probably want the
            # behavior of the remote function in the second driver to be
            # independent of whether or not the function was invoked by the
            # first driver. This is an argument for repickling the function,
            # which we do here.
            try:
                self._pickled_function = pickle.dumps(self._function)
            except TypeError as e:
                msg = (
                    "Could not serialize the function "
                    f"{self._function_descriptor.repr}. Check "
                    "https://docs.ray.io/en/master/ray-core/objects/serialization.html#troubleshooting "  # noqa
                    "for more information."
                )
                raise TypeError(msg) from e

            self._last_export_session_and_job = worker.current_session_and_job
            worker.function_actor_manager.export(self)

        kwargs = {} if kwargs is None else kwargs
        args = [] if args is None else args

        # fill task required options
        for k, v in ray_option_utils.task_options.items():
            if k == "max_retries":
                # TODO(swang): We need to override max_retries here because the default
                # value gets set at Ray import time. Ideally, we should allow setting
                # default values from env vars for other options too.
                v.default_value = os.environ.get(
                    "RAY_TASK_MAX_RETRIES", v.default_value
                )
                v.default_value = int(v.default_value)
            task_options[k] = task_options.get(k, v.default_value)
        # "max_calls" already takes effects and should not apply again.
        # Remove the default value here.
        task_options.pop("max_calls", None)

        # TODO(suquark): cleanup these fields
        name = task_options["name"]
        runtime_env = parse_runtime_env(task_options["runtime_env"])
        placement_group = task_options["placement_group"]
        placement_group_bundle_index = task_options["placement_group_bundle_index"]
        placement_group_capture_child_tasks = task_options[
            "placement_group_capture_child_tasks"
        ]
        scheduling_strategy = task_options["scheduling_strategy"]
        num_returns = task_options["num_returns"]
        max_retries = task_options["max_retries"]
        retry_exceptions = task_options["retry_exceptions"]

        resources = ray._private.utils.resources_from_ray_options(task_options)

        if scheduling_strategy is None or isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ):
            if isinstance(scheduling_strategy, PlacementGroupSchedulingStrategy):
                placement_group = scheduling_strategy.placement_group
                placement_group_bundle_index = (
                    scheduling_strategy.placement_group_bundle_index
                )
                placement_group_capture_child_tasks = (
                    scheduling_strategy.placement_group_capture_child_tasks
                )

            if placement_group_capture_child_tasks is None:
                placement_group_capture_child_tasks = (
                    worker.should_capture_child_tasks_in_placement_group
                )
            placement_group = _configure_placement_group_based_on_context(
                placement_group_capture_child_tasks,
                placement_group_bundle_index,
                resources,
                {},  # no placement_resources for tasks
                self._function_descriptor.function_name,
                placement_group=placement_group,
            )
            if not placement_group.is_empty:
                scheduling_strategy = PlacementGroupSchedulingStrategy(
                    placement_group,
                    placement_group_bundle_index,
                    placement_group_capture_child_tasks,
                )
            else:
                scheduling_strategy = "DEFAULT"

        serialized_runtime_env_info = None
        if runtime_env is not None:
            serialized_runtime_env_info = get_runtime_env_info(
                runtime_env,
                is_job_runtime_env=False,
                serialize=True,
            )

        if _task_launch_hook:
            _task_launch_hook(self._function_descriptor, resources, scheduling_strategy)

        def invocation(args, kwargs):
            if self._is_cross_language:
                list_args = cross_language._format_args(worker, args, kwargs)
            elif not args and not kwargs and not self._function_signature:
                list_args = []
            else:
                list_args = ray._private.signature.flatten_args(
                    self._function_signature, args, kwargs
                )

            if worker.mode == ray._private.worker.LOCAL_MODE:
                assert (
                    not self._is_cross_language
                ), "Cross language remote function cannot be executed locally."
            object_refs = worker.core_worker.submit_task(
                self._language,
                self._function_descriptor,
                list_args,
                name if name is not None else "",
                num_returns,
                resources,
                max_retries,
                retry_exceptions,
                scheduling_strategy,
                worker.debugger_breakpoint,
                serialized_runtime_env_info or "{}",
            )
            # Reset worker's debug context from the last "remote" command
            # (which applies only to this .remote call).
            worker.debugger_breakpoint = b""
            if len(object_refs) == 1:
                return object_refs[0]
            elif len(object_refs) > 1:
                return object_refs

        if self._decorator is not None:
            invocation = self._decorator(invocation)

        return invocation(args, kwargs)

    @DeveloperAPI
    def bind(self, *args, **kwargs):
        """
        For Ray DAG building that creates static graph from decorated
        class or functions.
        """

        from ray.dag.function_node import FunctionNode

        return FunctionNode(self._function, args, kwargs, self._default_options)
