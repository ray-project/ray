from functools import wraps
import inspect
import logging
import uuid

from ray import cloudpickle as pickle
from ray.util.scheduling_strategies import (
    PlacementGroupSchedulingStrategy,
    SchedulingStrategyT,
)
from ray._raylet import PythonFunctionDescriptor
from ray import cross_language, Language
from ray._private.client_mode_hook import client_mode_convert_function
from ray._private.client_mode_hook import client_mode_should_convert
from ray.util.placement_group import configure_placement_group_based_on_context
import ray._private.signature
from ray.utils import get_runtime_env_info, parse_runtime_env
from ray.util.tracing.tracing_helper import (
    _tracing_task_invocation,
    _inject_tracing_into_function,
)


# Default parameters for remote functions.
DEFAULT_REMOTE_FUNCTION_CPUS = 1
DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS = 1
DEFAULT_REMOTE_FUNCTION_MAX_CALLS = 0
# Normal tasks may be retried on failure this many times.
# TODO(swang): Allow this to be set globally for an application.
DEFAULT_REMOTE_FUNCTION_NUM_TASK_RETRIES = 3
DEFAULT_REMOTE_FUNCTION_RETRY_EXCEPTIONS = False

logger = logging.getLogger(__name__)


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
        num_cpus,
        num_gpus,
        memory,
        object_store_memory,
        resources,
        accelerator_type,
        num_returns,
        max_calls,
        max_retries,
        retry_exceptions,
        runtime_env,
        placement_group,
        scheduling_strategy: SchedulingStrategyT,
    ):
        if inspect.iscoroutinefunction(function):
            raise ValueError(
                "'async def' should not be used for remote tasks. You can wrap the "
                "async function with `asyncio.get_event_loop.run_until(f())`. "
                "See more at https://docs.ray.io/en/latest/ray-core/async_api.html#asyncio-for-remote-tasks"  # noqa
            )
        self._language = language
        self._function = _inject_tracing_into_function(function)
        self._function_name = function.__module__ + "." + function.__name__
        self._function_descriptor = function_descriptor
        self._is_cross_language = language != Language.PYTHON
        self._num_cpus = DEFAULT_REMOTE_FUNCTION_CPUS if num_cpus is None else num_cpus
        self._num_gpus = num_gpus
        self._memory = memory
        if object_store_memory is not None:
            raise NotImplementedError(
                "setting object_store_memory is not implemented for tasks"
            )
        self._object_store_memory = None
        self._resources = resources
        self._accelerator_type = accelerator_type
        self._num_returns = (
            DEFAULT_REMOTE_FUNCTION_NUM_RETURN_VALS
            if num_returns is None
            else num_returns
        )
        self._max_calls = (
            DEFAULT_REMOTE_FUNCTION_MAX_CALLS if max_calls is None else max_calls
        )
        self._max_retries = (
            DEFAULT_REMOTE_FUNCTION_NUM_TASK_RETRIES
            if max_retries is None
            else max_retries
        )
        self._retry_exceptions = (
            DEFAULT_REMOTE_FUNCTION_RETRY_EXCEPTIONS
            if retry_exceptions is None
            else retry_exceptions
        )

        self._runtime_env = parse_runtime_env(runtime_env)

        self._placement_group = placement_group
        self._decorator = getattr(function, "__ray_invocation_decorator__", None)
        self._function_signature = ray._private.signature.extract_signature(
            self._function
        )
        self._scheduling_strategy = scheduling_strategy

        self._last_export_session_and_job = None
        self._uuid = uuid.uuid4()

        # Override task.remote's signature and docstring
        @wraps(function)
        def _remote_proxy(*args, **kwargs):
            return self._remote(args=args, kwargs=kwargs)

        self.remote = _remote_proxy

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote functions cannot be called directly. Instead "
            f"of running '{self._function_name}()', "
            f"try '{self._function_name}.remote()'."
        )

    def options(
        self,
        args=None,
        kwargs=None,
        num_returns=None,
        num_cpus=None,
        num_gpus=None,
        memory=None,
        object_store_memory=None,
        accelerator_type=None,
        resources=None,
        max_retries=None,
        retry_exceptions=None,
        placement_group="default",
        placement_group_bundle_index=-1,
        placement_group_capture_child_tasks=None,
        runtime_env=None,
        name="",
        scheduling_strategy: SchedulingStrategyT = None,
    ):
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
        new_runtime_env = parse_runtime_env(runtime_env)

        options = dict(
            num_returns=num_returns,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            object_store_memory=object_store_memory,
            accelerator_type=accelerator_type,
            resources=resources,
            max_retries=max_retries,
            retry_exceptions=retry_exceptions,
            placement_group=placement_group,
            placement_group_bundle_index=placement_group_bundle_index,
            placement_group_capture_child_tasks=(placement_group_capture_child_tasks),
            runtime_env=new_runtime_env,
            name=name,
            scheduling_strategy=scheduling_strategy,
        )

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                return func_cls._remote(args=args, kwargs=kwargs, **options)

            def bind(self, *args, **kwargs):
                """
                **Experimental**

                For ray DAG building. Implementation and interface subject to changes.
                """
                from ray.experimental.dag.function_node import FunctionNode

                return FunctionNode(
                    func_cls._function,
                    args,
                    kwargs,
                    options,
                )

        return FuncWrapper()

    @_tracing_task_invocation
    def _remote(
        self,
        args=None,
        kwargs=None,
        num_returns=None,
        num_cpus=None,
        num_gpus=None,
        memory=None,
        object_store_memory=None,
        accelerator_type=None,
        resources=None,
        max_retries=None,
        retry_exceptions=None,
        placement_group="default",
        placement_group_bundle_index=-1,
        placement_group_capture_child_tasks=None,
        runtime_env=None,
        name="",
        scheduling_strategy: SchedulingStrategyT = None,
    ):
        """Submit the remote function for execution."""

        if client_mode_should_convert(auto_init=True):
            return client_mode_convert_function(
                self,
                args,
                kwargs,
                num_returns=num_returns,
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory=memory,
                object_store_memory=object_store_memory,
                accelerator_type=accelerator_type,
                resources=resources,
                max_retries=max_retries,
                retry_exceptions=retry_exceptions,
                placement_group=placement_group,
                placement_group_bundle_index=placement_group_bundle_index,
                placement_group_capture_child_tasks=(
                    placement_group_capture_child_tasks
                ),
                runtime_env=runtime_env,
                name=name,
                scheduling_strategy=scheduling_strategy,
            )

        worker = ray.worker.global_worker
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
                    "https://docs.ray.io/en/master/serialization.html#troubleshooting "
                    "for more information."
                )
                raise TypeError(msg) from e

            self._last_export_session_and_job = worker.current_session_and_job
            worker.function_actor_manager.export(self)

        kwargs = {} if kwargs is None else kwargs
        args = [] if args is None else args

        if num_returns is None:
            num_returns = self._num_returns
        if max_retries is None:
            max_retries = self._max_retries
        if retry_exceptions is None:
            retry_exceptions = self._retry_exceptions
        if scheduling_strategy is None:
            scheduling_strategy = self._scheduling_strategy

        resources = ray._private.utils.resources_from_resource_arguments(
            self._num_cpus,
            self._num_gpus,
            self._memory,
            self._object_store_memory,
            self._resources,
            self._accelerator_type,
            num_cpus,
            num_gpus,
            memory,
            object_store_memory,
            resources,
            accelerator_type,
        )

        if (placement_group != "default") and (scheduling_strategy is not None):
            raise ValueError(
                "Placement groups should be specified via the "
                "scheduling_strategy option. "
                "The placement_group option is deprecated."
            )

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
            if placement_group == "default":
                placement_group = self._placement_group
            placement_group = configure_placement_group_based_on_context(
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

        if not runtime_env or runtime_env == "{}":
            runtime_env = self._runtime_env
        serialized_runtime_env_info = None
        if runtime_env is not None:
            serialized_runtime_env_info = get_runtime_env_info(
                runtime_env,
                is_job_runtime_env=False,
                serialize=True,
            )

        def invocation(args, kwargs):
            if self._is_cross_language:
                list_args = cross_language.format_args(worker, args, kwargs)
            elif not args and not kwargs and not self._function_signature:
                list_args = []
            else:
                list_args = ray._private.signature.flatten_args(
                    self._function_signature, args, kwargs
                )

            if worker.mode == ray.worker.LOCAL_MODE:
                assert (
                    not self._is_cross_language
                ), "Cross language remote function cannot be executed locally."
            object_refs = worker.core_worker.submit_task(
                self._language,
                self._function_descriptor,
                list_args,
                name,
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

    def bind(self, *args, **kwargs):
        """
        **Experimental**

        For ray DAG building. Implementation and interface subject to changes.
        """

        from ray.experimental.dag.function_node import FunctionNode

        return FunctionNode(self._function, args, kwargs, {})
