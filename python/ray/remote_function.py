import inspect
import logging
import os
import uuid
from functools import wraps
from threading import Lock
from typing import Optional

import ray._private.signature
from ray import Language, cross_language
from ray._private import ray_option_utils
from ray._private.auto_init_hook import wrap_auto_init
from ray._private.client_mode_hook import (
    client_mode_convert_function,
    client_mode_should_convert,
)
from ray._private.ray_option_utils import _warn_if_using_deprecated_placement_group
from ray._private.serialization import pickle_dumps
from ray._private.utils import get_runtime_env_info, parse_runtime_env
from ray._raylet import (
    STREAMING_GENERATOR_RETURN,
    ObjectRefGenerator,
    PythonFunctionDescriptor,
)
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
        _memory: The heap memory request in bytes for this task/actor,
            rounded down to the nearest integer.
        _resources: The default custom resource requirements for invocations of
            this remote function.
        _num_returns: The default number of return values for invocations
            of this remote function.
        _max_calls: The number of times a worker can execute this function
            before exiting.
        _max_retries: The number of times this task may be retried
            on worker failure.
        _retry_exceptions: Whether application-level errors should be retried.
            This can be a boolean or a list/tuple of exceptions that should be retried.
        _runtime_env: The runtime environment for this task.
        _decorator: An optional decorator that should be applied to the remote
            function invocation (as opposed to the function execution) before
            invoking the function. The decorator must return a function that
            takes in two arguments ("args" and "kwargs"). In most cases, it
            should call the function that was passed into the decorator and
            return the resulting ObjectRefs. For an example, see
            "test_decorated_function" in "python/ray/tests/test_basic.py".
        _function_signature: The function signature.
        _last_export_cluster_and_job: A pair of the last exported cluster
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
                "async function with `asyncio.run(f())`. See more at:"
                "https://docs.ray.io/en/latest/ray-core/actors/async_api.html "
            )
        self._default_options = task_options

        # When gpu is used, set the task non-recyclable by default.
        # https://github.com/ray-project/ray/issues/29624 for more context.
        # Note: Ray task worker process is not being reused when nsight
        # profiler is running, as nsight generate report once the process exit.
        num_gpus = self._default_options.get("num_gpus") or 0
        if (
            num_gpus > 0 and self._default_options.get("max_calls", None) is None
        ) or "nsight" in (self._default_options.get("runtime_env") or {}):
            self._default_options["max_calls"] = 1

        # TODO(suquark): This is a workaround for class attributes of options.
        # They are being used in some other places, mostly tests. Need cleanup later.
        # E.g., actors uses "__ray_metadata__" to collect options, we can so something
        # similar for remote functions.
        for k, v in ray_option_utils.task_options.items():
            setattr(self, "_" + k, task_options.get(k, v.default_value))
        self._runtime_env = parse_runtime_env(self._runtime_env)
        if "runtime_env" in self._default_options:
            self._default_options["runtime_env"] = self._runtime_env

        # Pre-calculate runtime env info, to avoid re-calculation at `remote`
        # invocation. When `remote` call has specified extra `option` field,
        # runtime env will be overwritten and re-serialized.
        #
        # Caveat: To support dynamic runtime envs in
        # `func.option(runtime_env={...}).remote()`, we recalculate the serialized
        # runtime env info in the `option` call. But it's acceptable since
        # pre-calculation here only happens once at `RemoteFunction` initialization.
        self._serialized_base_runtime_env_info = ""
        if self._runtime_env:
            self._serialized_base_runtime_env_info = get_runtime_env_info(
                self._runtime_env,
                is_job_runtime_env=False,
                serialize=True,
            )

        self._language = language
        self._is_generator = inspect.isgeneratorfunction(function)
        self._function = function
        self._function_signature = None
        # Guards trace injection to enforce exactly once semantics
        self._inject_lock = Lock()
        self._function_name = function.__module__ + "." + function.__name__
        self._function_descriptor = function_descriptor
        self._is_cross_language = language != Language.PYTHON
        self._decorator = getattr(function, "__ray_invocation_decorator__", None)
        self._last_export_cluster_and_job = None
        self._uuid = uuid.uuid4()

        # Override task.remote's signature and docstring
        @wraps(function)
        def _remote_proxy(*args, **kwargs):
            return self._remote(
                serialized_runtime_env_info=self._serialized_base_runtime_env_info,
                args=args,
                kwargs=kwargs,
                **self._default_options,
            )

        self.remote = _remote_proxy

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "Remote functions cannot be called directly. Instead "
            f"of running '{self._function_name}()', "
            f"try '{self._function_name}.remote()'."
        )

    # Lock is not picklable
    def __getstate__(self):
        attrs = self.__dict__.copy()
        del attrs["_inject_lock"]
        return attrs

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.__dict__["_inject_lock"] = Lock()

    def options(self, **task_options):
        """Configures and overrides the task invocation parameters.

        The arguments are the same as those that can be passed to :obj:`ray.remote`.
        Overriding `max_calls` is not supported.

        Args:
            num_returns: It specifies the number of object refs returned by
                the remote function invocation.
            num_cpus: The quantity of CPU cores to reserve
                for this task or for the lifetime of the actor.
            num_gpus: The quantity of GPUs to reserve
                for this task or for the lifetime of the actor.
            resources (Dict[str, float]): The quantity of various custom resources
                to reserve for this task or for the lifetime of the actor.
                This is a dictionary mapping strings (resource names) to floats.
            accelerator_type: If specified, requires that the task or actor run
                on a node with the specified type of accelerator.
                See :ref:`accelerator types <accelerator_types>`.
            memory: The heap memory request in bytes for this task/actor,
                rounded down to the nearest integer.
            object_store_memory: The object store memory request for actors only.
            max_calls: This specifies the
                maximum number of times that a given worker can execute
                the given remote function before it must exit
                (this can be used to address memory leaks in third-party
                libraries or to reclaim resources that cannot easily be
                released, e.g., GPU memory that was acquired by TensorFlow).
                By default this is infinite for CPU tasks and 1 for GPU tasks
                (to force GPU tasks to release resources after finishing).
            max_retries: This specifies the maximum number of times that the remote
                function should be rerun when the worker process executing it
                crashes unexpectedly. The minimum valid value is 0,
                the default is 3 (default), and a value of -1 indicates
                infinite retries.
            runtime_env (Dict[str, Any]): Specifies the runtime environment for
                this actor or task and its children. See
                :ref:`runtime-environments` for detailed documentation.
            retry_exceptions: This specifies whether application-level errors
                should be retried up to max_retries times.
            scheduling_strategy: Strategy about how to
                schedule a remote function or actor. Possible values are
                None: ray will figure out the scheduling strategy to use, it
                will either be the PlacementGroupSchedulingStrategy using parent's
                placement group if parent has one and has
                placement_group_capture_child_tasks set to true,
                or "DEFAULT";
                "DEFAULT": default hybrid scheduling;
                "SPREAD": best effort spread scheduling;
                `PlacementGroupSchedulingStrategy`:
                placement group based scheduling;
                `NodeAffinitySchedulingStrategy`:
                node id based affinity scheduling.
            enable_task_events: This specifies whether to enable task events for this
                task. If set to True, task events such as (task running, finished)
                are emitted, and available to Ray Dashboard and State API.
                See :ref:`state-api-overview-ref` for more details.
            _metadata: Extended options for Ray libraries. For example,
                _metadata={"workflows.io/options": <workflow options>} for
                Ray workflows.
            _labels: The key-value labels of a task.

        Examples:

        .. code-block:: python

            @ray.remote(num_gpus=1, max_calls=1, num_returns=2)
            def f():
               return 1, 2
            # Task g will require 2 gpus instead of 1.
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

        # Only update runtime_env and re-calculate serialized runtime env info when
        # ".options()" specifies new runtime_env.
        serialized_runtime_env_info = self._serialized_base_runtime_env_info
        if "runtime_env" in task_options:
            updated_options["runtime_env"] = parse_runtime_env(
                updated_options["runtime_env"]
            )
            # Re-calculate runtime env info based on updated runtime env.
            if updated_options["runtime_env"]:
                serialized_runtime_env_info = get_runtime_env_info(
                    updated_options["runtime_env"],
                    is_job_runtime_env=False,
                    serialize=True,
                )

        class FuncWrapper:
            def remote(self, *args, **kwargs):
                return func_cls._remote(
                    args=args,
                    kwargs=kwargs,
                    serialized_runtime_env_info=serialized_runtime_env_info,
                    **updated_options,
                )

            @DeveloperAPI
            def bind(self, *args, **kwargs):
                """
                For Ray DAG building that creates static graph from decorated
                class or functions.
                """
                from ray.dag.function_node import FunctionNode

                return FunctionNode(func_cls._function, args, kwargs, updated_options)

        return FuncWrapper()

    @wrap_auto_init
    @_tracing_task_invocation
    def _remote(
        self,
        args=None,
        kwargs=None,
        serialized_runtime_env_info: Optional[str] = None,
        **task_options,
    ):
        """Submit the remote function for execution."""
        # We pop the "max_calls" coming from "@ray.remote" here. We no longer need
        # it in "_remote()".
        task_options.pop("max_calls", None)
        if client_mode_should_convert():
            return client_mode_convert_function(self, args, kwargs, **task_options)

        from ray.util.insight import record_control_flow

        record_control_flow(None, self._function_name.split(".")[-1])

        worker = ray._private.worker.global_worker
        worker.check_connected()

        if worker.mode != ray._private.worker.WORKER_MODE:
            # Only need to record on the driver side
            # since workers are created via tasks or actors
            # launched from the driver.
            from ray._private.usage import usage_lib

            usage_lib.record_library_usage("core")

        # We cannot do this when the function is first defined, because we need
        # ray.init() to have been called when this executes
        with self._inject_lock:
            if self._function_signature is None:
                self._function = _inject_tracing_into_function(self._function)
                self._function_signature = ray._private.signature.extract_signature(
                    self._function
                )

        # If this function was not exported in this cluster and job, we need to
        # export this function again, because the current GCS doesn't have it.
        if (
            not self._is_cross_language
            and self._last_export_cluster_and_job != worker.current_cluster_and_job
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
            self._pickled_function = pickle_dumps(
                self._function,
                f"Could not serialize the function {self._function_descriptor.repr}",
            )

            self._last_export_cluster_and_job = worker.current_cluster_and_job
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
        placement_group = task_options["placement_group"]
        placement_group_bundle_index = task_options["placement_group_bundle_index"]
        placement_group_capture_child_tasks = task_options[
            "placement_group_capture_child_tasks"
        ]
        scheduling_strategy = task_options["scheduling_strategy"]

        num_returns = task_options["num_returns"]
        if num_returns is None:
            if self._is_generator:
                num_returns = "streaming"
            else:
                num_returns = 1

        if num_returns == "dynamic":
            num_returns = -1
        elif num_returns == "streaming":
            # TODO(sang): This is a temporary private API.
            # Remove it when we migrate to the streaming generator.
            num_returns = ray._raylet.STREAMING_GENERATOR_RETURN
        generator_backpressure_num_objects = task_options[
            "_generator_backpressure_num_objects"
        ]
        if generator_backpressure_num_objects is None:
            generator_backpressure_num_objects = -1

        max_retries = task_options["max_retries"]
        retry_exceptions = task_options["retry_exceptions"]
        if isinstance(retry_exceptions, (list, tuple)):
            retry_exception_allowlist = tuple(retry_exceptions)
            retry_exceptions = True
        else:
            retry_exception_allowlist = None

        if scheduling_strategy is None or not isinstance(
            scheduling_strategy, PlacementGroupSchedulingStrategy
        ):
            _warn_if_using_deprecated_placement_group(task_options, 4)

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

        if _task_launch_hook:
            _task_launch_hook(self._function_descriptor, resources, scheduling_strategy)

        # Override enable_task_events to default for actor if not specified (i.e. None)
        enable_task_events = task_options.get("enable_task_events")
        labels = task_options.get("_labels")

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
                retry_exception_allowlist,
                scheduling_strategy,
                worker.debugger_breakpoint,
                serialized_runtime_env_info or "{}",
                generator_backpressure_num_objects,
                enable_task_events,
                labels,
            )
            # Reset worker's debug context from the last "remote" command
            # (which applies only to this .remote call).
            worker.debugger_breakpoint = b""
            if num_returns == STREAMING_GENERATOR_RETURN:
                # Streaming generator will return a single ref
                # that is for the generator task.
                assert len(object_refs) == 1
                generator_ref = object_refs[0]
                return ObjectRefGenerator(generator_ref, worker)
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
