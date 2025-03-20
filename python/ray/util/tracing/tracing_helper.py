import importlib
import inspect
import logging
import os
from contextlib import contextmanager
from functools import wraps
from inspect import Parameter
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    cast,
)

import ray
import ray._private.worker
from ray._private.inspect_util import (
    is_class_method,
    is_function_or_method,
    is_static_method,
)
from ray.runtime_context import get_runtime_context

logger = logging.getLogger(__name__)


def _create_span(name, kind, attributes):
    span_data = {
        "name": name,
        "kind": kind,
        "attributes": attributes,
    }

    def set_attribute(self, key, value):
        span_data["attributes"][key] = value

    def get_attribute(self, key):
        return span_data["attributes"].get(key)

    return type(
        "Span",
        (),
        {
            "name": name,
            "kind": kind,
            "attributes": span_data["attributes"],
            "set_attribute": set_attribute,
            "get_attribute": get_attribute,
        },
    )()


def _create_tracer():
    def start_as_current_span(self, name, kind=None, attributes=None):
        @contextmanager
        def span_context():
            if kind == "PRODUCER":
                span = _create_span(name, kind, attributes)
                yield span
            else:
                yield None

        return span_context()

    Tracer = type(
        "Tracer",
        (),
        {
            "start_as_current_span": start_as_current_span,
            "SpanKind": type(
                "SpanKind", (), {"PRODUCER": "PRODUCER", "CONSUMER": "CONSUMER"}
            ),
        },
    )

    def get_tracer(self, name):
        return Tracer()

    return type(
        "TraceModule",
        (),
        {
            "get_tracer": get_tracer,
            "SpanKind": type(
                "SpanKind", (), {"PRODUCER": "PRODUCER", "CONSUMER": "CONSUMER"}
            ),
        },
    )()


def _create_context():
    def attach(self, context):
        return None

    def detach(self, token):
        return None

    return type(
        "Context",
        (),
        {
            "attach": attach,
            "detach": detach,
        },
    )()


def _create_propagator():
    def inject(self, carrier):
        return None

    def extract(self, carrier):
        return carrier

    return type("Propagator", (), {"inject": inject, "extract": extract})()


def _create_mock_context():
    values = {}

    def get(self, key):
        return values.get(key)

    def set(self, key, value):
        values[key] = value

    return type("MockContext", (), {"get": get, "set": set})


class _InsightTelemetryProxy:
    def __init__(self):
        self.allowed_functions = {"trace", "context", "propagate", "Context"}
        self._trace = None
        self._context = None
        self._propagate = None
        self._Context = None

    @property
    def trace(self):
        """Mock opentelemetry.trace module"""
        if self._trace is None:
            self._trace = _create_tracer()
        return self._trace

    @property
    def context(self):
        """Mock opentelemetry.context module"""
        if self._context is None:
            self._context = _create_context()
        return self._context

    @property
    def propagate(self):
        """Mock opentelemetry.propagate module"""
        if self._propagate is None:
            self._propagate = _create_propagator()
        return self._propagate

    @property
    def Context(self):
        """Mock opentelemetry.context.Context class"""
        if self._Context is None:
            self._Context = _create_mock_context()
        return self._Context


class _OpenTelemetryProxy:
    """
    This proxy makes it possible for tracing to be disabled when opentelemetry
    is not installed on the cluster, but is installed locally.

    The check for `opentelemetry`'s existence must happen where the functions
    are executed because `opentelemetry` may be present where the functions
    are pickled. This can happen when `ray[full]` is installed locally by `ray`
    (no extra dependencies) is installed on the cluster.
    """

    allowed_functions = {"trace", "context", "propagate", "Context"}

    def __getattr__(self, name):
        if name in _OpenTelemetryProxy.allowed_functions:
            return getattr(self, f"_{name}")()
        else:
            raise AttributeError(f"Attribute does not exist: {name}")

    def _trace(self):
        return self._try_import("opentelemetry.trace")

    def _context(self):
        return self._try_import("opentelemetry.context")

    def _propagate(self):
        return self._try_import("opentelemetry.propagate")

    def _Context(self):
        context = self._context()
        if context:
            return context.context.Context
        else:
            return None

    def try_all(self):
        self._trace()
        self._context()
        self._propagate()
        self._Context()

    def _try_import(self, module):
        try:
            return importlib.import_module(module)
        except ImportError:
            if os.getenv("RAY_TRACING_ENABLED", "False").lower() in ["true", "1"]:
                raise ImportError(
                    "Install opentelemetry with "
                    "'pip install opentelemetry-api==1.0.0rc1' "
                    "and 'pip install opentelemetry-sdk==1.0.0rc1' to enable "
                    "tracing. See more at docs.ray.io/tracing.html"
                )


_global_is_tracing_enabled = False
_opentelemetry = None


def _is_tracing_enabled() -> bool:
    """Checks environment variable feature flag to see if tracing is turned on.
    Tracing is off by default."""
    from ray.util.insight import is_flow_insight_enabled

    if is_flow_insight_enabled():
        return True
    return _global_is_tracing_enabled


def _enable_tracing():
    global _global_is_tracing_enabled, _opentelemetry
    _global_is_tracing_enabled = True
    _opentelemetry = _OpenTelemetryProxy()
    _opentelemetry.try_all()


if os.getenv("RAY_FLOW_INSIGHT", "0") == "1":
    if _opentelemetry is None:
        _opentelemetry = _InsightTelemetryProxy()


def _sort_params_list(params_list: List[Parameter]):
    """Given a list of Parameters, if a kwargs Parameter exists,
    move it to the end of the list."""
    for i, param in enumerate(params_list):
        if param.kind == Parameter.VAR_KEYWORD:
            params_list.append(params_list.pop(i))
            break
    return params_list


def _add_param_to_signature(function: Callable, new_param: Parameter):
    """Add additional Parameter to function signature."""
    old_sig = inspect.signature(function)
    old_sig_list_repr = list(old_sig.parameters.values())
    # If new_param is already in signature, do not add it again.
    if any(param.name == new_param.name for param in old_sig_list_repr):
        return old_sig
    new_params = _sort_params_list(old_sig_list_repr + [new_param])
    new_sig = old_sig.replace(parameters=new_params)
    return new_sig


class _ImportFromStringError(Exception):
    pass


def _import_from_string(import_str: Union[ModuleType, str]) -> ModuleType:
    """Given a string that is in format "<module>:<attribute>",
    import the attribute."""
    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = (
            'Import string "{import_str}" must be in format' '"<module>:<attribute>".'
        )
        raise _ImportFromStringError(message.format(import_str=import_str))

    try:
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Could not import module "{module_str}".'
        raise _ImportFromStringError(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise _ImportFromStringError(
            message.format(attrs_str=attrs_str, module_str=module_str)
        )

    return instance


class _DictPropagator:
    def inject_current_context() -> Dict[Any, Any]:
        """Inject trace context into otel propagator."""
        context_dict: Dict[Any, Any] = {}
        _opentelemetry.propagate.inject(context_dict)
        return context_dict

    def extract(context_dict: Dict[Any, Any]) -> "_opentelemetry.Context":
        """Given a trace context, extract as a Context."""
        return cast(
            _opentelemetry.Context, _opentelemetry.propagate.extract(context_dict)
        )


@contextmanager
def _use_context(
    parent_context: "_opentelemetry.Context",
) -> Generator[None, None, None]:
    """Uses the Ray trace context for the span."""
    if parent_context is not None:
        new_context = parent_context
    else:
        new_context = _opentelemetry.Context()

    from ray.util.insight import report_trace_info

    report_trace_info(new_context)

    token = _opentelemetry.context.attach(new_context)
    try:
        yield
    finally:
        _opentelemetry.context.detach(token)


def _function_hydrate_span_args(function_name: str):
    """Get the Attributes of the function that will be reported as attributes
    in the trace."""
    runtime_context = get_runtime_context()

    span_args = {
        "ray.remote": "function",
        "ray.function": function_name,
        "ray.pid": str(os.getpid()),
        "ray.job_id": runtime_context.get_job_id(),
        "ray.node_id": runtime_context.get_node_id(),
    }

    # We only get task ID for workers
    if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
        task_id = runtime_context.get_task_id()
        if task_id:
            span_args["ray.task_id"] = task_id

    worker_id = getattr(ray._private.worker.global_worker, "worker_id", None)
    if worker_id:
        span_args["ray.worker_id"] = worker_id.hex()

    return span_args


def _function_span_producer_name(func: Callable[..., Any]) -> str:
    """Returns the function span name that has span kind of producer."""
    return f"{func} ray.remote"


def _function_span_consumer_name(func: Callable[..., Any]) -> str:
    """Returns the function span name that has span kind of consumer."""
    return f"{func} ray.remote_worker"


def _actor_hydrate_span_args(
    class_: Union[str, Callable[..., Any]],
    method: Union[str, Callable[..., Any]],
):
    """Get the Attributes of the actor that will be reported as attributes
    in the trace."""
    if callable(class_):
        class_ = class_.__name__
    if callable(method):
        method = method.__name__

    runtime_context = get_runtime_context()
    span_args = {
        "ray.remote": "actor",
        "ray.actor_class": class_,
        "ray.actor_method": method,
        "ray.function": f"{class_}.{method}",
        "ray.pid": str(os.getpid()),
        "ray.job_id": runtime_context.get_job_id(),
        "ray.node_id": runtime_context.get_node_id(),
    }

    # We only get actor ID for workers
    if ray._private.worker.global_worker.mode == ray._private.worker.WORKER_MODE:
        actor_id = runtime_context.get_actor_id()

        if actor_id:
            span_args["ray.actor_id"] = actor_id

    worker_id = getattr(ray._private.worker.global_worker, "worker_id", None)
    if worker_id:
        span_args["ray.worker_id"] = worker_id.hex()

    return span_args


def _actor_span_producer_name(
    class_: Union[str, Callable[..., Any]],
    method: Union[str, Callable[..., Any]],
) -> str:
    """Returns the actor span name that has span kind of producer."""
    if not isinstance(class_, str):
        class_ = class_.__name__
    if not isinstance(method, str):
        method = method.__name__

    return f"{class_}.{method} ray.remote"


def _actor_span_consumer_name(
    class_: Union[str, Callable[..., Any]],
    method: Union[str, Callable[..., Any]],
) -> str:
    """Returns the actor span name that has span kind of consumer."""
    if not isinstance(class_, str):
        class_ = class_.__name__
    if not isinstance(method, str):
        method = method.__name__

    return f"{class_}.{method} ray.remote_worker"


def _tracing_task_invocation(method):
    """Trace the execution of a remote task. Inject
    the current span context into kwargs for propagation."""

    @wraps(method)
    def _invocation_remote_span(
        self,
        args: Any = None,  # from tracing
        kwargs: MutableMapping[Any, Any] = None,  # from tracing
        *_args: Any,  # from Ray
        **_kwargs: Any,  # from Ray
    ) -> Any:
        # If tracing feature flag is not on, perform a no-op.
        # Tracing doesn't work for cross lang yet.
        if not _is_tracing_enabled() or self._is_cross_language:
            if kwargs is not None:
                assert "_ray_trace_ctx" not in kwargs
            return method(self, args, kwargs, *_args, **_kwargs)

        assert "_ray_trace_ctx" not in kwargs
        tracer = _opentelemetry.trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            _function_span_producer_name(self._function_name),
            kind=_opentelemetry.trace.SpanKind.PRODUCER,
            attributes=_function_hydrate_span_args(self._function_name),
        ):
            # Inject a _ray_trace_ctx as a dictionary
            kwargs["_ray_trace_ctx"] = _DictPropagator.inject_current_context()
            from ray.util.insight import is_flow_insight_enabled, get_caller_info

            if is_flow_insight_enabled():
                kwargs["_ray_trace_ctx"] = get_caller_info()
            return method(self, args, kwargs, *_args, **_kwargs)

    return _invocation_remote_span


def _inject_tracing_into_function(function):
    """Wrap the function argument passed to RemoteFunction's __init__ so that
    future execution of that function will include tracing.
    Use the provided trace context from kwargs.
    """
    if not _is_tracing_enabled():
        return function

    function.__signature__ = _add_param_to_signature(
        function,
        inspect.Parameter(
            "_ray_trace_ctx", inspect.Parameter.KEYWORD_ONLY, default=None
        ),
    )

    @wraps(function)
    def _function_with_tracing(
        *args: Any,
        _ray_trace_ctx: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> Any:
        if _ray_trace_ctx is None:
            return function(*args, **kwargs)

        tracer = _opentelemetry.trace.get_tracer(__name__)
        function_name = function.__module__ + "." + function.__name__

        # Retrieves the context from the _ray_trace_ctx dictionary we injected
        with _use_context(
            _DictPropagator.extract(_ray_trace_ctx)
        ), tracer.start_as_current_span(
            _function_span_consumer_name(function_name),
            kind=_opentelemetry.trace.SpanKind.CONSUMER,
            attributes=_function_hydrate_span_args(function_name),
        ):
            return function(*args, **kwargs)

    return _function_with_tracing


def _tracing_actor_creation(method):
    """Trace the creation of an actor. Inject
    the current span context into kwargs for propagation."""

    @wraps(method)
    def _invocation_actor_class_remote_span(
        self,
        args: Any = tuple(),  # from tracing
        kwargs: MutableMapping[Any, Any] = None,  # from tracing
        *_args: Any,  # from Ray
        **_kwargs: Any,  # from Ray
    ):
        if kwargs is None:
            kwargs = {}

        if self.__ray_metadata__.class_name == "_ray_internal_insight_monitor":
            return method(self, args, kwargs, *_args, **_kwargs)

        # If tracing feature flag is not on, perform a no-op
        if not _is_tracing_enabled():
            assert "_ray_trace_ctx" not in kwargs
            return method(self, args, kwargs, *_args, **_kwargs)

        class_name = self.__ray_metadata__.class_name
        method_name = "__init__"
        assert "_ray_trace_ctx" not in _kwargs
        tracer = _opentelemetry.trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            name=_actor_span_producer_name(class_name, method_name),
            kind=_opentelemetry.trace.SpanKind.PRODUCER,
            attributes=_actor_hydrate_span_args(class_name, method_name),
        ) as span:
            # Inject a _ray_trace_ctx as a dictionary
            kwargs["_ray_trace_ctx"] = _DictPropagator.inject_current_context()

            from ray.util.insight import is_flow_insight_enabled, get_caller_info

            if is_flow_insight_enabled():
                kwargs["_ray_trace_ctx"] = get_caller_info()

            result = method(self, args, kwargs, *_args, **_kwargs)

            span.set_attribute("ray.actor_id", result._ray_actor_id.hex())

            return result

    return _invocation_actor_class_remote_span


def _tracing_actor_method_invocation(method):
    """Trace the invocation of an actor method."""

    @wraps(method)
    def _start_span(
        self,
        args: Sequence[Any] = None,
        kwargs: MutableMapping[Any, Any] = None,
        *_args: Any,
        **_kwargs: Any,
    ) -> Any:
        # If tracing feature flag is not on, perform a no-op
        if not _is_tracing_enabled() or self._actor_ref()._ray_is_cross_language:
            if kwargs is not None:
                assert "_ray_trace_ctx" not in kwargs
            return method(self, args, kwargs, *_args, **_kwargs)

        if (
            self._actor_ref()._ray_actor_creation_function_descriptor.class_name
            == "_ray_internal_insight_monitor"
        ):
            return method(self, args, kwargs, *_args, **_kwargs)

        class_name = (
            self._actor_ref()._ray_actor_creation_function_descriptor.class_name
        )
        method_name = self._method_name
        assert "_ray_trace_ctx" not in _kwargs

        tracer = _opentelemetry.trace.get_tracer(__name__)
        with tracer.start_as_current_span(
            name=_actor_span_producer_name(class_name, method_name),
            kind=_opentelemetry.trace.SpanKind.PRODUCER,
            attributes=_actor_hydrate_span_args(class_name, method_name),
        ) as span:
            # Inject a _ray_trace_ctx as a dictionary
            kwargs["_ray_trace_ctx"] = _DictPropagator.inject_current_context()

            span.set_attribute("ray.actor_id", self._actor_ref()._ray_actor_id.hex())

            from ray.util.insight import is_flow_insight_enabled, get_caller_info

            if is_flow_insight_enabled():
                kwargs["_ray_trace_ctx"] = get_caller_info()

            return method(self, args, kwargs, *_args, **_kwargs)

    return _start_span


def _inject_tracing_into_class(_cls):
    """Given a class that will be made into an actor,
    inject tracing into all of the methods."""

    def span_wrapper(method: Callable[..., Any]) -> Any:
        def _resume_span(
            self: Any,
            *_args: Any,
            _ray_trace_ctx: Optional[Dict[str, Any]] = None,
            **_kwargs: Any,
        ) -> Any:
            """
            Wrap the user's function with a function that
            will extract the trace context
            """
            # If tracing feature flag is not on, perform a no-op
            if not _is_tracing_enabled() or _ray_trace_ctx is None:
                return method(self, *_args, **_kwargs)

            if self.__class__.__name__ == "_ray_internal_insight_monitor":
                return method(self, *_args, **_kwargs)

            tracer: _opentelemetry.trace.Tracer = _opentelemetry.trace.get_tracer(
                __name__
            )

            # Retrieves the context from the _ray_trace_ctx dictionary we
            # injected.
            with _use_context(
                _DictPropagator.extract(_ray_trace_ctx)
            ), tracer.start_as_current_span(
                _actor_span_consumer_name(self.__class__.__name__, method),
                kind=_opentelemetry.trace.SpanKind.CONSUMER,
                attributes=_actor_hydrate_span_args(self.__class__.__name__, method),
            ):
                return method(self, *_args, **_kwargs)

        return _resume_span

    def async_span_wrapper(method: Callable[..., Any]) -> Any:
        async def _resume_span(
            self: Any,
            *_args: Any,
            _ray_trace_ctx: Optional[Dict[str, Any]] = None,
            **_kwargs: Any,
        ) -> Any:
            """
            Wrap the user's function with a function that
            will extract the trace context
            """
            # If tracing feature flag is not on, perform a no-op
            if not _is_tracing_enabled() or _ray_trace_ctx is None:
                return await method(self, *_args, **_kwargs)

            if self.__class__.__name__ == "_ray_internal_insight_monitor":
                return await method(self, *_args, **_kwargs)

            tracer = _opentelemetry.trace.get_tracer(__name__)

            # Retrieves the context from the _ray_trace_ctx dictionary we
            # injected, or starts a new context
            with _use_context(
                _DictPropagator.extract(_ray_trace_ctx)
            ), tracer.start_as_current_span(
                _actor_span_consumer_name(self.__class__.__name__, method.__name__),
                kind=_opentelemetry.trace.SpanKind.CONSUMER,
                attributes=_actor_hydrate_span_args(
                    self.__class__.__name__, method.__name__
                ),
            ):
                return await method(self, *_args, **_kwargs)

        return _resume_span

    if _cls.__name__ == "_ray_internal_insight_monitor":
        return _cls

    methods = inspect.getmembers(_cls, is_function_or_method)
    for name, method in methods:
        # Skip tracing for staticmethod or classmethod, because these method
        # might not be called directly by remote calls. Additionally, they are
        # tricky to get wrapped and unwrapped.
        if is_static_method(_cls, name) or is_class_method(method):
            continue

        if inspect.isgeneratorfunction(method) or inspect.isasyncgenfunction(method):
            # Right now, this method somehow changes the signature of the method
            # when they are generator.
            # TODO(sang): Fix it.
            continue

        # Don't decorate the __del__ magic method.
        # It's because the __del__ can be called after Python
        # modules are garbage colleted, which means the modules
        # used for the decorator (e.g., `span_wrapper`) may not be
        # available. For example, it is not guranteed that
        # `_is_tracing_enabled` is available when `__del__` is called.
        # Tracing `__del__` is also not very useful.
        # https://joekuan.wordpress.com/2015/06/30/python-3-__del__-method-and-imported-modules/ # noqa
        if name == "__del__":
            continue

        # Add _ray_trace_ctx to method signature
        method.__signature__ = _add_param_to_signature(
            method,
            inspect.Parameter(
                "_ray_trace_ctx", inspect.Parameter.KEYWORD_ONLY, default=None
            ),
        )

        if inspect.iscoroutinefunction(method):
            # If the method was async, swap out sync wrapper into async
            wrapped_method = wraps(method)(async_span_wrapper(method))
        else:
            wrapped_method = wraps(method)(span_wrapper(method))

        setattr(_cls, name, wrapped_method)

    return _cls
