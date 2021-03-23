from contextlib import contextmanager
import inspect
import os
import wrapt

from opentelemetry import context, trace
from opentelemetry.context.context import Context
from opentelemetry.util import types
from opentelemetry.util.types import Attributes
from opentelemetry import propagators  # type: ignore
from opentelemetry.trace.propagation.textmap import DictGetter

from typing import (
    Any,
    cast,
    Callable,
    Dict,
    Generator,
    List,
    MutableMapping,
    Optional,
    Sequence,
    Type,
    Union,
)

from ray.runtime_context import get_runtime_context
from ray.util.inspect import is_function_or_method
import ray.worker

_nameable = Union[str, Callable[..., Any]]


class DictPropagator:
    def inject_current_context() -> Dict[Any, Any]:
        context_dict: Dict[Any, Any] = {}
        propagators.inject(dict.__setitem__, context_dict)
        return context_dict

    def extract(context_dict: Dict[Any, Any]) -> Context:
        return cast(Context, propagators.extract(DictGetter(), context_dict))


def get_formatted_current_trace_id() -> str:
    current_span = trace.get_current_span()

    assert current_span is not None, "Expected to find a trace-id for this API request"

    trace_id = current_span.get_span_context().trace_id
    return trace.format_trace_id(trace_id)[
        2:] if trace_id != 0 else "NO_TRACE_ID"


def nest_tracing_attributes(attributes: Dict[str, types.AttributeValue],
                            parent: str) -> Dict[str, types.AttributeValue]:
    return {f"{parent}.{key}": value for (key, value) in attributes.items()}


@contextmanager
def use_context(parent_context: Context, ) -> Generator[None, None, None]:
    new_context = parent_context if parent_context is not None else Context()
    token = context.attach(new_context)
    try:
        yield
    finally:
        context.detach(token)


def _function_hydrate_span_args(func: Callable[..., Any]) -> Attributes:
    # TODO: once ray 1.2.0 is released, we can just use get_runtime_context().get() to get all runtime context information
    runtime_context = get_runtime_context().get()

    span_args = {
        "ray.remote": "function",
        "ray.function": func,
        "ray.pid": str(os.getpid()),
        "ray.job_id": runtime_context["job_id"].hex(),
        "ray.node_id": runtime_context["node_id"].hex(),
    }

    # We only get task ID for workers
    if ray.worker.global_worker.mode == ray.worker.WORKER_MODE:
        task_id = runtime_context["task_id"].hex() if runtime_context["task_id"] else None
        if task_id:
            span_args["ray.task_id"] = task_id

    worker_id = getattr(ray.worker.global_worker, "worker_id", None)
    if worker_id:
        span_args["ray.worker_id"] = worker_id.hex()

    return span_args


def _function_span_producer_name(func: Callable[..., Any]) -> str:
    args = _function_hydrate_span_args(func)
    assert args is not None
    name = args["ray.function"]

    return f"{name} ray.remote"


def _function_span_consumer_name(func: Callable[..., Any]) -> str:
    args = _function_hydrate_span_args(func)
    assert args is not None
    name = args["ray.function"]

    return f"{name} ray.remote_worker"


def _actor_hydrate_span_args(class_: _nameable,
                             method: _nameable) -> Attributes:
    if callable(class_):
        class_ = class_.__name__
    if callable(method):
        method = method.__name__

    runtime_context = get_runtime_context().get()

    span_args = {
        "ray.remote": "actor",
        "ray.actor_class": class_,
        "ray.actor_method": method,
        "ray.function": f"{class_}.{method}",
        "ray.pid": str(os.getpid()),

        "ray.job_id": runtime_context["job_id"].hex(),
        "ray.node_id": runtime_context["node_id"].hex(),
    }

    # We only get actor ID for workers
    if ray.worker.global_worker.mode == ray.worker.WORKER_MODE:
        actor_id = runtime_context["actor_id"].hex(
        ) if runtime_context["actor_id"] else None

        if actor_id:
            span_args["ray.actor_id"] = actor_id

    worker_id = getattr(ray.worker.global_worker, "worker_id", None)
    if worker_id:
        span_args["ray.worker_id"] = worker_id.hex()

    return span_args


def _actor_span_producer_name(class_: _nameable, method: _nameable) -> str:
    args = _actor_hydrate_span_args(class_, method)
    assert args is not None
    name = args["ray.function"]

    return f"{name} ray.remote"


def _actor_span_consumer_name(class_: _nameable, method: _nameable) -> str:
    args = _actor_hydrate_span_args(class_, method)
    assert args is not None
    name = args["ray.function"]

    return f"{name} ray.remote_worker"


def _tracing_wrap_function(function):
    def _resume_trace(
            *args: Any,
            _ray_trace_ctx: Optional[Dict[str, Any]] = None,
            **kwargs: Any,
    ) -> Any:
        tracer = trace.get_tracer(__name__)

        assert (_ray_trace_ctx is
                not None), f"Missing ray_trace_ctx!: {args}, {kwargs}"

        # Set a new context if given a _ray_trace_ctx, or default to current_context
        # Retrieves the context from the _ray_trace_ctx dictionary we injected
        with use_context(DictPropagator.extract(
                _ray_trace_ctx)), tracer.start_as_current_span(
                    _function_span_consumer_name(_function_name),
                    kind=trace.SpanKind.CONSUMER,
                    attributes=_function_hydrate_span_args(_function_name),
                ):
            return function(*args, **kwargs)

    return _resume_trace


@wrapt.decorator
def _propagate_trace(
        wrapped: Callable[..., Any],
        instance: Any,
        args: List[Any],
        kwargs: MutableMapping[Any, Any],
) -> Any:
    """
    Make sure we inject our current span context into the kwargs for propagation
    before running our remote tasks
    """

    def _start_remote_span(
            args: Any,
            kwargs: MutableMapping[Any, Any],
            *_args: Any,
            **_kwargs: Any,
    ) -> Any:
        assert "_ray_trace_ctx" not in kwargs

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
                _function_span_producer_name(instance._function_name),
                kind=trace.SpanKind.PRODUCER,
                attributes=_function_hydrate_span_args(
                    instance._function_name),
        ):
            # Inject a _ray_trace_ctx as a dictionary that we'll pop out on the other side
            kwargs["_ray_trace_ctx"] = (  # YAPF formatting
                DictPropagator.inject_current_context())
            return wrapped(args, kwargs, *_args, **_kwargs)

    return _start_remote_span(*args, **kwargs)


@wrapt.decorator
def actor_class_tracing_local(
        wrapped: Callable[..., Any],
        instance: Any,
        args: Sequence[Any],
        kwargs: MutableMapping[Any, Any],
) -> Any:
    """
        Make sure we inject our current span context into the kwargs for propigation
        before running our remote tasks
        """
    class_name = instance._wrapped
    method_name = "__init__"

    def _start_span(
            args: Sequence[Any],
            kwargs: MutableMapping[Any, Any],
            *_args: Any,
            **_kwargs: Any,
    ) -> Any:
        assert "_ray_trace_ctx" not in _kwargs

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
                name=_actor_span_producer_name(class_name, method_name),
                kind=trace.SpanKind.PRODUCER,
                attributes=_actor_hydrate_span_args(class_name, method_name),
        ) as span:
            # Inject a _ray_trace_ctx as a dictionary that we'll pop out on the other side
            kwargs["_ray_trace_ctx"] = DictPropagator.inject_current_context()

            result = wrapped(args, kwargs, *_args, **_kwargs)

            span.set_attribute("ray.actor_id", result._ray_actor_id.hex())

            return result

    return _start_span(*args, **kwargs)


@wrapt.decorator
def actor_method_tracing_local(
        wrapped: Callable[..., Any],
        instance: Any,
        args: Sequence[Any],
        kwargs: MutableMapping[Any, Any],
) -> Any:
    class_name = (instance._actor_ref()
                  ._ray_actor_creation_function_descriptor.class_name)
    method_name = instance._method_name

    def _start_span(
            args: Sequence[Any],
            kwargs: MutableMapping[Any, Any],
            *_args: Any,
            **_kwargs: Any,
    ) -> Any:
        assert "_ray_trace_ctx" not in _kwargs

        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(
                name=_actor_span_producer_name(class_name, method_name),
                kind=trace.SpanKind.PRODUCER,
                attributes=_actor_hydrate_span_args(class_name, method_name),
        ) as span:
            # Inject a _ray_trace_ctx as a dictionary that we'll pop out on the other side
            kwargs["_ray_trace_ctx"] = DictPropagator.inject_current_context()

            span.set_attribute("ray.actor_id",
                               instance._actor_ref()._ray_actor_id.hex())

            return wrapped(args, kwargs, *_args, **_kwargs)

    return _start_span(*args, **kwargs)


@wrapt.decorator
def make_tracing_actor(
        wrapped: Callable[..., Any],
        instance: Any,
        args: Sequence[Any],
        kwargs: MutableMapping[Any, Any],
) -> Any:
    def span_wrapper(method: Callable[..., Any]) -> Any:
        def _resume_span(
                self: Any,
                *_args: Any,
                _ray_trace_ctx: Optional[Dict[str, Any]] = None,
                **_kwargs: Any,
        ) -> Any:
            """
            Wrap whatever the user's function is with with a function that will
            extract the trace context
            """
            tracer: trace.Tracer = trace.get_tracer(__name__)

            # Set a new context if given a _ray_trace_ctx, or default to current_context
            # Retrieves the context from the _ray_trace_ctx dictionary we injected
            if _ray_trace_ctx:
                with use_context(DictPropagator.extract(
                        _ray_trace_ctx)), tracer.start_as_current_span(
                            _actor_span_consumer_name(self._wrapped, method),
                            kind=trace.SpanKind.CONSUMER,
                            attributes=_actor_hydrate_span_args(
                                self._wrapped, method),
                        ):
                    return method(self, *_args, **_kwargs)
            else:
                with tracer.start_as_current_span(
                        _actor_span_consumer_name(self._wrapped, method),
                        kind=trace.SpanKind.CONSUMER,
                        attributes=_actor_hydrate_span_args(
                            self._wrapped, method),
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
            Wrap whatever the user's function is with with a function that will
            extract the trace context
            """
            tracer = trace.get_tracer(__name__)

            # Set a new context if given a _ray_trace_ctx, or default to current_context
            # Retrieves the context from the _ray_trace_ctx dictionary we injected
            if _ray_trace_ctx:
                with use_context(DictPropagator.extract(
                        _ray_trace_ctx)), tracer.start_as_current_span(
                            _actor_span_consumer_name(self._wrapped.__name__,
                                                      method.__name__),
                            kind=trace.SpanKind.CONSUMER,
                            attributes=_actor_hydrate_span_args(
                                self._wrapped.__name__, method.__name__),
                        ):
                    return await method(self, *_args, **_kwargs)
            else:
                with tracer.start_as_current_span(
                        _actor_span_consumer_name(self._wrapped.__name__,
                                                  method.__name__),
                        kind=trace.SpanKind.CONSUMER,
                        attributes=_actor_hydrate_span_args(
                            self._wrapped.__name__, method.__name__),
                ):
                    return await method(self, *_args, **_kwargs)

        return _resume_span

    def _make_actor(_cls: Type[Any], *_args: Any, **_kwargs: Any) -> Any:
        class TracingWrapper(
                _cls
        ):  # type: ignore # I can't figure out how to annotate _cls appropriately to make mypy happy
            _wrapped = _cls

        # This is here to make it so that when Ray's internals inspect
        # TracingWrapper they capture the right `class_name`.
        TracingWrapper.__name__ = _cls.__name__

        methods = inspect.getmembers(TracingWrapper, is_function_or_method)
        for name, method in methods:
            if inspect.iscoroutinefunction(method):
                # If the method was async, convert out sync wrapper into async
                wrapped_method = async_span_wrapper(method)
            else:
                wrapped_method = span_wrapper(method)

            setattr(TracingWrapper, name, wrapped_method)

        return wrapped(TracingWrapper, *_args, **_kwargs)

    return _make_actor(*args, **kwargs)
