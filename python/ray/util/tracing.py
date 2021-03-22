# from contextlib import contextmanager
# from typing import Dict, Generator, Optional

# from opentelemetry import context, trace
# from opentelemetry.context.context import Context
# from opentelemetry.util import types

# @contextmanager
# def use_context(
#     parent_context: Context,
# ) -> Generator[None, None, None]:
#     new_context = parent_context if parent_context is not None else Context()
#     token = context.attach(new_context)
#     try:
#         yield
#     finally:
#         context.detach(token)
from contextlib import contextmanager
from typing import Dict, Generator

from opentelemetry import context, trace
from opentelemetry.context.context import Context
from opentelemetry.util import types


def get_formatted_current_trace_id() -> str:
    current_span = trace.get_current_span()

    assert current_span is not None, "Expected to find a trace-id for this API request"

    trace_id = current_span.get_span_context().trace_id
    return trace.format_trace_id(trace_id)[
        2:] if trace_id != 0 else "NO_TRACE_ID"


def nest_tracing_attributes(attributes: Dict[str, types.AttributeValue],
                            parent: str) -> Dict[str, types.AttributeValue]:
    return {f"{parent}.{key}": value for (key, value) in attributes.items()}


# Sentinel value representing "use the current context"
CURRENT_CONTEXT = Context()


@contextmanager
def use_context(parent_context: Context, ) -> Generator[None, None, None]:
    new_context = parent_context if parent_context is not None else Context()
    token = context.attach(new_context)
    try:
        yield
    finally:
        context.detach(token)
