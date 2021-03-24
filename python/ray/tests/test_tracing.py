import ray
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)
from typing import Any
import pytest

def _setup_tracing(*args: Any, **kwargs: Any) -> None:
    if getattr(ray, "__traced__", False):
        return

    ray.__traced__ = True
    # Sets the tracer_provider. This is only allowed once per execution context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleExportSpanProcessor(ConsoleSpanExporter())
    )

@pytest
def test_tracing_task():
    ray.worker.global_worker.run_function_on_all_workers(_setup_tracing)

    ray.init()
    @ray.remote
    def f():
        print("foo")

    obj_ref = f.remote()
    ray.get(obj_ref)

    # assert that the file has 2 traces

# @ray.remote
# class Counter(object):
#     def __init__(self):
#         self.value = 0

#     def increment(self):
#         self.value += 1
#         return self.value

# # Create an actor from this class.
# counter = Counter.remote() 