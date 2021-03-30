import ray
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)
from typing import Any

import json
import os
import pytest

# We will write the span output to "/tmp/tracing.json"
span_output_file = "/tmp/tracing.json"


def _setup_tracing(*args: Any, **kwargs: Any) -> None:
    if getattr(ray, "__traced__", False):
        return

    print("file exists in setup tracing" + str(os.path.exists(span_output_file)))

    ray.__traced__ = True
    # Sets the tracer_provider. This is only allowed once per execution context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleExportSpanProcessor(
            ConsoleSpanExporter(
                out=open(span_output_file, "a"),
                formatter=lambda span: span.to_json(indent=None) + os.linesep,
            )
        )
    )


def test_tracing_task():
    os.environ["RAY_TRACING_ENABLED"] = "True"
    if os.path.exists(span_output_file):
        os.remove(span_output_file)
    print("file exists in test_tracing" + str(os.path.exists(span_output_file)))
    ray.worker.global_worker.run_function_on_all_workers(_setup_tracing)

    ray.init()

    @ray.remote
    def f():
        print("foo")

    obj_ref = f.remote()
    ray.get(obj_ref)

    span_string = ""
    with open(span_output_file) as f:
        Lines = f.readlines()
        for line in Lines:
            span_string += line
            # json_obj = json.loads(line)

    json_obj = json.loads(span_string)
    assert len(json_obj) == 2


# @ray.remote
# class Counter(object):
#     def __init__(self):
#         self.value = 0

#     def increment(self):
#         self.value += 1
#         return self.value

# # Create an actor from this class.
# counter = Counter.remote()

if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
