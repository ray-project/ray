import os

# TODO: use py_test(env = ...) in the build file with bazel 4.0
os.environ["RAY_TRACING_ENABLED"] = "True"

from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)  # noqa: E402
import pytest  # noqa: E402
import shutil  # noqa: E402
import tempfile  # noqa: E402
from typing import Any  # noqa: E402
import ray  # noqa: E402

# Create temporary spans folder for trace output.
spans_dir = tempfile.gettempdir() + "/spans"


@pytest.fixture(scope="session")
def cleanup_dirs():
    """Cleanup temporary spans_dir folder at beginning and end of test."""
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)
    os.makedirs(spans_dir)
    yield
    print("at end of test")
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)


def _setup_tracing(*args: Any, **kwargs: Any) -> None:
    if getattr(ray, "__traced__", False):
        return

    ray.__traced__ = True
    # Sets the tracer_provider. This is only allowed once per execution
    # context and will log a warning if attempted multiple times.
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        SimpleExportSpanProcessor(
            ConsoleSpanExporter(
                out=open(f"{spans_dir}/{os.getpid()}.txt", "w"),
                formatter=lambda span: span.to_json(indent=None) + os.linesep,
            )))


def test_tracing(ray_start_regular_shared, cleanup_dirs):
    ray.worker.global_worker.run_function_on_all_workers(_setup_tracing)

    @ray.remote
    def f(value):
        return value + 1

    obj_ref = f.remote(2)
    ray.get(obj_ref)

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    # Create an actor from this class.
    counter = Counter.remote()
    obj_ref = counter.increment.remote()
    assert ray.get(obj_ref) == 1

    span_string = ""
    num_spans = 0
    for entry in os.listdir(spans_dir):
        if os.path.exists(f"{spans_dir}/{entry}"):
            with open(f"{spans_dir}/{entry}") as f:
                Lines = f.readlines()
                # print("lines" + str(Lines))
                for line in Lines:
                    span_string += line
                    num_spans += 1
    print(f"spans dir is {spans_dir}")
    assert num_spans == 6
    assert all([
        "f ray.remote" in span_string,  # YAPF formatting
        "f ray.remote_worker" in span_string,
        "Counter.__init__ ray.remote" in span_string,
        "Counter.increment ray.remote" in span_string,
        "Counter.__init__ ray.remote_worker" in span_string,
        "Counter.increment ray.remote_worker" in span_string,
    ])


if __name__ == "__main__":
    # import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
