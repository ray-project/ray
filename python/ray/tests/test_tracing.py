import os

# We must set the environment variable before importing ray
# TODO: use py_test(env = ...) in the build file with bazel 4.0
os.environ["RAY_TRACING_ENABLED"] = "True"

from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SimpleExportSpanProcessor,
)  # noqa: E402
import asyncio  # noqa: E402
import glob  # noqa: E402
import json  # noqa: E402
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
    yield spans_dir
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)


def _setup_tracing(*args: Any, **kwargs: Any) -> None:
    """This setup is what users currently need to do to enable tracing for Ray.
    We should consider doing this automatically in the future."""
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

    @ray.remote
    class AsyncActor:
        # multiple invocation of this method can be running in
        # the event loop at the same time
        async def run_concurrent(self):
            await asyncio.sleep(2)  # concurrent workload here

    actor = AsyncActor.remote()
    ray.get([actor.run_concurrent.remote() for _ in range(4)])

    span_string = ""
    span_list = []
    for entry in glob.glob(f"{spans_dir}/**/*.txt", recursive=True):
        with open(entry) as f:
            for line in f.readlines():
                span_string += line
                span_list.append(json.loads(line))
    assert len(span_list) == 16

    # The spans could show up in a different order, so just check that
    # all spans are as expected
    span_names = {}
    for span in span_list:
        span_name = span["name"]
        if span_name in span_names:
            span_names[span_name] += 1
        else:
            span_names[span_name] = 1

    assert span_names == {
        # spans from function
        "test_tracing.f ray.remote": 1,
        "test_tracing.f ray.remote_worker": 1,
        # spans from actor
        "test_tracing.<locals>.Counter.__init__ ray.remote": 1,
        "test_tracing.<locals>.Counter.increment ray.remote": 1,
        "Counter.__init__ ray.remote_worker": 1,
        "Counter.increment ray.remote_worker": 1,
        # spans from async actor
        "test_tracing.<locals>.AsyncActor.__init__ ray.remote": 1,
        "test_tracing.<locals>.AsyncActor.run_concurrent ray.remote": 4,
        "AsyncActor.__init__ ray.remote_worker": 1,
        "AsyncActor.run_concurrent ray.remote_worker": 4,
        }


if __name__ == "__main__":
    # import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
