import asyncio
import glob
import json
import os
import pytest
import shutil
import tempfile

import ray

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


def test_tracing(cleanup_dirs):
    ray.init(_tracing_startup_hook="ray.tests.enable_tracing:_setup_tracing")

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
    import sys

    sys.exit(pytest.main(["-v", __file__]))
