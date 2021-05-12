import asyncio
import glob
import json
import os
import pytest
import shutil

import ray
from ray.test_utils import check_call_ray
from ray.tests.enable_tracing import spans_dir


@pytest.fixture()
def cleanup_dirs():
    """Cleanup temporary spans_dir folder at beginning and end of test."""
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)
    os.makedirs(spans_dir)
    yield
    # Enable tracing only sets up tracing once per driver process.
    # We set ray.__traced__ to False here so that each
    # test will re-set up tracing.
    ray.__traced__ = False
    if os.path.exists(spans_dir):
        shutil.rmtree(spans_dir)


@pytest.fixture()
def ray_start_cli_tracing(scope="function"):
    """Start ray with tracing-startup-hook, and clean up at end of test."""
    check_call_ray(["stop", "--force"], )
    check_call_ray([
        "start", "--head", "--tracing-startup-hook",
        "ray.tests.enable_tracing:setup_tracing"
    ], )
    ray.init(address="auto")
    yield
    ray.shutdown()
    check_call_ray(["stop", "--force"])


@pytest.fixture()
def ray_start_init_tracing(scope="function"):
    """Call ray.init with tracing-startup-hook, and clean up at end of test."""
    ray.init(_tracing_startup_hook="ray.tests.enable_tracing:setup_tracing")
    yield
    ray.shutdown()


def get_span_list():
    """Read span files and return list of span names."""
    span_string = ""
    span_list = []
    for entry in glob.glob(f"{spans_dir}/**/*.txt", recursive=True):
        with open(entry) as f:
            for line in f.readlines():
                span_string += line
                span_list.append(json.loads(line))
    return span_list


def get_span_dict(span_list):
    """Given a list of span names, return dictionary of span names."""
    span_names = {}
    for span in span_list:
        span_name = span["name"]
        if span_name in span_names:
            span_names[span_name] += 1
        else:
            span_names[span_name] = 1
    return span_names


def task_helper():
    """Run a Ray task and check the spans produced."""

    @ray.remote
    def f(value):
        return value + 1

    obj_ref = f.remote(2)
    ray.get(obj_ref)

    span_list = get_span_list()
    assert len(span_list) == 2

    # The spans could show up in a different order, so just check that
    # all spans are as expected
    span_names = get_span_dict(span_list)
    return span_names == {
        "test_tracing.f ray.remote": 1,
        "test_tracing.f ray.remote_worker": 1,
    }


def sync_actor_helper():
    """Run a Ray sync actor and check the spans produced."""

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

    span_list = get_span_list()
    assert len(span_list) == 4

    # The spans could show up in a different order, so just check that
    # all spans are as expected
    span_names = get_span_dict(span_list)
    return span_names == {
        "sync_actor_helper.<locals>.Counter.__init__ ray.remote": 1,
        "sync_actor_helper.<locals>.Counter.increment ray.remote": 1,
        "Counter.__init__ ray.remote_worker": 1,
        "Counter.increment ray.remote_worker": 1,
    }


def async_actor_helper():
    """Run a Ray async actor and check the spans produced."""

    @ray.remote
    class AsyncActor:
        # multiple invocation of this method can be running in
        # the event loop at the same time
        async def run_concurrent(self):
            await asyncio.sleep(2)  # concurrent workload here

    actor = AsyncActor.remote()
    ray.get([actor.run_concurrent.remote() for _ in range(4)])

    span_list = get_span_list()
    assert len(span_list) == 10

    # The spans could show up in a different order, so just check that
    # all spans are as expected
    span_names = get_span_dict(span_list)
    return span_names == {
        "async_actor_helper.<locals>.AsyncActor.__init__ ray.remote": 1,
        "async_actor_helper.<locals>.AsyncActor.run_concurrent ray.remote": 4,
        "AsyncActor.__init__ ray.remote_worker": 1,
        "AsyncActor.run_concurrent ray.remote_worker": 4
    }


def test_tracing_task_init_workflow(cleanup_dirs, ray_start_init_tracing):
    assert task_helper()


def test_tracing_task_start_workflow(cleanup_dirs, ray_start_cli_tracing):
    assert task_helper()


def test_tracing_sync_actor_init_workflow(cleanup_dirs,
                                          ray_start_init_tracing):
    assert sync_actor_helper()


def test_tracing_sync_actor_start_workflow(cleanup_dirs,
                                           ray_start_cli_tracing):
    assert sync_actor_helper()


def test_tracing_async_actor_init_workflow(cleanup_dirs,
                                           ray_start_init_tracing):
    assert async_actor_helper()


def test_tracing_async_actor_start_workflow(cleanup_dirs,
                                            ray_start_cli_tracing):
    assert async_actor_helper()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
