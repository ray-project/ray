import asyncio
import json
import time

import ray
import requests
import pytest

from ray._private.profiling import chrome_tracing_dump
from ray.experimental.state.api import list_tasks
from ray._private.test_utils import wait_for_condition


def test_timeline(shutdown_only):
    context = ray.init()

    @ray.remote
    def f():
        pass

    @ray.remote
    class Actor:
        def ready(self):
            pass

    @ray.remote
    class AsyncActor:
        async def f(self):
            await asyncio.sleep(5)
        
        async def g(self):
            await asyncio.sleep(5)

    @ray.remote
    class ThreadedActor:
        def f(self):
            import time
            time.sleep(5)
        
        def g(self):
            import time
            time.sleep(5)

    [f.remote() for _ in range(4)]
    a = Actor.remote()
    b = AsyncActor.remote()
    c = ThreadedActor.options(max_concurrency=15).remote()

    [a.ready.remote() for _ in range(4)]
    ray.get(b.f.remote())
    [b.f.remote() for _ in range(4)]
    [b.g.remote() for _ in range(4)]
    [c.f.remote() for _ in range(4)]
    [c.g.remote() for _ in range(4)]

    result = json.loads(chrome_tracing_dump(list_tasks(detail=True)))


def test_timeline_data_loss(shutdown_only):
    ray.init(
        _system_config={
            "task_events_max_num_task_in_gcs": 10,
            "task_events_max_num_task_events_in_buffer": 10
        }
    )
    # resp = requests.get(f"{dashboard_url}/api/v0/tasks/timeline")
    # print(resp.json())


def test_timeline_request(shutdown_only):
    context = ray.init()
    dashboard_url = f"http://{context['webui_url']}"

    @ray.remote
    def f():
        pass

    ray.get([f.remote() for _ in range(5)])

    # Make sure the API works.
    def verify():
        resp = requests.get(f"{dashboard_url}/api/v0/tasks/timeline")
        resp.raise_for_status()
        assert resp.json(), "No result has returned"
        return True

    wait_for_condition(verify, timeout=10)


if __name__ == "__main__":
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
