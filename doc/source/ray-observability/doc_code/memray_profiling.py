# __memray_profiling_start__
import memray
import ray


@ray.remote
class Actor:
    def __init__(self):
        # Every memory allocation after `__enter__` method will be tracked.
        memray.Tracker(
            "/tmp/ray/session_latest/logs/"
            f"{ray.get_runtime_context().get_actor_id()}_mem_profile.bin"
        ).__enter__()
        self.arr = [bytearray(b"1" * 1000000)]

    def append(self):
        self.arr.append(bytearray(b"1" * 1000000))


a = Actor.remote()
ray.get(a.append.remote())
# __memray_profiling_end__


# __memray_profiling_task_start__
import memray  # noqa
import ray  # noqa


@ray.remote
def task():
    with memray.Tracker(
        "/tmp/ray/session_latest/logs/"
        f"{ray.get_runtime_context().get_task_id()}_mem_profile.bin"
    ):
        arr = bytearray(b"1" * 1000000)  # noqa


ray.get(task.remote())
# __memray_profiling_task_end__
