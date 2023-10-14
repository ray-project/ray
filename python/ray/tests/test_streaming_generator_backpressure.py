import asyncio
import pytest
import numpy as np
import sys
import time

import ray
from ray._private.test_utils import wait_for_condition
from ray.util.state import list_tasks


@pytest.mark.parametrize("store_in_plasma", [False, True])
@pytest.mark.parametrize("actor", [False, True])
def test_streaming_generator_backpressure_basic(shutdown_only, store_in_plasma, actor):
    """Verify backpressure works with
    _streaming_generator_backpressure_size_bytes = 0
    """
    ray.init(num_cpus=1)

    @ray.remote
    class Reporter:
        def __init__(self):
            self.reported = set()

        def report(self, i):
            self.reported.add(i)

        def reported(self):
            return self.reported

    TOTAL_RETURN = 10

    if store_in_plasma:
        threshold = 40 * 1024 * 1024  # 40MB
    else:
        threshold = 0

    @ray.remote(
        num_returns="streaming", _streaming_generator_backpressure_size_bytes=threshold
    )
    def f(reporter):
        for i in range(TOTAL_RETURN):
            print("yield ", i)
            ray.get(reporter.report.remote(i))
            if store_in_plasma:
                yield np.random.rand(5 * 1024 * 1024)  # 40 MB
            else:
                yield i

    @ray.remote
    class A:
        def f(self, reporter):
            for i in range(TOTAL_RETURN):
                print("yield ", i)
                ray.get(reporter.report.remote(i))
                if store_in_plasma:
                    yield np.random.rand(5 * 1024 * 1024)  # 40 MB
                else:
                    yield i

    reporter = Reporter.remote()

    def check_reported(i):
        return i in ray.get(reporter.reported.remote())

    if actor:
        a = A.remote()
        gen = a.f.options(
            num_returns="streaming",
            _streaming_generator_backpressure_size_bytes=threshold,
        ).remote(reporter)
    else:
        gen = f.remote(reporter)

    for i in range(TOTAL_RETURN - 4):
        print("iteration ", i)
        r, _ = ray.wait([gen])
        assert len(r) == 1
        wait_for_condition(lambda: check_reported(i))
        wait_for_condition(lambda: not check_reported(i + 1))
        # Wait a little bit to make sure it is backpressured.
        time.sleep(2)
        wait_for_condition(lambda: not check_reported(i + 1))
        # Consume the ref -> task will progress.
        ray.get(next(gen))
        wait_for_condition(lambda: check_reported(i + 1))

    """
    Verify deleting a generator will stop backpressure
    and proceed a task.
    """

    del gen
    del r
    wait_for_condition(lambda: check_reported(TOTAL_RETURN - 1))


@pytest.mark.parametrize("store_in_plasma", [False, True])
def test_streaming_generator_backpressure_multiple_objects(
    shutdown_only, store_in_plasma
):
    """Verify backpressure works when it needs more than 1 objects
    to backpressure.
    """
    ray.init()

    @ray.remote
    class Reporter:
        def __init__(self):
            self.reported = set()

        def report(self, i):
            self.reported.add(i)

        def reported(self):
            return self.reported

    TOTAL_RETURN = 6

    if store_in_plasma:
        threshold = 150 * 1024 * 1024  # 150MB
    else:
        threshold = 15 * 1024  # 15KB

    @ray.remote(
        num_returns="streaming", _streaming_generator_backpressure_size_bytes=threshold
    )
    def f(reporter):
        for i in range(TOTAL_RETURN):
            print("yield ", i)
            ray.get(reporter.report.remote(i))
            if store_in_plasma:
                yield np.random.rand(10 * 1024 * 1024)  # 80 MB
            else:
                yield np.random.rand(1024)  # 8KB

    reporter = Reporter.remote()

    def check_reported(i):
        return i in ray.get(reporter.reported.remote())

    gen = f.remote(reporter)
    # It is backpressured for every 2 objects.
    for i in range(0, TOTAL_RETURN - 2, 2):
        print("iteration ", i)
        r, _ = ray.wait([gen])
        assert len(r) == 1
        wait_for_condition(lambda: check_reported(i))
        wait_for_condition(lambda: check_reported(i + 1))
        wait_for_condition(lambda: not check_reported(i + 2))
        # Wait a little bit to make sure it is backpressured.
        time.sleep(2)
        wait_for_condition(lambda: not check_reported(i + 2))
        # Consume the ref -> task will progress.
        ray.get(next(gen))
        ray.get(next(gen))
        wait_for_condition(lambda: check_reported(i + 2))
        wait_for_condition(lambda: check_reported(i + 3))


def test_caller_failure_doesnt_hang(shutdown_only):
    """
    Verify if the caller fails (e.g., exception or crash),
    the generator is not backpressured forever.
    """
    ray.init(num_cpus=2)

    @ray.remote(num_returns="streaming", _streaming_generator_backpressure_size_bytes=0)
    def f():
        for i in range(5):
            print("yield", i)
            yield i

    @ray.remote
    class Caller:
        def f(self, failure_type, hang, task_name):
            gen = f.options(name=task_name).remote()
            assert ray.get(next(gen)) == 0

            if hang:
                time.sleep(300)

            if failure_type == "exc":
                raise ValueError
            elif failure_type == "exit":
                sys.exit(1)
            else:
                pass

    # If caller finishes, f should finish.
    print("Check caller finishes")
    caller = Caller.remote()
    r = caller.f.remote(None, False, "1")  # noqa

    def verify():
        task = list_tasks(filters=[("name", "=", "1")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails with an exception, f should finish.
    print("Check caller raises an exception")
    caller = Caller.remote()
    r = caller.f.remote("exc", False, "2")  # noqa

    def verify():
        task = list_tasks(filters=[("name", "=", "2")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails with an exit, f should finish.
    print("Check caller exits")
    caller = Caller.remote()
    r = caller.f.remote("exit", False, "3")  # noqa

    def verify():
        task = list_tasks(filters=[("name", "=", "3")])[0]
        assert task.state == "FINISHED"
        return True

    wait_for_condition(verify)

    # If caller fails by a system exit, f should fail.
    print("Check caller killed")
    caller = Caller.remote()
    r = caller.f.remote(None, True, "4")  # noqa
    wait_for_condition(
        lambda: list_tasks(filters=[("name", "=", "4")])[0].state == "RUNNING"
    )
    ray.kill(caller)

    def verify():
        task = list_tasks(filters=[("name", "=", "4")])[0]
        assert task.state == "FAILED"
        return True

    wait_for_condition(verify)


def test_backpressure_not_work_with_async_actor(shutdown_only):
    """
    Verify using backpressure + async actor raises an exception
    """
    ray.init(num_cpus=1)

    @ray.remote
    class A:
        async def f(self):
            for i in range(10):
                print("yield", i)
                yield i

    a = A.remote()
    gen = a.f.options(
        num_returns="streaming", _streaming_generator_backpressure_size_bytes=0
    ).remote()
    with pytest.raises(ValueError):
        ray.get(next(gen))


def test_threaded_actor_generator_backpressure(shutdown_only):
    ray.init()

    @ray.remote(max_concurrency=10)
    class Actor:
        def f(self):
            for i in range(30):
                time.sleep(0.1)
                print("yield", i)
                yield np.ones(1024 * 1024) * i

    async def main():
        a = Actor.remote()

        async def run():
            i = 0
            gen = a.f.options(
                num_returns="streaming", _streaming_generator_backpressure_size_bytes=0
            ).remote()
            async for ref in gen:
                val = ray.get(ref)
                print(val)
                print(ref)
                assert np.array_equal(val, np.ones(1024 * 1024) * i)
                i += 1
                del ref

        coroutines = [run() for _ in range(10)]

        await asyncio.gather(*coroutines)

    asyncio.run(main())


# Backpressure + failure


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
