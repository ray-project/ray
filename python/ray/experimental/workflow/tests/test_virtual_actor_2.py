import time
import ray
import pytest
from filelock import FileLock
from ray.tests.conftest import *  # noqa
from ray.experimental import workflow


@workflow.virtual_actor
class Counter:
    def __init__(self, x: int):
        self.x = x

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        return self.x

    @workflow.virtual_actor.readonly
    def readonly_incr(self):
        self.x += 1
        return self.x

    def add(self, y):
        self.x += y
        return self.x

    def __getstate__(self):
        return self.x

    def __setstate__(self, state):
        self.x = state


@workflow.virtual_actor
class IndirectCounter:
    def __init__(self, x):
        actor = Counter.get_or_create("counter", x)
        ray.get(actor.ready())

    @workflow.virtual_actor.readonly
    def readonly_get(self):
        actor = workflow.get_actor("counter")
        return actor.readonly_get.run()

    @workflow.virtual_actor.readonly
    def readonly_incr(self):
        actor = workflow.get_actor("counter")
        return actor.readonly_incr.run()

    def add(self, y):
        actor = workflow.get_actor("counter")
        return actor.add.run(y)

    @workflow.virtual_actor.readonly
    def readonly_workload(self):
        # simulate a workload
        time.sleep(1)

    def __getstate__(self):
        return

    def __setstate__(self, state):
        pass


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4
        # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_indirect_actor_writer(workflow_start_regular):
    actor = IndirectCounter.get_or_create("indirect_counter", 0)
    ray.get(actor.ready())
    assert actor.readonly_get.run() == 0
    array = []
    s = 0
    for i in range(1, 10):
        s += i
        array.append(s)
    assert [actor.add.run(i) for i in range(1, 10)] == array
    assert actor.readonly_get.run() == 45

    array = []
    for i in range(10, 20):
        s += i
        array.append(s)
    assert ray.get([actor.add.run_async(i) for i in range(10, 20)]) == array


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4
        # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_wf_in_actor(workflow_start_regular, tmp_path):
    fail_flag = tmp_path / "fail"
    cnt = tmp_path / "count"
    cnt.write_text(str(0))
    lock_file = tmp_path / "lock"

    @workflow.step
    def start_session():
        if fail_flag.exists():
            raise Exception()
        v = int(cnt.read_text()) + 1
        cnt.write_text(str(v))
        with FileLock(str(lock_file)):
            return "UP"

    @workflow.virtual_actor
    class Session:
        def __init__(self):
            self._session_status = "DOWN"

        @workflow.virtual_actor.readonly
        def get_status(self):
            return self._session_status

        def update_session(self, up):
            (ret, err) = up
            if err is None:
                self._session_status = ret
            else:
                self._session_status = err
            return self._session_status

        def session_start(self):
            step = start_session.step()
            return step

        def session_start_with_status(self):
            self._session_status = "STARTING"
            return self.update_session.step(
                start_session.options(catch_exceptions=True).step())

        def __getstate__(self):
            return self._session_status

        def __setstate__(self, state):
            self._session_status = state

    actor = Session.get_or_create("session_id")
    fail_flag.touch()
    with pytest.raises(Exception):
        actor.session_start.run()
    fail_flag.unlink()
    ray.get(workflow.resume("session_id"))
    # After resume, it'll rerun start_session which will
    # generate 1
    assert cnt.read_text() == "1"
    assert actor.session_start.run() == "UP"
    assert cnt.read_text() == "2"
    assert actor.session_start_with_status.run() == "UP"
    assert cnt.read_text() == "3"

    # Now test a new session.
    actor = Session.get_or_create("session_id")
    fail_flag.touch()
    assert isinstance(actor.session_start_with_status.run(), Exception)
    assert cnt.read_text() == "3"
    lock = FileLock(str(lock_file))
    lock.acquire()
    fail_flag.unlink()
    ret = actor.session_start_with_status.run_async()
    for i in range(0, 60):
        if cnt.read_text() == "4":
            break
        time.sleep(1)
    assert cnt.read_text() == "4"
    # This means when return from session_start_with_status,
    # the session got updated
    assert actor.get_status.run() == "STARTING"
    lock.release()
    assert ray.get(ret) == "UP"


@pytest.mark.parametrize(
    "workflow_start_regular",
    [{
        "num_cpus": 4
        # We need more CPUs, otherwise 'create()' blocks 'get()'
    }],
    indirect=True)
def test_wf_in_actor_chain(workflow_start_regular, tmp_path):
    file_lock = [str(tmp_path / str(i)) for i in range(5)]
    @workflow.virtual_actor
    class Counter:
        def __init__(self):
            self._counter = 0

        def incr(self, n):
            with FileLock(file_lock[n]):
                self._counter += 1
            if n == 0:
                return self._counter
            else:
                return self.incr.step(n - 1)

        @workflow.virtual_actor.readonly
        def val(self):
            return self._counter

        def __getstate__(self):
            return self._counter

        def __setstate__(self, v):
            self._counter = v

    locks = [FileLock(f) for f in file_lock]
    for l in locks:
        l.acquire()

    c = Counter.get_or_create("counter")
    ray.get(c.ready())
    final_ret = c.incr.run_async(len(file_lock) - 1)
    for i in range(0, len(file_lock)):
        locks[len(file_lock) - i - 1].release()
        val = c.val.run()
        for _ in range(0, 60):
            if val == i + 1:
                break
            val = c.val.run()
            time.sleep(1)
        assert val == i + 1
    assert ray.get(final_ret) == 5

if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
