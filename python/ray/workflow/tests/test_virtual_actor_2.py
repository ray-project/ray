import time
import ray
import pytest
from filelock import FileLock
from ray.tests.conftest import *  # noqa
from ray import workflow


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
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
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
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
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
                start_session.options(catch_exceptions=True).step()
            )

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
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
@pytest.mark.skip(reason="Return a list of virtual actor sub method is not supported.")
def test_wf_in_actor_seq(workflow_start_regular, tmp_path):
    record = tmp_path / "record"
    record.touch()

    @workflow.step
    def join(*args):
        return args

    @workflow.virtual_actor
    class Q:
        def write(self, n):
            r = record.read_text()
            r += f"{n}\n"
            record.write_text(r)

        def write_queue(self, n):
            return join.step(*[self.write.step(i) for i in range(n)])

        def __getstate__(self):
            return None

        def __setstate__(self, v):
            pass

    q = Q.get_or_create("queue")
    q.write_queue.run(5)
    assert "\n".join([str(i) for i in range(5)]) == record.read_text()


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
def test_wf_in_actor_seq_2(workflow_start_regular, tmp_path):
    record = tmp_path / "record"
    record.touch()

    file_lock = tmp_path / "lock"

    @workflow.step
    def join(*args):
        return args

    @workflow.step
    def write(n):
        with FileLock(str(file_lock)):
            r = record.read_text()
            r += f"{n}\n"
            record.write_text(r)

    @workflow.virtual_actor
    class Q:
        def write_queue(self, n):
            return join.step(*[write.step(i) for i in range(n)])

        def __getstate__(self):
            return None

        def __setstate__(self, v):
            pass

    q = Q.get_or_create("queue")
    q.write_queue.run(5)
    assert {str(i) for i in range(5)} == set(record.read_text().split())


@pytest.mark.parametrize(
    "workflow_start_regular",
    [
        {
            "num_cpus": 4
            # We need more CPUs, otherwise 'create()' blocks 'get()'
        }
    ],
    indirect=True,
)
@pytest.mark.skip(
    reason="The test become flaky on CI recently, but we could not "
    "reproduce it locally. Skip it temporarily."
)
def test_wf_in_actor_seq_3(workflow_start_regular, tmp_path):
    @workflow.virtual_actor
    class Counter:
        def __init__(self):
            self.n = 0

        def incr(self, n):
            self.n += 1
            if n - 1 > 0:
                return self.incr.step(n - 1)

        @workflow.virtual_actor.readonly
        def get(self):
            return self.n

        def __getstate__(self):
            return self.n

        def __setstate__(self, n):
            self.n = n

    c = Counter.get_or_create("c1")
    # Incr will return a nested workflow. For the step function inside the
    # nested workflow, they'll be executed one-by-one. But obj2's finish cant
    # guarentee obj1's finish.
    obj1 = c.incr.run_async(10)  # noqa
    obj2 = c.incr.run(10)  # noqa
    # TODO(suquark): The test is flaky sometimes (only on CI), which might indicates
    # some bugs. This is a workaroundde temporarily.
    time.sleep(3)
    assert c.get.run() == 20


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
