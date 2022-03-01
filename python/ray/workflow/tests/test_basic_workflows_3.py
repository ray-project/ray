import pytest
from filelock import FileLock
from pathlib import Path
from ray import workflow
from ray.tests.conftest import *  # noqa


def test_wf_run(workflow_start_regular, tmp_path):
    counter = tmp_path / "counter"
    counter.write_text("0")

    @workflow.step
    def f():
        v = int(counter.read_text()) + 1
        counter.write_text(str(v))

    f.step().run("abc")
    assert counter.read_text() == "1"
    # This will not rerun the job from beginning
    f.step().run("abc")
    assert counter.read_text() == "1"


def test_wf_no_run():
    @workflow.step
    def f1():
        pass

    f1.step()

    @workflow.step
    def f2(*w):
        pass

    f = f2.step(*[f1.step() for _ in range(10)])

    with pytest.raises(Exception):
        f.run()


def test_dedupe_indirect(workflow_start_regular, tmp_path):
    counter = Path(tmp_path) / "counter.txt"
    lock = Path(tmp_path) / "lock.txt"
    counter.write_text("0")

    @workflow.step
    def incr():
        with FileLock(str(lock)):
            c = int(counter.read_text())
            c += 1
            counter.write_text(f"{c}")

    @workflow.step
    def identity(a):
        return a

    @workflow.step
    def join(*a):
        return counter.read_text()

    # Here a is passed to two steps and we need to ensure
    # it's only executed once
    a = incr.step()
    i1 = identity.step(a)
    i2 = identity.step(a)
    assert "1" == join.step(i1, i2).run()
    assert "2" == join.step(i1, i2).run()
    # pass a multiple times
    assert "3" == join.step(a, a, a, a).run()
    assert "4" == join.step(a, a, a, a).run()


def test_run_off_main_thread(workflow_start_regular):
    @workflow.step
    def fake_data(num: int):
        return list(range(num))

    succ = False

    # Start new thread here ⚠️
    def run():
        global succ
        # Setup the workflow.
        data = fake_data.step(10)
        assert data.run(workflow_id="run") == list(range(10))

    import threading

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert workflow.get_status("run") == workflow.SUCCESSFUL


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
