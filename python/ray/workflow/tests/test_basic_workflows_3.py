import pytest
from filelock import FileLock
from pathlib import Path

import ray
from ray import workflow
from ray.tests.conftest import *  # noqa


def test_wf_run(workflow_start_regular, tmp_path):
    counter = tmp_path / "counter"
    counter.write_text("0")

    @ray.remote
    def f():
        v = int(counter.read_text()) + 1
        counter.write_text(str(v))

    workflow.create(f.bind()).run("abc")
    assert counter.read_text() == "1"
    # This will not rerun the job from beginning
    workflow.create(f.bind()).run("abc")
    assert counter.read_text() == "1"


def test_wf_no_run():
    @ray.remote
    def f1():
        pass

    f1.bind()

    @ray.remote
    def f2(*w):
        pass

    f = workflow.create(f2.bind(*[f1.bind() for _ in range(10)]))

    with pytest.raises(Exception):
        f.run()


def test_dedupe_indirect(workflow_start_regular, tmp_path):
    counter = Path(tmp_path) / "counter.txt"
    lock = Path(tmp_path) / "lock.txt"
    counter.write_text("0")

    @ray.remote
    def incr():
        with FileLock(str(lock)):
            c = int(counter.read_text())
            c += 1
            counter.write_text(f"{c}")

    @ray.remote
    def identity(a):
        return a

    @ray.remote
    def join(*a):
        return counter.read_text()

    # Here a is passed to two steps and we need to ensure
    # it's only executed once
    a = incr.bind()
    i1 = identity.bind(a)
    i2 = identity.bind(a)
    assert "1" == workflow.create(join.bind(i1, i2)).run()
    assert "2" == workflow.create(join.bind(i1, i2)).run()
    # pass a multiple times
    assert "3" == workflow.create(join.bind(a, a, a, a)).run()
    assert "4" == workflow.create(join.bind(a, a, a, a)).run()


def test_run_off_main_thread(workflow_start_regular):
    @ray.remote
    def fake_data(num: int):
        return list(range(num))

    succ = False

    # Start new thread here ⚠️
    def run():
        global succ
        # Setup the workflow.
        data = workflow.create(fake_data.bind(10))
        assert data.run(workflow_id="run") == list(range(10))

    import threading

    t = threading.Thread(target=run)
    t.start()
    t.join()
    assert workflow.get_status("run") == workflow.SUCCESSFUL


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
