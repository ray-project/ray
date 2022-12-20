import time

import pytest

import ray
from ray.train._internal.worker_group import WorkerGroup


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_worker_creation(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(num_workers=2)
    assert len(wg.workers) == 2
    time.sleep(1)
    # Make sure both CPUs are being used by the actors.
    assert "CPU" not in ray.available_resources()
    wg.shutdown()


def test_worker_creation_num_cpus(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(num_cpus_per_worker=2)
    time.sleep(1)
    assert len(wg.workers) == 1
    # Make sure both CPUs are being used by the actor.
    assert "CPU" not in ray.available_resources()
    wg.shutdown()


def test_worker_shutdown(ray_start_2_cpus):
    assert ray.available_resources()["CPU"] == 2
    wg = WorkerGroup(num_workers=2)
    time.sleep(1)
    assert "CPU" not in ray.available_resources()
    assert len(ray._private.state.actors()) == 2
    wg.shutdown()
    time.sleep(1)
    assert ray.available_resources()["CPU"] == 2

    with pytest.raises(RuntimeError):
        wg.execute(lambda: 1)


def test_worker_restart(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    with pytest.raises(RuntimeError):
        wg.start()
    # Avoid race condition.
    time.sleep(1)
    wg.shutdown(0)
    wg.start()
    wg.execute(lambda: 1)


def test_execute_async(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    futures = wg.execute_async(lambda: 1)
    assert len(futures) == 2
    outputs = ray.get(futures)
    assert all(o == 1 for o in outputs)


def test_execute(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    outputs = wg.execute(lambda: 1)
    assert len(outputs) == 2
    assert all(o == 1 for o in outputs)


def test_execute_args(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)
    outputs = wg.execute(lambda x: x, 1)
    assert len(outputs) == 2
    assert all(o == 1 for o in outputs)


def test_execute_single(ray_start_2_cpus):
    wg = WorkerGroup(num_workers=2)

    def f():
        import os

        os.environ["TEST"] = "1"

    wg.execute_single(1, f)

    def check():
        import os

        return os.environ.get("TEST", "0")

    assert wg.execute(check) == ["0", "1"]


def test_bad_resources(ray_start_2_cpus):
    with pytest.raises(ValueError):
        WorkerGroup(num_workers=-1)

    with pytest.raises(ValueError):
        WorkerGroup(num_cpus_per_worker=-1)

    with pytest.raises(ValueError):
        WorkerGroup(num_gpus_per_worker=-1)


def test_placement_group(ray_start_2_cpus):
    """Tests that workers can be removed and added to a placement group."""
    num_workers = 2
    bundle = {"CPU": 1}
    bundles = [bundle.copy() for _ in range(num_workers)]
    placement_group = ray.util.placement_group(bundles)
    wg = WorkerGroup(num_workers=num_workers, placement_group=placement_group)
    wg.remove_workers([0])
    wg.add_workers(1)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
