import pytest
import ray
import sys
import os
import numpy


@pytest.fixture(autouse=True)
def change_test_dir(request, monkeypatch):
    monkeypatch.chdir(os.path.dirname(__file__))
    yield


def compute_pi(samples):
    count = 0
    for x, y in samples:
        if x**2 + y**2 <= 1:
            count += 1
    pi = 4 * float(count) / len(samples)
    return pi


def run():
    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    nprocs = comm.Get_size()
    myrank = comm.Get_rank()

    if myrank == 0:
        numpy.random.seed(1)
        N = 100000 // nprocs
        samples = numpy.random.random((nprocs, N, 2))
    else:
        samples = None

    samples = comm.scatter(samples, root=0)

    mypi = compute_pi(samples) / nprocs

    pi = comm.reduce(mypi, root=0)

    if myrank == 0:
        return pi


@pytest.mark.skipif(sys.platform != "linux", reason="Only test MPI on linux.")
def test_mpi_func_pi(change_test_dir, ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "test_mpi.run",
            },
        }
    )
    def calc_pi():
        return run()

    assert "3.14" == "%.2f" % (ray.get(calc_pi.remote()))


@pytest.mark.skipif(sys.platform != "linux", reason="Only test MPI on linux.")
def test_mpi_actor_pi(change_test_dir, ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "test_mpi.run",
            },
        }
    )
    class Actor:
        def calc_pi(self):
            return run()

    actor = Actor.remote()

    assert "3.14" == "%.2f" % (ray.get(actor.calc_pi.remote()))


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
