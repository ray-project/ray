import pytest
import ray
import os

mpi_worker_file = os.path.join(os.path.dirname(__file__), "mpi_worker.py")

@pytest.mark.skipif(sys.platform != "linux", reason="Only test MPI on linux.")
def test_mpi_func_pi(ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": mpi_worker_file,
            }
        }
    )
    def calc_pi():
        from mpi_worker import run

        return run()

    assert "3.14" == "%.2f" % (ray.get(calc_pi.remote()))


@pytest.mark.skipif(sys.platform != "linux", reason="Only test MPI on linux.")
def test_mpi_actor_pi(ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": mpi_worker_file,
            }
        }
    )
    class Actor:
        def calc_pi(self):
            from mpi_worker import run

            return run()

    actor = Actor.remote()

    assert "3.14" == "%.2f" % (ray.get(actor.calc_pi.remote()))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
