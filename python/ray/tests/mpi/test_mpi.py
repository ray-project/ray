import pytest
import ray


def test_mpi_func_pi(ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "mpi_worker.py",
            }
        }
    )
    def calc_pi():
        from mpi_worker import run

        return run()

    assert "3.14" == "%.2f" % (ray.get(calc_pi.remote()))


def test_mpi_actor_pi(ray_start_regular):
    @ray.remote(
        runtime_env={
            "mpi": {
                "args": ["-n", "4"],
                "worker_entry": "mpi_worker.py",
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
