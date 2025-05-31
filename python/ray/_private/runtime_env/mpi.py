import logging
import os
import subprocess
from typing import List, Optional

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


def mpi_init():
    """Initialize the MPI cluster. When using MPI cluster, this must be called first."""

    if hasattr(mpi_init, "inited"):
        assert mpi_init.inited is True
        return

    from mpi4py import MPI

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    if rank == 0:
        from ray._private.accelerators import get_all_accelerator_managers

        device_vars = [
            m.get_visible_accelerator_ids_env_var()
            for m in get_all_accelerator_managers()
        ]
        visible_devices = {
            n: os.environ.get(n) for n in device_vars if os.environ.get(n)
        }
        comm.bcast(visible_devices)
        with open(f"/tmp/{os.getpid()}.{rank}", "w") as f:
            f.write(str(visible_devices))
    else:
        visible_devices = comm.bcast(None)
        os.environ.update(visible_devices)
    mpi_init.inited = True


class MPIPlugin(RuntimeEnvPlugin):
    """Plugin for enabling MPI cluster functionality in runtime environments.

    This plugin enables an MPI cluster to run on top of Ray. It handles the setup
    and configuration of MPI processes for distributed computing tasks.

    To use this plugin, add "mpi" to the runtime environment configuration:

    Example:
        @ray.remote(
            runtime_env={
                "mpi": {
                    "args": ["-n", "4"],
                    "worker_entry": worker_entry,
                }
            }
        )
        def calc_pi():
            ...

    Here worker_entry should be function for the MPI worker to run.
    For example, it should be `'py_module.worker_func'`. The module should be able to
    be imported in the runtime.

    In the mpi worker with rank==0, it'll be the normal ray function or actor.
    For the worker with rank > 0, it'll just run `worker_func`.

    ray.runtime_env.mpi_init must be called in the ray actors/tasks before any MPI
    communication.
    """

    priority = 90
    name = "mpi"

    def modify_context(
        self,
        uris: List[str],  # noqa: ARG002
        runtime_env: "RuntimeEnv",  # noqa: F821 ARG002
        context: RuntimeEnvContext,
        logger: Optional[logging.Logger] = default_logger,  # noqa: ARG002
    ) -> None:
        mpi_config = runtime_env.mpi()
        if mpi_config is None:
            return
        try:
            proc = subprocess.run(
                ["mpirun", "--version"], capture_output=True, check=True
            )
        except subprocess.CalledProcessError:
            logger.exception(
                "Failed to run mpi run. Please make sure mpi has been installed"
            )
            # The worker will fail to run and exception will be thrown in runtime
            # env agent.
            raise

        logger.info(f"Running MPI plugin\n {proc.stdout.decode()}")

        # worker_entry should be a file either in the working dir
        # or visible inside the cluster.
        worker_entry = mpi_config.get("worker_entry")

        assert (
            worker_entry is not None
        ), "`worker_entry` must be setup in the runtime env."

        cmds = (
            ["mpirun"]
            + mpi_config.get("args", [])
            + [
                context.py_executable,
                "-m",
                "ray._private.runtime_env.mpi_runner",
                worker_entry,
            ]
        )
        # Construct the start cmd
        context.py_executable = " ".join(cmds)
