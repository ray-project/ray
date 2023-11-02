import sys
import os
import logging
import argparse
from typing import List, Optional
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
import subprocess
default_logger = logging.getLogger(__name__)


class MPIPlugin(RuntimeEnvPlugin):
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
            proc = subprocess.run(["mpirun", "--version"], capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to run mpi run. Please make sure mpi has been installed")
            raise

        logger.info(f"Running MPI plugin\n {proc.stdout.decode()}")

        from pathlib import Path
        # mpirun -n 10 python mpi.py worker_entry_func
        worker_entry = mpi_config["worker_entry"]
        assert Path(worker_entry).is_file()
        cmds = (
            ["mpirun"]
            + mpi_config.get("args", [])
            + [
                context.py_executable,
                str(Path(__file__).absolute()),
                str(Path(worker_entry).absolute()),
            ]
        )
        # Construct the start cmd
        context.py_executable = " ".join(cmds)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup MPI worker")
    parser.add_argument("worker_entry")
    parser.add_argument("main_entry")

    args, remaining_args = parser.parse_known_args()

    from mpi4py import MPI

    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()

    entry_file = args.main_entry if rank == 0 else args.worker_entry

    import importlib
    sys.argv[1:] = remaining_args
    spec = importlib.util.spec_from_file_location('__main__', entry_file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
