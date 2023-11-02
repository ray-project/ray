import os
import argparse
import logging
from typing import List, Optional
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

logger = logging.getLogger(__name__)


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
        mpi_config = runtime_env.mpi
        if mpi_config is None:
            return
        logger.info("Running MPI plugin")
        from pathlib import Path

        # mpirun -n 10 python mpi.py worker_entry_func
        cmds = (
            ["mpirun"]
            + mpi_config["args"]
            + [
                context.py_executable,
                str(Path(__file__).absolute()),
                mpi_config["worker_entry"],
            ]
        )
        # Construct the start cmd
        context.py_executable = " ".join(cmds)


if __name__ == "__main__":
    print("MPI WORKERRRRRR")
    parser = argparse.ArgumentParser(description="Setup MPI worker")
    parser.add_argument("worker_entry_func")
    parser.add_argument("main_entry")

    args, remaining_args = parser.parse_known_args()

    from mpi4py import MPI

    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()
    if rank == 0:
        _, main_file = os.path.split(args.main_entry)
        module, _ = os.path.split(main_file)
        spec = importlib.util.spec_from_file_location(module, args.main_entry)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        main = getattr(module, "main")
        main(remaining_args)
    else:
        module, func = args.worker_entry_func.rsplit(".", 1)
        import importlib

        module = importlib.import_module(module)
        func = getattr(module, func)
        # pass arguments are not supported for now.
        # The user should use os envs
        func()
