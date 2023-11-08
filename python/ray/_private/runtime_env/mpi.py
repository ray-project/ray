import sys
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

        from pathlib import Path

        worker_entry = mpi_config.get("worker_entry", None)
        assert (
            worker_entry is not None
        ), "`worker_entry` must be setup in the runtime env."
        assert Path(worker_entry).is_file(), "`worker_entry` must be a file."

        cmds = (
            ["mpirun"]
            + mpi_config.get("args", [])
            + [
                context.py_executable,
                "-m",
                "ray._private.runtime_env.mpi_runner",
                worker_entry
            ]
        )
        # Construct the start cmd
        context.py_executable = " ".join(cmds)
