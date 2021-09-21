import os
import re
import subprocess
import time
from typing import Any, Dict, Optional, Union, Tuple
import uuid

import yaml
from zipfile import ZipFile
from pathlib import Path
from ray.experimental.internal_kv import _internal_kv_get
from urllib.parse import urlparse

from ray.util.client.server.background.const import ANYSCALE_BACKGROUND_JOB_CONTEXT
from ray.util.client.server.background.job_context import BackgroundJob, BackgroundJobContext
import ray
from ray._private.runtime_env import working_dir as working_dir_pkg
@ray.remote
class BackgroundJobRunner:
    """
    This class is an actor that runs a shell command on the head node for an anyscale cluster.
    This class will:
    1. Pass a BackgroundJobContext as an environment variable
    2. execute the command in a subprocess (and stream logs appropriately)
    3. Gracefully exit when the command is complete
    """
    def run_background_job(
        self, command: str, self_handle: Any, config_path: str, pkg_uri: str
    ) -> None:

        namespace = ray.get_runtime_context().namespace
        # Update the context with the runtime env uris
        uris = ray.get_runtime_context().runtime_env.get("uris") or []
        context = BackgroundJobContext(namespace, uris)
        env_vars = {
            "PYTHONUNBUFFERED": "1",  # Make sure python subprocess streams logs https://docs.python.org/3/using/cmdline.html#cmdoption-u
            "RAY_ADDRESS": "auto",  # Make sure that internal ray.init has an anyscale RAY_ADDRESS
            ANYSCALE_BACKGROUND_JOB_CONTEXT: context.to_json(),
        }
        env = {**os.environ, **env_vars}

        print("Inside of the actor!!!!")
        print("Inside of the actor!!!!")
        print("Inside of the actor!!!!")

        # GCS version
        code = _internal_kv_get(pkg_uri)
        cur_path = Path().resolve()
        pkg_zip_file = Path(cur_path, "compressed_code")
        pkg_zip_file.write_bytes(code)
        print(f"Uncompressing code to {pkg_zip_file}")
        with ZipFile(pkg_zip_file, 'r') as zip_ref:
            zip_ref.extractall(cur_path)

        # Local disk version
        # print(f"config path: {config_path}")
        # dir_path = os.path.dirname(config_path)
        # print(f"dir path: {dir_path}")
        # path = "/tmp/ray/packaging/github_snapshot_996584529c1834eff63bc59425e5f4cbe2b4d7e1/python/ray/experimental/job/example_job"
        # print(f"dir_path == path is {dir_path == path}")
        # print(f"checking into ... {dir_path}")
        # os.system(f"cd {dir_path}")
        # os.system(f"ls")


        try:
            _run_kill_child(command, shell=True, check=True, env=env)  # noqa
        finally:
            # allow time for any logs to propogate before the task exits
            time.sleep(1)

            self_handle.stop.remote()

    def stop(self) -> None:

        ray.actor.exit_actor()



def _run_kill_child(
    *popenargs, input=None, timeout=None, check=False, **kwargs
) -> subprocess.CompletedProcess:
    """
    This function is a fork of subprocess.run with fewer args.
    The goal is to create a child subprocess that is GUARANTEED to exit when the parent exits
    This is accomplished by:
    1. Making sure the child is the head of a new process group
    2. Create a third "Killer" process that is responsible for killing the child when the parent dies
    3. Killer process checks every second if the parent is dead.
    4. Killing the entire process group when we want to kill the child

    Arguments are the same as subprocess.run
    """
    # Start new session ensures that this subprocess starts as a new process group
    with subprocess.Popen(start_new_session=True, *popenargs, **kwargs) as process:
        parent_pid = os.getpid()
        child_pid = process.pid
        child_pgid = os.getpgid(child_pid)

        # Open a new subprocess to kill the child process when the parent process dies
        # kill -s 0 parent_pid will succeed if the parent is alive.
        # If it fails, SIGKILL the child process group and exit
        subprocess.Popen(
            f"while kill -s 0 {parent_pid}; do sleep 1; done; kill -9 -{child_pgid}",
            shell=True,
            # Suppress output
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        try:
            stdout, stderr = process.communicate(input, timeout=timeout)
        except:  # noqa      (this is taken from subprocess.run directly)
            # Including KeyboardInterrupt, communicate handled that.
            process.kill()
            # We don't call process.wait() as .__exit__ does that for us.
            raise

        retcode = process.poll()
        if check and retcode:
            raise subprocess.CalledProcessError(
                retcode, process.args, output=stdout, stderr=stderr
            )
    return subprocess.CompletedProcess(process.args, retcode or 0, stdout, stderr)
