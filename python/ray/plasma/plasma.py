from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import subprocess
import sys
import time

from ray.tempfile_services import get_object_store_socket_name

__all__ = ["start_plasma_store", "DEFAULT_PLASMA_STORE_MEMORY"]

DEFAULT_PLASMA_STORE_MEMORY = 10**9


def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False,
                       use_profiler=False,
                       stdout_file=None,
                       stderr_file=None,
                       plasma_directory=None,
                       huge_pages=False,
                       socket_name=None):
    """Start a plasma store process.

    Args:
        use_valgrind (bool): True if the plasma store should be started inside
            of valgrind. If this is True, use_profiler must be False.
        use_profiler (bool): True if the plasma store should be started inside
            a profiler. If this is True, use_valgrind must be False.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        plasma_directory: A directory where the Plasma memory mapped files will
            be created.
        huge_pages: a boolean flag indicating whether to start the
            Object Store with hugetlbfs support. Requires plasma_directory.
        socket_name (str): If provided, it will specify the socket
            name used by the plasma store.

    Return:
        A tuple of the name of the plasma store socket and the process ID of
            the plasma store process.
    """
    if use_valgrind and use_profiler:
        raise Exception("Cannot use valgrind and profiler at the same time.")

    if huge_pages and not (sys.platform == "linux"
                           or sys.platform == "linux2"):
        raise Exception("The huge_pages argument is only supported on "
                        "Linux.")

    if huge_pages and plasma_directory is None:
        raise Exception("If huge_pages is True, then the "
                        "plasma_directory argument must be provided.")

    if not isinstance(plasma_store_memory, int):
        raise Exception("plasma_store_memory should be an integer.")

    plasma_store_executable = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "../core/src/plasma/plasma_store_server")
    plasma_store_name = socket_name or get_object_store_socket_name()
    command = [
        plasma_store_executable, "-s", plasma_store_name, "-m",
        str(plasma_store_memory)
    ]
    if plasma_directory is not None:
        command += ["-d", plasma_directory]
    if huge_pages:
        command += ["-h"]
    if use_valgrind:
        pid = subprocess.Popen(
            [
                "valgrind", "--track-origins=yes", "--leak-check=full",
                "--show-leak-kinds=all", "--leak-check-heuristics=stdstring",
                "--error-exitcode=1"
            ] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    elif use_profiler:
        pid = subprocess.Popen(
            ["valgrind", "--tool=callgrind"] + command,
            stdout=stdout_file,
            stderr=stderr_file)
        time.sleep(1.0)
    else:
        pid = subprocess.Popen(command, stdout=stdout_file, stderr=stderr_file)
        time.sleep(0.1)
    return plasma_store_name, pid
