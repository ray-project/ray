from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import random
import subprocess
import sys
import time

__all__ = [
    "start_plasma_store", "start_plasma_manager", "DEFAULT_PLASMA_STORE_MEMORY"
]

PLASMA_WAIT_TIMEOUT = 2**30

DEFAULT_PLASMA_STORE_MEMORY = 10**9


def random_name():
    return str(random.randint(0, 99999999))


def start_plasma_store(plasma_store_memory=DEFAULT_PLASMA_STORE_MEMORY,
                       use_valgrind=False,
                       use_profiler=False,
                       stdout_file=None,
                       stderr_file=None,
                       plasma_directory=None,
                       huge_pages=False):
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
    plasma_store_name = "/tmp/plasma_store{}".format(random_name())
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


def new_port():
    return random.randint(10000, 65535)


def start_plasma_manager(store_name,
                         redis_address,
                         node_ip_address="127.0.0.1",
                         plasma_manager_port=None,
                         num_retries=20,
                         use_valgrind=False,
                         run_profiler=False,
                         stdout_file=None,
                         stderr_file=None):
    """Start a plasma manager and return the ports it listens on.

    Args:
        store_name (str): The name of the plasma store socket.
        redis_address (str): The address of the Redis server.
        node_ip_address (str): The IP address of the node.
        plasma_manager_port (int): The port to use for the plasma manager. If
            this is not provided, a port will be generated at random.
        use_valgrind (bool): True if the Plasma manager should be started
            inside of valgrind and False otherwise.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.

      Returns:
          A tuple of the Plasma manager socket name, the process ID of the
              Plasma manager process, and the port that the manager is
              listening on.

    Raises:
        Exception: An exception is raised if the manager could not be started.
    """
    plasma_manager_executable = os.path.join(
        os.path.abspath(os.path.dirname(__file__)),
        "../core/src/plasma/plasma_manager")
    plasma_manager_name = "/tmp/plasma_manager{}".format(random_name())
    if plasma_manager_port is not None:
        if num_retries != 1:
            raise Exception("num_retries must be 1 if port is specified.")
    else:
        plasma_manager_port = new_port()
    process = None
    counter = 0
    while counter < num_retries:
        if counter > 0:
            print("Plasma manager failed to start, retrying now.")
        command = [
            plasma_manager_executable,
            "-s",
            store_name,
            "-m",
            plasma_manager_name,
            "-h",
            node_ip_address,
            "-p",
            str(plasma_manager_port),
            "-r",
            redis_address,
        ]
        if use_valgrind:
            process = subprocess.Popen(
                [
                    "valgrind", "--track-origins=yes", "--leak-check=full",
                    "--show-leak-kinds=all", "--error-exitcode=1"
                ] + command,
                stdout=stdout_file,
                stderr=stderr_file)
        elif run_profiler:
            process = subprocess.Popen(
                (["valgrind", "--tool=callgrind"] + command),
                stdout=stdout_file,
                stderr=stderr_file)
        else:
            process = subprocess.Popen(
                command, stdout=stdout_file, stderr=stderr_file)
        # This sleep is critical. If the plasma_manager fails to start because
        # the port is already in use, then we need it to fail within 0.1
        # seconds.
        if use_valgrind:
            time.sleep(1)
        else:
            time.sleep(0.1)
        # See if the process has terminated
        if process.poll() is None:
            return plasma_manager_name, process, plasma_manager_port
        # Generate a new port and try again.
        plasma_manager_port = new_port()
        counter += 1
    raise Exception("Couldn't start plasma manager.")
