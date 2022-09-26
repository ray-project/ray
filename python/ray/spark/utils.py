import subprocess
from urllib.parse import urlparse
import os
import sys


def is_in_databricks_runtime():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


class ShellCommandException(Exception):
    @classmethod
    def from_completed_process(cls, process):
        lines = [
            f"Non-zero exit code: {process.returncode}",
            f"Command: {process.args}",
        ]
        if process.stdout:
            lines += [
                "",
                "STDOUT:",
                process.stdout,
            ]
        if process.stderr:
            lines += [
                "",
                "STDERR:",
                process.stderr,
            ]
        return cls("\n".join(lines))


def exec_cmd(
    cmd,
    *,
    throw_on_error=True,
    extra_env=None,
    capture_output=True,
    synchronous=True,
    stream_output=False,
    **kwargs,
):
    """
    A convenience wrapper of `subprocess.Popen` for running a command from a Python script.

    :param cmd: The command to run, as a list of strings.
    :param throw_on_error: If True, raises an Exception if the exit code of the program is nonzero.
    :param extra_env: Extra environment variables to be defined when running the child process.
                      If this argument is specified, `kwargs` cannot contain `env`.
    :param capture_output: If True, stdout and stderr will be captured and included in an exception
                           message on failure; if False, these streams won't be captured.
    :param synchronous: If True, wait for the command to complete and return a CompletedProcess
                        instance, If False, does not wait for the command to complete and return
                        a Popen instance, and ignore the `throw_on_error` argument.
    :param stream_output: If True, stream the command's stdout and stderr to `sys.stdout`
                          as a unified stream during execution.
                          If False, do not stream the command's stdout and stderr to `sys.stdout`.
    :param kwargs: Keyword arguments (except `text`) passed to `subprocess.Popen`.
    :return:  If synchronous is True, return a `subprocess.CompletedProcess` instance,
              otherwise return a Popen instance.
    """
    illegal_kwargs = set(kwargs.keys()).intersection({"text"})
    if illegal_kwargs:
        raise ValueError(f"`kwargs` cannot contain {list(illegal_kwargs)}")

    env = kwargs.pop("env", None)
    if extra_env is not None and env is not None:
        raise ValueError("`extra_env` and `env` cannot be used at the same time")

    if capture_output and stream_output:
        raise ValueError(
            "`capture_output=True` and `stream_output=True` cannot be specified at the same time"
        )

    env = env if extra_env is None else {**os.environ, **extra_env}

    # In Python < 3.8, `subprocess.Popen` doesn't accept a command containing path-like
    # objects (e.g. `["ls", pathlib.Path("abc")]`) on Windows. To avoid this issue,
    # stringify all elements in `cmd`. Note `str(pathlib.Path("abc"))` returns 'abc'.
    cmd = list(map(str, cmd))

    if capture_output or stream_output:
        if kwargs.get("stdout") is not None or kwargs.get("stderr") is not None:
            raise ValueError(
                "stdout and stderr arguments may not be used with capture_output or stream_output"
            )
        kwargs["stdout"] = subprocess.PIPE
        if capture_output:
            kwargs["stderr"] = subprocess.PIPE
        elif stream_output:
            # Redirect stderr to stdout in order to combine the streams for unified printing to
            # `sys.stdout`, as documented in
            # https://docs.python.org/3/library/subprocess.html#subprocess.run
            kwargs["stderr"] = subprocess.STDOUT

    process = subprocess.Popen(
        cmd,
        env=env,
        text=True,
        **kwargs,
    )
    if not synchronous:
        return process

    if stream_output:
        for output_char in iter(lambda: process.stdout.read(1), ""):
            sys.stdout.write(output_char)

    stdout, stderr = process.communicate()
    returncode = process.poll()
    comp_process = subprocess.CompletedProcess(
        process.args,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )
    if throw_on_error and returncode != 0:
        raise ShellCommandException.from_completed_process(comp_process)
    return comp_process


def get_safe_port(ip):
    import socket
    """Returns an ephemeral port that is very likely to be free to bind to."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((ip, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def check_port_open(host, port):
    import socket
    from contextlib import closing
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


def get_spark_session():
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def get_spark_driver_hostname(spark_session):
    spark_master_url = spark_session.conf.get("spark.master")
    if spark_master_url.lower().startswith("local"):
        return "127.0.0.1"
    else:
        parsed_spark_master_url = urlparse(spark_master_url)
        if parsed_spark_master_url.scheme.lower() != "spark" or \
                not parsed_spark_master_url.hostname:
            raise ValueError(f"Unsupported spark.master URL: {spark_master_url}")
        return parsed_spark_master_url.hostname
