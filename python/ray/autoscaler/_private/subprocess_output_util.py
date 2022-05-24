import os
import re
import subprocess
import tempfile
import time
import sys

from ray.autoscaler._private.cli_logger import cli_logger, cf

CONN_REFUSED_PATIENCE = 30  # how long to wait for sshd to run

_redirect_output = False  # Whether to log command output to a temporary file
_allow_interactive = True  # whether to pass on stdin to running commands.


def is_output_redirected():
    return _redirect_output


def set_output_redirected(val: bool):
    """Choose between logging to a temporary file and to `sys.stdout`.

    The default is to log to a file.

    Args:
        val (bool): If true, subprocess output will be redirected to
                    a temporary file.
    """
    global _redirect_output
    _redirect_output = val


def does_allow_interactive():
    return _allow_interactive


def set_allow_interactive(val: bool):
    """Choose whether to pass on stdin to running commands.

    The default is to pipe stdin and close it immediately.

    Args:
        val (bool): If true, stdin will be passed to commands.
    """
    global _allow_interactive
    _allow_interactive = val


class ProcessRunnerError(Exception):
    def __init__(self, msg, msg_type, code=None, command=None, special_case=None):
        super(ProcessRunnerError, self).__init__(
            "{} (discovered={}): type={}, code={}, command={}".format(
                msg, special_case, msg_type, code, command
            )
        )

        self.msg_type = msg_type
        self.code = code
        self.command = command

        self.special_case = special_case


_ssh_output_regexes = {
    "known_host_update": re.compile(
        r"\s*Warning: Permanently added '.+' \(.+\) " r"to the list of known hosts.\s*"
    ),
    "connection_closed": re.compile(r"\s*Shared connection to .+ closed.\s*"),
    "timeout": re.compile(
        r"\s*ssh: connect to host .+ port .+: " r"Operation timed out\s*"
    ),
    "conn_refused": re.compile(
        r"\s*ssh: connect to host .+ port .+: Connection refused\s*"
    )
    # todo: check for other connection failures for better error messages?
}


def _read_subprocess_stream(f, output_file, is_stdout=False):
    """Read and process a subprocess output stream.

    The goal is to find error messages and respond to them in a clever way.
    Currently just used for SSH messages (CONN_REFUSED, TIMEOUT, etc.), so
    the user does not get confused by these.

    Ran in a thread each for both `stdout` and `stderr` to
    allow for cross-platform asynchronous IO.

    Note: `select`-based IO is another option, but Windows has
    no support for `select`ing pipes, and Linux support varies somewhat.
    Spefically, Older *nix systems might also have quirks in how they
    handle `select` on pipes.

    Args:
        f: File object for the stream.
        output_file: File object to which filtered output is written.
        is_stdout (bool):
            When `is_stdout` is `False`, the stream is assumed to
            be `stderr`. Different error message detectors are used,
            and the output is displayed to the user unless it matches
            a special case (e.g. SSH timeout), in which case this is
            left up to the caller.
    """

    detected_special_case = None
    while True:
        # ! Readline here is crucial.
        # ! Normal `read()` will block until EOF instead of until
        #   something is available.
        line = f.readline()

        if line is None or line == "":
            # EOF
            break

        if line[-1] == "\n":
            line = line[:-1]

        if not is_stdout:
            if _ssh_output_regexes["connection_closed"].fullmatch(line) is not None:
                # Do not log "connection closed" messages which SSH
                # puts in stderr for no reason.
                #
                # They are never errors since the connection will
                # close no matter whether the command succeeds or not.
                continue

            if _ssh_output_regexes["timeout"].fullmatch(line) is not None:
                # Timeout is not really an error but rather a special
                # condition. It should be handled by the caller, since
                # network conditions/nodes in the early stages of boot
                # are expected to sometimes cause connection timeouts.
                if detected_special_case is not None:
                    raise ValueError(
                        "Bug: ssh_timeout conflicts with another "
                        "special codition: " + detected_special_case
                    )

                detected_special_case = "ssh_timeout"
                continue

            if _ssh_output_regexes["conn_refused"].fullmatch(line) is not None:
                # Connection refused is not really an error but
                # rather a special condition. It should be handled by
                # the caller, since network conditions/nodes in the
                # early stages of boot are expected to sometimes cause
                # CONN_REFUSED.
                if detected_special_case is not None:
                    raise ValueError(
                        "Bug: ssh_conn_refused conflicts with another "
                        "special codition: " + detected_special_case
                    )

                detected_special_case = "ssh_conn_refused"
                continue

            if _ssh_output_regexes["known_host_update"].fullmatch(line) is not None:
                # Since we ignore SSH host control anyway
                # (-o UserKnownHostsFile=/dev/null),
                # we should silence the host control warnings.
                continue

            cli_logger.error(line)

        if output_file is not None and output_file != subprocess.DEVNULL:
            output_file.write(line + "\n")

    return detected_special_case


def _run_and_process_output(
    cmd,
    stdout_file,
    process_runner=subprocess,
    stderr_file=None,
    use_login_shells=False,
):
    """Run a command and process its output for special cases.

    Calls a standard 'check_call' if process_runner is not subprocess.

    Specifically, run all command output through regex to detect
    error conditions and filter out non-error messages that went to stderr
    anyway (SSH writes ALL of its "system" messages to stderr even if they
    are not actually errors).

    Args:
        cmd (List[str]): Command to run.
        process_runner: Used for command execution. Assumed to have
            'check_call' and 'check_output' inplemented.
        stdout_file: File to redirect stdout to.
        stderr_file: File to redirect stderr to.

    Implementation notes:
    1. `use_login_shells` disables special processing
    If we run interactive apps, output processing will likely get
    overwhelmed with the interactive output elements.
    Thus, we disable output processing for login shells. This makes
    the logging experience considerably worse, but it only degrades
    to old-style logging.

    For example, `pip install` outputs HUNDREDS of progress-bar lines
    when downloading a package, and we have to
    read + regex + write all of them.

    After all, even just printing output to console can often slow
    down a fast-printing app, and we do more than just print, and
    all that from Python, which is much slower than C regarding
    stream processing.

    2. `stdin=PIPE` for subprocesses
    Do not inherit stdin as it messes with bash signals
    (ctrl-C for SIGINT) and these commands aren't supposed to
    take input anyway.

    3. `ThreadPoolExecutor` without the `Pool`
    We use `ThreadPoolExecutor` to create futures from threads.
    Threads are never reused.

    This approach allows us to have no custom synchronization by
    off-loading the return value and exception passing to the
    standard library (`ThreadPoolExecutor` internals).

    This instance will be `shutdown()` ASAP so it's fine to
    create one in such a weird place.

    The code is thus 100% thread-safe as long as the stream readers
    are read-only except for return values and possible exceptions.
    """
    stdin_overwrite = subprocess.PIPE
    # This already should be validated in a higher place of the stack.
    assert not (
        does_allow_interactive() and is_output_redirected()
    ), "Cannot redirect output while in interactive mode."
    if process_runner != subprocess or (
        does_allow_interactive() and not is_output_redirected()
    ):
        stdin_overwrite = None

    # See implementation note #1

    if use_login_shells or process_runner != subprocess:
        return process_runner.check_call(
            cmd,
            # See implementation note #2
            stdin=stdin_overwrite,
            stdout=stdout_file,
            stderr=stderr_file,
        )

    with subprocess.Popen(
        cmd,
        # See implementation note #2
        stdin=stdin_overwrite,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,  # line buffering
        universal_newlines=True,  # text mode outputs
    ) as p:
        from concurrent.futures import ThreadPoolExecutor

        # Closing stdin might be necessary to signal EOF to some
        # apps (they might get stuck waiting for input forever otherwise).
        p.stdin.close()

        # See implementation note #3
        with ThreadPoolExecutor(max_workers=2) as executor:
            stdout_future = executor.submit(
                _read_subprocess_stream, p.stdout, stdout_file, is_stdout=True
            )
            stderr_future = executor.submit(
                _read_subprocess_stream, p.stderr, stderr_file, is_stdout=False
            )
            # Wait for completion.
            executor.shutdown()

            # Update `p.returncode`
            p.poll()

            detected_special_case = stdout_future.result()
            if stderr_future.result() is not None:
                if detected_special_case is not None:
                    # This might some day need to be changed.
                    # We should probably make sure the two special cases
                    # are compatible then and that we can handle both by
                    # e.g. reporting both to the caller.
                    raise ValueError(
                        "Bug: found a special case in both stdout and "
                        "stderr. This is not valid behavior at the time "
                        "of writing this code."
                    )
                detected_special_case = stderr_future.result()

            if p.returncode > 0:
                # Process failed, but not due to a signal, since signals
                # set the exit code to a negative value.
                raise ProcessRunnerError(
                    "Command failed",
                    "ssh_command_failed",
                    code=p.returncode,
                    command=cmd,
                    special_case=detected_special_case,
                )
            elif p.returncode < 0:
                # Process failed due to a signal, since signals
                # set the exit code to a negative value.
                raise ProcessRunnerError(
                    "Command failed",
                    "ssh_command_failed",
                    code=p.returncode,
                    command=cmd,
                    special_case="died_to_signal",
                )

            return p.returncode


def run_cmd_redirected(
    cmd, process_runner=subprocess, silent=False, use_login_shells=False
):
    """Run a command and optionally redirect output to a file.

    Args:
        cmd (List[str]): Command to run.
        process_runner: Process runner used for executing commands.
        silent (bool): If true, the command output will be silenced completely
                       (redirected to /dev/null), unless verbose logging
                       is enabled. Use this for running utility commands like
                       rsync.
    """
    if silent and cli_logger.verbosity < 1:
        return _run_and_process_output(
            cmd,
            process_runner=process_runner,
            stdout_file=process_runner.DEVNULL,
            stderr_file=process_runner.DEVNULL,
            use_login_shells=use_login_shells,
        )

    if not is_output_redirected():
        return _run_and_process_output(
            cmd,
            process_runner=process_runner,
            stdout_file=sys.stdout,
            stderr_file=sys.stderr,
            use_login_shells=use_login_shells,
        )
    else:
        tmpfile_path = os.path.join(
            tempfile.gettempdir(), "ray-up-{}-{}.txt".format(cmd[0], time.time())
        )
        with open(
            tmpfile_path,
            mode="w",
            # line buffering
            buffering=1,
        ) as tmp:
            cli_logger.verbose("Command stdout is redirected to {}", cf.bold(tmp.name))

            return _run_and_process_output(
                cmd,
                process_runner=process_runner,
                stdout_file=tmp,
                stderr_file=tmp,
                use_login_shells=use_login_shells,
            )


def handle_ssh_fails(e, first_conn_refused_time, retry_interval):
    """Handle SSH system failures coming from a subprocess.

    Args:
        e: The `ProcessRunnerException` to handle.
        first_conn_refused_time:
            The time (as reported by this function) or None,
            indicating the last time a CONN_REFUSED error was caught.

            After exceeding a patience value, the program will be aborted
            since SSH will likely never recover.
        retry_interval: The interval after which the command will be retried,
                        used here just to inform the user.
    """
    if e.msg_type != "ssh_command_failed":
        return

    if e.special_case == "ssh_conn_refused":
        if (
            first_conn_refused_time is not None
            and time.time() - first_conn_refused_time > CONN_REFUSED_PATIENCE
        ):
            cli_logger.error(
                "SSH connection was being refused "
                "for {} seconds. Head node assumed "
                "unreachable.",
                cf.bold(str(CONN_REFUSED_PATIENCE)),
            )
            cli_logger.abort(
                "Check the node's firewall settings "
                "and the cloud network configuration."
            )

        cli_logger.warning("SSH connection was refused.")
        cli_logger.warning(
            "This might mean that the SSH daemon is "
            "still setting up, or that "
            "the host is inaccessable (e.g. due to "
            "a firewall)."
        )

        return time.time()

    if e.special_case in ["ssh_timeout", "ssh_conn_refused"]:
        cli_logger.print(
            "SSH still not available, retrying in {} seconds.",
            cf.bold(str(retry_interval)),
        )
    else:
        raise e

    return first_conn_refused_time
