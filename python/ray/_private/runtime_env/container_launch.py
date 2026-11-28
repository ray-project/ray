"""Bridge launcher processes and the runtime env agent for worker containers.

The agent owns creation. Options with create/start equivalents use that split;
run-only options retain Podman's run semantics with a launcher stdio handoff.
"""

import http.client
import ipaddress
import os
import re
import signal
import socket
import tempfile
import time
from array import array
from contextlib import ExitStack
from urllib.parse import urlsplit


def split_container_options(options, help_text, create_help, global_help):
    """Split run options into create/start settings in the runtime env agent.

    The agent fetches Podman's help once. Its option declarations distinguish
    bare booleans from flags with a value, avoiding a version-specific flag list.

    ``options`` contains expanded run option tokens, without the image or command.
    ``help_text`` combines Podman's ``--help`` and ``run --help`` output.
    Separate create and global declarations determine each option's scope.

    Return create options, start options, stdout/stderr discard flags, global
    options, command-specific run options, and whether run itself is required.
    """

    def declarations(text):
        schema, aliases = {}, {}
        for line in text.splitlines():
            declaration = re.split(r"\s{2,}", line.strip(), maxsplit=1)[0]
            match = re.fullmatch(r"(?:-(.), )?(--[\w-]+)(?: (.+))?", declaration)
            if match:
                short, name, value_type = match.groups()
                schema[name] = value_type is not None
                if short:
                    aliases[short] = name
        return schema, aliases

    schema, aliases = declarations(help_text)
    create_schema, _ = declarations(create_help)
    global_schema, _ = declarations(global_help)
    if "--detach" not in schema or "--interactive" not in schema:
        raise RuntimeError("Cannot read Podman's run option schema")

    parsed = []
    index = 0
    while index < len(options):
        token = options[index]
        if token.startswith("--") and token != "--":
            name, separator, value = token.partition("=")
            if name not in schema:
                raise ValueError(f"Unknown Podman run option: {name}")
            if schema[name] and not separator:
                index += 1
                if index == len(options):
                    raise ValueError(f"{name} requires a value")
                # A declared value is data even if it looks like a flag, as in
                # ``--label --detach``.
                value = options[index]
            elif not separator:
                value = "true"
            parsed.append((name, value))
        elif token.startswith("-") and len(token) > 1 and token != "--":
            # Expand combined flags such as -dit. A value-taking short flag
            # consumes the remaining suffix, or the next token, verbatim.
            flags = token[1:]
            while flags:
                short, flags = flags[0], flags[1:]
                if short not in aliases:
                    raise ValueError(f"Unknown Podman run option: -{short}")
                name = aliases[short]
                value = "true"
                if schema[name]:
                    if flags:
                        value, flags = flags.removeprefix("="), ""
                    else:
                        index += 1
                        if index == len(options):
                            raise ValueError(f"{name} requires a value")
                        value = options[index]
                elif flags.startswith("="):
                    value, flags = flags[1:], ""
                parsed.append((name, value))
        else:
            raise ValueError("Expected a Podman run option before the image")
        index += 1

    def boolean(value):
        if value.lower() not in {"true", "false", "1", "0", "t", "f"}:
            raise ValueError("Invalid Podman boolean option")
        return value.lower() in {"true", "1", "t"}

    # Container configuration belongs to create; attachment and signal handling
    # belong to the launcher's start. Detach means omitting start --attach.
    create, start, global_options, run_options, streams = [], [], [], [], set()
    use_run = False
    detached, interactive = False, False
    for name, value in parsed:
        if name in global_schema:
            global_options.append(name + "=" + value)
            continue
        # Rebuild from parsed options so flag-looking values stay data. Global
        # options are emitted separately before every Podman subcommand.
        run_options.append(name + "=" + value)
        if name == "--detach":
            detached = boolean(value)
        elif name in ("--sig-proxy", "--detach-keys"):
            start.append(name + "=" + value)
        elif name == "--attach":
            streams.update(value.lower().split(","))
        else:
            if name in create_schema:
                create.append(name + "=" + value)
            else:
                # Some run options (e.g. passwd and rmi) have no create/start
                # equivalent. Preserve them by executing run itself in the agent.
                use_run = True
            if name == "--interactive":
                interactive = boolean(value)
    if streams - {"stdin", "stdout", "stderr"} or (detached and streams):
        raise ValueError("Invalid Podman attach/detach combination")
    if not detached:
        start.insert(0, "--attach")
        if interactive and (not streams or "stdin" in streams):
            start.append("--interactive")
    # start --attach cannot select individual output streams. The launcher
    # redirects unselected streams to /dev/null to preserve run -a semantics.
    return (
        create,
        start,
        bool(streams and "stdout" not in streams),
        bool(streams and "stderr" not in streams),
        global_options,
        run_options,
        use_run,
    )


def _wait_for_managed_run(listener, deadline, resources):
    """Give the agent the launcher's streams, then relay signals and exit status."""
    listener.settimeout(max(0, deadline - time.monotonic()))
    with listener.accept()[0] as channel:
        # The connected channel is sufficient now. Remove the private listener
        # before a long-lived worker (or a killed launcher) can leave it behind.
        resources.close()
        channel.sendmsg(
            [b"F"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array("i", [0, 1, 2]))]
        )
        previous = {}

        def forward(signum, _frame):
            try:
                channel.sendall(f"{signum}\n".encode())
            except OSError:
                pass

        try:
            for signum in (
                signal.SIGTERM,
                signal.SIGINT,
                signal.SIGHUP,
                signal.SIGQUIT,
                signal.SIGWINCH,
                signal.SIGUSR1,
                signal.SIGUSR2,
                signal.SIGCONT,
            ):
                previous[signum] = signal.signal(signum, forward)
            with channel.makefile("rb") as reply:
                status = reply.readline(32)
            if not re.fullmatch(rb"-?\d+\n", status):
                raise RuntimeError("Lost agent-owned Podman run")
            returncode = int(status)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
    return returncode


def run_worker_container(address, options, entrypoint, launcher_env):
    """Request creation from the agent, then exec start in the worker launcher.

    ``address`` is the agent's loopback control address, including its port.
    ``options`` contains expanded run options, including any --entrypoint option;
    ``entrypoint`` is the create command's positional tail: [image, *worker_args].
    ``launcher_env`` is the host environment for both create and start.

    Usually exec replaces this process with Podman start. Run-only options use
    agent-owned run, with this process forwarding IO, signals, and exit status.
    """
    from ray._private.authentication.http_token_authentication import (
        get_auth_headers_if_auth_enabled,
    )
    from ray.core.generated import runtime_env_agent_pb2 as protocol

    if not address:
        raise RuntimeError("The runtime env agent container endpoint is missing")
    endpoint = urlsplit("http://" + address)
    if not ipaddress.ip_address(endpoint.hostname).is_loopback:
        raise RuntimeError("Worker container control requires a loopback endpoint")
    # Raylet supplies one absolute monotonic startup deadline for all attempts.
    deadline_ms = int(launcher_env["RAY_WORKER_CONTAINER_DEADLINE_MS"])
    deadline = deadline_ms / 1000
    # Reuse the identity and inputs after a lost reply. The agent makes retries
    # idempotent by retaining the launch record and sharing one creation task.
    request = protocol.CreateWorkerContainerRequest(
        launch=protocol.WorkerContainerLaunch(
            launch_id=launcher_env["RAY_WORKER_CONTAINER_LAUNCH_ID"],
            session_id=launcher_env["RAY_WORKER_CONTAINER_SESSION_ID"],
            job_id=launcher_env["RAY_WORKER_CONTAINER_JOB_ID"],
            deadline_ms=deadline_ms,
        ),
        options=options,
        entrypoint=entrypoint,
        environment=launcher_env,
        working_directory=os.getcwd(),
    )
    headers = {"Content-Type": "application/octet-stream"}
    headers.update(get_auth_headers_if_auth_enabled({}))
    with ExitStack() as resources:
        listener = None
        while True:
            reply = _request_creation(endpoint, request, headers, deadline)
            if not reply.use_run:
                break
            if listener is not None:
                returncode = _wait_for_managed_run(listener, deadline, resources)
                break
            # Only run-only options need an FD handoff. A private, short path
            # also fits AF_UNIX's path limit on hosts with long TMPDIR values.
            directory = resources.enter_context(
                tempfile.TemporaryDirectory(prefix="ray-run-", dir="/tmp")
            )
            listener = resources.enter_context(
                socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            )
            request.stdio_socket = os.path.join(directory, "stdio")
            listener.bind(request.stdio_socket)
            listener.listen(1)
    if reply.use_run:
        if returncode < 0:
            if -returncode not in (signal.SIGKILL, signal.SIGSTOP):
                signal.signal(-returncode, signal.SIG_DFL)
            os.kill(os.getpid(), -returncode)
        raise SystemExit(returncode)
    if not re.fullmatch("[a-f0-9]{64}", reply.container_id):
        raise RuntimeError("Agent returned an invalid worker container ID")
    for stream, discard in ((1, reply.discard_stdout), (2, reply.discard_stderr)):
        if discard:
            with open(os.devnull, "w") as sink:
                os.dup2(sink.fileno(), stream)
    command = [
        "podman",
        *reply.global_options,
        "start",
        *reply.start_options,
        reply.container_id,
    ]
    # Keep the launcher's PID; the container's Python worker is a separate process.
    os.execvpe(command[0], command, launcher_env)


def _request_creation(endpoint, request, headers, deadline):
    from ray.core.generated import runtime_env_agent_pb2 as protocol

    delay = 0.1
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("Worker container launch deadline exceeded")
        connection = http.client.HTTPConnection(
            endpoint.hostname, endpoint.port, timeout=remaining
        )
        try:
            connection.request(
                "POST", "/create_worker_container", request.SerializeToString(), headers
            )
            response = connection.getresponse()
            body = response.read()
            if response.status != 200:
                raise RuntimeError(f"Container agent returned HTTP {response.status}")
            reply = protocol.CreateWorkerContainerReply.FromString(body)
            if reply.status != protocol.AGENT_RPC_STATUS_OK:
                raise RuntimeError(reply.error_message)
            return reply
        except (OSError, http.client.HTTPException):
            time.sleep(max(0, min(delay, deadline - time.monotonic())))
            delay = min(delay * 2, 1)
        finally:
            connection.close()
