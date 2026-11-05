"""Request node-local creation, then exec attach without changing the shim PID."""

import http.client
import ipaddress
import os
import re
import time
from urllib.parse import urlsplit


def split_container_options(options, help_text):
    """Separate run-only flags using the installed Podman's option schema.

    Cobra's help distinguishes bare booleans from flags with a value. Read it
    once per agent, rather than maintaining a version-specific boolean allowlist.
    Values are consumed verbatim, including values beginning with a dash.
    """
    schema, aliases = {}, {}
    for line in help_text.splitlines():
        declaration = re.split(r"\s{2,}", line.strip(), maxsplit=1)[0]
        match = re.fullmatch(r"(?:-(.), )?(--[\w-]+)(?: (.+))?", declaration)
        if match:
            short, name, value_type = match.groups()
            schema[name] = value_type is not None
            if short:
                aliases[short] = name
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
                value = options[index]
            elif not separator:
                value = "true"
            parsed.append((name, value))
        elif token.startswith("-") and len(token) > 1 and token != "--":
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

    create, start, streams = [], [], set()
    detached, interactive = False, False
    for name, value in parsed:
        if name == "--detach":
            detached = boolean(value)
        elif name in ("--sig-proxy", "--detach-keys"):
            start.append(name + "=" + value)
        elif name == "--attach":
            streams.update(value.lower().split(","))
        else:
            create.append(name + "=" + value)
            if name == "--interactive":
                interactive = boolean(value)
    if streams - {"stdin", "stdout", "stderr"} or (detached and streams):
        raise ValueError("Invalid Podman attach/detach combination")
    if not detached:
        start.insert(0, "--attach")
        if interactive and (not streams or "stdin" in streams):
            start.append("--interactive")
    return (
        create,
        start,
        bool(streams and "stdout" not in streams),
        bool(streams and "stderr" not in streams),
    )


def run_worker_container(address, options, entrypoint, launcher_env):
    from ray._private.authentication.http_token_authentication import (
        get_auth_headers_if_auth_enabled,
    )
    from ray.core.generated import runtime_env_agent_pb2 as protocol

    if not address:
        raise RuntimeError("The runtime env agent container endpoint is missing")
    endpoint = urlsplit("http://" + address)
    if not ipaddress.ip_address(endpoint.hostname).is_loopback:
        raise RuntimeError("Worker container control requires a loopback endpoint")
    deadline_ms = int(launcher_env["RAY_WORKER_CONTAINER_DEADLINE_MS"])
    deadline = deadline_ms / 1000
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
            break
        except (OSError, http.client.HTTPException):
            time.sleep(max(0, min(delay, deadline - time.monotonic())))
            delay = min(delay * 2, 1)
        finally:
            connection.close()
    if reply.status != protocol.AGENT_RPC_STATUS_OK:
        raise RuntimeError(reply.error_message)
    if not re.fullmatch("[a-f0-9]{64}", reply.container_id):
        raise RuntimeError("Agent returned an invalid worker container ID")
    for stream, discard in ((1, reply.discard_stdout), (2, reply.discard_stderr)):
        if discard:
            with open(os.devnull, "w") as sink:
                os.dup2(sink.fileno(), stream)
    command = ["podman", "start", *reply.start_options, reply.container_id]
    os.execvpe(command[0], command, launcher_env)
