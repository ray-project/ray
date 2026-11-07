"""Request node-local creation, then exec attach without changing the shim PID."""

import http.client
import ipaddress
import os
import re
import time
from urllib.parse import urlsplit

# Bare boolean flags do not consume the next token. Other flags retain their
# value verbatim, even if that value looks like an attach/detach option.
_BOOLEAN_OPTIONS = {
    "--detach",
    "--disable-content-trust",
    "--help",
    "--init",
    "--interactive",
    "--no-healthcheck",
    "--oom-kill-disable",
    "--privileged",
    "--publish-all",
    "--quiet",
    "--read-only",
    "--read-only-tmpfs",
    "--replace",
    "--rm",
    "--rmi",
    "--sig-proxy",
    "--tls-verify",
    "--tty",
}


def split_container_options(options):
    create_options, attach_options = [], []
    index = 0
    while index < len(options):
        option = options[index]
        name = option.split("=", 1)[0]
        if name in ("--cidfile", "--name") or option == "--":
            raise ValueError("Ray owns the worker container name and cidfile")
        if name in ("--attach", "-a"):
            raise ValueError("Worker containers attach both stdout and stderr")
        if name == "--sig-proxy":
            attach_options.append(option)
        elif name == "--detach-keys":
            attach_options.append(option)
            if "=" not in option:
                index += 1
                if index == len(options):
                    raise ValueError("--detach-keys requires a value")
                attach_options.append(options[index])
        elif name == "--detach":
            if option != "--detach=false":
                raise ValueError("Worker containers require foreground attachment")
        elif option.startswith("-") and not option.startswith("--"):
            flags = option[1:]
            if flags and all(flag in "dit" for flag in flags):
                if "d" in flags:
                    raise ValueError("Worker containers require foreground attachment")
                create_options.append(option)
                if "i" in flags:
                    attach_options.append("--interactive")
            else:
                create_options.append(option)
                if len(flags) == 1 and flags not in "hPq":
                    index += 1
                    if index == len(options):
                        raise ValueError(f"{option} requires a value")
                    create_options.append(options[index])
        else:
            create_options.append(option)
            if name == "--interactive" and option != "--interactive=false":
                attach_options.append("--interactive")
            if (
                option.startswith("--")
                and "=" not in option
                and name not in _BOOLEAN_OPTIONS
            ):
                index += 1
                if index == len(options):
                    raise ValueError(f"{option} requires a value")
                create_options.append(options[index])
        index += 1
    return create_options, attach_options


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
    expires_at_ms = int(launcher_env["RAY_WORKER_CONTAINER_DEADLINE_MS"])
    deadline = expires_at_ms / 1000
    create_options, attach_options = split_container_options(options)
    request = protocol.CreateWorkerContainerRequest(
        launch=protocol.WorkerContainerLaunch(
            launch_id=launcher_env["RAY_WORKER_CONTAINER_LAUNCH_ID"],
            session_id=launcher_env["RAY_WORKER_CONTAINER_SESSION_ID"],
            job_id=launcher_env["RAY_WORKER_CONTAINER_JOB_ID"],
            expires_at_ms=expires_at_ms,
        ),
        options=create_options,
        entrypoint=entrypoint,
        environment=launcher_env,
        working_directory=os.getcwd(),
    )
    headers = {"Content-Type": "application/octet-stream"}
    headers.update(get_auth_headers_if_auth_enabled({}))
    delay = 0.1
    while True:
        remaining = deadline - time.time()
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
            time.sleep(max(0, min(delay, deadline - time.time())))
            delay = min(delay * 2, 1)
        finally:
            connection.close()
    if reply.status != protocol.AGENT_RPC_STATUS_OK:
        raise RuntimeError(reply.error_message)
    if not re.fullmatch("[a-f0-9]{64}", reply.container_id):
        raise RuntimeError("Agent returned an invalid worker container ID")
    command = ["podman", "start", "--attach", *attach_options, reply.container_id]
    os.execvpe(command[0], command, launcher_env)
