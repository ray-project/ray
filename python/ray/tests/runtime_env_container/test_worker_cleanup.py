"""Real Linux Podman coverage; --manager supports execution without a Ray wheel."""

import argparse
import asyncio
import importlib.util
import json
import os
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path

WORKER_CODE = """
import json, os, subprocess, time
from pathlib import Path
p = subprocess.Popen(['sleep', '600'], start_new_session=True)
Path('/shared/ready').write_text(json.dumps([os.getpid(), p.pid]))
while not Path('/shared/exit').exists():
    time.sleep(.05)
"""


def load_manager(path):
    if path:
        spec = importlib.util.spec_from_file_location("cleanup_manager", path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        return module
    from ray._private.runtime_env import worker_container_manager

    return worker_container_manager


async def until(condition):
    async def wait():
        while not condition():
            await asyncio.sleep(0.02)

    await asyncio.wait_for(wait(), 30)


def containers(owner):
    return (
        subprocess.check_output(
            [
                "podman",
                "ps",
                "--all",
                "--quiet",
                "--filter",
                "label=ray.worker_container_session=" + owner["session_id"],
            ],
            timeout=30,
        )
        .decode()
        .split()
    )


async def check_scenario(args, scenario):
    module = load_manager(args.manager)
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        os.chmod(work, 0o777)
        owner = dict(
            launch_id=secrets.token_hex(28),
            session_id=secrets.token_hex(28),
            job_id="01000000",
            deadline_ms=int((time.monotonic() + 25) * 1000),
        )
        gate = asyncio.Event()
        gate.set()
        started = asyncio.Event()

        class Manager(module.WorkerContainerManager):
            async def _run(self, command, *a, **kw):
                if command[0] == "create" and scenario in (
                    "during_create",
                    "cancelled_request",
                ):
                    started.set()
                    await gate.wait()
                return await super()._run(command, *a, **kw)

        manager = Manager(operation_timeout=10)
        await manager.start(owner["session_id"])
        shim = subprocess.Popen(
            [sys.executable, __file__, "--attach"], stdin=subprocess.PIPE
        )
        pids, volumes = [], []
        options = [
            "--cgroup-manager=cgroupfs",
            "--userns=keep-id",
            "--network=host",
            "--pid=host",
            "--ipc=host",
            "--volume=/anonymous",
            "--entrypoint=python",
            "-v",
            str(work) + ":/shared:Z",
        ]
        worker = ["-c", WORKER_CODE]
        if scenario == "sitecustomize":
            (work / "sitecustomize.py").write_text(WORKER_CODE)
            options += ["--env=PYTHONPATH=/shared", "--env=PYTHONDONTWRITEBYTECODE=1"]
            worker = ["-c", "raise AssertionError('sitecustomize should block')"]

        async def create():
            return await manager.create_registered(
                owner, options, [args.image, *worker], dict(os.environ), str(work)
            )

        try:
            if scenario == "hung_registry":
                accepted = asyncio.Event()
                connections = []

                async def registry(reader, writer):
                    connections.append(writer)
                    accepted.set()

                server = await asyncio.start_server(registry, "127.0.0.1", 0)
                port = server.sockets[0].getsockname()[1]
                manager.operation_timeout = 2
                command = asyncio.create_task(
                    manager._run(
                        [
                            "create",
                            "--tls-verify=false",
                            "--pull=always",
                            f"localhost:{port}/unavailable:latest",
                            "true",
                        ]
                    )
                )
                try:
                    await asyncio.wait_for(accepted.wait(), 5)
                    try:
                        await command
                    except asyncio.TimeoutError:
                        pass
                    else:
                        raise AssertionError(
                            "Hung registry did not hit the operation deadline"
                        )
                finally:
                    command.cancel()
                    await asyncio.gather(command, return_exceptions=True)
                    server.close()
                    for writer in connections:
                        writer.close()
                    await server.wait_closed()
            elif scenario == "before_create":
                manager.stop(owner)
                try:
                    await create()
                except RuntimeError:
                    pass
                else:
                    raise AssertionError("cancelled launch created")
                await manager.records[owner["launch_id"]].cleanup
            else:
                manager.register(owner)
                if scenario in ("during_create", "cancelled_request"):
                    gate.clear()
                    request = asyncio.create_task(create())
                    await started.wait()
                    if scenario == "cancelled_request":
                        request.cancel()
                        await asyncio.gather(request, return_exceptions=True)
                    assert manager.stop(owner)
                    gate.set()
                    await asyncio.gather(request, return_exceptions=True)
                    await manager.records[owner["launch_id"]].cleanup
                else:
                    cid = await create()
                    shim.stdin.write(cid.encode() + b"\n")
                    shim.stdin.flush()
                    shim.stdin.close()
                    await until((work / "ready").exists)
                    pids = json.loads((work / "ready").read_text())
                    info = json.loads(
                        subprocess.check_output(["podman", "inspect", cid], timeout=30)
                    )[0]
                    volumes = [
                        mount["Name"]
                        for mount in info["Mounts"]
                        if mount["Type"] == "volume"
                    ]
                    assert len(pids) == 2 and volumes
                    if scenario == "exit_before_registration":
                        (work / "exit").touch()
                        assert shim.wait(timeout=20) == 0
                    else:
                        shim.kill()
                        if scenario != "zombie_shim":
                            shim.wait(timeout=10)
                    assert manager.stop(owner)
                    await manager.records[owner["launch_id"]].cleanup
            assert not containers(owner)
            await until(lambda: all(not Path(f"/proc/{pid}").exists() for pid in pids))
            for volume in volumes:
                assert (
                    subprocess.run(
                        ["podman", "volume", "exists", volume], timeout=30
                    ).returncode
                    == 1
                )
            print(
                f"PASS {scenario}: no container, descendants, or anonymous volumes",
                flush=True,
            )
        finally:
            gate.set()
            if shim.poll() is None:
                shim.kill()
            shim.wait(timeout=10)
            if shim.stdin and not shim.stdin.closed:
                shim.stdin.close()
            await manager.close()
            remaining = containers(owner)
            if remaining:
                subprocess.run(
                    ["podman", "rm", "--force", "--volumes", *remaining],
                    timeout=30,
                    check=True,
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--image")
    parser.add_argument("--manager")
    parser.add_argument("--attach", action="store_true")
    args = parser.parse_args()
    if args.attach:
        cid = sys.stdin.readline().strip()
        os.execvp("podman", ["podman", "start", "--attach", cid])
    for scenario in (
        "exit_before_registration",
        "zombie_shim",
        "before_create",
        "during_create",
        "cancelled_request",
        "sitecustomize",
        "hung_registry",
    ):
        asyncio.run(check_scenario(args, scenario))
