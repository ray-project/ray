"""Own Podman creation until it settles, independently of the worker shim.

Raylet and the agent fate-share. Records fence launches within this session;
cleanup after the node itself dies is outside this manager's scope.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import socket
import time
from array import array
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)
LAUNCH_LABEL = "ray.worker_launch_id"
SESSION_LABEL = "ray.worker_container_session"


class UnsettledCommandError(RuntimeError):
    def __init__(self, completion):
        super().__init__(
            "Podman command has not terminated; retaining cleanup ownership"
        )
        self.completion = completion


@dataclass
class Launch:
    identity: dict
    granted: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False
    creator: object = None
    cleanup: object = None
    expiry: object = None
    unsettled: object = None
    digest: str = ""
    global_options: list = field(default_factory=list)
    environment: object = None
    cwd: object = None
    run_process: object = None
    managed_run: bool = False


class WorkerContainerManager:
    def __init__(
        self,
        operation_timeout=30,
        cleanup_attempts=3,
        max_pending=4096,
        max_concurrent_operations=10,
        podman_command=None,
    ):
        if (
            operation_timeout <= 0
            or cleanup_attempts < 1
            or max_pending < 1
            or max_concurrent_operations < 1
        ):
            raise ValueError(
                "Container timeout, attempts, and capacity must be positive"
            )
        self.operation_timeout = operation_timeout
        self.cleanup_attempts = cleanup_attempts
        self.max_pending = max_pending
        self.podman_command = podman_command or ["podman"]
        self.records = {}
        self.session = None
        self._commands = set()
        self._command_slots = asyncio.Semaphore(max_concurrent_operations)
        self._closing = False

    async def start(self, session):
        if not re.fullmatch("[a-f0-9]{56}", session):
            raise RuntimeError("Worker containers require the raylet NodeID")
        self.session = session

    def check_admission(self):
        if self._closing or any(
            record.cancelled
            for record in self.records.values()
            if record.cleanup is not None and not record.cleanup.done()
        ):
            raise RuntimeError("Worker container cleanup is pending; retry the launch")
        if sum(not r.cancelled for r in self.records.values()) >= self.max_pending:
            raise RuntimeError(
                "Too many pending worker container launches on this node"
            )

    def _entry(self, identity, stopping=False):
        if (
            identity["session_id"] != self.session
            or not re.fullmatch("[a-f0-9]{56}", identity["launch_id"])
            or not re.fullmatch("[a-f0-9]{8}", identity["job_id"])
        ):
            raise RuntimeError("Invalid raylet-owned container launch")
        deadline = identity["deadline_ms"] / 1000
        if not stopping and deadline <= time.monotonic():
            raise RuntimeError("Worker container launch expired")
        record = self.records.get(identity["launch_id"])
        if record is None:
            # Cancellation must remain admissible even at creation capacity.
            if not stopping:
                self.check_admission()
            record = Launch(dict(identity))
            self.records[identity["launch_id"]] = record
            record.expiry = asyncio.get_running_loop().call_later(
                max(0, deadline - time.monotonic()), self._retire, record
            )
        elif record.identity != identity:
            raise RuntimeError("Worker container launch identity changed")
        return record

    def register(self, identity):
        record = self._entry(identity)
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        record.granted.set()

    async def create_registered(
        self,
        identity,
        options,
        entrypoint,
        environment,
        cwd,
        global_options=(),
        stdio_socket="",
    ):
        record = self._entry(identity)
        await asyncio.wait_for(
            record.granted.wait(),
            max(0, identity["deadline_ms"] / 1000 - time.monotonic()),
        )
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        digest = hashlib.sha256(
            json.dumps(
                [
                    options,
                    entrypoint,
                    environment,
                    cwd,
                    list(global_options),
                    stdio_socket,
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        if record.digest and record.digest != digest:
            raise RuntimeError("Duplicate container launch has different inputs")
        if record.creator is None:
            if self._closing:
                raise RuntimeError("Container manager is closing")
            record.digest = digest
            # Freeze the engine context before starting any command, including a
            # creator which fails or is cancelled before returning its CID.
            record.global_options = list(global_options)
            record.environment = dict(environment)
            record.cwd = cwd
            record.managed_run = bool(stdio_socket)
            record.creator = asyncio.create_task(
                self._run_attached(record, options, entrypoint, stdio_socket)
                if stdio_socket
                else self._create(record, options, entrypoint)
            )
            record.creator.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
            record.creator.add_done_callback(lambda _: self._retire(record))
        if stdio_socket:
            # The launcher now sends stdio and waits for run's exit on the Unix
            # socket. HTTP retries only join this task; they never rerun Podman.
            return ""
        cid = await asyncio.shield(record.creator)
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        return cid

    async def _create(self, record, options, entrypoint):
        identity = record.identity
        try:
            output = await self._run(
                [
                    *record.global_options,
                    "create",
                    *options,
                    "--label=" + LAUNCH_LABEL + "=" + identity["launch_id"],
                    "--label=" + SESSION_LABEL + "=" + identity["session_id"],
                    "--label=ray.job_id=" + identity["job_id"],
                    *entrypoint,
                ],
                record.environment,
                record.cwd,
                timeout=max(0, identity["deadline_ms"] / 1000 - time.monotonic()),
            )
            cid = output.strip()
            if not re.fullmatch("[a-f0-9]{64}", cid):
                raise RuntimeError("Podman returned an invalid container ID")
            return cid
        except (OSError, RuntimeError, asyncio.TimeoutError) as error:
            if isinstance(error, UnsettledCommandError):
                self._retain_command(record, error.completion)
            self.stop(identity)
            raise

    async def _run_attached(self, record, options, entrypoint, stdio_socket):
        """Preserve run semantics and retain ownership until descendant IO closes."""
        loop = asyncio.get_running_loop()
        channel = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        channel.setblocking(False)
        descriptors = []
        signals = None
        completion = None
        spawning = None
        returncode = 1

        async def connect():
            await loop.sock_connect(channel, stdio_socket)
            received = loop.create_future()

            def readable():
                if received.done():
                    return
                try:
                    data, control, flags, _ = channel.recvmsg(
                        1, socket.CMSG_SPACE(3 * array("i").itemsize)
                    )
                except BlockingIOError:
                    return
                except OSError as error:
                    loop.remove_reader(channel.fileno())
                    received.set_exception(error)
                    return
                for level, kind, value in control:
                    if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                        values = array("i")
                        values.frombytes(value)
                        descriptors.extend(values)
                if data != b"F" or flags & socket.MSG_CTRUNC or len(descriptors) != 3:
                    received.set_exception(RuntimeError("Invalid worker stdio handoff"))
                else:
                    received.set_result(None)
                loop.remove_reader(channel.fileno())

            loop.add_reader(channel.fileno(), readable)
            try:
                await received
            finally:
                loop.remove_reader(channel.fileno())

        async def forward_signals():
            pending = b""
            while True:
                data = await loop.sock_recv(channel, 128)
                if not data:
                    self.stop(record.identity)
                    return
                pending += data
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    if line.isdigit() and int(line) in signal.valid_signals():
                        try:
                            os.kill(record.run_process.pid, int(line))
                        except ProcessLookupError:
                            pass
                if len(pending) > 16:
                    raise RuntimeError("Invalid worker signal")

        async def relay(reader, descriptor):
            writable = True
            while data := await reader.read(65536):
                # Regular files and pipes both work here. Never change blocking
                # flags on the launcher's shared open file descriptions.
                while data and writable:
                    try:
                        written = await asyncio.to_thread(os.write, descriptor, data)
                        data = data[written:]
                    except OSError:
                        writable = False
                # Still drain after launcher exit: EOF, rather than the parent
                # PID alone, fences wrappers with escaped creation descendants.

        async def spawn():
            nonlocal completion
            record.run_process = await asyncio.create_subprocess_exec(
                *self.podman_command,
                "run",
                *options,
                "--label=" + LAUNCH_LABEL + "=" + record.identity["launch_id"],
                "--label=" + SESSION_LABEL + "=" + record.identity["session_id"],
                "--label=ray.job_id=" + record.identity["job_id"],
                *entrypoint,
                stdin=descriptors[0],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=record.environment,
                cwd=record.cwd,
                start_new_session=True,
            )

            async def settled():
                await asyncio.gather(
                    relay(record.run_process.stdout, descriptors[1]),
                    relay(record.run_process.stderr, descriptors[2]),
                )
                return await record.run_process.wait()

            completion = asyncio.create_task(settled())
            self._commands.add(completion)
            completion.add_done_callback(self._commands.discard)

        try:
            await asyncio.wait_for(
                connect(),
                max(
                    0,
                    min(
                        self.operation_timeout,
                        record.identity["deadline_ms"] / 1000 - time.monotonic(),
                    ),
                ),
            )
            if record.cancelled:
                return
            # An attached run can live as long as the worker. It must not hold a
            # short engine-operation slot for its entire lifetime.
            spawning = asyncio.create_task(spawn())
            await asyncio.shield(spawning)
            if record.cancelled:
                self._kill_run(record)
            signals = asyncio.create_task(forward_signals())
            returncode = await asyncio.shield(completion)
        except (OSError, RuntimeError, asyncio.TimeoutError):
            self.stop(record.identity)
            raise
        except asyncio.CancelledError:
            if spawning is not None:
                await asyncio.shield(spawning)
            self._kill_run(record)
            if completion is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(completion), self.operation_timeout
                    )
                except asyncio.TimeoutError:
                    self._retain_command(record, completion)
            raise
        finally:
            if signals is not None:
                signals.cancel()
                await asyncio.gather(signals, return_exceptions=True)

            def close_descriptors(_=None):
                for descriptor in descriptors:
                    os.close(descriptor)

            if completion is not None and not completion.done():
                completion.add_done_callback(close_descriptors)
            else:
                close_descriptors()
            try:
                await loop.sock_sendall(channel, f"{returncode}\n".encode())
            except OSError:
                pass
            channel.close()

    def _kill_run(self, record):
        if record.run_process is not None and record.run_process.returncode is None:
            try:
                os.killpg(record.run_process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                # Keep awaiting this creator; cleanup cannot race its late effects.
                logger.warning(
                    "Cannot signal Podman run for %s", record.identity["launch_id"]
                )

    def stop(self, identity):
        """Accept cancellation immediately, including launches denied admission."""
        record = self._entry(identity, stopping=True)
        record.cancelled = True
        self._kill_run(record)
        record.granted.set()
        self._schedule_cleanup(record)
        return True

    def _schedule_cleanup(self, record):
        if record.cleanup is None or record.cleanup.done():
            record.cleanup = asyncio.create_task(self._cleanup(record))
            record.cleanup.add_done_callback(lambda _: self._retire(record))

    def _retain_command(self, record, completion):
        record.unsettled = completion

        def settled(_):
            record.unsettled = None

        completion.add_done_callback(settled)

    async def _cleanup(self, record):
        attempt = 0
        while not self._closing:
            try:
                if record.creator is not None:
                    try:
                        if record.managed_run:
                            # run may wait for the container, including behind a
                            # wrapper we cannot signal. Remove while it settles,
                            # but never treat an early empty query as completion.
                            while not record.creator.done():
                                await self._remove(record)
                                try:
                                    await asyncio.wait_for(
                                        asyncio.shield(record.creator), 1
                                    )
                                except asyncio.TimeoutError:
                                    pass
                        await asyncio.shield(record.creator)
                    except (OSError, RuntimeError, asyncio.TimeoutError):
                        pass
                if record.unsettled is not None:
                    # An unsignallable command still owns its slot. Wait for exit
                    # without spending cleanup retries or delaying removal afterward.
                    try:
                        await asyncio.shield(record.unsettled)
                    except (OSError, RuntimeError, asyncio.TimeoutError):
                        pass
                await self._remove(record)
                return
            except (OSError, RuntimeError, asyncio.TimeoutError) as error:
                if isinstance(error, UnsettledCommandError):
                    self._retain_command(record, error.completion)
                    continue
                attempt += 1
                logger.warning(
                    "Container cleanup %s failed (%s/%s): %s",
                    record.identity["launch_id"],
                    attempt,
                    self.cleanup_attempts,
                    error,
                )
                if attempt < self.cleanup_attempts:
                    await asyncio.sleep(2 ** (attempt - 1))
                else:
                    logger.error(
                        "Container cleanup %s exhausted retries; retrying in %s seconds",
                        record.identity["launch_id"],
                        self.operation_timeout,
                    )
                    # Keep one task per cancelled launch, even without new demand.
                    # close() cancels this sleep along with in-flight cleanup.
                    await asyncio.sleep(self.operation_timeout)
                    attempt = 0

    async def _remove(self, record):
        identity = record.identity
        query = [
            *record.global_options,
            "ps",
            "--all",
            "--quiet",
            "--no-trunc",
            "--filter",
            "label=" + SESSION_LABEL + "=" + identity["session_id"],
            "--filter",
            "label=" + LAUNCH_LABEL + "=" + identity["launch_id"],
        ]
        ids = (await self._run(query, record.environment, record.cwd)).split()
        if any(not re.fullmatch("[a-f0-9]{64}", cid) for cid in ids):
            raise RuntimeError("Invalid container IDs during cleanup")
        if ids:
            await self._run(
                [
                    *record.global_options,
                    "rm",
                    "--force",
                    "--ignore",
                    "--volumes",
                    "--time=0",
                    *ids,
                ],
                record.environment,
                record.cwd,
            )
            if (await self._run(query, record.environment, record.cwd)).strip():
                raise RuntimeError("Worker container still exists after removal")

    def _retire(self, record):
        if record.creator is not None and not record.creator.done():
            return
        if record.cleanup is not None and not record.cleanup.done():
            return
        if record.identity["deadline_ms"] / 1000 > time.monotonic():
            return
        self.records.pop(record.identity["launch_id"], None)
        record.expiry.cancel()

    async def _run(self, args, environment=None, cwd=None, timeout=None):
        # Bound engine subprocesses during mass timeouts. Queueing consumes the
        # same operation budget as execution; Register/Stop never wait here.
        budget = (
            self.operation_timeout
            if timeout is None
            else min(timeout, self.operation_timeout)
        )
        deadline = time.monotonic() + budget
        await asyncio.wait_for(self._command_slots.acquire(), max(0, budget))
        release = True
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            return await self._run_command(args, environment, cwd, remaining)
        except UnsettledCommandError as error:
            release = False
            # EPERM is not completion: do not admit another process into this
            # slot until the unsignallable command really terminates.
            error.completion.add_done_callback(lambda _: self._command_slots.release())
            raise
        finally:
            if release:
                self._command_slots.release()

    async def _run_command(self, args, environment, cwd, timeout):
        process = await asyncio.create_subprocess_exec(
            *self.podman_command,
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
            cwd=cwd,
            start_new_session=True,
        )
        communication = asyncio.create_task(process.communicate())
        self._commands.add(communication)
        communication.add_done_callback(self._commands.discard)
        try:
            stdout, _ = await asyncio.wait_for(
                asyncio.shield(communication),
                self.operation_timeout
                if timeout is None
                else min(timeout, self.operation_timeout),
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if process.returncode is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except PermissionError:
                    # sudo/engine wrappers may change credentials. Do not confuse
                    # failure to signal them with proof that creation has stopped.
                    raise UnsettledCommandError(communication)
            try:
                await asyncio.wait_for(
                    asyncio.shield(communication), self.operation_timeout
                )
            except asyncio.TimeoutError:
                raise UnsettledCommandError(communication)
            raise
        if process.returncode != 0:
            # Engine errors can echo credentials. Never log raw argv/env/output.
            raise RuntimeError(
                f"Podman command failed with exit code {process.returncode}"
            )
        return stdout.decode()

    async def close(self):
        self._closing = True
        tasks = []
        for record in self.records.values():
            record.expiry.cancel()
            tasks.extend(
                task for task in (record.creator, record.cleanup) if task is not None
            )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
