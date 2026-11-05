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
import time
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
    failed: bool = False
    retry_after: float = 0
    digest: str = ""


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
        # A failed cleanup gets another bounded attempt when there is demand.
        # There is no permanent node-wide failure bit or healthy-worker poller.
        for record in list(self.records.values()):
            if record.failed and time.monotonic() >= record.retry_after:
                self._schedule_cleanup(record)
        if self._closing or any(
            record.cancelled
            for record in self.records.values()
            if record.cleanup is not None
            and (record.failed or not record.cleanup.done())
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

    async def create_registered(self, identity, options, entrypoint, environment, cwd):
        record = self._entry(identity)
        await asyncio.wait_for(
            record.granted.wait(),
            max(0, identity["deadline_ms"] / 1000 - time.monotonic()),
        )
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        digest = hashlib.sha256(
            json.dumps([options, entrypoint, environment, cwd], sort_keys=True).encode()
        ).hexdigest()
        if record.digest and record.digest != digest:
            raise RuntimeError("Duplicate container launch has different inputs")
        if record.creator is None:
            if self._closing:
                raise RuntimeError("Container manager is closing")
            record.digest = digest
            record.creator = asyncio.create_task(
                self._create(record, options, entrypoint, environment, cwd)
            )
            record.creator.add_done_callback(
                lambda task: task.exception() if not task.cancelled() else None
            )
            record.creator.add_done_callback(lambda _: self._retire(record))
        cid = await asyncio.shield(record.creator)
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        return cid

    async def _create(self, record, options, entrypoint, environment, cwd):
        identity = record.identity
        try:
            output = await self._run(
                [
                    "create",
                    *options,
                    "--label=" + LAUNCH_LABEL + "=" + identity["launch_id"],
                    "--label=" + SESSION_LABEL + "=" + identity["session_id"],
                    "--label=ray.job_id=" + identity["job_id"],
                    *entrypoint,
                ],
                environment,
                cwd,
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

    def stop(self, identity):
        """Accept cancellation immediately, including launches denied admission."""
        record = self._entry(identity, stopping=True)
        record.cancelled = True
        record.granted.set()
        self._schedule_cleanup(record)
        return True

    def _schedule_cleanup(self, record):
        if record.cleanup is None or record.cleanup.done():
            record.failed = False
            record.cleanup = asyncio.create_task(self._cleanup(record))
            record.cleanup.add_done_callback(lambda _: self._retire(record))

    def _retain_command(self, record, completion):
        record.unsettled = completion

        def settled(_):
            record.unsettled = None
            if not self._closing:
                # The creator is now unable to materialize another container.
                # Resume cleanup even if its previous retry budget was exhausted.
                self._schedule_cleanup(record)

        completion.add_done_callback(settled)

    async def _cleanup(self, record):
        for attempt in range(self.cleanup_attempts):
            try:
                if record.creator is not None:
                    try:
                        await asyncio.shield(record.creator)
                    except (OSError, RuntimeError, asyncio.TimeoutError):
                        pass
                if record.unsettled is not None and not record.unsettled.done():
                    raise RuntimeError("Podman command is still running")
                await self._remove(record.identity)
                record.failed = False
                return
            except (OSError, RuntimeError, asyncio.TimeoutError) as error:
                if isinstance(error, UnsettledCommandError):
                    self._retain_command(record, error.completion)
                logger.warning(
                    "Container cleanup %s failed (%s/%s): %s",
                    record.identity["launch_id"],
                    attempt + 1,
                    self.cleanup_attempts,
                    error,
                )
                if attempt + 1 < self.cleanup_attempts:
                    await asyncio.sleep(2**attempt)
        record.failed = True
        record.retry_after = time.monotonic() + self.operation_timeout
        logger.error(
            "Container cleanup exhausted retries; a later launch will retry it"
        )

    async def _remove(self, identity):
        query = [
            "ps",
            "--all",
            "--quiet",
            "--no-trunc",
            "--filter",
            "label=" + SESSION_LABEL + "=" + identity["session_id"],
            "--filter",
            "label=" + LAUNCH_LABEL + "=" + identity["launch_id"],
        ]
        ids = (await self._run(query)).split()
        if any(not re.fullmatch("[a-f0-9]{64}", cid) for cid in ids):
            raise RuntimeError("Invalid container IDs during cleanup")
        if ids:
            await self._run(
                ["rm", "--force", "--ignore", "--volumes", "--time=0", *ids]
            )
            if (await self._run(query)).strip():
                raise RuntimeError("Worker container still exists after removal")

    def _retire(self, record):
        if record.failed or (record.creator is not None and not record.creator.done()):
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
                f"Podman {args[0]} failed with exit code {process.returncode}"
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
