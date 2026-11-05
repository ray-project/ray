"""Bounded, session-owned Podman operations; agent and raylet fate-share.

Only a session owner marker/lock is kept on disk. Creators inherit that lock so
another raylet cannot sweep a dead session while creation is still in progress.
Launch fences expire with raylet-issued deadlines; there is no operation journal.
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
from pathlib import Path

try:
    import fcntl
except ImportError:
    fcntl = None

logger = logging.getLogger(__name__)
LAUNCH_LABEL = "ray.worker_launch_id"
SESSION_LABEL = "ray.worker_container_session"


def _birth(pid):
    try:
        started = Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[19]
        try:
            boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        except OSError:
            boot = ""
        return boot + ":" + started
    except FileNotFoundError:
        if os.path.isdir("/proc"):
            return None
        import psutil  # Unit tests on macOS; production containers require Linux.

        try:
            return str(psutil.Process(pid).create_time())
        except psutil.NoSuchProcess:
            return None


class UnsettledCommandError(RuntimeError):
    pass


@dataclass
class Launch:
    identity: dict
    granted: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False
    creator: object = None
    cleanup: object = None
    expiry: object = None
    failed: bool = False
    unsettled: bool = False
    digest: str = ""


class WorkerContainerManager:
    def __init__(
        self,
        root,
        operation_timeout=30,
        cleanup_attempts=3,
        max_pending=4096,
        podman_command=None,
    ):
        if operation_timeout <= 0 or cleanup_attempts < 1 or max_pending < 1:
            raise ValueError(
                "Container operation timeout, attempts, and capacity must be positive"
            )
        self.root = Path(root)
        self.operation_timeout = operation_timeout
        self.cleanup_attempts = cleanup_attempts
        self.max_pending = max_pending
        self.podman_command = podman_command or ["podman"]
        self.records = {}
        self.session = None
        self._owner_lock = None
        self._recoveries = set()
        self._commands = set()
        self._failed = False
        self._reject_before = 0
        self._closing = False
        self._marking_used = None
        self._owner = None

    async def start(self, session, owner_pid):
        if not re.fullmatch("[a-f0-9]{56}", session):
            raise RuntimeError("Worker containers require the raylet NodeID")
        self.session = session
        try:
            owners = await asyncio.to_thread(self._claim, owner_pid)
        except (OSError, ValueError, KeyError, TypeError) as error:
            self._failed = True
            logger.error(
                "Cannot initialize worker container ownership at %s: %s",
                self.root,
                error,
            )
            return
        for path, owner in owners:
            task = asyncio.create_task(self._recover(path, owner))
            self._recoveries.add(task)
            task.add_done_callback(self._recoveries.discard)

    def _claim(self, owner_pid):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock = open(self.root / (self.session + ".lock"), "a")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            owners = []
            for path in self.root.glob("*.json"):
                owner = json.loads(path.read_text())
                if (
                    not isinstance(owner, dict)
                    or not re.fullmatch("[a-f0-9]{56}", path.stem)
                    or not isinstance(owner.get("birth"), str)
                    or not owner.get("birth")
                    or type(owner.get("pid")) is not int
                    or owner["pid"] <= 0
                    or type(owner.get("used", True)) is not bool
                ):
                    raise ValueError(f"Invalid container session owner: {path}")
                if path.stem != self.session and _birth(owner["pid"]) != owner["birth"]:
                    owners.append((path, owner))
            marker = self.root / (self.session + ".json")
            temporary = marker.with_suffix(".tmp")
            self._owner = {"pid": owner_pid, "birth": _birth(owner_pid), "used": False}
            temporary.write_text(json.dumps(self._owner))
            os.chmod(temporary, 0o600)
            os.replace(temporary, marker)
        except BaseException:
            lock.close()
            raise
        self._owner_lock = lock
        return owners

    def check_admission(self):
        if self._closing or self._failed or self._recoveries:
            raise RuntimeError(
                "Worker container cleanup is pending or failed on this node; "
                "new containers are disabled. See runtime_env_agent.log"
            )
        if sum(
            record.cancelled and (record.cleanup is None or not record.cleanup.done())
            for record in self.records.values()
        ) >= min(32, self.max_pending):
            raise RuntimeError(
                "Too many pending worker container cleanups on this node"
            )
        if len(self.records) >= self.max_pending:
            raise RuntimeError(
                "Too many pending worker container launches on this node"
            )

    def _entry(self, identity, allow_expired=False):
        if (
            identity["session_id"] != self.session
            or not re.fullmatch("[a-f0-9]{56}", identity["launch_id"])
            or not re.fullmatch("[a-f0-9]{8}", identity["job_id"])
        ):
            raise RuntimeError("Invalid raylet-owned container launch")
        deadline = identity["expires_at_ms"]
        if not allow_expired and (
            deadline <= time.time() * 1000 or deadline <= self._reject_before
        ):
            raise RuntimeError("Worker container launch expired or was cancelled")
        record = self.records.get(identity["launch_id"])
        if record is None:
            if len(self.records) >= self.max_pending:
                raise RuntimeError("Too many pending worker container launches")
            record = Launch(dict(identity))
            self.records[identity["launch_id"]] = record
            record.expiry = asyncio.get_running_loop().call_later(
                max(0, deadline / 1000 - time.time()), self._retire, record
            )
        elif record.identity != identity:
            raise RuntimeError("Worker container launch identity changed")
        return record

    def register(self, identity):
        if identity["launch_id"] not in self.records:
            self.check_admission()
        record = self._entry(identity)
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        record.granted.set()

    async def create_registered(self, identity, options, entrypoint, environment, cwd):
        if identity["launch_id"] not in self.records:
            self.check_admission()
        record = self._entry(identity)
        await asyncio.wait_for(
            record.granted.wait(),
            max(0, identity["expires_at_ms"] / 1000 - time.time()),
        )
        if record.cancelled:
            raise RuntimeError("Worker container launch was cancelled")
        digest = hashlib.sha256(
            json.dumps([options, entrypoint, environment, cwd], sort_keys=True).encode()
        ).hexdigest()
        if record.digest and record.digest != digest:
            raise RuntimeError("Duplicate container launch has different inputs")
        if record.creator is None:
            if self._failed or self._closing or self._recoveries:
                raise RuntimeError("Container cleanup is unhealthy on this node")
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

    def _name(self, identity):
        return "ray-worker-" + identity["session_id"] + "-" + identity["launch_id"]

    async def _create(self, record, options, entrypoint, environment, cwd):
        identity = record.identity
        try:
            output = await self._run(
                [
                    "create",
                    *options,
                    "--name=" + self._name(identity),
                    "--label=" + LAUNCH_LABEL + "=" + identity["launch_id"],
                    "--label=" + SESSION_LABEL + "=" + identity["session_id"],
                    "--label=ray.job_id=" + identity["job_id"],
                    *entrypoint,
                ],
                environment,
                cwd,
                timeout=max(0, identity["expires_at_ms"] / 1000 - time.time()),
            )
            cid = output.strip()
            if not re.fullmatch("[a-f0-9]{64}", cid):
                raise RuntimeError("Podman returned an invalid container ID")
            return cid
        except (OSError, RuntimeError, asyncio.TimeoutError) as error:
            record.unsettled = isinstance(error, UnsettledCommandError)
            self.stop(identity)
            raise

    def stop(self, identity):
        """Accept cancellation immediately; only the agent retries cleanup."""
        if self._owner_lock is None:
            raise RuntimeError("Container ownership is unavailable on this node")
        try:
            record = self._entry(identity, allow_expired=True)
        except RuntimeError:
            self._reject_before = max(self._reject_before, identity["expires_at_ms"])
            self._failed = True
            raise
        record.cancelled = True
        record.granted.set()
        if record.cleanup is None:
            record.cleanup = asyncio.create_task(self._cleanup(record))
        return True

    async def _cleanup(self, record):
        for attempt in range(self.cleanup_attempts):
            try:
                if record.creator is not None:
                    try:
                        await asyncio.shield(record.creator)
                    except (OSError, RuntimeError, asyncio.TimeoutError):
                        pass
                if record.unsettled:
                    raise RuntimeError(
                        "Creator has not terminated; retaining cleanup ownership"
                    )
                await asyncio.wait_for(
                    self._remove(record.identity), self.operation_timeout
                )
                self._retire(record)
                return
            except (OSError, RuntimeError, asyncio.TimeoutError) as error:
                logger.warning(
                    "Container cleanup %s failed (attempt %s/%s): %s",
                    record.identity["launch_id"],
                    attempt + 1,
                    self.cleanup_attempts,
                    error,
                )
                if attempt + 1 < self.cleanup_attempts:
                    await asyncio.sleep(2**attempt)
        record.failed = True
        self._failed = True
        logger.error(
            "Container cleanup exhausted retries; disabling new containers on this node"
        )

    async def _remove(self, identity):
        await self._run(
            ["rm", "--force", "--ignore", "--volumes", "--time=0", self._name(identity)]
        )
        found = await self._run(
            [
                "ps",
                "--all",
                "--quiet",
                "--filter",
                "name=^" + self._name(identity) + "$",
            ]
        )
        if found.strip():
            raise RuntimeError("Worker container still exists after removal")

    def _retire(self, record):
        if record.failed or (record.creator is not None and not record.creator.done()):
            return
        if (
            record.cancelled
            and record.cleanup is not None
            and not record.cleanup.done()
        ):
            record.cleanup.add_done_callback(lambda _: self._retire(record))
            return
        if asyncio.get_running_loop().time() < record.expiry.when():
            return
        self._reject_before = max(self._reject_before, record.identity["expires_at_ms"])
        self.records.pop(record.identity["launch_id"], None)
        record.expiry.cancel()

    def _mark_used(self):
        marker = self.root / (self.session + ".json")
        temporary = marker.with_suffix(".tmp")
        temporary.write_text(json.dumps(dict(self._owner, used=True)))
        os.chmod(temporary, 0o600)
        os.replace(temporary, marker)

    async def _run(self, args, environment=None, cwd=None, timeout=None):
        # One write per session, before its first creator. Unused sessions can
        # subsequently be discarded without requiring Podman to be installed.
        if args[0] == "create":
            if self._marking_used is None:
                self._marking_used = asyncio.create_task(
                    asyncio.to_thread(self._mark_used)
                )
            await asyncio.wait_for(
                asyncio.shield(self._marking_used), self.operation_timeout
            )
        process = await asyncio.create_subprocess_exec(
            *self.podman_command,
            *args,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
            cwd=cwd,
            start_new_session=True,
            pass_fds=(self._owner_lock.fileno(),),
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
            try:
                await asyncio.wait_for(
                    asyncio.shield(communication), self.operation_timeout
                )
            except asyncio.TimeoutError:
                self._failed = True
                raise UnsettledCommandError(
                    "Podman creator could not be terminated; new containers disabled"
                )
            raise
        if process.returncode != 0:
            # Engine errors can echo credentials. Never log raw argv/environment/output.
            raise RuntimeError(
                f"Podman {args[0]} failed with exit code {process.returncode}"
            )
        return stdout.decode()

    async def _recover(self, path, owner):
        lock = None
        try:
            for attempt in range(self.cleanup_attempts):
                try:
                    lock = await asyncio.to_thread(self._lock_dead_owner, path, owner)
                    if lock is None:
                        return
                    if not owner.get("used", True):
                        await asyncio.to_thread(self._forget_owner, path)
                        return
                    query = [
                        "ps",
                        "--all",
                        "--quiet",
                        "--no-trunc",
                        "--filter",
                        "label=" + SESSION_LABEL + "=" + path.stem,
                    ]
                    ids = (await self._run(query)).split()
                    if any(not re.fullmatch("[a-f0-9]{64}", cid) for cid in ids):
                        raise RuntimeError(
                            "Invalid container IDs during dead-session cleanup"
                        )
                    if ids:
                        await self._run(
                            ["rm", "--force", "--ignore", "--volumes", "--time=0", *ids]
                        )
                    if (await self._run(query)).strip():
                        raise RuntimeError("Dead-session containers remain")
                    await asyncio.to_thread(self._forget_owner, path)
                    return
                except (OSError, RuntimeError, asyncio.TimeoutError) as error:
                    logger.warning(
                        "Dead container session %s cleanup pending: %s",
                        path.stem,
                        error,
                    )
                    if lock is not None:
                        lock.close()
                        lock = None
                    if attempt + 1 < self.cleanup_attempts:
                        await asyncio.sleep(2**attempt)
            self._failed = True
            logger.error(
                "Dead-session cleanup failed; disabling new containers on this node"
            )
        finally:
            if lock is not None:
                lock.close()

    def _lock_dead_owner(self, path, owner):
        if _birth(owner["pid"]) == owner["birth"]:
            return None
        lock = open(path.with_suffix(".lock"), "a")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            lock.close()
            raise
        return lock

    def _forget_owner(self, path):
        path.unlink(missing_ok=True)
        path.with_suffix(".lock").unlink(missing_ok=True)

    async def close(self):
        self._closing = True
        tasks = list(self._recoveries)
        for record in self.records.values():
            record.expiry.cancel()
            tasks.extend(
                task for task in (record.creator, record.cleanup) if task is not None
            )
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._owner_lock is not None:
            self._owner_lock.close()
            self._owner_lock = None
