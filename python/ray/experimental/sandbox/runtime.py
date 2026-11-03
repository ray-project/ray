import asyncio
import collections
import concurrent.futures
import logging
import os
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Union

from ray.experimental.sandbox._internal.cgroup import SandboxCgroupManager
from ray.experimental.sandbox.backend.base import (
    ExecResult,
    SandboxStatus,
)
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig, parse_memory_bytes
from ray.experimental.sandbox.exceptions import SandboxCreationError
from ray.experimental.sandbox.image_manager import ImageManager
from ray.experimental.sandbox.warm_pool_store import (
    CLAIMED,
    READY,
    WarmPoolEntry,
    WarmPoolStore,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Warm-pool boots run a few at a time, so filling a pool leaves CPU for the
# creates that boot cold meanwhile.
_WARM_BOOT_CONCURRENCY = 4
# After a warm-pool boot fails (say, its image can't be pulled), its profile
# boots nothing for this long, and then a timer refills it.
_WARM_RETRY_SECONDS = 30.0
# A pool with a store publishes at least this often, and its entries go
# stale after three periods without a publish.
_PUBLISH_HEARTBEAT_SECONDS = 10.0
# After a publish, the pool waits this long before the next, so a burst of
# creates publishes a few times rather than once per create.
_PUBLISH_COALESCE_SECONDS = 0.05
# Hosts that publish a warm pool from this process: each needs its own name,
# since a publish replaces everything recorded for its host.
_PUBLISHING_HOSTS = set()
_PUBLISHING_HOSTS_LOCK = threading.Lock()


def _boot_key(cfg: SandboxConfig) -> Optional[tuple]:
    """What a booted sandbox must share with a create to stand in for it.

    Everything that is fixed when the sandbox boots; ``env``, ``ttl_seconds``
    and ``timeout_seconds`` are not. None for a create no booted sandbox can
    serve (a custom OCI spec transform).
    """
    if cfg._oci_spec_transform_fn is not None:
        return None
    return (
        cfg.image,
        float(cfg.cpu or 0.0),
        parse_memory_bytes(cfg.memory) or 0,
        cfg.workdir,
        bool(cfg.rootless),
        cfg.network,
        tuple(cfg.dns) if cfg.dns is not None else None,
        tuple(cfg.cidr_allowlist) if cfg.cidr_allowlist is not None else None,
        tuple(cfg.capabilities) if cfg.capabilities is not None else None,
        cfg.shell,
        bool(cfg.readonly),
        parse_memory_bytes(cfg.disk) or 0,
        bool(cfg._ignore_cgroups),
        tuple(cfg.gpu_ids or ()),
        tuple(sorted((cfg.mounts or {}).items())),
    )


# The states of a warm-pool slot. Ready and claimed slots are the ones a
# store records.
_EMPTY = "empty"
_BOOTING = "booting"
_READY = READY
_CLAIMED = CLAIMED
_DELETING = "deleting"


def _default_host() -> str:
    """The current actor's name, else its ID, else this host and process."""
    try:
        import ray

        if ray.is_initialized():
            context = ray.get_runtime_context()
            name = context.get_actor_name()
            if name:
                return name
            actor_id = context.get_actor_id()
            if actor_id:
                return actor_id
    except Exception:
        pass
    return f"{socket.gethostname()}:{os.getpid()}"


def _node_id() -> Optional[str]:
    try:
        import ray

        if ray.is_initialized():
            return ray.get_runtime_context().get_node_id()
    except Exception:
        pass
    return None


class _Slot:
    """A place for one sandbox in a warm pool.

    A slot is empty, booting, ready (booted and not handed out), claimed
    (handed to a create) or deleting (its sandbox died while ready). It
    holds one sandbox at a time and boots the next only once the last one
    is deleted.
    """

    __slots__ = ("key", "index", "state", "sandbox_id", "since")

    def __init__(self, key: tuple, index: int):
        self.key = key
        self.index = index
        self.state = _EMPTY
        self.sandbox_id: Optional[str] = None
        # When the slot entered its state, in seconds since the epoch.
        self.since = 0.0

    def enter(self, state: str) -> None:
        self.state = state
        self.since = time.time()


class _WarmPool:
    """A fixed number of sandbox slots per profile, booted ahead of creates.

    A create that matches a profile takes the sandbox of a ready slot, and
    the slot stays that sandbox's until it's deleted; only then does the
    pool boot a new one in it. So a profile never holds more than its size
    of sandboxes, ready or in use, and creates that find none ready boot
    outside the pool.
    """

    def __init__(
        self,
        runtime: "SandboxRuntime",
        profiles: List[Dict[str, Any]],
        store: Optional[WarmPoolStore] = None,
        host: Optional[str] = None,
    ):
        self._runtime = runtime
        self._lock = threading.Lock()
        # Notified whenever a slot fills or empties, and on close, for wait_full.
        self._changed = threading.Condition(self._lock)
        # Notified when what a store records changes, for the publisher.
        self._publish_wanted = threading.Condition(self._lock)
        self._dirty = False
        # Boot key -> the SandboxConfig its slots boot with.
        self._profiles: Dict[tuple, SandboxConfig] = {}
        # Boot key -> the profile's name, which a store records.
        self._names: Dict[tuple, str] = {}
        self._slots: Dict[tuple, List[_Slot]] = {}
        # Boot key -> its ready slots, in the order they became ready.
        self._ready: Dict[tuple, collections.deque] = {}
        # Sandbox ID -> the slot holding it, ready or claimed.
        self._held: Dict[str, _Slot] = {}
        self._failed_at: Dict[tuple, float] = {}
        # Boot key -> the timer that refills a profile once the pause after
        # its failed boot is over.
        self._retries: Dict[tuple, threading.Timer] = {}
        self._closed = False
        for profile in profiles:
            kwargs = dict(profile)
            size = int(kwargs.pop("size", 0))
            name = kwargs.pop("name", None)
            kwargs.pop("ttl_seconds", None)
            cfg = SandboxConfig(**kwargs)
            key = _boot_key(cfg)
            if key is None or size <= 0:
                raise ValueError(
                    f"Invalid warm_pool profile {profile!r}: it needs a positive "
                    "'size' and no _oci_spec_transform_fn."
                )
            if key in self._profiles:
                raise ValueError(
                    f"Invalid warm_pool profile {profile!r}: an earlier profile "
                    "boots the same sandboxes (only env, ttl_seconds and "
                    "timeout_seconds differ)."
                )
            name = name or cfg.image
            if name in self._names.values():
                raise ValueError(
                    f"Invalid warm_pool profile {profile!r}: an earlier profile "
                    f"is named {name!r}. Give profiles that boot the same image "
                    "a 'name' each."
                )
            self._profiles[key] = cfg
            self._names[key] = name
            self._slots[key] = [_Slot(key, index) for index in range(size)]
            self._ready[key] = collections.deque()
        self._store = store
        self._host = host or _default_host()
        self._node_id = _node_id()
        self._publisher = None
        if store is not None:
            with _PUBLISHING_HOSTS_LOCK:
                if self._host in _PUBLISHING_HOSTS:
                    raise ValueError(
                        f"Another warm pool in this process publishes as host "
                        f"{self._host!r}, or a closed one is still sending its "
                        "last publish; give each pool a warm_pool_host of its "
                        "own."
                    )
                _PUBLISHING_HOSTS.add(self._host)
            self._publisher = threading.Thread(
                target=self._publish_loop, name="sandbox-warm-pool-publish", daemon=True
            )
            self._publisher.start()
        # Boots queue here and run a few at a time.
        self._boots = concurrent.futures.ThreadPoolExecutor(
            max_workers=_WARM_BOOT_CONCURRENCY,
            thread_name_prefix="sandbox-warm-pool",
        )
        self.refill_all()

    def take(self, cfg: SandboxConfig, prefer: Optional[str] = None) -> Optional[str]:
        """Claim a booted sandbox for ``cfg``, or return None.

        The sandbox's slot stays claimed until the sandbox is deleted, so
        taking one boots nothing.

        Args:
            cfg: The create's configuration.
            prefer: A sandbox to claim before any other, if it's still ready
                for ``cfg``: the one a caller chose from the store's records.

        Returns:
            The claimed sandbox's ID, or None if none is ready for ``cfg``.
        """
        key = _boot_key(cfg)
        if key not in self._profiles:
            return None
        taken = None
        dead = []
        with self._lock:
            ready = self._ready[key]
            preferred = self._held.get(prefer) if prefer is not None else None
            if (
                preferred is not None
                and preferred.key == key
                and preferred.state == _READY
            ):
                ready.remove(preferred)
                ready.appendleft(preferred)
            while ready and taken is None:
                slot = ready.popleft()
                if slot.state != _READY:  # deleted while it waited
                    continue
                if self._runtime.get_status(slot.sandbox_id) == SandboxStatus.RUNNING:
                    slot.enter(_CLAIMED)
                    taken = slot.sandbox_id
                else:
                    slot.enter(_DELETING)
                    dead.append(slot.sandbox_id)
                self._mark_dirty()
        for instance_id in dead:  # died while ready: free what it holds
            threading.Thread(
                target=self._discard, args=(instance_id,), daemon=True
            ).start()
        return taken

    def _discard(self, instance_id: str) -> None:
        """Delete a sandbox that died while ready, then boot its slot again."""
        self._delete_quietly(instance_id, "dead warm-pool")
        self.release(instance_id)

    def _delete_quietly(self, instance_id: str, what: str) -> None:
        try:
            self._runtime._backend.delete_sandbox(instance_id)
        except Exception as exc:
            logger.warning("Failed to delete %s sandbox %s: %s", what, instance_id, exc)

    def release(self, instance_id: str) -> None:
        """Empty the slot of a pooled sandbox that's been deleted, and refill it.

        Sandboxes the pool didn't boot are ignored.
        """
        with self._lock:
            slot = self._held.pop(instance_id, None)
            if slot is None:
                return
            slot.enter(_EMPTY)
            slot.sandbox_id = None
            self._mark_dirty()
            self._changed.notify_all()
        self.refill(slot.key)

    def _paused_for(self, key: tuple) -> float:
        """Seconds until a profile whose boot failed boots again, else <= 0."""
        failed_at = self._failed_at.get(key)
        if failed_at is None:
            return 0.0
        return failed_at + _WARM_RETRY_SECONDS - time.monotonic()

    def _refill_later(self, key: tuple, delay: float) -> None:
        """Refill a profile after ``delay`` seconds, unless a timer already will.

        The caller holds the lock.
        """
        if self._closed or key in self._retries:
            return
        timer = threading.Timer(delay, self._retry, args=(key,))
        timer.daemon = True
        self._retries[key] = timer
        timer.start()

    def _retry(self, key: tuple) -> None:
        with self._lock:
            self._retries.pop(key, None)
        self.refill(key)

    def refill(self, key: tuple) -> None:
        """Boot a sandbox in each of a profile's empty slots."""
        with self._lock:
            if self._closed:
                return
            paused_for = self._paused_for(key)
            if paused_for > 0:
                self._refill_later(key, paused_for)
                return
            for slot in self._slots[key]:
                if slot.state == _EMPTY:
                    slot.enter(_BOOTING)
                    # Submitted under the lock, so none lands after close's
                    # shutdown.
                    self._boots.submit(self._boot, slot)

    def refill_all(self) -> None:
        for key in self._profiles:
            self.refill(key)

    def _boot(self, slot: _Slot) -> None:
        instance_id = None
        with self._lock:
            # Another boot of this profile failed while this one queued: the
            # timer that failure started refills the profile.
            boot = not self._closed and self._paused_for(slot.key) <= 0
        if boot:
            try:
                instance_id = self._runtime._backend.create_sandbox(
                    self._profiles[slot.key]
                )
            except Exception as exc:
                logger.warning("A warm-pool sandbox failed to boot: %s", exc)
                with self._lock:
                    self._failed_at[slot.key] = time.monotonic()
                    self._refill_later(slot.key, _WARM_RETRY_SECONDS)
        with self._lock:
            if instance_id is not None and not self._closed:
                slot.enter(_READY)
                slot.sandbox_id = instance_id
                self._held[instance_id] = slot
                self._ready[slot.key].append(slot)
                self._mark_dirty()
                instance_id = None
            else:
                slot.enter(_EMPTY)
            self._changed.notify_all()
        if instance_id is not None:  # closed while it booted
            self._delete_quietly(instance_id, "warm-pool")

    def _full(self) -> bool:
        return all(
            slot.state in (_READY, _CLAIMED)
            for slots in self._slots.values()
            for slot in slots
        )

    def wait_full(self, timeout_seconds: float) -> bool:
        """Wait until every slot holds a booted sandbox; False on timeout or close."""
        deadline = time.monotonic() + timeout_seconds
        with self._changed:
            while not self._full():
                remaining = deadline - time.monotonic()
                if self._closed or remaining <= 0:
                    return False
                self._changed.wait(remaining)
        return True

    def close(self) -> List[str]:
        """Stop refilling; the ready sandboxes, for the caller to delete.

        Claimed sandboxes stay their creates'. Deleting them later boots
        nothing.
        """
        with self._lock:
            if self._closed:
                return []
            self._closed = True
            ready = []
            for queue in self._ready.values():
                for slot in queue:
                    if slot.state == _READY:
                        ready.append(slot.sandbox_id)
                        self._held.pop(slot.sandbox_id, None)
                        slot.enter(_EMPTY)
                        slot.sandbox_id = None
                queue.clear()
            retries = list(self._retries.values())
            self._retries.clear()
            self._changed.notify_all()
            self._publish_wanted.notify_all()
        for timer in retries:
            timer.cancel()
        # Queued boots never start. One already booting deletes its sandbox
        # when it finishes.
        self._boots.shutdown(wait=False, cancel_futures=True)
        if self._publisher is not None:
            # Its last publish removes this host from the store.
            self._publisher.join(timeout=_PUBLISH_HEARTBEAT_SECONDS)
        return ready

    def _mark_dirty(self) -> None:
        """Have the publisher publish soon. The caller holds the lock."""
        if self._store is not None:
            self._dirty = True
            self._publish_wanted.notify()

    def entries(self, expires_at: float) -> List[WarmPoolEntry]:
        """The pool's ready and claimed sandboxes. The caller holds the lock."""
        return [
            WarmPoolEntry(
                host=self._host,
                profile=self._names[key],
                slot=slot.index,
                sandbox_id=slot.sandbox_id,
                state=slot.state,
                node_id=self._node_id,
                since=slot.since,
                expires_at=expires_at,
            )
            for key, slots in self._slots.items()
            for slot in slots
            if slot.state in (_READY, _CLAIMED)
        ]

    def _publish_loop(self) -> None:
        """Publish the pool when it changes and every heartbeat; last, empty.

        Only once the last publish is done does another pool in this process
        get to publish as this host, so that no publish of this one, late
        with a slow store, replaces the other's records.
        """
        try:
            self._publish_until_closed()
        finally:
            with _PUBLISHING_HOSTS_LOCK:
                _PUBLISHING_HOSTS.discard(self._host)

    def _publish_until_closed(self) -> None:
        last_warned = None
        while True:
            with self._lock:
                self._publish_wanted.wait_for(
                    lambda: self._dirty or self._closed,
                    timeout=_PUBLISH_HEARTBEAT_SECONDS,
                )
                closed = self._closed
                self._dirty = False
                expires_at = time.time() + 3 * _PUBLISH_HEARTBEAT_SECONDS
                entries = [] if closed else self.entries(expires_at)
            try:
                self._store.publish(self._host, entries)
                if last_warned is not None:
                    logger.info(
                        "Publishing the warm pool of %s works again.", self._host
                    )
                    last_warned = None
            except Exception as exc:
                # Warn about the first failure, then at most once a minute.
                if last_warned is None or time.monotonic() - last_warned > 60:
                    logger.warning(
                        "Failed to publish the warm pool of %s: %s", self._host, exc
                    )
                    last_warned = time.monotonic()
            if closed:
                return
            time.sleep(_PUBLISH_COALESCE_SECONDS)


@PublicAPI(stability="alpha")
class SandboxRuntime:
    """Low-level interface for managing local sandbox runtime environments.

    Args:
        warm_pool: Sandboxes to keep booted ahead of creates: a list of
            profiles, each the ``create()`` keyword arguments of one boot
            configuration plus its pool ``"size"``. A create whose arguments
            match a profile's (``env``, ``ttl_seconds`` and
            ``timeout_seconds`` aside) takes a booted sandbox at once, which
            runs every command with the create's ``env`` (on top of the
            profile's). A profile holds at most ``size`` sandboxes, ready or
            in use: the runtime boots a new one when a sandbox it handed out
            is deleted, not when it's handed out, and creates that find none
            ready boot cold, outside the pool. A profile may also have a
            ``"name"``, which ``warm_pool_store`` records; it defaults to
            the profile's image. None or empty keeps no pool.
        warm_pool_store: Where to publish the warm pool's sandboxes, ready
            and in use, so that callers on other actors can find a runtime
            with ready ones and send their creates there. See
            :class:`~ray.experimental.sandbox.WarmPoolStore`; None
            publishes nothing.
        warm_pool_host: The name that callers use to reach this runtime,
            recorded with each of its sandboxes in ``warm_pool_store``.
            Defaults to the current actor's name, or its ID for an unnamed
            actor, or the host name and process ID outside an actor.
    """

    def __init__(
        self,
        warm_pool: Optional[List[Dict[str, Any]]] = None,
        warm_pool_store: Optional[WarmPoolStore] = None,
        warm_pool_host: Optional[str] = None,
    ):
        if warm_pool_store is not None and not warm_pool:
            raise ValueError("warm_pool_store needs a warm_pool to publish.")
        self._image_manager = ImageManager()
        self._backend = GVisorSandboxBackend(image_manager=self._image_manager)
        self._ttl_timers: Dict[str, threading.Timer] = {}
        self._warm = (
            _WarmPool(self, warm_pool, warm_pool_store, warm_pool_host)
            if warm_pool
            else None
        )

    @property
    def image_manager(self) -> ImageManager:
        """The ImageManager instance used by this runtime."""
        return self._image_manager

    @property
    def backend(self) -> GVisorSandboxBackend:
        """The backend instance used by this runtime."""
        return self._backend

    def pull_image(self, image: str, timeout_seconds: float = 120.0) -> str:
        """Download and extract container image into local cache.

        Args:
            image: Container image name or tar path.
            timeout_seconds: Request timeout.

        Returns:
            Extracted image directory path.
        """
        return self._image_manager.pull_image(image, timeout_seconds=timeout_seconds)

    def check_resources(
        self,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
    ) -> None:
        """Reject unsupported resource limits before downloading an image.

        Args:
            cpu: CPU limit to enforce. Zero means no limit.
            memory: Memory limit to enforce, such as "512Mi". Zero means no limit.
        """
        try:
            config = SandboxConfig(image="resource-preflight", cpu=cpu, memory=memory)
            memory_bytes = parse_memory_bytes(config.memory) or 0
            if config.cpu > 0 or memory_bytes > 0:
                SandboxCgroupManager().check(config.cpu, memory_bytes)
        except Exception as err:
            raise SandboxCreationError(
                f"Cannot enforce sandbox resources: {err}"
            ) from err

    def get_termination_reason(self, instance_id: str) -> Optional[str]:
        """Report an unexpected runtime exit, including a cgroup OOM kill.

        Args:
            instance_id: Sandbox instance ID.

        Returns:
            Why the sandbox stopped unexpectedly, or None if it's running or
            this runtime no longer tracks it.
        """
        return self._backend.get_termination_reason(instance_id)

    def create(
        self,
        image: str,
        cpu: float = 0.0,
        memory: Union[str, int, float] = 0,
        env: Optional[Dict[str, str]] = None,
        workdir: Optional[str] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        rootless: bool = True,
        network: str = "none",
        dns: Optional[List[str]] = None,
        cidr_allowlist: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        readonly: bool = True,
        gpu_ids: Optional[List[Union[int, str]]] = None,
        _oci_spec_transform_fn: Optional[Callable[[Dict], Optional[Dict]]] = None,
        _ignore_cgroups: bool = False,
        disk: Union[str, int, float] = 0,
        warm_sandbox: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Provision the sandbox instance and return unique instance ID.

        Args:
            image: Container image for the sandbox environment.
            cpu: Number of CPU cores allocated to the sandbox.
            memory: Amount of memory allocated to the sandbox (e.g. "1Gi", "512Mi").
            env: Environment variables to inject into the sandbox.
            workdir: Working directory for commands; None uses the image's
                WORKDIR. On a readonly rootfs an explicit workdir is also the
                sandbox's only writable path; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            ttl_seconds: Optional time-to-live in seconds, wall-clock from
                creation (not idle time), enforced by this runtime with a
                daemon timer. None (default) or <= 0 disables it.
            timeout_seconds: Timeout in seconds for sandbox creation.
            rootless: If True, run gVisor in rootless mode.
            network: Network mode ("none", "public", "host", "sandbox");
                see :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                "public" is the recommended internet-access mode.
            dns: Optional nameserver IPs for the generated /etc/resolv.conf
                (public resolvers by default for "public").
            cidr_allowlist: Optional egress allowlist for network="public":
                only the listed destinations are reachable (plus DNS to
                the ``dns`` resolvers). None means the public internet,
                with internal addresses left out as a best effort, and a
                denylist keeps instance metadata and the Kubernetes API
                service closed whatever the list says; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
                Change it later with :meth:`set_egress_allowlist`.
            capabilities: Linux capabilities, written exactly (None keeps
                the runtime default; ``[]`` means none). Use
                ``DOCKER_DEFAULT_CAPABILITIES`` for Docker parity.
            readonly: If True (default), mount container image rootfs in read-only mode
                such that only ``workdir`` is writable. If False, the entire root filesystem
                is writable. Writes are isolated within a per-sandbox copy-on-write overlay
                filesystem, ensuring multiple sandboxes running the same container image do
                not interfere with each other or modify the base image.
            gpu_ids: GPU device ids/UUIDs to expose via CDI; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            _oci_spec_transform_fn: PRIVATE — development/testing only. Called with the fully-built OCI
                spec dict before it is written; may mutate in place or return a new dict. Must be
                cloudpickle-serializable. No stability guarantees. Accepts a transform function.
            _ignore_cgroups: PRIVATE — testing only. If True, passes --ignore-cgroups to runsc.
            disk: Size cap of the writable rootfs (e.g. "10Gi"); writes past
                it fail with ENOSPC. Zero means no cap. Requires
                ``readonly=False`` or a ``workdir``; see
                :class:`~ray.experimental.sandbox.config.SandboxConfig`.
            warm_sandbox: The ``sandbox_id`` of a
                :class:`~ray.experimental.sandbox.WarmPoolEntry` that this
                runtime published: the create takes that sandbox if it's
                still ready for these arguments. A caller that sends one
                create per ready record then never sends a runtime more
                creates than it has sandboxes ready. Otherwise the create
                takes any ready sandbox, or boots one, as without it.
            **kwargs: Additional parameters.

        Returns:
            A unique string identifier for the created sandbox.
        """
        cfg = SandboxConfig(
            image=image,
            cpu=cpu,
            memory=memory,
            env=env or {},
            workdir=workdir,
            ttl_seconds=ttl_seconds,
            timeout_seconds=timeout_seconds,
            rootless=rootless,
            network=network,
            dns=dns,
            cidr_allowlist=cidr_allowlist,
            capabilities=capabilities,
            readonly=readonly,
            gpu_ids=gpu_ids,
            _oci_spec_transform_fn=_oci_spec_transform_fn,
            _ignore_cgroups=_ignore_cgroups,
            disk=disk,
            **kwargs,
        )
        instance_id = (
            self._warm.take(cfg, prefer=warm_sandbox)
            if self._warm is not None
            else None
        )
        if instance_id is not None:
            # It booted with the profile's env and TTL; from here on it has
            # this create's.
            try:
                self._backend.assign_config(instance_id, cfg)
            except Exception as exc:
                # Say it died since take checked it. Deleting it frees its
                # slot, and the create boots cold instead.
                logger.warning(
                    "Couldn't hand warm-pool sandbox %s to a create, which boots "
                    "cold: %s",
                    instance_id,
                    exc,
                )
                try:
                    self.delete(instance_id)
                except Exception as delete_exc:
                    logger.warning(
                        "Failed to delete warm-pool sandbox %s: %s",
                        instance_id,
                        delete_exc,
                    )
                instance_id = None
        if instance_id is None:
            instance_id = self._backend.create_sandbox(cfg)
        if cfg.ttl_seconds is not None and cfg.ttl_seconds > 0:
            timer = threading.Timer(cfg.ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def _expire(self, instance_id: str) -> None:
        """TTL callback: best-effort delete (the sandbox may already be gone)."""
        try:
            self.delete(instance_id)
        except Exception:
            pass

    def exec(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return self._backend.exec_command(
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    async def exec_async(
        self,
        instance_id: str,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: Optional[str] = None,
        user: Optional[str] = None,
    ) -> ExecResult:
        """Execute a command inside the specified sandbox asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            command: Command to execute, either as a string or a list of arguments.
            timeout: Maximum execution time in seconds.
            cwd: Working directory inside the sandbox for command execution.
            env: Environment variables to set for the command.
            shell: Optional shell for string commands, overriding the
                sandbox's configured shell (default /bin/bash).
            user: Optional user to run as: a numeric uid, "uid:gid", or a
                user (optionally ":group") name, resolved against the
                /etc/passwd and /etc/group inside the running sandbox
                (default: the image user).

        Returns:
            ExecResult containing exit code, stdout, and stderr.
        """
        return await asyncio.to_thread(
            self.exec,
            instance_id,
            command,
            timeout=timeout,
            cwd=cwd,
            env=env,
            shell=shell,
            user=user,
        )

    def upload_file(self, instance_id: str, local_path: str, remote_path: str) -> None:
        """Copy local file into the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            local_path: Path to the source file on the local filesystem.
            remote_path: Destination path inside the sandbox.
        """
        with open(local_path, "rb") as f:
            content = f.read()
        self._backend.write_file(instance_id, remote_path, content)

    def download_file(
        self, instance_id: str, remote_path: str, local_path: str
    ) -> None:
        """Copy file from the sandbox to local.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            remote_path: Path to the source file inside the sandbox.
            local_path: Destination path on the local filesystem.
        """
        content = self._backend.read_file(instance_id, remote_path)
        local_dir = os.path.dirname(os.path.abspath(local_path))
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(content)

    def write_file(
        self,
        instance_id: str,
        path: str,
        content: Union[str, bytes],
        append: bool = False,
    ) -> None:
        """Write string or binary content directly to a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Destination file path inside the sandbox.
            content: String or binary content to write into the file.
            append: Append to the file instead of truncating it.
        """
        self._backend.write_file(instance_id, path, content, append=append)

    def read_file(self, instance_id: str, path: str) -> bytes:
        """Read binary content from a file inside the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            path: Path to the file inside the sandbox to read.

        Returns:
            File content as bytes.
        """
        return self._backend.read_file(instance_id, path)

    def get_status(self, instance_id: str) -> SandboxStatus:
        """Query operational status of the sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.

        Returns:
            SandboxStatus of the sandbox instance.
        """
        return self._backend.get_status(instance_id)

    def set_egress_allowlist(self, instance_id: str, cidrs: List[str]) -> None:
        """Replace the egress allowlist of a network="public" sandbox.

        The new list takes effect for the next packet of every flow; flows
        it no longer permits stall rather than being reset. No list opens
        the denylist; ``ALLOW_ALL_CIDRS`` opens everything else, private
        networks and any Ray node on them included.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            cidrs: The new allowlist, in the forms ``cidr_allowlist`` takes.
        """
        self._backend.set_egress_allowlist(instance_id, cidrs)

    def checkpoint(
        self,
        instance_id: str,
        checkpoint_path: Optional[str] = None,
        leave_running: bool = True,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Create a checkpoint of the running sandbox.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            checkpoint_path: Path where the checkpoint directory will be saved.
                If None, a default directory inside the sandbox storage root is used.
            leave_running: If True, keep the sandbox running after checkpointing.
            timeout_seconds: Timeout for the checkpoint operation.
            **kwargs: Backend-specific arguments.

        Returns:
            Absolute path to the checkpoint directory.
        """
        res = self._backend.checkpoint_sandbox(
            sandbox_id=instance_id,
            checkpoint_path=checkpoint_path,
            leave_running=leave_running,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if not leave_running:
            timer = self._ttl_timers.pop(instance_id, None)
            if timer is not None:
                timer.cancel()
            # The backend deleted the stopped sandbox.
            if self._warm is not None:
                self._warm.release(instance_id)

        if isinstance(res, dict) and "checkpoint_path" in res:
            return res["checkpoint_path"]
        return checkpoint_path or ""

    def restore(
        self,
        checkpoint_path: str,
        cpu: Optional[float] = None,
        memory: Optional[Union[str, int, float]] = None,
        ttl_seconds: Optional[int] = None,
        timeout_seconds: float = 30.0,
        **kwargs,
    ) -> str:
        """Restore a sandbox instance from a previously saved checkpoint.

        The restored sandbox preserves the full execution state, network mode,
        mounts, Linux capabilities, and process environment of the checkpointed
        sandbox. Cgroup resource allocations (cpu, memory) and lifecycle limits
        (ttl_seconds, timeout_seconds) can optionally be overridden.

        Args:
            checkpoint_path: Path to the checkpoint directory.
            cpu: CPU allocation override.
            memory: Memory allocation override.
            ttl_seconds: Optional time-to-live for the restored sandbox.
            timeout_seconds: Timeout for the restore operation.
            **kwargs: Backend-specific arguments.

        Returns:
            A unique string identifier for the restored sandbox.
        """
        instance_id = self._backend.restore_sandbox(
            checkpoint_path=checkpoint_path,
            cpu=cpu,
            memory=memory,
            timeout_seconds=timeout_seconds,
            **kwargs,
        )
        if ttl_seconds is not None and ttl_seconds > 0:
            timer = threading.Timer(ttl_seconds, self._expire, args=(instance_id,))
            timer.daemon = True
            self._ttl_timers[instance_id] = timer
            timer.start()
        return instance_id

    def pause(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Pause a running sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for pause operation.
        """
        self._backend.pause_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def resume(self, instance_id: str, timeout_seconds: float = 10.0) -> None:
        """Resume a paused sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
            timeout_seconds: Timeout for resume operation.
        """
        self._backend.resume_sandbox(instance_id, timeout_seconds=timeout_seconds)

    def delete(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        timer = self._ttl_timers.pop(instance_id, None)
        if timer is not None:
            timer.cancel()
        try:
            self._backend.delete_sandbox(instance_id)
        finally:
            # The backend forgets a sandbox before it frees it, so a failed
            # delete still empties the sandbox's warm-pool slot.
            if self._warm is not None:
                self._warm.release(instance_id)

    def wait_for_warm_pool(self, timeout_seconds: float = 300.0) -> bool:
        """Wait until every warm-pool slot holds a booted sandbox, ready or in use.

        Args:
            timeout_seconds: How long to wait.

        Returns:
            True once the pools are full; False on timeout, or with no pool.
        """
        if self._warm is None:
            return False
        return self._warm.wait_full(timeout_seconds)

    def close(self) -> None:
        """Delete the warm pool's ready sandboxes, and stop refilling it.

        Sandboxes that ``create`` returned stay; delete them with ``delete``,
        which then boots nothing in their place.
        """
        if self._warm is not None:
            for instance_id in self._warm.close():
                try:
                    self._backend.delete_sandbox(instance_id)
                except Exception as exc:
                    logger.warning(
                        "Failed to delete warm-pool sandbox %s: %s", instance_id, exc
                    )

    def terminate(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        self.delete(instance_id)

    async def delete_async(self, instance_id: str) -> None:
        """Clean up and terminate the sandbox instance asynchronously.

        Args:
            instance_id: Unique identifier of the sandbox instance.
        """
        await asyncio.to_thread(self.delete, instance_id)
