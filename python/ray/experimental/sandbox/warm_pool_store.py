"""Stores where warm pools record their sandboxes, for callers on other actors.

A warm pool belongs to the ``SandboxRuntime`` that boots it, and only that
runtime hands its sandboxes out. Given a store, the runtime publishes what its
pool holds, so a caller anywhere can find a runtime with a sandbox ready and
send its create there.
"""

import abc
import dataclasses
import logging
import random
import time
from typing import Dict, List, Optional

import ray
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# How long a RayWarmPoolStore waits for its actor to answer.
_CALL_TIMEOUT_SECONDS = 10.0

# States a published sandbox can be in.
READY = "ready"
CLAIMED = "claimed"


@PublicAPI(stability="alpha")
@dataclasses.dataclass(frozen=True)
class WarmPoolEntry:
    """One sandbox that a warm pool holds, as its runtime published it.

    Args:
        host: The runtime holding the sandbox, by the name its callers use
            to reach it, such as its actor's name.
        profile: The name of the warm-pool profile the sandbox was booted for.
        slot: The sandbox's slot in the profile's pool, from 0 to its size
            minus 1. A slot keeps its number when a new sandbox boots in it.
        sandbox_id: The sandbox's instance ID on its host.
        state: ``"ready"`` while no create has taken it, ``"claimed"``
            after one has, until it's deleted.
        node_id: The Ray node the sandbox runs on, or None outside Ray.
        since: When the sandbox entered ``state``, in seconds since the epoch.
        expires_at: When the entry goes stale, in seconds since the epoch,
            unless the host publishes again first, as it does every few
            seconds while it runs.
    """

    host: str
    profile: str
    slot: int
    sandbox_id: str
    state: str
    node_id: Optional[str]
    since: float
    expires_at: float


@PublicAPI(stability="alpha")
class WarmPoolStore(abc.ABC):
    """Where warm pools record their sandboxes, so callers can find ready ones.

    A ``SandboxRuntime`` given a store publishes every sandbox its warm pool
    holds, ready or claimed, whenever that changes and every few seconds.
    Callers ask the store for ready sandboxes, then send their create to the
    host that holds one.

    Only a host hands out its sandboxes, and only those still ready on it, so
    a store doesn't need to be exact: a create sent on a stale entry boots a
    sandbox cold, and a sandbox is never handed out twice. Implement both
    methods to keep the records elsewhere, such as in a database; they may be
    called from many processes at once.
    """

    @abc.abstractmethod
    def publish(self, host: str, entries: List[WarmPoolEntry]) -> None:
        """Replace everything recorded for ``host`` with ``entries``.

        Args:
            host: The host publishing.
            entries: Every sandbox the host's pool holds, ready or claimed.
                Empty when the pool closes, which removes the host.
        """

    @abc.abstractmethod
    def ready(self, profile: str) -> List[WarmPoolEntry]:
        """Return the ready sandboxes of a profile, on every host.

        Args:
            profile: The profile name.

        Returns:
            The profile's ready entries that haven't expired, in no
            particular order. Callers that all take the first entry should
            get different ones, so that they don't all go to one host.
        """


@ray.remote(num_cpus=0)
class _WarmPoolDirectory:
    """The records behind a RayWarmPoolStore, kept by host."""

    def __init__(self):
        # Host -> what it last published.
        self._hosts: Dict[str, List[WarmPoolEntry]] = {}
        # Host -> when what it last published expires.
        self._expires: Dict[str, float] = {}
        # Profile -> host -> that host's ready entries.
        self._ready: Dict[str, Dict[str, List[WarmPoolEntry]]] = {}

    def publish(self, host: str, entries: List[WarmPoolEntry]) -> None:
        self._drop_expired()
        self._drop(host)
        if not entries:
            return
        self._hosts[host] = entries
        self._expires[host] = max(entry.expires_at for entry in entries)
        for entry in entries:
            if entry.state == READY:
                self._ready.setdefault(entry.profile, {}).setdefault(host, []).append(
                    entry
                )

    def ready(self, profile: str) -> List[WarmPoolEntry]:
        self._drop_expired()
        entries = [
            entry
            for entries in self._ready.get(profile, {}).values()
            for entry in entries
        ]
        # Callers between two publishes get the same entries. Shuffled, they
        # don't all send their create to the first one's host.
        random.shuffle(entries)
        return entries

    def entries(self) -> List[WarmPoolEntry]:
        self._drop_expired()
        return [entry for entries in self._hosts.values() for entry in entries]

    def _drop_expired(self) -> None:
        """Forget the hosts that stopped publishing, claimed sandboxes and all."""
        now = time.time()
        for host in [h for h, expires_at in self._expires.items() if expires_at <= now]:
            self._drop(host)

    def _drop(self, host: str) -> None:
        self._expires.pop(host, None)
        for entry in self._hosts.pop(host, []):
            by_host = self._ready.get(entry.profile)
            if by_host is not None:
                by_host.pop(host, None)
                if not by_host:
                    del self._ready[entry.profile]


@PublicAPI(stability="alpha")
class RayWarmPoolStore(WarmPoolStore):
    """A ``WarmPoolStore`` kept in a detached Ray actor, for one Ray cluster.

    Stores with the same name and namespace share one actor, which the first
    of them to be used creates. It holds the records in memory: if it
    restarts, every host's next publish, within a few seconds, fills it
    again. A store pickles to its name and namespace, so pass it to actors
    and tasks freely.

    Args:
        name: The name of the store's actor.
        namespace: The namespace of the store's actor.
    """

    def __init__(
        self, name: str = "ray-sandbox-warm-pool", namespace: str = "ray_sandbox"
    ):
        self._name = name
        self._namespace = namespace
        self._actor = None

    def __reduce__(self):
        return (RayWarmPoolStore, (self._name, self._namespace))

    # Stores with one name and namespace are one store.
    def __eq__(self, other):
        return isinstance(other, RayWarmPoolStore) and (
            (self._name, self._namespace) == (other._name, other._namespace)
        )

    def __hash__(self):
        return hash((self._name, self._namespace))

    def _directory(self):
        if self._actor is None:
            self._actor = _WarmPoolDirectory.options(
                name=self._name,
                namespace=self._namespace,
                lifetime="detached",
                get_if_exists=True,
                max_restarts=-1,
            ).remote()
        return self._actor

    def _call(self, method: str, *args):
        def call():
            return ray.get(
                getattr(self._directory(), method).remote(*args),
                timeout=_CALL_TIMEOUT_SECONDS,
            )

        try:
            return call()
        except ray.exceptions.ActorDiedError:
            # The actor is gone for good, say killed without a restart: start
            # a new one, which the hosts' next publishes fill.
            self._actor = None
            return call()

    def _read(self, method: str, *args) -> List[WarmPoolEntry]:
        """Read from the actor, or nothing while it can't answer.

        The records are a hint: a caller that finds none boots its sandbox
        cold, which beats failing its create because the actor is restarting
        or its node hangs.
        """
        try:
            return self._call(method, *args)
        except (ray.exceptions.RayActorError, ray.exceptions.GetTimeoutError) as err:
            logger.warning(
                "The warm pool store %r can't answer, so it lists nothing: %s",
                self._name,
                err,
            )
            return []

    def publish(self, host: str, entries: List[WarmPoolEntry]) -> None:
        """Replace everything recorded for ``host`` with ``entries``.

        Args:
            host: The host publishing.
            entries: Every sandbox the host's pool holds, ready or claimed.
                Empty when the pool closes, which removes the host.
        """
        self._call("publish", host, entries)

    def ready(self, profile: str) -> List[WarmPoolEntry]:
        """Return the ready sandboxes of a profile, on every host.

        Args:
            profile: The profile name.

        Returns:
            The profile's ready entries that haven't expired, in no
            particular order, or none while the store's actor can't answer.
        """
        return self._read("ready", profile)

    def entries(self) -> List[WarmPoolEntry]:
        """Return every sandbox recorded, ready or claimed, on every host.

        Returns:
            The latest entries of each host that hasn't expired, or none
            while the store's actor can't answer.
        """
        return self._read("entries")
