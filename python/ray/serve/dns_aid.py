"""DNS-AID integration for Ray Serve deployment discovery.

When dns_aid_enabled=True in ServeConfig (or DNS_AID_ENABLED=1 is set),
ServeController publishes a DNS-AID SVCB ServiceMode record whenever a
deployment reaches HEALTHY state, and deregisters it when the deployment
is deleted or unhealthy.

External AI agents can then discover Serve deployments via standards-compliant
SVCB DNS records (RFC 9460) without any access to the Ray dashboard or GCS.

Optional dependency: pip install dns-aid
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional, Set, Tuple

from ray._common.utils import run_background_task

logger = logging.getLogger(__name__)


@dataclass
class DnsAidConfig:
    """Configuration for DNS-AID deployment discovery integration.

    When enabled, ServeController publishes a DNS-AID SVCB ServiceMode record
    (RFC 9460) for each deployment in HEALTHY state, allowing external agents
    to discover Serve deployments via DNS without dashboard or GCS access.

    All fields fall back to environment variables so zero-config works
    with just ``DNS_AID_ENABLED=1 DNS_AID_ZONE=agents.example.com``:

    - ``DNS_AID_ENABLED`` — "1"/"true"/"yes" to enable
    - ``DNS_AID_ZONE``   — DNS zone (required), e.g. ``agents.example.com``
    - ``DNS_AID_SERVER`` — DNS server hostname or IP
    - ``DNS_AID_PORT``   — DNS server port (default 53)
    - ``DNS_AID_BACKEND`` — DNS backend (default "ddns")

    Optional dependency: ``pip install dns-aid``
    """

    enabled: bool = False
    zone: Optional[str] = None  # DNS_AID_ZONE env var fallback
    server: Optional[str] = None  # DNS_AID_SERVER env var fallback
    dns_port: int = 53  # DNS_AID_PORT env var fallback
    backend: str = "ddns"  # DNS_AID_BACKEND env var fallback
    ttl: int = 45

    def __post_init__(self):
        # Env vars are true *fallbacks*: they only apply when the
        # constructor value matches the dataclass default so that
        # explicit programmatic values always win.
        if not self.zone:
            self.zone = os.environ.get("DNS_AID_ZONE")
        if not self.server:
            self.server = os.environ.get("DNS_AID_SERVER")
        if self.dns_port == 53:  # still at default
            env_port = os.environ.get("DNS_AID_PORT")
            if env_port:
                try:
                    self.dns_port = int(env_port)
                except ValueError:
                    logger.warning(
                        "DNS-AID: DNS_AID_PORT=%r is not a valid"
                        " integer; using default %d",
                        env_port,
                        self.dns_port,
                    )
        if self.backend == "ddns":  # still at default
            env_backend = os.environ.get("DNS_AID_BACKEND")
            if env_backend:
                self.backend = env_backend


class DnsAidManager:
    """Non-blocking DNS-AID registration manager for Serve deployments.

    All network I/O is fire-and-forget via ``run_background_task`` so it
    never blocks the ServeController event loop.  Errors are logged but
    never raised; DNS registration is always best-effort.

    Record names use both app_name and deployment name to avoid collisions
    when two applications share a deployment name:
        _{app_name}--{dep_name}._a2a._agents.{zone}
    """

    def __init__(self, config: DnsAidConfig):
        self._config = config
        # (app_name, dep_name) for currently registered deployments
        self._registered: Set[Tuple[str, str]] = set()
        self._deregister_all_called = False
        # Set when deregister_all fires so that in-flight register
        # tasks can detect they should immediately undo their work.
        self._teardown_in_progress = False

    # ------------------------------------------------------------------
    # Public interface (sync wrappers that schedule async tasks)
    # ------------------------------------------------------------------

    def set_enabled(self, enabled: bool) -> None:
        """Toggle DNS-AID registration on or off at runtime."""
        self._config.enabled = enabled
        if enabled:
            # Reset flags so that a future deregister_all (e.g. on
            # shutdown) can run and new registrations proceed normally.
            self._deregister_all_called = False
            self._teardown_in_progress = False

    def is_enabled(self) -> bool:
        """Return True if DNS-AID is currently active."""
        return self._is_enabled()

    def get_registered(self) -> Set[Tuple[str, str]]:
        """Return a snapshot of confirmed registrations.

        The controller uses this to detect registrations that failed
        (scheduled but not confirmed) and retry them on the next
        control-loop iteration.
        """
        return set(self._registered)

    def register(
        self,
        dep_name: str,
        route_prefix: str,
        replicas: int,
        ingress_host: str,
        ingress_port: int,
        app_name: str,
    ) -> None:
        """Schedule a fire-and-forget DNS-AID registration."""
        if not self._is_enabled():
            return
        run_background_task(
            self._register_async(
                dep_name,
                route_prefix,
                replicas,
                ingress_host,
                ingress_port,
                app_name,
            )
        )

    def deregister(self, dep_name: str, app_name: str) -> None:
        """Schedule a fire-and-forget DNS-AID deregistration."""
        if not self._is_enabled():
            return
        run_background_task(
            self._deregister_async(dep_name, app_name)
        )

    def deregister_all(
        self,
        known_keys: Optional[Set[Tuple[str, str]]] = None,
        force: bool = False,
    ) -> None:
        """Schedule deregistration for every registered deployment.

        Called on controller shutdown or when DNS-AID is disabled at
        runtime, so DNS records don't outlive the Serve instance.
        Idempotent: only fires deregistration tasks on the first call.

        Args:
            known_keys: Optional set of ``(app_name, dep_name)`` tuples
                the caller believes are registered.  Merged with the
                internal ``_registered`` set so that in-flight
                registrations whose confirmation hasn't arrived yet
                are also covered.
            force: If True, skip the ``_is_enabled()`` check.  Used
                when DNS-AID is being disabled at runtime — the records
                must still be cleaned up even though the feature is now
                off.
        """
        if self._deregister_all_called:
            return
        if not force and not self._is_enabled():
            return
        self._deregister_all_called = True
        # Signal in-flight register tasks to undo their work.
        self._teardown_in_progress = True
        all_keys = set(self._registered)
        if known_keys:
            all_keys |= known_keys
        for app_name, dep_name in all_keys:
            run_background_task(
                self._deregister_async(dep_name, app_name)
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_enabled(self) -> bool:
        """Check whether DNS-AID is active.

        The ``DNS_AID_ENABLED`` env var is an operator-level override
        that intentionally takes priority over programmatic config
        (matching the precedence pattern used by ``RAY_SERVE_*`` env
        vars elsewhere in Ray).  When the env var is unset, the
        ``DnsAidConfig.enabled`` field (settable at runtime via
        ``set_enabled``) is used.
        """
        env = os.environ.get("DNS_AID_ENABLED", "").strip().lower()
        if env in ("0", "false", "no"):
            return False
        if env in ("1", "true", "yes"):
            return True
        return self._config.enabled

    def _agent_name(self, dep_name: str, app_name: str) -> str:
        """Return the DNS-AID agent name for this deployment.

        Encodes both app_name and dep_name using '--' as a separator so
        that two applications with identically-named deployments get
        distinct records.
        """
        return f"{app_name}--{dep_name}".lower()

    def _get_backend(self):
        """Create a DNS backend instance with configured server/port."""
        from dns_aid.backends import create_backend

        backend = create_backend(self._config.backend)
        # Pass server and port to the backend unconditionally so
        # that explicit config (including the default port 53) is
        # always forwarded rather than relying on backend defaults.
        if hasattr(backend, "server") and self._config.server:
            backend.server = self._config.server
        if hasattr(backend, "port"):
            backend.port = self._config.dns_port
        return backend

    async def _register_async(
        self,
        dep_name: str,
        route_prefix: str,
        replicas: int,
        ingress_host: str,
        ingress_port: int,
        app_name: str,
    ) -> None:
        try:
            from dns_aid import delete, publish

            zone = self._config.zone
            if not zone:
                logger.warning(
                    "DNS-AID: zone not configured; skipping "
                    "registration for deployment %r. Set "
                    "DNS_AID_ZONE or config.zone.",
                    dep_name,
                )
                return

            agent_name = self._agent_name(dep_name, app_name)
            backend = self._get_backend()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: publish(
                    name=agent_name,
                    domain=zone,
                    protocol="a2a",
                    endpoint=ingress_host,
                    port=ingress_port,
                    capabilities=[
                        route_prefix or f"/{dep_name}",
                    ],
                    description=(
                        f"Ray Serve deployment {dep_name}"
                        f" (app: {app_name or 'default'},"
                        f" replicas: {replicas})"
                    ),
                    category="ray-serve",
                    ttl=self._config.ttl,
                    backend=backend,
                ),
            )
            if self._teardown_in_progress:
                # A deregister_all was issued while this registration
                # was in flight.  Undo immediately to avoid orphans.
                await loop.run_in_executor(
                    None,
                    lambda: delete(
                        name=agent_name,
                        domain=zone,
                        protocol="a2a",
                        backend=backend,
                    ),
                )
                logger.info(
                    "DNS-AID: reverted in-flight register for %s"
                    " (teardown in progress)",
                    agent_name,
                )
                return
            self._registered.add((app_name, dep_name))
            logger.info(
                "DNS-AID: registered %s -> %s:%d",
                agent_name,
                ingress_host,
                ingress_port,
            )
        except ImportError:
            logger.debug(
                "DNS-AID: dns-aid package not installed; skipping "
                "registration for deployment %r. "
                "Install with: pip install dns-aid",
                dep_name,
            )
        except Exception:
            logger.exception(
                "DNS-AID: failed to register deployment %r (app %r)",
                dep_name,
                app_name,
            )

    async def _deregister_async(
        self, dep_name: str, app_name: str
    ) -> None:
        try:
            from dns_aid import delete

            zone = self._config.zone
            if not zone:
                return

            agent_name = self._agent_name(dep_name, app_name)
            backend = self._get_backend()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: delete(
                    name=agent_name,
                    domain=zone,
                    protocol="a2a",
                    backend=backend,
                ),
            )
            self._registered.discard((app_name, dep_name))
            logger.info(
                "DNS-AID: deregistered %s (app %r)",
                agent_name,
                app_name,
            )
        except ImportError:
            pass  # package not installed; nothing to deregister
        except Exception:
            logger.exception(
                "DNS-AID: failed to deregister deployment %r (app %r)",
                dep_name,
                app_name,
            )
