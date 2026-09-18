"""Thin wrapper around the OCI Python SDK used by the OCI node provider.

The wrapper centralises three concerns so that ``config.py`` and
``node_provider.py`` stay focused on autoscaler semantics:

* Authentication. Three modes are supported and resolved in this order:
  an API-key or session-token profile from an OCI config file
  (``provider.oci_config_file`` / ``provider.oci_config_profile``), and, when
  no such file exists (which is the normal situation on the head node),
  instance principals. Session-token profiles (``security_token_file`` in the
  profile, produced by ``oci session authenticate``) are detected
  automatically, which is how most enterprise tenancies are used.
* Client construction with the SDK's built-in retry strategy, so that rate
  limits (HTTP 429) and transient errors are retried with jittered backoff.
* Pagination and tag helpers, including OCI's free-form tag limits.

The ``oci`` package is imported lazily so that ``import ray`` never depends on
it; a clear error is raised if the SDK is missing.
"""

import logging
import os
import re
import threading
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# OCI free-form tag limits (see "Tagging Overview" in the OCI documentation).
MAX_FREEFORM_TAGS = 10
MAX_FREEFORM_TAG_KEY_LEN = 100
MAX_FREEFORM_TAG_VALUE_LEN = 256

# Instance lifecycle states returned by the Compute API.
PROVISIONING = "PROVISIONING"
STARTING = "STARTING"
RUNNING = "RUNNING"
STOPPING = "STOPPING"
STOPPED = "STOPPED"
TERMINATING = "TERMINATING"
TERMINATED = "TERMINATED"

TERMINATED_STATES = frozenset({TERMINATING, TERMINATED})
STOPPED_STATES = frozenset({STOPPING, STOPPED})

DEFAULT_OCI_CONFIG_FILE = "~/.oci/config"
DEFAULT_OCI_CONFIG_PROFILE = "DEFAULT"

_import_lock = threading.Lock()


def import_oci():
    """Import and return the ``oci`` SDK, raising a helpful error if missing."""
    try:
        with _import_lock:
            import oci  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "The Ray OCI cluster launcher requires the Oracle Cloud "
            "Infrastructure Python SDK. Install it with `pip install oci`."
        ) from e
    return oci


def is_service_error(exc: BaseException, *statuses: int, code: Optional[str] = None):
    """Return True if ``exc`` is an ``oci.exceptions.ServiceError`` matching
    one of ``statuses`` (and ``code`` if given)."""
    status = getattr(exc, "status", None)
    if status is None or type(exc).__name__ != "ServiceError":
        return False
    if statuses and status not in statuses:
        return False
    if code is not None and getattr(exc, "code", None) != code:
        return False
    return True


def is_not_found_or_not_authorized(exc: BaseException) -> bool:
    """OCI returns 404 NotAuthorizedOrNotFound both when a resource does not
    exist and when the caller lacks permission to see it."""
    return is_service_error(exc, 404)


def is_out_of_capacity(exc: BaseException) -> bool:
    """Detect the "Out of host capacity" launch failure."""
    message = str(getattr(exc, "message", "") or exc)
    return is_service_error(exc, 500) and "capacity" in message.lower()


def validate_freeform_tags(tags: Dict[str, str]) -> Dict[str, str]:
    """Validate and normalise a free-form tag dict against OCI limits.

    Values are stringified; oversized keys/values or too many tags raise a
    ``ValueError`` rather than failing later inside the Compute API with a
    less descriptive error.
    """
    if len(tags) > MAX_FREEFORM_TAGS:
        raise ValueError(
            f"OCI instances support at most {MAX_FREEFORM_TAGS} free-form tags, "
            f"but {len(tags)} were requested: {sorted(tags)}. Reduce the number "
            "of user-specified `freeform_tags` in `node_config`."
        )
    normalised = {}
    for key, value in tags.items():
        value = "" if value is None else str(value)
        if len(key) > MAX_FREEFORM_TAG_KEY_LEN:
            raise ValueError(
                f"Free-form tag key {key!r} exceeds {MAX_FREEFORM_TAG_KEY_LEN} "
                "characters."
            )
        if len(value) > MAX_FREEFORM_TAG_VALUE_LEN:
            raise ValueError(
                f"Value of free-form tag {key!r} exceeds "
                f"{MAX_FREEFORM_TAG_VALUE_LEN} characters."
            )
        normalised[key] = value
    return normalised


def short_id(ocid: str, length: int = 6) -> str:
    """Return the trailing ``length`` characters of an OCID for logging."""
    return ocid[-length:] if ocid else ""


def dns_label(name: str, max_len: int = 15) -> str:
    """Derive a valid VCN/subnet DNS label (letters/digits, starts with a
    letter, at most 15 chars) from ``name``."""
    label = re.sub(r"[^a-z0-9]", "", name.lower())
    if not label or not label[0].isalpha():
        label = "ray" + label
    return label[:max_len]


class OCIClient:
    """Authenticated OCI SDK clients for one region.

    Args:
        provider_config: The ``provider`` section of the cluster config.
    """

    def __init__(self, provider_config: Dict[str, Any]):
        self.oci = import_oci()
        self.provider_config = provider_config
        self.region = provider_config["region"]
        self.compartment_id = provider_config["compartment_id"]
        self._retry = self.oci.retry.DEFAULT_RETRY_STRATEGY
        self._config, self._signer, self.auth_mode = self._resolve_auth()
        self._clients: Dict[str, Any] = {}
        self._clients_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------
    def _resolve_auth(self):
        """Resolve credentials; see the module docstring for the order."""
        oci = self.oci
        config_file = os.path.expanduser(
            self.provider_config.get("oci_config_file") or DEFAULT_OCI_CONFIG_FILE
        )
        profile = (
            self.provider_config.get("oci_config_profile") or DEFAULT_OCI_CONFIG_PROFILE
        )
        use_instance_principal = self.provider_config.get(
            "use_instance_principal", True
        )

        if os.path.exists(config_file):
            try:
                cfg = oci.config.from_file(config_file, profile)
            except oci.exceptions.ProfileNotFound:
                if not use_instance_principal:
                    raise
                cfg = None
            if cfg is not None:
                cfg = dict(cfg)
                cfg["region"] = self.region
                token_file = cfg.get("security_token_file")
                if token_file:
                    # Session-token profile created by `oci session authenticate`.
                    with open(os.path.expanduser(token_file)) as f:
                        token = f.read().strip()
                    private_key = oci.signer.load_private_key_from_file(
                        os.path.expanduser(cfg["key_file"]), cfg.get("pass_phrase")
                    )
                    signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
                    return cfg, signer, "security_token"
                oci.config.validate_config(cfg)
                return cfg, None, "api_key"

        if use_instance_principal:
            try:
                signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            except Exception as e:
                raise RuntimeError(
                    f"No OCI config file was found at {config_file} and instance "
                    "principal authentication failed. On your workstation, run "
                    "`oci setup config` or `oci session authenticate` (and set "
                    "`oci_config_file` / `oci_config_profile` under `provider`). "
                    "On the head node, either keep `use_instance_principal: true` "
                    "so the launcher grants the instance the required IAM policy, "
                    "or copy credentials to the head with `file_mounts`."
                ) from e
            return {"region": self.region}, signer, "instance_principal"

        raise RuntimeError(
            f"No OCI config file was found at {config_file} (profile {profile!r}) "
            "and `use_instance_principal` is false. Provide credentials on this "
            "machine, e.g. via `file_mounts` when running on the head node."
        )

    @property
    def caller_tenancy_id(self) -> Optional[str]:
        """Tenancy of the authenticated principal (may differ from the tenancy
        owning ``compartment_id`` in cross-tenancy setups)."""
        if self._signer is not None and hasattr(self._signer, "tenancy_id"):
            return self._signer.tenancy_id
        return self._config.get("tenancy")

    # ------------------------------------------------------------------
    # Clients
    # ------------------------------------------------------------------
    def _make_client(self, cls, region: Optional[str] = None):
        cfg = dict(self._config)
        cfg["region"] = region or self.region
        kwargs = {"retry_strategy": self._retry}
        if self._signer is not None:
            kwargs["signer"] = self._signer
        return cls(cfg, **kwargs)

    def _client(self, name: str, cls, region: Optional[str] = None):
        key = (name, region or self.region)
        with self._clients_lock:
            client = self._clients.get(key)
            if client is None:
                client = self._make_client(cls, region)
                self._clients[key] = client
            return client

    @property
    def compute(self):
        return self._client("compute", self.oci.core.ComputeClient)

    @property
    def network(self):
        return self._client("network", self.oci.core.VirtualNetworkClient)

    def identity(self, region: Optional[str] = None):
        return self._client("identity", self.oci.identity.IdentityClient, region)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def list_all(self, list_fn, *args, **kwargs) -> List[Any]:
        """Run a paginated ``list_*`` SDK call and return every item."""
        return self.oci.pagination.list_call_get_all_results(
            list_fn, *args, **kwargs
        ).data

    def models(self):
        """Shortcut to ``oci.core.models``."""
        return self.oci.core.models

    def identity_models(self):
        """Shortcut to ``oci.identity.models``."""
        return self.oci.identity.models

    def home_region_of_tenancy(self, tenancy_id: str) -> str:
        """Return the home region name of ``tenancy_id`` (IAM writes must be
        sent there). Falls back to the provider region on failure."""
        try:
            key = self.identity().get_tenancy(tenancy_id).data.home_region_key
            return self.oci.regions.REGIONS_SHORT_NAMES.get(key.lower(), self.region)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Could not determine the home region of tenancy ...%s (%s); "
                "using %s for IAM calls.",
                short_id(tenancy_id),
                e,
                self.region,
            )
            return self.region

    def tenancy_of_compartment(self, compartment_id: str) -> str:
        """Walk up the compartment tree to find the owning tenancy OCID."""
        current = compartment_id
        for _ in range(32):
            if current.startswith("ocid1.tenancy."):
                return current
            current = self.identity().get_compartment(current).data.compartment_id
        raise RuntimeError(
            f"Could not resolve the tenancy of compartment ...{short_id(compartment_id)}"
        )
