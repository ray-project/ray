"""Registry credentials from the Docker CLI's configuration file.

Ray pulls sandbox images itself, with no Docker daemon, and takes a
registry's credentials from the file the Docker CLI keeps them in,
``$DOCKER_CONFIG/config.json`` (by default ``~/.docker/config.json``), read
the way the CLI reads it: the credential helper that ``credHelpers`` names
for the registry, else the ``credsStore`` helper, else the registry's
``auths`` entry. A helper is a ``docker-credential-<name>`` program on
``PATH`` that answers with the registry's credentials when asked; one like
``docker-credential-ecr-login`` mints them from the node's cloud identity,
so no secret has to sit on disk.

The credentials only ever live in the worker that pulls the image: they go
to the registry and the token service it names, never into the image cache
or a sandbox.
"""

import base64
import json
import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from ray.experimental.sandbox.exceptions import SandboxCreationError

# The key the Docker CLI files Docker Hub's credentials under.
_DOCKER_HUB_KEY = "https://index.docker.io/v1/"
# Docker Hub's hostnames, any of which may key its auths entry.
_DOCKER_HUB_HOSTS = ("index.docker.io", "docker.io", "registry-1.docker.io")
# What a credential helper prints, exiting non-zero, when it has nothing
# stored for the server it was asked about.
_HELPER_NOT_FOUND = "credentials not found in native keychain"
# The username with which a helper hands back an identity token rather than
# a password.
_IDENTITY_TOKEN_USERNAME = "<token>"


@dataclass(frozen=True)
class RegistryCredentials:
    """A registry's credentials, as the Docker CLI resolves them.

    The secrets stay out of ``repr``, so logging the object leaks nothing.
    """

    username: str = ""
    password: str = field(default="", repr=False)
    # An OAuth2 refresh token that a bearer-token registry's token service
    # trades for pull tokens (``identitytoken``).
    identity_token: str = field(default="", repr=False)
    # A bearer token the registry takes as is (``registrytoken``).
    registry_token: str = field(default="", repr=False)

    def basic_authorization(self) -> str:
        """The HTTP basic ``Authorization`` header value for these credentials."""
        pair = f"{self.username}:{self.password}".encode("utf-8")
        return "Basic " + base64.b64encode(pair).decode("ascii")


def config_path() -> str:
    """The Docker CLI's config file: ``$DOCKER_CONFIG/config.json``."""
    config_dir = os.environ.get("DOCKER_CONFIG") or os.path.join(
        os.path.expanduser("~"), ".docker"
    )
    return os.path.join(config_dir, "config.json")


def _hostname(key: str) -> str:
    """A registry or config key without its scheme or path."""
    return key.split("://", 1)[-1].split("/", 1)[0]


def registry_credentials(
    registry: str, timeout: float
) -> Optional[RegistryCredentials]:
    """Look up a registry's credentials the way the Docker CLI does.

    The config file is read on every call, so a mounted file that gets
    rotated takes effect at the next pull.

    Args:
        registry: The registry's host and optional port, as an image
            reference names it, with an optional scheme;
            ``registry-1.docker.io`` for Docker Hub.
        timeout: Seconds a credential helper gets to answer.

    Returns:
        The credentials, or None when the config has none for the
        registry: no config file, no entry for it, or a helper with
        nothing stored for it.

    Raises:
        SandboxCreationError: If the config file can't be read, or the
            registry's credential helper can't run or fails.
    """
    path = config_path()
    try:
        with open(path, encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as err:
        raise SandboxCreationError(
            f"Cannot read the Docker config file {path}: {err}"
        ) from err

    host = _hostname(registry)
    key = _DOCKER_HUB_KEY if host == "registry-1.docker.io" else host
    try:
        # A registry listed in credHelpers, even with an empty helper, never
        # falls back to credsStore.
        helpers = config.get("credHelpers") or {}
        helper = helpers.get(key, config.get("credsStore"))
        if helper:
            return _helper_credentials(helper, key, timeout)

        auths = config.get("auths") or {}
        entry = auths.get(key)
        if entry is None:
            # Older ``docker login`` versions keyed entries by URL, and a
            # Kubernetes Secret may key Docker Hub's by any of its hostnames.
            hosts = _DOCKER_HUB_HOSTS if key == _DOCKER_HUB_KEY else (key,)
            entry = next((v for k, v in auths.items() if _hostname(k) in hosts), None)
        return _auths_credentials(entry, key, path) if entry else None
    except (AttributeError, TypeError) as err:
        # A field of the wrong type. The message names types, not values, so
        # it carries no secret.
        raise SandboxCreationError(
            f"The Docker config file {path} is malformed: {err}"
        ) from None


def _helper_credentials(
    helper: str, server: str, timeout: float
) -> Optional[RegistryCredentials]:
    """Ask ``docker-credential-<helper>`` for ``server``'s credentials."""
    program = f"docker-credential-{helper}"
    try:
        proc = subprocess.run(
            [program, "get"],
            input=server.encode("utf-8"),
            capture_output=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as err:
        raise SandboxCreationError(
            f"Cannot run {program}, the credential helper that {config_path()} "
            f"names for {server}: {err}"
        ) from err
    output = proc.stdout.decode("utf-8", "replace").strip()
    if proc.returncode != 0:
        if output == _HELPER_NOT_FOUND:
            return None
        detail = output or proc.stderr.decode("utf-8", "replace").strip()
        raise SandboxCreationError(
            f"{program} failed to get credentials for {server}: {detail}"
        )
    try:
        reply = json.loads(output)
        username, secret = reply["Username"], reply["Secret"]
    except (ValueError, KeyError, TypeError):
        # The output may hold the secret, so it stays out of the error.
        raise SandboxCreationError(
            f"{program} answered for {server} with malformed credentials"
        ) from None
    if not secret:
        return None
    if username == _IDENTITY_TOKEN_USERNAME:
        return RegistryCredentials(identity_token=secret)
    return RegistryCredentials(username=username, password=secret)


def _auths_credentials(
    entry: Dict[str, Any], server: str, path: str
) -> Optional[RegistryCredentials]:
    """Credentials from an ``auths`` entry, as ``docker login`` writes them.

    A malformed field fails the lookup with an error that names the field but
    not its value, which may be a secret.
    """
    username = entry.get("username", "")
    password = entry.get("password", "")
    if entry.get("auth"):
        try:
            decoded = base64.b64decode(entry["auth"]).decode("utf-8")
        except ValueError:
            decoded = ""
        # Base64 of ``username:password``, as the Docker CLI requires.
        username, colon, password = decoded.partition(":")
        if not colon or not username:
            raise SandboxCreationError(
                f"The auths entry for {server} in {path} has a malformed auth field"
            )
    registry_token = entry.get("registrytoken", "")
    if "\r" in registry_token or "\n" in registry_token:
        # The token goes into a header as is, and http.client's error for a
        # line break in a header quotes the whole value.
        raise SandboxCreationError(
            f"The auths entry for {server} in {path} has a malformed "
            "registrytoken field"
        )
    creds = RegistryCredentials(
        username=username,
        password=password.strip("\x00"),
        identity_token=entry.get("identitytoken", ""),
        registry_token=registry_token,
    )
    # An empty entry is a placeholder, like those ``docker login`` writes
    # beside a credsStore.
    return creds if creds != RegistryCredentials() else None
