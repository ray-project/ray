"""crane calls shared by the Trivy scan tools.

crane is the only thing that talks to a registry: it pulls images and the
vulnerability DBs with the job's own login, so Trivy never needs network
access or credentials.
"""

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import runfiles


class RegistryError(Exception):
    """A crane call failed or returned something unexpected."""


def crane_binary() -> str:
    return runfiles.Create().Rlocation("crane_linux_x86_64/crane")


def _crane(args: Sequence[str]) -> str:
    result = subprocess.run(
        [crane_binary(), *args], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RegistryError(f"crane {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def digest(ref: str, platform: Optional[str] = None) -> str:
    args = ["digest", ref]
    if platform:
        args += ["--platform", platform]
    return _crane(args).strip()


def manifest(ref: str) -> dict[str, Any]:
    return json.loads(_crane(["manifest", ref]))


def config(ref: str) -> dict[str, Any]:
    return json.loads(_crane(["config", ref]))


def pull(refs: Sequence[str], layout: Path, platform: str) -> None:
    """Pull every ref into one OCI layout, storing shared layers once."""
    _crane(["pull", "--format", "oci", "--platform", platform, *refs, str(layout)])


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def fetch_blob(repo: str, blob_digest: str, dest: Path) -> None:
    """Download a blob by digest, and refuse it unless its sha256 matches."""
    with open(dest, "wb") as out:
        result = subprocess.run(
            [crane_binary(), "blob", f"{repo}@{blob_digest}"],
            stdout=out,
            stderr=subprocess.PIPE,
            check=False,
        )
    if result.returncode != 0:
        raise RegistryError(
            f"crane blob {repo}@{blob_digest} failed: {result.stderr.decode().strip()}"
        )
    actual = sha256_file(dest)
    if actual != blob_digest:
        dest.unlink()
        raise RegistryError(f"blob {blob_digest} downloaded with digest {actual}")


def login_if_ecr(repo: str, ecr_login: Callable[[str], None]) -> None:
    """Log in to the repo's registry when it is ECR; others need no login."""
    host = repo.split("/")[0]
    if ".dkr.ecr." in host:
        ecr_login(host)
