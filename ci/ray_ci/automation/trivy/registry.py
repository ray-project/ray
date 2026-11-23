"""crane calls shared by the Trivy scan tools.

crane is the only thing that talks to a registry: it pulls images and the
vulnerability DBs with the job's own login, so Trivy never needs network
access or credentials.

Every call has a timeout, because crane's own limits cover only connecting: a
registry that stops sending mid-response would otherwise hang the job. This
module doesn't reuse ci/ray_ci/automation/crane_lib.py, which logs every line
crane prints (whole manifests here) and depends on ray_ci_lib.
"""

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

import runfiles

# Seconds. Small calls (digest, manifest, config), then a blob or an image
# pull, which can be several GB.
CALL_TIMEOUT = 2 * 60
TRANSFER_TIMEOUT = 30 * 60
SHA256_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


class RegistryError(Exception):
    """A crane call failed, timed out or returned something unexpected."""


def crane_binary() -> str:
    path = runfiles.Create().Rlocation("crane_linux_x86_64/crane")
    if not path:
        raise RegistryError("crane is not in this program's runfiles")
    return path


def _run(args: Sequence[str], timeout: int, **kwargs: Any) -> Any:
    """Run crane; a timeout, or a crane that can't start, is a RegistryError."""
    what = f"crane {' '.join(args)}"
    try:
        return subprocess.run(
            [crane_binary(), *args], timeout=timeout, check=False, **kwargs
        )
    except subprocess.TimeoutExpired:
        raise RegistryError(f"{what} timed out after {timeout}s") from None
    except OSError as e:
        raise RegistryError(f"{what} could not run: {e}") from e


def _crane(args: Sequence[str], transfer: bool = False) -> str:
    timeout = TRANSFER_TIMEOUT if transfer else CALL_TIMEOUT
    result = _run(args, timeout, capture_output=True, text=True, errors="replace")
    if result.returncode != 0:
        raise RegistryError(f"crane {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout


def _crane_json(args: Sequence[str]) -> dict[str, Any]:
    output = _crane(args)
    try:
        data = json.loads(output)
    except ValueError as e:
        raise RegistryError(f"crane {' '.join(args)} printed no JSON: {e}") from e
    if not isinstance(data, dict):
        raise RegistryError(f"crane {' '.join(args)} printed no JSON object")
    return data


def digest(ref: str, platform: Optional[str] = None) -> str:
    args = ["digest", ref]
    if platform:
        args += ["--platform", platform]
    value = _crane(args).strip()
    if not SHA256_DIGEST.fullmatch(value):
        raise RegistryError(f"crane {' '.join(args)} printed {value!r}, not a digest")
    return value


def manifest(ref: str) -> dict[str, Any]:
    return _crane_json(["manifest", ref])


def config(ref: str) -> dict[str, Any]:
    return _crane_json(["config", ref])


def pull(refs: Sequence[str], layout: Path, platform: str) -> None:
    """Pull every ref into one OCI layout, storing shared layers once."""
    _crane(
        ["pull", "--format", "oci", "--platform", platform, *refs, str(layout)],
        transfer=True,
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def fetch_blob(repo: str, blob_digest: str, dest: Path) -> None:
    """Download a blob by digest, and refuse it unless its sha256 matches. A
    download that fails or is refused leaves nothing at ``dest``."""
    args = ["blob", f"{repo}@{blob_digest}"]
    try:
        with open(dest, "wb") as out:
            result = _run(args, TRANSFER_TIMEOUT, stdout=out, stderr=subprocess.PIPE)
        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace").strip()
            raise RegistryError(f"crane {' '.join(args)} failed: {stderr}")
        actual = sha256_file(dest)
        if actual != blob_digest:
            raise RegistryError(f"blob {blob_digest} downloaded with digest {actual}")
    except Exception:
        dest.unlink(missing_ok=True)
        raise


def login_if_ecr(repo: str, ecr_login: Callable[[str], None]) -> None:
    """Log in to the repo's registry when it is ECR; others need no login."""
    host = repo.split("/")[0]
    if ".dkr.ecr." in host:
        ecr_login(host)
