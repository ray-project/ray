import hashlib
import json
import os
import shutil
from typing import Dict, List, Optional

from typing_extensions import TypedDict

_INSTALL_PYTHON_DEPS_SCRIPT = """\
#!/bin/bash

set -euo pipefail

LOCK_FILE="${1:-python_depset.lock}"

if [[ ! -f "${LOCK_FILE}" ]]; then
    echo "Lock file ${LOCK_FILE} does not exist" >/dev/stderr
    exit 1
fi

uv pip install --system --no-deps --index-strategy unsafe-best-match \\
    -r "${LOCK_FILE}"
"""


class BuildContext(TypedDict, total=False):
    """
    Build context for custom BYOD image builds.

    Attributes:
        envs: Environment variables to set in the image.
        post_build_script: Filename of the post-build script.
        post_build_script_digest: SHA256 digest of the post-build script.
        python_depset: Filename of the Python dependencies lock file.
        python_depset_digest: SHA256 digest of the Python dependencies lock file.
        install_python_deps_script_digest: SHA256 digest of the install script.
    """

    envs: Dict[str, str]

    post_build_script: str
    post_build_script_digest: str

    python_depset: str
    python_depset_digest: str
    install_python_deps_script_digest: str


def make_build_context(
    base_dir: str,
    envs: Optional[Dict[str, str]] = None,
    post_build_script: Optional[str] = None,
    python_depset: Optional[str] = None,
) -> BuildContext:
    """
    Create a BuildContext with computed file digests.

    Args:
        base_dir: Directory containing the source files.
        envs: Environment variables to set in the image.
        post_build_script: Filename of the post-build script.
        python_depset: Filename of the Python dependencies lock file.

    Returns:
        A BuildContext with filenames and their SHA256 digests.
    """
    ctx: BuildContext = {}

    if envs:
        ctx["envs"] = envs

    if post_build_script:
        ctx["post_build_script"] = post_build_script
        path = os.path.join(base_dir, post_build_script)
        ctx["post_build_script_digest"] = _sha256_file(path)

    if python_depset:
        ctx["python_depset"] = python_depset
        path = os.path.join(base_dir, python_depset)
        ctx["python_depset_digest"] = _sha256_file(path)
        ctx["install_python_deps_script_digest"] = _sha256_str(
            _INSTALL_PYTHON_DEPS_SCRIPT
        )

    return ctx


def encode_build_context(ctx: BuildContext) -> str:
    """Encode a BuildContext to deterministic minified JSON."""
    return json.dumps(ctx, sort_keys=True, separators=(",", ":"))


def decode_build_context(data: str) -> BuildContext:
    """Decode a JSON string to a BuildContext."""
    return json.loads(data)


def build_context_digest(ctx: BuildContext) -> str:
    """Compute SHA256 digest of the encoded BuildContext."""
    encoded = encode_build_context(ctx)
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    return f"sha256:{digest}"


def fill_build_context_dir(
    ctx: BuildContext,
    source_dir: str,
    context_dir: str,
) -> None:
    """
    Generate Dockerfile and copy source files to the build directory.

    Args:
        ctx: The BuildContext specifying what to include.
        source_dir: Source directory containing the original files.
        context_dir: Target directory for the generated Dockerfile and copied files.
    """
    dockerfile: List[str] = ["# syntax=docker/dockerfile:1.3-labs"]
    dockerfile.append("ARG BASE_IMAGE")
    dockerfile.append("FROM ${BASE_IMAGE}")

    if "envs" in ctx and ctx["envs"]:
        dockerfile.append("ENV \\")
        env_lines = [f"  {k}={v}" for k, v in sorted(ctx["envs"].items())]
        dockerfile.append(" \\\n".join(env_lines))

    if "python_depset" in ctx:
        shutil.copy(
            os.path.join(source_dir, ctx["python_depset"]),
            os.path.join(context_dir, "python_depset.lock"),
        )
        with open(os.path.join(context_dir, "install_python_deps.sh"), "w") as f:
            f.write(_INSTALL_PYTHON_DEPS_SCRIPT)
        dockerfile.append("COPY install_python_deps.sh /tmp/install_python_deps.sh")
        dockerfile.append("COPY python_depset.lock python_depset.lock")
        dockerfile.append("RUN bash /tmp/install_python_deps.sh python_depset.lock")

    if "post_build_script" in ctx:
        shutil.copy(
            os.path.join(source_dir, ctx["post_build_script"]),
            os.path.join(context_dir, "post_build_script.sh"),
        )
        dockerfile.append("COPY post_build_script.sh /tmp/post_build_script.sh")
        dockerfile.append("RUN bash /tmp/post_build_script.sh")

    dockerfile_path = os.path.join(context_dir, "Dockerfile")
    with open(dockerfile_path, "w") as f:
        f.write("\n".join(dockerfile) + "\n")


def _sha256_file(path: str) -> str:
    with open(path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    return f"sha256:{digest}"


def _sha256_str(content: str) -> str:
    digest = hashlib.sha256(content.encode()).hexdigest()
    return f"sha256:{digest}"
