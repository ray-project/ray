"""Image build plans: the steps a node runs to build an image, and their keys.

A plan is an ordered list of steps applied on top of a base image. Each step
becomes one cached layer on the node that builds it, named by a key that
hashes the step's definition together with its parent's key, so two images
that share their first k steps share those k layers. See
:class:`~ray.experimental.sandbox.image_builder.ImageBuilder`.

Steps are plain values. Front ends compile their own image APIs to them --
the Modal-compatible ``Image`` does -- and the node-side builder executes
them.
"""

import hashlib
import hmac
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional, Tuple, Union

from ray.util.annotations import DeveloperAPI

# Part of every layer key. Bump it whenever a step's meaning changes for the
# same definition -- a generated command, how a layer is normalized -- so
# that nodes rebuild instead of serving layers made the old way.
BUILDER_VERSION = "ray-2026.10"
DEFAULT_SHELL: Tuple[str, ...] = ("/bin/sh", "-c")
# The modification time COPY layers give every file and directory they write:
# 1980-01-01, the earliest a ZIP entry can carry, so building a wheel from
# copied sources still works. A COPY layer's key leaves timestamps out, as
# Modal's does, so its content must leave them out too: then one key means one
# set of files, whichever client sent them and whichever node built them.
COPY_MTIME = 315532800


def _pairs(env: Optional[Mapping[str, str]]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((env or {}).items()))


@DeveloperAPI
@dataclass(frozen=True)
class RunStep:
    """Run shell commands in a build sandbox and keep what they change.

    Attributes:
        commands: Run in order, each as ``[*shell, command]``; the first that
            exits non-zero fails the build.
        env: The image's environment at this point, which the layer
            depends on.
        build_env: Variables for this step only. They are part of the key but
            not of the image, and their values are never written to disk:
            the node keys them with its own secret first (see
            :func:`step_key`), so a cache directory name cannot be used to
            guess them.
        workdir: Working directory for the commands, created if missing.
            None uses the image's own WORKDIR.
        shell: The argv prefix each command is appended to.
        image_env: Variables set in the image's own config once the commands
            ran, like a Dockerfile ``ENV`` after the ``RUN``: later steps and
            sandboxes on the image see them. ``$VAR`` and ``${VAR}`` expand
            against the image's environment, so ``PATH=/x:$PATH`` prepends.
        force_token: Rebuild this step unless the cached layer was already
            rebuilt for this token. Not part of the key.
    """

    commands: Tuple[str, ...]
    env: Tuple[Tuple[str, str], ...] = ()
    build_env: Tuple[Tuple[str, str], ...] = ()
    workdir: Optional[str] = None
    shell: Tuple[str, ...] = DEFAULT_SHELL
    image_env: Tuple[Tuple[str, str], ...] = ()
    force_token: Optional[str] = field(default=None, compare=False)

    def __post_init__(self):
        if not self.commands or not all(isinstance(c, str) for c in self.commands):
            raise ValueError("RunStep needs at least one command, all strings.")
        if not self.shell or not all(isinstance(s, str) for s in self.shell):
            raise ValueError("RunStep.shell must be a non-empty argv of strings.")

    def definition(self, build_env_key: Optional[bytes] = None) -> Dict[str, Any]:
        build_env = dict(self.build_env)
        if build_env_key is not None:
            build_env = {
                name: hmac.new(
                    build_env_key, value.encode("utf-8"), hashlib.sha256
                ).hexdigest()
                for name, value in build_env.items()
            }
        return {
            "kind": "run",
            "commands": list(self.commands),
            "env": dict(self.env),
            "build_env": build_env,
            "workdir": self.workdir,
            "shell": list(self.shell),
            "image_env": [list(pair) for pair in self.image_env],
        }

    def record(self) -> Dict[str, Any]:
        """The definition as written to disk: build-only values left out."""
        data = self.definition()
        data["build_env"] = sorted(dict(self.build_env))
        return data

    def summary(self) -> str:
        text = " && ".join(self.commands)
        return f"RUN {text if len(text) <= 200 else text[:197] + '...'}"


@DeveloperAPI
@dataclass(frozen=True)
class CopyStep:
    """Copy files from the client into the image.

    Attributes:
        digest: sha256 over the sorted ``(destination, mode, content sha256)``
            of every file: what the layer's key depends on. Timestamps are
            left out, as Modal leaves them out.
        archive: The files as a tar, each at its destination path relative to
            ``/``: an ``ObjectRef`` to its bytes, or the bytes themselves.
            Only fetched when the layer is not cached. Not part of the key.
        description: For logs, such as "COPY 12 files to /root/app".
        force_token: As for :class:`RunStep`.
    """

    digest: str
    archive: Any = field(default=None, compare=False, repr=False)
    description: str = field(default="", compare=False)
    force_token: Optional[str] = field(default=None, compare=False)

    def definition(self, build_env_key: Optional[bytes] = None) -> Dict[str, Any]:
        return {"kind": "copy", "digest": self.digest}

    def record(self) -> Dict[str, Any]:
        return {**self.definition(), "description": self.description}

    def summary(self) -> str:
        return self.description or f"COPY {self.digest[:12]}"


@DeveloperAPI
@dataclass(frozen=True)
class CopyFromImageStep:
    """Copy a path out of another image, like ``COPY --from=<image>``.

    Attributes:
        image: The source image reference; pulled and cached like any other.
        src: Absolute path inside the source image.
        dest: Absolute destination path.
        force_token: As for :class:`RunStep`.
    """

    image: str
    src: str
    dest: str
    force_token: Optional[str] = field(default=None, compare=False)

    def definition(self, build_env_key: Optional[bytes] = None) -> Dict[str, Any]:
        return {
            "kind": "copy_from_image",
            "image": self.image,
            "src": self.src,
            "dest": self.dest,
        }

    def record(self) -> Dict[str, Any]:
        return self.definition()

    def summary(self) -> str:
        return f"COPY --from={self.image} {self.src} {self.dest}"


BuildStep = Union[RunStep, CopyStep, CopyFromImageStep]


@DeveloperAPI
@dataclass(frozen=True)
class ImageBuildPlan:
    """The steps to apply, in order, on top of a base image.

    Attributes:
        steps: One cached layer each.
    """

    steps: Tuple[BuildStep, ...]

    def __post_init__(self):
        if not self.steps:
            raise ValueError("An ImageBuildPlan needs at least one step.")
        for step in self.steps:
            if not isinstance(step, (RunStep, CopyStep, CopyFromImageStep)):
                raise TypeError(f"Not a build step: {step!r}")


def _canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def base_key(
    reference: str, arch: str, tar_stat: Optional[Tuple[int, int]] = None
) -> str:
    """The key the first layer of a plan hashes as its parent.

    Args:
        reference: The base image reference, as given.
        arch: The node's architecture; caches are per node.
        tar_stat: ``(size, mtime_ns)`` when the base is a local tar, so a
            changed tar starts a new chain.

    Returns:
        The key, a sha256 hex digest.
    """
    return hashlib.sha256(
        _canonical(
            {
                "v": BUILDER_VERSION,
                "base": reference,
                "arch": arch,
                "tar": list(tar_stat) if tar_stat else None,
            }
        )
    ).hexdigest()


def step_key(
    parent_key: str,
    parent_epoch: int,
    step: BuildStep,
    build_env_key: Optional[bytes] = None,
) -> str:
    """The key of ``step`` built on the layer (or base) ``parent_key``.

    Args:
        parent_key: The parent layer's key, or the base key.
        parent_epoch: The parent's force epoch: forcing a rebuild of the
            parent advances it, which re-keys every descendant.
        step: The step.
        build_env_key: A secret the node keys ``build_env`` values with
            (HMAC-SHA256) before hashing them in. Without it a key, which
            names a directory anyone on the node can list, would let a
            low-entropy secret be guessed offline from the step's otherwise
            public definition.

    Returns:
        The key, a sha256 hex digest.
    """
    return hashlib.sha256(
        _canonical(
            {
                "v": BUILDER_VERSION,
                "parent": parent_key,
                "parent_epoch": parent_epoch,
                "step": step.definition(build_env_key),
            }
        )
    ).hexdigest()


def env_pairs(env: Optional[Mapping[str, str]]) -> Tuple[Tuple[str, str], ...]:
    """``env`` as the sorted pairs :class:`RunStep` stores."""
    return _pairs(env)
