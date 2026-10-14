"""How :class:`~ray.experimental.sandbox.modal.image.Image` compiles to build steps.

An Image records build steps as *specs*: pure descriptions that read no file.
Files are read only when a sandbox is created, by :func:`prepare_build_plan`,
as Modal reads them only when an image is loaded -- so a module that defines
an image at import time keeps importing where those files are absent.

The commands generated here are Modal's, at image builder version 2025.06,
so a layer runs what it would on Modal: ``pip_install("a", "b")`` runs
``python -m pip install a b``, packages sorted and shell-quoted.
"""

import collections
import hashlib
import json
import mmap
import os
import re
import shlex
import sys
import tarfile
import tempfile
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from ray.experimental.sandbox.image_build import (
    COPY_MTIME,
    DEFAULT_SHELL,
    BuildStep,
    CopyFromImageStep,
    CopyStep,
    ImageBuildPlan,
    RunStep,
)
from ray.experimental.sandbox.modal.exception import InvalidError, NotFoundError

# The Modal image builder version whose commands are generated here.
MODAL_BUILDER_VERSION = "2025.06"
SUPPORTED_PYTHON_SERIES = ("3.10", "3.11", "3.12", "3.13", "3.14", "3.14t")
# From Modal's base-images.json for 2025.06.
_PYTHON_MICRO_VERSIONS = ("3.9.22", "3.10.17", "3.11.12", "3.12.10", "3.13.3", "3.14.2")
DEBIAN_CODENAME = "bookworm"
MICROMAMBA_TAG = "2.1.1-debian12-slim"
PACKAGE_TOOLS = "pip wheel uv"
UV_ROOT = "/.uv"
UV_IMAGE = "ghcr.io/astral-sh/uv"
# Modal's debian_slim and micromamba give the main process 48 hours.
SLEEP_CMD = ("sleep", str(48 * 3600))


# -- versions and arguments (ported from modal/_image.py) --------------------


def validate_python_version(
    python_version: Optional[str],
    allow_micro_granularity: bool = True,
    allow_free_threading: bool = False,
    caller_name: str = "",
) -> str:
    """Modal's ``_validate_python_version`` for builder version 2025.06."""
    if python_version is None:
        python_version = series_version = "{}.{}".format(*sys.version_info)
    elif not isinstance(python_version, str):
        raise InvalidError(
            "Python version must be specified as a string, not "
            f"{type(python_version).__name__}"
        )
    elif not re.match(r"^3(?:\.\d{1,2}){1,2}(rc\d*)?t?$", python_version):
        raise InvalidError(f"Invalid Python version: {python_version!r}")
    elif not allow_free_threading and python_version.endswith("t"):
        context = f"with {caller_name}" if caller_name else ""
        raise InvalidError(f"Free threaded Python is not supported {context}")
    else:
        components = python_version.split(".")
        if len(components) == 3 and not allow_micro_granularity:
            raise InvalidError(
                "Python version must be specified as 'major.minor' for this interface;"
                f" micro-level specification ({python_version!r}) is not valid."
            )
        suffix = "t" if len(components) == 3 and python_version.endswith("t") else ""
        series_version = f"{components[0]}.{components[1]}{suffix}"
    if series_version not in SUPPORTED_PYTHON_SERIES:
        raise InvalidError(
            f"Unsupported Python version: {python_version!r}."
            f" When using the {MODAL_BUILDER_VERSION!r} Image builder, Modal supports"
            f" the following series: {list(SUPPORTED_PYTHON_SERIES)!r}."
        )
    return python_version


def dockerhub_python_version(
    python_version: Optional[str], caller_name: str = ""
) -> str:
    """The ``python:<version>`` tag Modal's ``debian_slim`` starts from."""
    python_version = validate_python_version(
        python_version, allow_free_threading=False, caller_name=caller_name
    )
    components = python_version.split(".")
    if len(components) > 2:
        return python_version
    series = "{}.{}".format(*components)
    micro = dict(tuple(v.rsplit(".", 1)) for v in _PYTHON_MICRO_VERSIONS)[series]
    return f"{series}.{micro}"


def make_pip_install_args(
    find_links: Optional[str] = None,
    index_url: Optional[str] = None,
    extra_index_url: Optional[str] = None,
    pre: bool = False,
    extra_options: str = "",
) -> str:
    """Modal's ``_make_pip_install_args``, quirks included."""
    flags = [
        ("--find-links", find_links),
        ("--index-url", index_url),
        ("--extra-index-url", extra_index_url),
    ]
    args = " ".join(
        f"{flag} {shlex.quote(value)}" for flag, value in flags if value is not None
    )
    if pre:
        # Modal's leading space, which leaves two before --pre when no other
        # flag precedes it.
        args += " --pre"
    if extra_options:
        if args:
            args += " "
        args += f"{extra_options}"
    return args


def check_packages(packages: Sequence[str], method: str) -> None:
    if any(pkg.startswith("-") for pkg in packages):
        raise InvalidError(
            f"Package list for `Image.{method}` cannot contain other arguments;"
            " try the `extra_options` parameter instead."
        )


def pip_install_command(packages: Iterable[str], extra_args: str) -> str:
    return f"python -m pip install {shlex.join(sorted(packages))} {extra_args}".strip()


def uv_copy_step(
    uv_version: Optional[str], force_token: Optional[str]
) -> CopyFromImageStep:
    return CopyFromImageStep(
        image=f"{UV_IMAGE}:{uv_version or 'latest'}",
        src="/uv",
        dest=f"{UV_ROOT}/uv",
        force_token=force_token,
    )


# -- specs ------------------------------------------------------------------


@dataclass(frozen=True)
class RunContext:
    """What a RUN step inherits from the image at the point it is added."""

    env: Tuple[Tuple[str, str], ...]
    build_env: Tuple[Tuple[str, str], ...]
    workdir: Optional[str]
    shell: Tuple[str, ...]
    force_token: Optional[str] = field(default=None, compare=False)

    def run(self, *commands: str, image_env=()) -> RunStep:
        return RunStep(
            commands=tuple(commands),
            env=self.env,
            build_env=self.build_env,
            workdir=self.workdir,
            shell=self.shell,
            image_env=tuple(image_env),
            force_token=self.force_token,
        )


@dataclass(frozen=True)
class CopySpec:
    """Local files to copy into the image: ``(local path, absolute destination)``."""

    files: Tuple[Tuple[str, str], ...]
    description: str = ""
    force_token: Optional[str] = field(default=None, compare=False)


@dataclass(frozen=True)
class PyprojectSpec:
    """``pip_install_from_pyproject``: its dependencies are read at build time."""

    path: str
    optional_dependencies: Tuple[str, ...]
    extra_args: str
    context: RunContext


@dataclass(frozen=True)
class UvSyncSpec:
    """``uv_sync``: which files it copies depends on what exists at build time."""

    project_dir: str
    groups: Tuple[str, ...]
    extras: Tuple[str, ...]
    frozen: bool
    extra_options: str
    uv_version: Optional[str]
    context: RunContext


@dataclass(frozen=True)
class PoetrySpec:
    """``poetry_install_from_file``: its lockfile is located at build time."""

    pyproject: str
    lockfile: Optional[str]
    ignore_lockfile: bool
    with_: Tuple[str, ...]
    without: Tuple[str, ...]
    only: Tuple[str, ...]
    poetry_version: Optional[str]
    old_installer: bool
    context: RunContext


def _load_toml(path: str) -> Dict:
    try:
        import tomllib  # Python 3.11+
    except ImportError:  # pragma: no cover - Python 3.10
        try:
            import tomli as tomllib
        except ImportError:
            raise ImportError(
                f"Reading {path} needs a TOML parser: on Python 3.10, "
                "`pip install tomli`."
            ) from None
    with open(path, "rb") as f:
        return tomllib.load(f)


def _compile_pyproject(spec: PyprojectSpec) -> List[BuildStep]:
    config = _load_toml(os.path.expanduser(spec.path))
    if "project" not in config or "dependencies" not in config["project"]:
        raise ValueError(
            "No [project.dependencies] section in pyproject.toml file. "
            "If your pyproject.toml instead declares [tool.poetry.dependencies], "
            "use `Image.poetry_install_from_file()`. "
            "See https://packaging.python.org/en/latest/guides/writing-pyproject-toml "
            "for further file format guidelines."
        )
    dependencies = list(config["project"]["dependencies"])
    if spec.optional_dependencies:
        optionals = config["project"]["optional-dependencies"]
        for group in spec.optional_dependencies:
            if group in optionals:
                dependencies.extend(optionals[group])
    return [spec.context.run(pip_install_command(dependencies, spec.extra_args))]


def _compile_uv_sync(spec: UvSyncSpec) -> List[BuildStep]:
    project_dir = os.path.expanduser(spec.project_dir)
    pyproject = os.path.join(project_dir, "pyproject.toml")
    if not os.path.exists(pyproject):
        raise InvalidError(f"Expected {pyproject} to exist")
    content = _load_toml(pyproject)
    if "workspace" in content.get("tool", {}).get("uv", {}):
        raise InvalidError("uv workspaces are not supported")
    args = [f"--project={UV_ROOT}", "--no-install-workspace", "--compile-bytecode"]
    args += [f"--group={group}" for group in spec.groups]
    args += [f"--extra={extra}" for extra in spec.extras]
    if spec.extra_options:
        args.append(spec.extra_options)
    files = [(pyproject, f"{UV_ROOT}/pyproject.toml")]
    uv_lock = os.path.join(project_dir, "uv.lock")
    if os.path.exists(uv_lock):
        files.append((uv_lock, f"{UV_ROOT}/uv.lock"))
        if spec.frozen:
            args.append("--frozen")
    token = spec.context.force_token
    return [
        uv_copy_step(spec.uv_version, token),
        CopySpec(tuple(files), "COPY pyproject.toml for uv sync", token),
        spec.context.run(
            f"{UV_ROOT}/uv sync {' '.join(args).strip()}",
            image_env=(("PATH", f"{UV_ROOT}/.venv/bin:$PATH"),),
        ),
    ]


def _compile_poetry(spec: PoetrySpec) -> List[BuildStep]:
    token = spec.context.force_token
    commands: List[str] = []
    if spec.poetry_version is not None:
        version_spec = (
            "" if spec.poetry_version == "latest" else f"=={spec.poetry_version}"
        )
        commands.append(f"python -m pip install poetry{version_spec}")
    if spec.old_installer:
        commands.append("poetry config experimental.new-installer false")
    files = [(os.path.expanduser(spec.pyproject), "/tmp/poetry/pyproject.toml")]
    if not spec.ignore_lockfile:
        lockfile = spec.lockfile
        if lockfile is None:
            inferred = os.path.join(
                os.path.dirname(spec.pyproject) or ".", "poetry.lock"
            )
            if not os.path.exists(inferred):
                raise NotFoundError(
                    f"poetry.lock not found at inferred location: {os.path.abspath(inferred)}. "
                    "If a lockfile is not needed, `ignore_lockfile=True` can be used."
                )
            lockfile = inferred
        files.insert(0, (os.path.expanduser(lockfile), "/tmp/poetry/poetry.lock"))
    install = "poetry install --no-root"
    if spec.with_:
        install += f" --with {','.join(spec.with_)}"
    if spec.without:
        install += f" --without {','.join(spec.without)}"
    if spec.only:
        install += f" --only {','.join(spec.only)}"
    install += " --compile"
    steps: List[BuildStep] = []
    if commands:
        steps.append(spec.context.run(*commands))
    steps.append(CopySpec(tuple(files), "COPY poetry project files", token))
    steps.append(
        spec.context.run(
            f"cd /tmp/poetry && poetry config virtualenvs.create false && {install}"
        )
    )
    return steps


def _expand(spec) -> List:
    if isinstance(spec, PyprojectSpec):
        return _compile_pyproject(spec)
    if isinstance(spec, UvSyncSpec):
        return _compile_uv_sync(spec)
    if isinstance(spec, PoetrySpec):
        return _compile_poetry(spec)
    return [spec]


# -- context packing --------------------------------------------------------

# sha256 of a local file by (path, size, mtime_ns, inode), so an unchanged
# file is not read again on every Sandbox.create.
_HASH_MEMO: "collections.OrderedDict[Tuple, str]" = collections.OrderedDict()
_HASH_MEMO_MAX = 65536
# ray.put refs of packed archives by digest, with their sizes. Holding a ref
# keeps the object alive, so repeated creates from one Image upload its files
# once; the bounds cap what that keeps in the object store.
_ARCHIVE_REFS: "collections.OrderedDict[str, Tuple[object, int]]" = (
    collections.OrderedDict()
)
_ARCHIVE_REFS_MAX = 32
_ARCHIVE_REFS_MAX_BYTES = 1 << 30
_MEMO_LOCK = threading.Lock()
_READ_CHUNK = 1 << 20


def _stat_key(path: str, st: os.stat_result) -> Tuple:
    return (path, st.st_size, st.st_mtime_ns, st.st_ino)


def _remember(memo: "collections.OrderedDict", key, value, limit: int) -> None:
    with _MEMO_LOCK:
        memo[key] = value
        memo.move_to_end(key)
        while len(memo) > limit:
            memo.popitem(last=False)


def _remember_archive(digest: str, ref: Any, size: int) -> None:
    """Memoize an uploaded archive; the newest stays even if over the bytes cap."""
    with _MEMO_LOCK:
        _ARCHIVE_REFS[digest] = (ref, size)
        _ARCHIVE_REFS.move_to_end(digest)
        total = sum(entry[1] for entry in _ARCHIVE_REFS.values())
        while len(_ARCHIVE_REFS) > 1 and (
            len(_ARCHIVE_REFS) > _ARCHIVE_REFS_MAX or total > _ARCHIVE_REFS_MAX_BYTES
        ):
            _, (_, dropped) = _ARCHIVE_REFS.popitem(last=False)
            total -= dropped


class _HashingReader:
    """A file read through for ``tarfile``, hashing what passes."""

    def __init__(self, f):
        self._f = f
        self.hash = hashlib.sha256()

    def read(self, size: int = -1) -> bytes:
        data = self._f.read(size)
        self.hash.update(data)
        return data


def _digest(entries: Iterable[Tuple[str, int, str]]) -> str:
    canonical = json.dumps(sorted(entries), separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def pack_copy(spec: CopySpec, put: Optional[Callable[[Any], Any]] = None) -> CopyStep:
    """Hash and pack a COPY spec's files into a :class:`CopyStep`.

    The digest covers each file's destination, permission bits and content
    hash; timestamps are left out, as Modal leaves them out. The archive
    holds each file at its destination path relative to ``/``, root-owned,
    with its mode and the fixed :data:`~ray.experimental.sandbox.image_build.COPY_MTIME`,
    as Docker's ``COPY`` writes it apart from the timestamp: what the key
    leaves out, the content leaves out too.

    Files are streamed, not held in memory: each is hashed while it is copied
    into a temporary archive, which is uploaded straight from its pages.

    Args:
        spec: The files.
        put: Stores the archive and returns a handle for the node to fetch;
            ``ray.put`` by default. It is given a uint8 array viewing the
            archive, which is only valid during the call, so it must copy it
            as ``ray.put`` does. Its results are memoized by digest.

    Returns:
        The COPY step, carrying the digest and the stored archive.

    Raises:
        FileNotFoundError: A file no longer exists.
    """
    if put is None:
        import ray

        put = ray.put
    stats = []
    for local, remote in spec.files:
        try:
            st = os.stat(local)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"File {local!r} added to the image (as {remote!r}) does not exist."
            ) from None
        stats.append((local, remote, st))
    entries = []
    for local, remote, st in stats:
        known = _HASH_MEMO.get(_stat_key(local, st))
        if known is None:
            entries = None
            break
        entries.append((remote, st.st_mode & 0o7777, known))
    if entries is not None:
        digest = _digest(entries)
        memo = _ARCHIVE_REFS.get(digest)
        if memo is not None:
            return CopyStep(digest, memo[0], spec.description, spec.force_token)
    import numpy as np

    entries = []
    with tempfile.TemporaryFile() as spool:
        with tarfile.open(
            fileobj=spool, mode="w", format=tarfile.PAX_FORMAT, copybufsize=_READ_CHUNK
        ) as tar:
            for local, remote, _ in sorted(stats, key=lambda item: item[1]):
                with open(local, "rb") as f:
                    # The file as opened, not as first stat'ed: the hash then
                    # matches both what is packed and the memo's key.
                    current = os.fstat(f.fileno())
                    mode = current.st_mode & 0o7777
                    info = tarfile.TarInfo(remote.lstrip("/"))
                    info.size = current.st_size
                    info.mode = mode
                    info.mtime = COPY_MTIME
                    reader = _HashingReader(f)
                    tar.addfile(info, reader)
                sha = reader.hash.hexdigest()
                _remember(_HASH_MEMO, _stat_key(local, current), sha, _HASH_MEMO_MAX)
                entries.append((remote, mode, sha))
        size = spool.tell()
        spool.flush()
        with mmap.mmap(spool.fileno(), size, access=mmap.ACCESS_READ) as pages:
            view = np.frombuffer(pages, dtype=np.uint8)
            try:
                ref = put(view)
            finally:
                # The map cannot close while an array still views it.
                del view
    digest = _digest(entries)
    _remember_archive(digest, ref, size)
    return CopyStep(digest, ref, spec.description, spec.force_token)


def prepare_build_plan(
    specs: Sequence, put: Optional[Callable[[Any], Any]] = None
) -> Optional[ImageBuildPlan]:
    """Turn an Image's build specs into a plan the node can execute.

    This is where local files are read: deferred specs are compiled and COPY
    files hashed and uploaded.

    Args:
        specs: The Image's build specs, in order.
        put: As for :func:`pack_copy`.

    Returns:
        The plan, or None for an Image with no build steps.
    """
    steps: List[BuildStep] = []
    for spec in specs:
        for item in _expand(spec):
            steps.append(pack_copy(item, put) if isinstance(item, CopySpec) else item)
    return ImageBuildPlan(tuple(steps)) if steps else None


__all__ = [
    "DEFAULT_SHELL",
    "CopySpec",
    "PoetrySpec",
    "PyprojectSpec",
    "RunContext",
    "UvSyncSpec",
    "prepare_build_plan",
]
