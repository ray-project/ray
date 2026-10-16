"""A Modal-compatible container image.

An ``Image`` is a value, not a handle: immutable, and every builder returns a
new one. It carries a base image reference, an ordered list of *build steps*,
and *runtime metadata*.

Build steps -- ``run_commands``, the package installers, ``debian_slim``,
``add_local_*(copy=True)`` -- change the image's filesystem. Each becomes a
cached layer on the node that runs the sandbox: the first sandbox on a node
builds what is missing, and later ones, or images sharing the same leading
steps, reuse it. See :mod:`ray.experimental.sandbox.image_builder`.

Runtime metadata -- ``env``, ``workdir``, ``shell``, ``cmd``, ``entrypoint``,
and ``copy=False`` file additions -- is applied when the sandbox is created,
so ``Image.env(...)`` is free. Build steps added after it still see it, as
they would see a Dockerfile ``ENV`` or ``WORKDIR``.

Members that depend on Modal's hosted control plane (image registry, Secrets,
cloud registry auth, server-side builds) raise ``NotSupportedError``, as do
Dockerfile parsing and ``run_function``, which are not implemented yet.
"""

import contextlib
import fnmatch
import importlib.util
import logging
import os
import shlex
import uuid
from pathlib import Path, PurePath, PurePosixPath
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from ray.experimental.sandbox.image_build import DEFAULT_SHELL, env_pairs
from ray.experimental.sandbox.modal import _image_build as _build
from ray.experimental.sandbox.modal._resolved_image import ResolvedImage, StartupFile
from ray.experimental.sandbox.modal.exception import InvalidError, NotSupportedError
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Modules already reported as unresolvable inside Image.imports(), so a loop
# over many sandboxes does not repeat the same line.
_REPORTED_DEFERRED_IMPORTS = set()

# Modal's MODAL_FORCE_BUILD forces every layer; one token per process, so a
# layer is rebuilt once per program run rather than once per sandbox.
_FORCE_BUILD_ENV = "MODAL_FORCE_BUILD"
_PROCESS_FORCE_TOKEN = uuid.uuid4().hex

_BUILD_AFTER_LOCAL_FILES = (
    "An image tried to run a build step after using `image.add_local_*` to include local files.\n"
    "\n"
    "Run `image.add_local_*` commands last in your image build to avoid rebuilding images with every local "
    "file change. Modal will then add these files to containers on startup instead, saving build time.\n"
    "If you need to run other build steps after adding local files, set `copy=True` to copy the files "
    "directly into the image, at the expense of some added build time.\n"
    "\n"
    "Example:\n"
    "\n"
    "my_image = (\n"
    "    Image.debian_slim()\n"
    '    .add_local_file("data.json", copy=True)\n'
    '    .run_commands("python -m mypak")  # this now works!\n'
    ")\n"
)


def _force_requested(force_build: bool) -> bool:
    return bool(force_build) or os.environ.get(_FORCE_BUILD_ENV, "").lower() not in (
        "",
        "0",
        "false",
    )


def _unsupported(name: str, reason: str, alternative: str = "") -> NotSupportedError:
    """Build the error for a Modal feature this backend does not implement.

    NotSupportedError subclasses NotImplementedError, so existing `except
    NotImplementedError` clauses keep working, while `except modal.Error` now
    catches these too -- which exception.py's docstring already promised.
    """
    suffix = f" {alternative}" if alternative else ""
    return NotSupportedError(
        f"{name} is not supported by the Ray sandbox backend: {reason}.{suffix}"
    )


def _reject(method: str, **kwargs) -> None:
    """Raise for build-step parameters that have no Ray equivalent."""
    reasons = {
        "secret": "private registry credentials are unavailable",
        "secrets": "Modal Secrets have no equivalent",
        "volumes": "build steps cannot mount volumes",
        "gpu": "build steps cannot request a GPU",
        "client": "there is no Modal client behind this layer",
    }
    for key, value in kwargs.items():
        if value:
            raise _unsupported(
                f"Image.{method}({key}=...)",
                reasons.get(key, "no Ray equivalent"),
            )


def _flatten_str_args(function_name: str, arg_name: str, args) -> Tuple[str, ...]:
    """Accept Modal's ``*args`` of strings or nested lists of strings.

    Raises ``InvalidError`` rather than ``TypeError`` to match Modal, whose
    ``_flatten_str_args`` raises it with this same message. Every variadic
    builder funnels through here, so using the builtin would put all of them
    outside ``except modal.exception.InvalidError``.
    """
    flat: List[str] = []
    for arg in args:
        if isinstance(arg, str):
            flat.append(arg)
        elif isinstance(arg, (list, tuple)):
            for item in arg:
                if not isinstance(item, str):
                    raise InvalidError(
                        f"{function_name}: {arg_name} must only contain strings"
                    )
                flat.append(item)
        else:
            raise InvalidError(f"{function_name}: {arg_name} must only contain strings")
    return tuple(flat)


def _argv_tuple(value, method: str) -> Tuple[str, ...]:
    """Validate an argv list.

    Without this, ``cmd("sh")`` would silently become ``('s', 'h')`` -- a
    string is iterable, so ``tuple()`` accepts it and produces nonsense.
    """
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise InvalidError(
            f"Image.{method}() must be a list of strings, not "
            f"{type(value).__name__}."
        )
    for item in value:
        if not isinstance(item, str):
            raise InvalidError(
                f"Image.{method}() must be a list of strings; found "
                f"{type(item).__name__}."
            )
    return tuple(value)


def _clean_env(env: Optional[Dict[str, Optional[str]]]) -> Dict[str, str]:
    """Drop the None values Modal uses to mean 'leave unset'."""
    if not env:
        return {}
    return {k: v for k, v in env.items() if v is not None}


def _non_python_files(relative: Path) -> bool:
    """Ignore anything that is not a ``.py`` file, plus dotfiles and caches.

    Modal defaults ``add_local_python_source(ignore=...)`` to
    ``~FilePatternMatcher("**/*.py")``, which is the "not a .py file" half
    alone. Dropping dot-prefixed components and ``__pycache__`` is **ours**,
    and a deliberate narrowing: a package directory routinely holds a ``.env``
    or a nested ``.git``, and Modal's default ships them. Fewer files travel
    here than there, which is the safe direction to differ in.
    """
    if any(part.startswith(".") or part == "__pycache__" for part in relative.parts):
        return True
    return relative.suffix != ".py"


def _matches_ignore(relative: str, ignore) -> bool:
    """Whether a relative path is excluded, .dockerignore-style.

    ``add_local_dir`` walks files only, and ``PurePath.match`` anchors from the
    right, so testing the file path alone means a directory pattern matches
    nothing: ``ignore=[".git"]`` would not exclude ``.git/config``, and the
    whole tree -- credentials included -- would be copied into the image. Each
    ancestor directory is therefore tested as well.
    """
    if not ignore:
        return False
    if callable(ignore):
        return bool(ignore(Path(relative)))

    path = PurePath(relative)
    candidates = [path, *path.parents]
    for pattern in ignore:
        for candidate in candidates:
            if candidate == PurePath("."):
                continue
            if candidate.match(pattern) or fnmatch.fnmatch(str(candidate), pattern):
                return True
    return False


@PublicAPI(stability="alpha")
class Image:
    """A container image definition.

    Start from :meth:`from_registry`, :meth:`debian_slim` or
    :meth:`micromamba`, then chain build steps and metadata onto it. Every
    method returns a new ``Image``; none mutates the receiver.
    """

    __slots__ = (
        "_reference",
        "_base_force_token",
        "_force_token",
        "_steps",
        "_env",
        "_workdir",
        "_shell",
        "_build_shell",
        "_cmd",
        "_entrypoint",
        "_startup_files",
    )

    _FIELDS = {
        "base_force_token": None,
        # Inherited by every build step added from here on, as Modal passes
        # `self.force_build or force_build` down the chain.
        "force_token": None,
        "steps": (),
        "env": {},
        "workdir": None,
        "shell": None,
        # The argv build steps run their commands with; Modal's SHELL.
        "build_shell": DEFAULT_SHELL,
        # None until set: `cmd([])` and `entrypoint([])` clear the base
        # image's CMD and ENTRYPOINT on Modal, so an empty argv has to stay
        # distinguishable from one that was never given.
        "cmd": None,
        "entrypoint": None,
        "startup_files": (),
    }

    def __init__(self, reference: str):
        """Create an Image from a registry reference or local tar path."""
        if not isinstance(reference, str) or not reference.strip():
            raise InvalidError("An image reference must be a non-empty string.")
        object.__setattr__(self, "_reference", reference.strip())
        for name, default in self._FIELDS.items():
            object.__setattr__(self, f"_{name}", default)

    # -- internals ---------------------------------------------------------

    @classmethod
    def _of(cls, reference: str, **overrides) -> "Image":
        image = cls.__new__(cls)
        object.__setattr__(image, "_reference", reference)
        for name, default in cls._FIELDS.items():
            object.__setattr__(image, f"_{name}", overrides.get(name, default))
        return image

    def _evolve(self, **changes) -> "Image":
        """Return a copy with the named attributes replaced."""
        reference = changes.pop("reference", self._reference)
        current = {name: getattr(self, f"_{name}") for name in self._FIELDS}
        current.update(changes)
        return Image._of(reference, **current)

    def _force_token_for(self, force_build: bool) -> Optional[str]:
        """The force token a new build step carries."""
        if self._force_token is not None:
            return self._force_token
        if force_build:
            return uuid.uuid4().hex
        if _force_requested(False):
            return _PROCESS_FORCE_TOKEN
        return None

    def _run_context(
        self, force_build: bool, env: Optional[Dict[str, Optional[str]]]
    ) -> "_build.RunContext":
        return _build.RunContext(
            env=env_pairs(self._env),
            build_env=env_pairs(_clean_env(env)),
            workdir=self._workdir,
            shell=self._build_shell,
            force_token=self._force_token_for(force_build),
        )

    def _add_steps(self, *specs, force_token: Optional[str]) -> "Image":
        """Append build steps; the shared tail of every build-step method."""
        if self._startup_files:
            raise InvalidError(_BUILD_AFTER_LOCAL_FILES)
        return self._evolve(
            steps=self._steps + tuple(specs),
            force_token=force_token,
        )

    def _build_step(
        self,
        method: str,
        build,
        *,
        force_build: bool,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        volumes: Optional[Dict[Any, Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Validate a build-step call and append what ``build(context)`` returns."""
        _reject(method, secrets=secrets, volumes=volumes, gpu=gpu)
        if self._startup_files:
            raise InvalidError(_BUILD_AFTER_LOCAL_FILES)
        context = self._run_context(force_build, env)
        return self._add_steps(*build(context), force_token=context.force_token)

    # -- identity ----------------------------------------------------------

    @property
    def reference(self) -> str:
        """The name this image resolves to."""
        return self._reference

    def __repr__(self) -> str:
        return f"Image({self._reference!r})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, Image):
            return NotImplemented
        return self._state() == other._state()

    def __hash__(self) -> int:
        return hash(self._state())

    def _state(self):
        # Force tokens are directives, not content: an image forced in one
        # program run equals the same definition unforced.
        return (
            self._reference,
            self._base_force_token is not None,
            self._steps,
            tuple(sorted(self._env.items())),
            self._workdir,
            self._shell,
            self._build_shell,
            self._cmd,
            self._entrypoint,
            self._startup_files,
        )

    # -- constructors ------------------------------------------------------

    @staticmethod
    def from_registry(
        tag: str,
        secret: Optional[Any] = None,
        *,
        setup_dockerfile_commands: Sequence[str] = (),
        force_build: bool = False,
        add_python: Optional[str] = None,
        # Named rather than swallowed: this is the spelling Modal's own docs
        # use, and it must fail as loudly as the singular form.
        secrets: Optional[Sequence[Any]] = None,
    ) -> "Image":
        """Start from a public registry reference such as ``"python:3.13-slim"``.

        Args:
            tag: Image reference to pull.
            secret: Unsupported; private registries need credentials.
            setup_dockerfile_commands: Dockerfile lines to run before anything
                else. Unsupported; Dockerfiles are not parsed yet.
            force_build: Drop the node's cached copy and pull the tag again,
                once per Image object on each node, and rebuild every step
                added on top.
            add_python: Unsupported; there is no prebuilt Python layer.
            secrets: Unsupported; the plural spelling of ``secret``.

        Returns:
            An :class:`Image` for that reference.

        Raises:
            NotImplementedError: A Modal-only argument was given.
        """
        _reject("from_registry", secret=secret, secrets=secrets)
        if setup_dockerfile_commands:
            raise _unsupported(
                "from_registry(setup_dockerfile_commands=...)",
                "Dockerfile syntax is not parsed",
                "Start from an image that already carries what you need.",
            )
        if add_python:
            raise _unsupported(
                "from_registry(add_python=...)",
                "there is no prebuilt Python layer to graft on",
                'Use a Python base image such as "python:3.13-slim".',
            )
        return Image(tag)._with_base_forced(force_build)

    def _with_base_forced(self, force_build: bool) -> "Image":
        """Mark the base for a re-pull, once per token, and force what follows."""
        if not _force_requested(force_build):
            return self
        token = uuid.uuid4().hex if force_build else _PROCESS_FORCE_TOKEN
        return self._evolve(base_force_token=token, force_token=token)

    @staticmethod
    def debian_slim(
        python_version: Optional[str] = None, force_build: bool = False
    ) -> "Image":
        """A slim Debian image with Python and a build toolchain, as on Modal.

        Builds ``python:<version>-slim-bookworm`` plus ``gcc gfortran
        build-essential`` and upgraded ``pip wheel uv`` -- Modal's
        definition at image builder version 2025.06 -- so a later
        ``pip_install`` of a package without a wheel compiles here as it
        does there. The main process defaults to ``sleep 172800``, as on
        Modal.

        Args:
            python_version: ``"3.12"`` or ``"3.12.10"``; None matches the
                local interpreter's major.minor.
            force_build: Rebuild instead of using the node's cached copy.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: The Python version is malformed or unsupported.
        """
        if isinstance(python_version, float):
            raise TypeError(
                "The `python_version` argument should be a string, not a float."
            )
        full_version = _build.dockerhub_python_version(
            python_version, caller_name="Image.debian_slim"
        )
        base = Image(f"python:{full_version}-slim-{_build.DEBIAN_CODENAME}")
        base = base._with_base_forced(force_build)
        image = base._build_step(
            "debian_slim",
            lambda ctx: [
                ctx.run(
                    "apt-get update",
                    "apt-get install -y gcc gfortran build-essential",
                    f"pip install --upgrade {_build.PACKAGE_TOOLS}",
                    "echo 'debconf debconf/frontend select Noninteractive' | "
                    "debconf-set-selections",
                )
            ],
            force_build=force_build,
        )
        return image._evolve(cmd=_build.SLEEP_CMD)

    @staticmethod
    def from_scratch(force_build: bool = False) -> "Image":
        """Unsupported: an empty root filesystem cannot start."""
        raise _unsupported(
            "Image.from_scratch()",
            "the sandbox container process is 'sleep infinity', which an "
            "empty root filesystem has no binary for",
            "Start from a minimal published image such as busybox:latest.",
        )

    @staticmethod
    def from_dockerfile(path, **kwargs) -> "Image":
        """Unsupported: Dockerfiles are not parsed."""
        raise _unsupported(
            "Image.from_dockerfile()",
            "Dockerfile syntax is not parsed",
            "Start from a published image with Image.from_registry().",
        )

    @staticmethod
    def from_aws_ecr(tag: str, secret: Optional[Any] = None, **kwargs) -> "Image":
        """Unsupported: AWS credential exchange is unavailable."""
        raise _unsupported(
            "Image.from_aws_ecr()",
            "the image client authenticates only to public registries",
        )

    @staticmethod
    def from_gcp_artifact_registry(
        tag: str, secret: Optional[Any] = None, **kwargs
    ) -> "Image":
        """Unsupported: GCP credential exchange is unavailable."""
        raise _unsupported(
            "Image.from_gcp_artifact_registry()",
            "the image client authenticates only to public registries",
        )

    @staticmethod
    def micromamba(
        python_version: Optional[str] = None, force_build: bool = False
    ) -> "Image":
        """A micromamba image with Python from conda-forge, as on Modal.

        Starts from ``mambaorg/micromamba:2.1.1-debian12-slim`` and installs
        ``python=<version> pip``. Later build steps run through micromamba's
        own shell, so they see the activated base environment.

        Args:
            python_version: ``"3.12"``; None matches the local interpreter.
            force_build: Rebuild instead of using the node's cached copy.

        Returns:
            A new :class:`Image`.
        """
        version = _build.validate_python_version(
            python_version, allow_free_threading=False, caller_name="Image.micromamba"
        )
        base = Image(f"mambaorg/micromamba:{_build.MICROMAMBA_TAG}")
        base = base._with_base_forced(force_build)
        base = base.env({"MAMBA_DOCKERFILE_ACTIVATE": "1"})._evolve(
            build_shell=("/usr/local/bin/_dockerfile_shell.sh",)
        )
        image = base._build_step(
            "micromamba",
            lambda ctx: [
                ctx.run(
                    f"micromamba install -n base -y python={version} pip -c conda-forge"
                )
            ],
            force_build=force_build,
        )
        return image._evolve(cmd=_build.SLEEP_CMD)

    @classmethod
    def from_id(cls, image_id: str, client: Optional[Any] = None) -> "Image":
        """Unsupported: images are not registered by id."""
        raise _unsupported(
            "Image.from_id()", "there is no image registry behind this layer"
        )

    @staticmethod
    def from_name(name: str, **kwargs) -> "Image":
        """Unsupported: images are not registered by name."""
        raise _unsupported(
            "Image.from_name()", "there is no image registry behind this layer"
        )

    # -- runtime metadata: no build ---------------------------------------

    def env(self, vars: Dict[str, str]) -> "Image":
        """Set environment variables for the image.

        Applied as sandbox configuration rather than a build layer, which is
        why this works with no image build behind it.

        Args:
            vars: Variables to set. Later calls win on conflicting keys.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: ``vars`` is not a dict of strings.
        """
        if not isinstance(vars, dict):
            raise InvalidError(f"vars must be a dict, not {type(vars).__name__}")
        for key, value in vars.items():
            # A non-string would otherwise reach SandboxConfig.env and only
            # fail much later, when the OCI spec is written.
            if not isinstance(key, str) or not isinstance(value, (str, type(None))):
                raise InvalidError("Image ENV variables must be strings.")
        # Modal's Image.env() rejects a None value outright; the
        # None-means-unset convention belongs to the build-step `env=`
        # parameters, not here. Accepting it is a deliberate widening -- no
        # Modal program breaks from it -- but it has to remove the key rather
        # than merely be dropped, or an earlier .env() would stay in place,
        # which is the opposite of what was asked for.
        merged = {**self._env, **_clean_env(vars)}
        for key, value in vars.items():
            if value is None:
                merged.pop(key, None)
        return self._evolve(env=merged)

    def workdir(self, path: Union[str, PurePosixPath]) -> "Image":
        """Set the working directory for commands run in the image.

        Args:
            path: Absolute path inside the image.

        Returns:
            A new :class:`Image`.
        """
        path = str(path)
        if not path.startswith("/"):
            raise InvalidError("workdir must be an absolute path.")
        return self._evolve(workdir=path)

    def shell(self, shell_commands: List[str]) -> "Image":
        """Set the shell that later build steps run their commands with.

        Like a Dockerfile ``SHELL``: each later ``run_commands`` (and
        installer) command runs as ``[*shell_commands, command]``, so the
        argv normally ends in ``-c``, as in ``["/bin/bash", "-o", "pipefail",
        "-c"]``. Build steps added before this keep the shell they had.

        The sandbox's own string-command shell is set to the first element,
        but nothing at runtime reaches it today: every command sent from this
        layer is an argv list.

        Args:
            shell_commands: The argv prefix, for example ``["/bin/sh", "-c"]``.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: ``shell_commands`` is not a non-empty list of strings.
        """
        argv = _argv_tuple(shell_commands, "shell")
        if not argv:
            raise InvalidError("Image.shell() needs at least the interpreter.")
        return self._evolve(shell=argv[0], build_shell=argv)

    def cmd(self, cmd: List[str]) -> "Image":
        """Set the default command for the sandbox's main process.

        Arguments passed to ``Sandbox.create()`` override this. An empty list
        clears the base image's CMD, as on Modal.

        Args:
            cmd: Command and arguments.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: ``cmd`` is not a list of strings.
        """
        return self._evolve(cmd=_argv_tuple(cmd, "cmd"))

    def entrypoint(self, entrypoint_commands: List[str]) -> "Image":
        """Set a prefix applied to the sandbox's main process.

        An empty list clears the base image's ENTRYPOINT, as on Modal, which is
        how a base image's own startup script is skipped. Setting one keeps the
        base image's CMD: measured, Modal does not apply Docker's rule that a
        new ENTRYPOINT resets an inherited CMD.

        Args:
            entrypoint_commands: Command and arguments to prefix with.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: ``entrypoint_commands`` is not a list of strings.
        """
        return self._evolve(entrypoint=_argv_tuple(entrypoint_commands, "entrypoint"))

    def pipe(self, func: Callable[..., "Image"], *args, **kwargs) -> "Image":
        """Apply a function to this image and return its result.

        Args:
            func: Callable taking the image plus any extra arguments.
            *args: Extra positional arguments for ``func``.
            **kwargs: Extra keyword arguments for ``func``.

        Returns:
            Whatever ``func`` returns, normally a new :class:`Image`.
        """
        return func(self, *args, **kwargs)

    @contextlib.contextmanager
    def imports(self):
        """Swallow ImportErrors for modules that exist only in the image.

        Modal's contract: the block's imports are expected to fail on the
        client, because the packages were installed into the image, not the
        caller's environment. Only code that actually runs inside the sandbox
        sees them.

        Yielding without catching would make ported code raise
        ``ModuleNotFoundError`` at import time -- the exact failure this is
        supposed to absorb. The miss is logged once per module so a genuine
        typo is still visible.
        """
        try:
            yield
        except ImportError as err:
            name = getattr(err, "name", None) or "a module"
            if name not in _REPORTED_DEFERRED_IMPORTS:
                _REPORTED_DEFERRED_IMPORTS.add(name)
                logger.info(
                    "Ignoring failed import of %r inside Image.imports(); it is "
                    "expected to resolve inside the sandbox, not here.",
                    name,
                )

    # -- local files -------------------------------------------------------

    def add_local_file(
        self,
        local_path: Union[str, os.PathLike],
        remote_path: str,
        *,
        copy: bool = False,
    ) -> "Image":
        """Add a local file to the image.

        By default the file is pushed once the sandbox is running, which
        needs no build; such additions must come after every build step.
        ``copy=True`` bakes it into the image as a cached layer instead, so
        build steps may follow; the layer is rebuilt when the file's content
        or mode changes.

        Args:
            local_path: Path to the file on the local machine.
            remote_path: Absolute destination path inside the image.
            copy: Copy the file into the image as a build step.

        Returns:
            A new :class:`Image`.
        """
        if not PurePosixPath(remote_path).is_absolute():
            raise InvalidError(
                "image.add_local_file() currently only supports absolute "
                "remote_path values"
            )
        if remote_path.endswith("/"):
            remote_path += Path(local_path).name
        if copy:
            return self._add_copy(
                ((str(local_path), remote_path),), f"COPY {local_path} {remote_path}"
            )
        return self._evolve(
            startup_files=self._startup_files
            + (StartupFile(str(local_path), remote_path),)
        )

    def add_local_dir(
        self,
        local_path: Union[str, os.PathLike],
        remote_path: str,
        *,
        copy: bool = False,
        ignore: Union[Sequence[str], Callable[[Path], bool]] = (),
    ) -> "Image":
        """Add a local directory tree to the image.

        Args:
            local_path: Directory on the local machine.
            remote_path: Absolute destination directory inside the image.
            copy: Copy the files into the image as a build step; see
                :meth:`add_local_file`.
            ignore: Glob patterns, or a predicate taking a relative
                :class:`~pathlib.Path` and returning True to skip it.

        Returns:
            A new :class:`Image`.
        """
        if not PurePosixPath(remote_path).is_absolute():
            raise InvalidError(
                "image.add_local_dir() currently only supports absolute "
                "remote_path values"
            )
        root = Path(local_path)
        if not root.is_dir():
            raise NotADirectoryError(f"'{local_path}' is not a directory")

        base = PurePosixPath(remote_path)
        # Collected, then added in one _evolve. Going through add_local_file
        # per entry rebuilt the whole startup_files tuple and a whole Image
        # each time, which is quadratic in the size of the tree -- and
        # add_local_dir is exactly the call that hands it a large one.
        added = [
            StartupFile(str(entry), str(base / entry.relative_to(root).as_posix()))
            for entry in sorted(p for p in root.rglob("*") if p.is_file())
            if not _matches_ignore(entry.relative_to(root).as_posix(), ignore)
        ]
        if not added:
            return self
        if copy:
            return self._add_copy(
                tuple((item.local_path, item.remote_path) for item in added),
                f"COPY {local_path} {remote_path} ({len(added)} files)",
            )
        return self._evolve(startup_files=self._startup_files + tuple(added))

    def _add_copy(self, files, description: str) -> "Image":
        token = self._force_token_for(False)
        return self._add_steps(
            _build.CopySpec(files, description, token), force_token=token
        )

    def add_local_python_source(
        self,
        *modules: str,
        copy: bool = False,
        ignore: Union[Sequence[str], Callable[[Path], bool]] = _non_python_files,
    ) -> "Image":
        """Add local Python modules or packages to the image.

        Each module is located on the local machine and copied to a path that
        keeps it importable, under ``/root``.

        Only ``.py`` files are included by default, matching Modal; dot-prefixed
        entries and ``__pycache__`` are skipped. Pass ``ignore`` to widen that.

        Args:
            *modules: Importable module or package names.
            copy: Copy the sources into the image as a build step.
            ignore: Glob patterns or a predicate for files to skip.

        Returns:
            A new :class:`Image`.

        Raises:
            ModuleNotFoundError: A named module could not be located.
        """
        image = self
        for module in _flatten_str_args("add_local_python_source", "modules", modules):
            spec = importlib.util.find_spec(module)
            if spec is None or (
                spec.origin is None and not spec.submodule_search_locations
            ):
                raise ModuleNotFoundError(f"Could not locate module '{module}'")
            if spec.submodule_search_locations:
                source = Path(list(spec.submodule_search_locations)[0])
                image = image.add_local_dir(
                    source, f"/root/{module.split('.')[-1]}", copy=copy, ignore=ignore
                )
            else:
                source = Path(spec.origin)
                image = image.add_local_file(source, f"/root/{source.name}", copy=copy)
        return image

    # -- build steps -------------------------------------------------------
    #
    # Each becomes one or more cached layers built on the node that runs the
    # sandbox. The commands are Modal's own (image builder version 2025.06).
    # `env=` reaches the build only, as Modal passes it as a build Secret.
    # `secrets`, `volumes` and `gpu` stay unsupported: there are no Modal
    # Secrets, and build sandboxes take no volumes or GPUs.

    def run_commands(
        self,
        *commands: Union[str, List[str]],
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        volumes: Optional[Dict[Any, Any]] = None,
        gpu: Optional[str] = None,
        force_build: bool = False,
    ) -> "Image":
        """Run shell commands and keep what they change, as one layer.

        Each command runs as ``[*shell, command]`` (``/bin/sh -c`` unless
        :meth:`shell` changed it), as root, in the image's ``workdir``, with
        the image's environment plus ``env``; the first that exits non-zero
        fails the build with :class:`~ray.experimental.sandbox.modal.exception.ImageBuildError`.

        Args:
            *commands: Shell commands, or lists of them.
            env: Variables for these commands only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            volumes: Unsupported.
            gpu: Unsupported.
            force_build: Rebuild this layer and every later one, once per
                Image object on each node.

        Returns:
            A new :class:`Image`, or this one if no commands were given.
        """
        cmds = _flatten_str_args("run_commands", "commands", commands)
        if not cmds:
            return self
        return self._build_step(
            "run_commands",
            lambda ctx: [ctx.run(*cmds)],
            force_build=force_build,
            env=env,
            secrets=secrets,
            volumes=volumes,
            gpu=gpu,
        )

    def pip_install(
        self,
        *packages: Union[str, List[str]],
        find_links: Optional[str] = None,
        index_url: Optional[str] = None,
        extra_index_url: Optional[str] = None,
        pre: bool = False,
        extra_options: str = "",
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install Python packages with ``python -m pip install``.

        Args:
            *packages: Requirement specifiers, or lists of them.
            find_links: Passed as ``--find-links``.
            index_url: Passed as ``--index-url``.
            extra_index_url: Passed as ``--extra-index-url``.
            pre: Pass ``--pre``.
            extra_options: Appended to the command as is.
            force_build: As for :meth:`run_commands`.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`, or this one if no packages were given.
        """
        pkgs = _flatten_str_args("pip_install", "packages", packages)
        if not pkgs:
            return self
        _build.check_packages(pkgs, "pip_install")
        extra_args = _build.make_pip_install_args(
            find_links, index_url, extra_index_url, pre, extra_options
        )
        return self._build_step(
            "pip_install",
            lambda ctx: [ctx.run(_build.pip_install_command(pkgs, extra_args))],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def apt_install(
        self,
        *packages: Union[str, List[str]],
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install Debian packages with ``apt-get``.

        Args:
            *packages: Package names, or lists of them.
            force_build: As for :meth:`run_commands`.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`, or this one if no packages were given.
        """
        pkgs = _flatten_str_args("apt_install", "packages", packages)
        if not pkgs:
            return self
        package_args = shlex.join(pkgs)
        return self._build_step(
            "apt_install",
            lambda ctx: [
                ctx.run("apt-get update", f"apt-get install -y {package_args}")
            ],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def pip_install_from_requirements(
        self,
        requirements_txt: Union[str, os.PathLike],
        find_links: Optional[str] = None,
        *,
        index_url: Optional[str] = None,
        extra_index_url: Optional[str] = None,
        pre: bool = False,
        extra_options: str = "",
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install the requirements in a local ``requirements.txt``.

        The file is read when a sandbox is created, and copied into the image
        as ``/.requirements.txt``, as on Modal.

        Args:
            requirements_txt: Path to the local requirements file.
            find_links: Passed as ``-f`` and ``--find-links``.
            index_url: Passed as ``--index-url``.
            extra_index_url: Passed as ``--extra-index-url``.
            pre: Pass ``--pre``.
            extra_options: Appended to the command as is.
            force_build: As for :meth:`run_commands`.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`.
        """
        path = os.path.expanduser(os.fspath(requirements_txt))
        find_links_arg = f" -f {find_links}" if find_links else ""
        extra_args = _build.make_pip_install_args(
            find_links, index_url, extra_index_url, pre, extra_options
        )
        command = (
            f"python -m pip install -r /.requirements.txt{find_links_arg} {extra_args}"
        ).strip()
        return self._build_step(
            "pip_install_from_requirements",
            lambda ctx: [
                _build.CopySpec(
                    ((path, "/.requirements.txt"),),
                    f"COPY {requirements_txt} /.requirements.txt",
                    ctx.force_token,
                ),
                ctx.run(command),
            ],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def pip_install_from_pyproject(
        self,
        pyproject_toml: Union[str, os.PathLike],
        optional_dependencies: Sequence[str] = (),
        *,
        find_links: Optional[str] = None,
        index_url: Optional[str] = None,
        extra_index_url: Optional[str] = None,
        pre: bool = False,
        extra_options: str = "",
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install the ``[project.dependencies]`` of a local ``pyproject.toml``.

        The file is read when a sandbox is created, as Modal reads it when the
        image is built.

        Args:
            pyproject_toml: Path to a PEP 621 ``pyproject.toml``.
            optional_dependencies: Keys of ``[project.optional-dependencies]``
                to install as well.
            find_links: Passed as ``--find-links``.
            index_url: Passed as ``--index-url``.
            extra_index_url: Passed as ``--extra-index-url``.
            pre: Pass ``--pre``.
            extra_options: Appended to the command as is.
            force_build: As for :meth:`run_commands`.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`.
        """
        path = os.fspath(pyproject_toml)
        extra_args = _build.make_pip_install_args(
            find_links, index_url, extra_index_url, pre, extra_options
        )
        return self._build_step(
            "pip_install_from_pyproject",
            lambda ctx: [
                _build.PyprojectSpec(
                    path, tuple(optional_dependencies), extra_args, ctx
                )
            ],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def uv_pip_install(
        self,
        *packages: Union[str, List[str]],
        requirements: Optional[List[str]] = None,
        find_links: Optional[str] = None,
        index_url: Optional[str] = None,
        extra_index_url: Optional[str] = None,
        pre: bool = False,
        extra_options: str = "",
        force_build: bool = False,
        uv_version: Optional[str] = None,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install Python packages with ``uv pip install``.

        ``uv`` is copied from ``ghcr.io/astral-sh/uv`` into ``/.uv/uv``, as on
        Modal, and installs into the image's own Python.

        Args:
            *packages: Requirement specifiers, or lists of them.
            requirements: Local requirements files, copied into the image.
            find_links: Passed as ``--find-links``.
            index_url: Passed as ``--index-url``.
            extra_index_url: Passed as ``--extra-index-url``.
            pre: Pass ``--prerelease allow``.
            extra_options: Appended to the command as is.
            force_build: As for :meth:`run_commands`.
            uv_version: The ``ghcr.io/astral-sh/uv`` tag; ``latest`` by default.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`, or this one if nothing is to be installed.
        """
        pkgs = _flatten_str_args("uv_pip_install", "packages", packages)
        if requirements is None or isinstance(requirements, list):
            requirements = requirements or []
        else:
            raise InvalidError("requirements must be None or a list of strings")
        if not pkgs and not requirements:
            return self
        _build.check_packages(pkgs, "uv_pip_install")
        args = ["--python $(command -v python)", "--compile-bytecode"]
        if find_links:
            args.append(f"--find-links {shlex.quote(find_links)}")
        if index_url:
            args.append(f"--index-url {shlex.quote(index_url)}")
        if extra_index_url:
            args.append(f"--extra-index-url {shlex.quote(extra_index_url)}")
        if pre:
            args.append("--prerelease allow")
        if extra_options:
            args.append(extra_options)
        files = []
        for idx, req in enumerate(requirements):
            dest = f"{_build.UV_ROOT}/{idx}/{os.path.basename(req)}"
            files.append((os.path.expanduser(req), dest))
        if files:
            args.append(" ".join(f"--requirements {dest}" for _, dest in files))
        args.extend(shlex.quote(p) for p in sorted(pkgs))
        command = f"{_build.UV_ROOT}/uv pip install {' '.join(args)}"

        def build(ctx):
            steps = [_build.uv_copy_step(uv_version, ctx.force_token)]
            if files:
                steps.append(
                    _build.CopySpec(
                        tuple(files), "COPY uv requirements", ctx.force_token
                    )
                )
            steps.append(ctx.run(command))
            return steps

        return self._build_step(
            "uv_pip_install",
            build,
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def uv_sync(
        self,
        uv_project_dir: str = "./",
        *,
        force_build: bool = False,
        groups: Optional[List[str]] = None,
        extras: Optional[List[str]] = None,
        frozen: bool = True,
        extra_options: str = "",
        uv_version: Optional[str] = None,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install a local uv project's dependencies with ``uv sync``.

        Its ``pyproject.toml`` (and ``uv.lock``, if present) are copied to
        ``/.uv`` and synced into ``/.uv/.venv``, which is put first on the
        image's ``PATH``, as on Modal. They are read when a sandbox is
        created.

        Args:
            uv_project_dir: Directory holding ``pyproject.toml``.
            force_build: As for :meth:`run_commands`.
            groups: Dependency groups to install.
            extras: Optional-dependency extras to install.
            frozen: With a ``uv.lock``, install exactly what it pins.
            extra_options: Appended to the command as is.
            uv_version: The ``ghcr.io/astral-sh/uv`` tag; ``latest`` by default.
            env: Variables for the sync only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`.
        """

        def _normalize(items, name) -> Tuple[str, ...]:
            if items is None:
                return ()
            if isinstance(items, list):
                return tuple(items)
            raise InvalidError(f"{name} must be None or a list of strings")

        groups_ = _normalize(groups, "groups")
        extras_ = _normalize(extras, "extras")
        return self._build_step(
            "uv_sync",
            lambda ctx: [
                _build.UvSyncSpec(
                    uv_project_dir,
                    groups_,
                    extras_,
                    frozen,
                    extra_options,
                    uv_version,
                    ctx,
                )
            ],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def poetry_install_from_file(
        self,
        poetry_pyproject_toml: str,
        poetry_lockfile: Optional[str] = None,
        *,
        ignore_lockfile: bool = False,
        force_build: bool = False,
        with_: List[str] = [],  # noqa: B006 -- Modal's signature
        without: List[str] = [],  # noqa: B006
        only: List[str] = [],  # noqa: B006
        poetry_version: Optional[str] = "latest",
        old_installer: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install a local Poetry project's dependencies.

        Poetry is pip-installed, the project files are copied to
        ``/tmp/poetry``, and ``poetry install --no-root`` runs there into the
        image's Python, as on Modal. The files are read when a sandbox is
        created.

        Args:
            poetry_pyproject_toml: Path to the project's ``pyproject.toml``.
            poetry_lockfile: Path to ``poetry.lock``; next to the pyproject by
                default.
            ignore_lockfile: Install without a lockfile.
            force_build: As for :meth:`run_commands`.
            with_: Optional groups to include.
            without: Groups to exclude.
            only: Install only these groups.
            poetry_version: Poetry to install; ``"latest"``, a version, or
                None to use the one already in the image.
            old_installer: Use Poetry's old installer.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`.
        """
        return self._build_step(
            "poetry_install_from_file",
            lambda ctx: [
                _build.PoetrySpec(
                    poetry_pyproject_toml,
                    poetry_lockfile,
                    ignore_lockfile,
                    tuple(with_),
                    tuple(without),
                    tuple(only),
                    poetry_version,
                    old_installer,
                    ctx,
                )
            ],
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    def micromamba_install(
        self,
        *packages: Union[str, List[str]],
        spec_file: Optional[str] = None,
        channels: List[str] = [],  # noqa: B006 -- Modal's signature
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Install packages with ``micromamba install`` (see :meth:`micromamba`).

        Args:
            *packages: Package specifiers, or lists of them.
            spec_file: A local environment file, copied into the image.
            channels: Channels to install from, in order.
            force_build: As for :meth:`run_commands`.
            env: Variables for the install only. Kept out of the image, but
                not out of what the step writes to files or prints.
            secrets: Unsupported.
            gpu: Unsupported.

        Returns:
            A new :class:`Image`, or this one if nothing is to be installed.
        """
        pkgs = _flatten_str_args("micromamba_install", "packages", packages)
        if not pkgs and spec_file is None:
            return self
        package_args = shlex.join(pkgs)
        channel_args = "".join(f" -c {channel}" for channel in channels)
        space = " " if package_args else ""
        remote_spec = "" if spec_file is None else f"/{os.path.basename(spec_file)}"
        file_arg = "" if spec_file is None else f"{space}-f {remote_spec} -n base"
        command = f"micromamba install {package_args}{file_arg}{channel_args} --yes"

        def build(ctx):
            steps = []
            if spec_file is not None:
                steps.append(
                    _build.CopySpec(
                        ((os.path.expanduser(spec_file), remote_spec),),
                        f"COPY {spec_file} {remote_spec}",
                        ctx.force_token,
                    )
                )
            steps.append(ctx.run(command))
            return steps

        return self._build_step(
            "micromamba_install",
            build,
            force_build=force_build,
            env=env,
            secrets=secrets,
            gpu=gpu,
        )

    # -- unsupported build steps ------------------------------------------

    def dockerfile_commands(self, *args, **kwargs) -> "Image":
        """Unsupported: Dockerfile syntax is not parsed yet."""
        raise _unsupported(
            "Image.dockerfile_commands()",
            "Dockerfile syntax is not parsed yet",
            "Use .run_commands(), .env(), .workdir(), and "
            ".add_local_file(copy=True).",
        )

    def run_function(self, raw_f, **kwargs) -> "Image":
        """Unsupported: functions cannot be executed during a build yet."""
        raise _unsupported(
            "Image.run_function()",
            "running a Python function during a build is not implemented yet",
            "Write a script into the image and invoke it with .run_commands().",
        )

    def pip_install_private_repos(self, *args, **kwargs) -> "Image":
        """Unsupported: private repositories need Modal Secrets."""
        raise _unsupported(
            "Image.pip_install_private_repos()",
            "Modal Secrets have no equivalent",
        )

    # -- unsupported control plane ----------------------------------------

    def build(self, app: Optional[Any] = None) -> "Image":
        """Unsupported: there is no server-side build to trigger."""
        raise _unsupported(
            "Image.build()",
            "images are materialized on the node that runs the sandbox",
            "Pass the image to Sandbox.create(); its base is pulled on the "
            "node that runs the sandbox.",
        )

    def publish(self, name: str, **kwargs) -> "Image":
        """Unsupported: there is no image registry to publish to."""
        raise _unsupported(
            "Image.publish()", "there is no image registry behind this layer"
        )

    def hydrate(self, client: Optional[Any] = None) -> "Image":
        """Unsupported: an Image is a local value with nothing to hydrate."""
        raise _unsupported(
            "Image.hydrate()", "an Image is a local value, not a server handle"
        )

    @property
    def logs(self):
        """Unsupported: there are no image builds to log."""
        raise _unsupported(
            "Image.logs",
            "images are built on the node that runs each sandbox; their build "
            "output goes to that sandbox's actor log",
        )


def resolve_image(image: Union["Image", str, None]) -> Optional[ResolvedImage]:
    """Split an Image into the pieces ``Sandbox.create()`` needs.

    Args:
        image: An :class:`Image`, a bare reference string, or None.

    Returns:
        A :class:`ResolvedImage`, or None if ``image`` is None.

    Raises:
        TypeError: ``image`` is neither a str nor an Image.
    """
    if image is None:
        return None
    if isinstance(image, str):
        image = Image(image)
    if not isinstance(image, Image):
        raise TypeError(f"image must be a str or Image, not {type(image).__name__}")

    return ResolvedImage(
        reference=image.reference,
        # A cached tag stays pinned on each node, as on Modal, so this is the
        # only way to pick up a tag that has since moved.
        force_token=image._base_force_token,
        build_specs=image._steps,
        env=dict(image._env),
        workdir=image._workdir,
        shell=image._shell,
        argv_prefix=image._entrypoint,
        default_cmd=image._cmd,
        startup_files=image._startup_files,
    )
