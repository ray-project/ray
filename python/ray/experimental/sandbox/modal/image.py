"""A Modal-compatible container image.

An ``Image`` is a value, not a handle: immutable, and every builder returns a
new one. It carries a base image reference plus *runtime metadata* -- ``env``,
``workdir``, ``shell``, ``cmd``, ``entrypoint``, and ``copy=False`` file
additions -- which is applied when the sandbox is created, so ``Image.env(...)``
is free. Modal spends a build layer on those only because its builder is
Dockerfile-shaped.

**Image building is not implemented in this version.** Members that would
mutate a filesystem -- ``run_commands``, the package installers,
``debian_slim`` -- raise ``NotSupportedError`` naming the nearest alternative,
as do the members that depend on Modal's hosted control plane (image registry,
Secrets, cloud registry auth, server-side builds). Start from a published image
with :meth:`Image.from_registry` instead.
"""

import contextlib
import fnmatch
import importlib.util
import logging
import os
from pathlib import Path, PurePath, PurePosixPath
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

from ray.experimental.sandbox.modal._resolved_image import ResolvedImage, StartupFile
from ray.experimental.sandbox.modal.exception import InvalidError, NotSupportedError
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)

# Modules already reported as unresolvable inside Image.imports(), so a loop
# over many sandboxes does not repeat the same line.
_REPORTED_DEFERRED_IMPORTS = set()


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


def _reject_copy(copy: bool) -> None:
    """Refuse ``copy=True``, which would need an image build."""
    if copy:
        raise _unsupported(
            "add_local_*(copy=True)",
            "baking a file into an image needs a build, which is not "
            "implemented in this version",
            "Use copy=False to push it once the sandbox is running.",
        )


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

    Build one with :meth:`from_registry`, then chain metadata onto it. Every
    method returns a new ``Image``; none mutates the receiver.
    """

    __slots__ = (
        "_reference",
        "_force_pull",
        "_env",
        "_workdir",
        "_shell",
        "_cmd",
        "_entrypoint",
        "_startup_files",
    )

    def __init__(self, reference: str):
        """Create an Image from a registry reference or local tar path."""
        if not isinstance(reference, str) or not reference.strip():
            raise InvalidError("An image reference must be a non-empty string.")
        object.__setattr__(self, "_reference", reference.strip())
        object.__setattr__(self, "_force_pull", False)
        object.__setattr__(self, "_env", {})
        object.__setattr__(self, "_workdir", None)
        object.__setattr__(self, "_shell", None)
        # None until set: `cmd([])` and `entrypoint([])` clear the base
        # image's CMD and ENTRYPOINT on Modal, so an empty argv has to stay
        # distinguishable from one that was never given.
        object.__setattr__(self, "_cmd", None)
        object.__setattr__(self, "_entrypoint", None)
        object.__setattr__(self, "_startup_files", ())

    # -- internals ---------------------------------------------------------

    @classmethod
    def _of(cls, reference: str, **overrides) -> "Image":
        image = cls.__new__(cls)
        object.__setattr__(image, "_reference", reference)
        object.__setattr__(image, "_force_pull", overrides.get("force_pull", False))
        object.__setattr__(image, "_env", overrides.get("env", {}))
        object.__setattr__(image, "_workdir", overrides.get("workdir"))
        object.__setattr__(image, "_shell", overrides.get("shell"))
        object.__setattr__(image, "_cmd", overrides.get("cmd"))
        object.__setattr__(image, "_entrypoint", overrides.get("entrypoint"))
        object.__setattr__(image, "_startup_files", overrides.get("startup_files", ()))
        return image

    def _evolve(self, **changes) -> "Image":
        """Return a copy with the named attributes replaced."""
        reference = changes.pop("reference", self._reference)
        current = {
            "force_pull": self._force_pull,
            "env": self._env,
            "workdir": self._workdir,
            "shell": self._shell,
            "cmd": self._cmd,
            "entrypoint": self._entrypoint,
            "startup_files": self._startup_files,
        }
        current.update(changes)
        return Image._of(reference, **current)

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
        return (
            self._reference,
            self._force_pull,
            tuple(sorted(self._env.items())),
            self._workdir,
            self._shell,
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
                else. Unsupported; image building is not implemented.
            force_build: Drop the node's cached copy and pull the tag again.
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
        image = Image(tag)
        if force_build:
            # With no build steps to replay, re-pulling the base is what
            # remains of Modal's force_build: it drops the node's cached copy
            # so the next create() fetches the tag again.
            return image._evolve(force_pull=True)
        return image

    @staticmethod
    def debian_slim(
        python_version: Optional[str] = None, force_build: bool = False
    ) -> "Image":
        """Unsupported: this image is built, not pulled.

        Modal's ``debian_slim`` is a slim Python base plus a build toolchain --
        ``gcc gfortran build-essential`` and a ``pip wheel uv`` upgrade -- which
        needs an image build to produce. Returning the bare base instead would
        diverge from Modal invisibly: a later ``pip_install`` of a package with
        no wheel would fail here and succeed there.
        """
        raise _unsupported(
            "Image.debian_slim()",
            "it needs an image build to add its toolchain, and image building "
            "is not implemented in this version",
            'Use Image.from_registry("python:3.13-slim") for the base alone.',
        )

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
        """Unsupported: no micromamba base image is provided."""
        raise _unsupported(
            "Image.micromamba()",
            "no Conda base image is bundled",
            "Start from a published micromamba image with from_registry().",
        )

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
        """Set the interpreter used for string commands.

        Accepted, validated and carried through to the sandbox, but nothing in
        this layer currently reaches it: the backend consults its configured
        shell only for a command given as a *string*, and every command sent
        from here is an argv list -- ``exec()`` and ``Probe`` both require one,
        the filesystem helpers build their own, and so does the main process.
        So this is inert today rather than wrong, and it is kept rather than
        dropped so that ported code neither fails nor has to change if a
        string-command path appears.

        Ray takes a single interpreter path where Modal takes a full argv, so
        only the argv forms that reduce to one map. A trailing ``-c`` is
        dropped rather than rejected: ``["/bin/bash", "-c"]`` is the usual
        Modal spelling, and the backend already appends ``-c`` itself when it
        runs a string command.

        Args:
            shell_commands: ``["/bin/sh"]`` or ``["/bin/sh", "-c"]``.

        Returns:
            A new :class:`Image`.

        Raises:
            InvalidError: ``shell_commands`` is not a list of strings.
            NotImplementedError: The argv does not reduce to one interpreter.
        """
        argv = _argv_tuple(shell_commands, "shell")
        if len(argv) == 2 and argv[1] == "-c":
            argv = argv[:1]
        if len(argv) != 1:
            raise _unsupported(
                "Image.shell() with a multi-element argv",
                "Ray sandboxes take a single interpreter path",
                'Pass ["/bin/sh"] or ["/bin/sh", "-c"].',
            )
        return self._evolve(shell=argv[0])

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

        The file is pushed once the sandbox is running, which needs no build.
        ``copy=True`` would bake it into the image instead and is unsupported,
        since image building is not implemented in this version.

        Args:
            local_path: Path to the file on the local machine.
            remote_path: Absolute destination path inside the image.
            copy: Unsupported; bakes the file in at build time.

        Returns:
            A new :class:`Image`.

        Raises:
            NotImplementedError: ``copy=True`` was given.
        """
        if not PurePosixPath(remote_path).is_absolute():
            raise InvalidError(
                "image.add_local_file() currently only supports absolute "
                "remote_path values"
            )
        if remote_path.endswith("/"):
            remote_path += Path(local_path).name
        _reject_copy(copy)
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
            copy: Unsupported; bakes the files in at build time.
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
        # Up front rather than once per file: an empty tree used to slip past
        # this entirely, because the only check was inside the loop body.
        _reject_copy(copy)

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
        return self._evolve(startup_files=self._startup_files + tuple(added))

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
            copy: Unsupported; bakes the sources in at build time.
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

    # -- build steps: unimplemented in this version ------------------------
    #
    # These carry Modal's full signature rather than *args/**kwargs, so a
    # caller's keyword argument is a NotSupportedError naming the limitation
    # and not a TypeError about an unexpected keyword. Restoring them means
    # filling in a body, not changing a signature.

    def run_commands(
        self,
        *commands: Union[str, List[str]],
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        volumes: Optional[Dict[Any, Any]] = None,
        gpu: Optional[str] = None,
        force_build: bool = False,
    ) -> "Image":
        """Unsupported: running commands needs an image build."""
        raise _unsupported(
            "Image.run_commands()",
            "image building is not implemented in this version",
            "Start from an image that already carries what you need, or run "
            "the commands with Sandbox.exec() once the sandbox is up.",
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
        """Unsupported: installing packages needs an image build."""
        raise _unsupported(
            "Image.pip_install()",
            "image building is not implemented in this version",
            "Start from an image with the packages preinstalled, or pip "
            "install with Sandbox.exec() once the sandbox is up.",
        )

    def apt_install(
        self,
        *packages: Union[str, List[str]],
        force_build: bool = False,
        env: Optional[Dict[str, Optional[str]]] = None,
        secrets: Optional[Sequence[Any]] = None,
        gpu: Optional[str] = None,
    ) -> "Image":
        """Unsupported: installing packages needs an image build."""
        raise _unsupported(
            "Image.apt_install()",
            "image building is not implemented in this version",
            "Start from an image with the packages preinstalled, or apt-get "
            "install with Sandbox.exec() once the sandbox is up.",
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
        """Unsupported: installing packages needs an image build."""
        raise _unsupported(
            "Image.pip_install_from_requirements()",
            "image building is not implemented in this version",
            "Add the file with add_local_file(copy=False) and install it with "
            "Sandbox.exec() once the sandbox is up.",
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
        """Unsupported: installing packages needs an image build."""
        raise _unsupported(
            "Image.pip_install_from_pyproject()",
            "image building is not implemented in this version",
            "Add the file with add_local_file(copy=False) and install it with "
            "Sandbox.exec() once the sandbox is up.",
        )

    # -- unsupported build steps ------------------------------------------

    def dockerfile_commands(self, *args, **kwargs) -> "Image":
        """Unsupported: Dockerfile syntax is not parsed."""
        raise _unsupported(
            "Image.dockerfile_commands()",
            "Dockerfile syntax is not parsed",
            "Use .run_commands(), .env(), .workdir(), and .add_local_file().",
        )

    def run_function(self, raw_f, **kwargs) -> "Image":
        """Unsupported: functions cannot be executed during a build."""
        raise _unsupported(
            "Image.run_function()",
            "there is no mechanism to run a serialized Python function inside "
            "a build container",
            "Write a script into the image and invoke it with .run_commands().",
        )

    def pip_install_private_repos(self, *args, **kwargs) -> "Image":
        """Unsupported: private repositories need Modal Secrets."""
        raise _unsupported(
            "Image.pip_install_private_repos()",
            "Modal Secrets have no equivalent",
        )

    def uv_pip_install(self, *args, **kwargs) -> "Image":
        """Unsupported: uv is not bundled."""
        raise _unsupported(
            "Image.uv_pip_install()",
            "uv is not installed in the build container",
            "Install uv with .run_commands(), then call it the same way.",
        )

    def uv_sync(self, *args, **kwargs) -> "Image":
        """Unsupported: uv is not bundled."""
        raise _unsupported(
            "Image.uv_sync()",
            "uv is not installed in the build container",
            "Install uv with .run_commands(), then call it the same way.",
        )

    def poetry_install_from_file(self, *args, **kwargs) -> "Image":
        """Unsupported: poetry is not bundled."""
        raise _unsupported(
            "Image.poetry_install_from_file()",
            "poetry is not installed in the build container",
            "Install poetry with .run_commands(), then call it the same way.",
        )

    def micromamba_install(self, *args, **kwargs) -> "Image":
        """Unsupported: micromamba is not bundled."""
        raise _unsupported(
            "Image.micromamba_install()",
            "micromamba is not installed in the build container",
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
            "images are pulled, not built, so there is no build output",
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
        force_pull=image._force_pull,
        env=dict(image._env),
        workdir=image._workdir,
        shell=image._shell,
        argv_prefix=image._entrypoint,
        default_cmd=image._cmd,
        startup_files=image._startup_files,
    )
