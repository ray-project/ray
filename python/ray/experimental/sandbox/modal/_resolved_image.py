"""What an :class:`Image` reduces to once the sandbox layer needs it.

An Image carries a base reference, build steps, and *runtime metadata* --
environment variables, a working directory, a shell, an argv. Resolving it is
a pure rearrangement of values: the build steps stay *specs* that read no
file until ``Sandbox.create()`` turns them into a plan for the node (see
``modal/_image_build.prepare_build_plan``).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class StartupFile:
    """A local file pushed into the sandbox after it starts (``copy=False``).

    Modal's default for ``add_local_*``. The content never enters the image; it
    streams from the client once the sandbox exists.

    Attributes:
        local_path: Path on the machine that created the Image.
        remote_path: Absolute destination path inside the sandbox.
    """

    local_path: str
    remote_path: str


@dataclass
class ResolvedImage:
    """What :func:`resolve_image` hands to ``Sandbox.create()``.

    Splits an Image into the pieces the sandbox layer needs: values for
    :class:`~ray.experimental.sandbox.config.SandboxConfig`, the argv of the
    main process, and files to push after startup.

    Attributes:
        reference: Registry reference or local tar path of the base image.
        force_token: Set when Modal's ``force_build`` was given for the base:
            each node drops its cached copy of ``reference`` and pulls it
            again, once per token, and rebuilds the layers on top.
        build_specs: The Image's build steps, in order, as specs.
        env: Environment variables to inject.
        workdir: Working directory for the main process, or None for the
            image's own WORKDIR.
        shell: Interpreter for string commands, or None for the default.
        argv_prefix: ENTRYPOINT to prefix onto the main process argv, or
            None for the base image's own. Empty clears the base image's.
        default_cmd: CMD to run when ``Sandbox.create()`` passes no
            arguments, or None for the base image's own. Empty clears the
            base image's.
        startup_files: Local files to push once the sandbox exists.
    """

    reference: str
    force_token: Optional[str] = None
    build_specs: Tuple[Any, ...] = ()
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    shell: Optional[str] = None
    argv_prefix: Optional[Tuple[str, ...]] = None
    default_cmd: Optional[Tuple[str, ...]] = None
    startup_files: Tuple[StartupFile, ...] = ()

    def main_argv(
        self,
        override: List[str],
        image_config: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Build the main process argv.

        Modal's precedence: arguments passed to ``Sandbox.create()`` override
        the image's CMD, and the ENTRYPOINT prefixes whichever wins.

        ``image_config`` is the base image's own config, whose ``Cmd`` and
        ``Entrypoint`` sit beneath anything set on the Image. Without it, a
        sandbox created from an application image starts idle -- no main
        process, no output, no exit code -- where Modal would have run the
        image's CMD.

        Each layer falls back to the base image's only when the Image never
        set it: ``entrypoint([])`` and ``cmd([])`` clear the base's, as
        measured on Modal. A sandbox left with nothing to run starts idle
        here; on Modal it fails to start, with exit code 128.
        """
        config = image_config or {}
        prefix = self.argv_prefix
        if prefix is None:
            prefix = tuple(config.get("Entrypoint") or ())
        default = self.default_cmd
        if default is None:
            default = tuple(config.get("Cmd") or ())

        chosen = list(override) if override else list(default)
        if not chosen and not prefix:
            return []
        return list(prefix) + chosen
