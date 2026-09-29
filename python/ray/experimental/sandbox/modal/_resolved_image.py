"""What an :class:`Image` reduces to once the sandbox layer needs it.

An Image in this version carries a base reference plus *runtime metadata* --
environment variables, a working directory, a shell, an argv -- and nothing
that mutates a filesystem. All of it lands in
:class:`~ray.experimental.sandbox.config.SandboxConfig` directly, so resolving
an Image is a pure rearrangement of values with no build step behind it.

Image building is not implemented in this version; see the "Divergences from
Modal" section of the package docstring for the members that raise instead.
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
        reference: Registry reference or local tar path to run.
        force_pull: Drop the node's cached copy of ``reference`` and pull it
            again before starting. Modal spells this ``force_build``; with no
            build steps to replay, re-pulling the base is what remains of it.
        env: Environment variables to inject.
        workdir: Working directory for the main process, or None for the
            image's own WORKDIR.
        shell: Interpreter for string commands, or None for the default.
        argv_prefix: ENTRYPOINT to prefix onto the main process argv.
        default_cmd: CMD to run when ``Sandbox.create()`` passes no arguments.
        startup_files: Local files to push once the sandbox exists.
    """

    reference: str
    force_pull: bool = False
    env: Dict[str, str] = field(default_factory=dict)
    workdir: Optional[str] = None
    shell: Optional[str] = None
    argv_prefix: Tuple[str, ...] = ()
    default_cmd: Tuple[str, ...] = ()
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
        """
        config = image_config or {}
        prefix = self.argv_prefix or tuple(config.get("Entrypoint") or ())
        default = self.default_cmd or tuple(config.get("Cmd") or ())

        chosen = list(override) if override else list(default)
        if not chosen and not prefix:
            return []
        return list(prefix) + chosen
