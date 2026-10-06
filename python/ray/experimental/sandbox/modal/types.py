"""Data types returned by the Modal-compatible Sandbox API."""

import enum
from dataclasses import dataclass
from typing import List, Literal, Optional

from ray.util.annotations import PublicAPI

# The runtimes a Sandbox can ask for, as Modal spells them. Only "gvisor" runs
# here; Sandbox.create refuses "vm".
SandboxRuntime = Literal["gvisor", "vm"]


@PublicAPI(stability="alpha")
class FileType(enum.Enum):
    """Type of a filesystem entry."""

    FILE = "file"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


@PublicAPI(stability="alpha")
class FileWatchEventType(enum.Enum):
    """Type of a filesystem watch event."""

    Unknown = "Unknown"
    Access = "Access"
    Create = "Create"
    Modify = "Modify"
    Remove = "Remove"


@PublicAPI(stability="alpha")
@dataclass(frozen=True)
class FileInfo:
    """Metadata for a file or directory entry in a Sandbox.

    Attributes:
        name: Final path component.
        path: Absolute path inside the Sandbox.
        type: Whether the entry is a file, directory, or symlink.
        size: Size in bytes.
        mode: Raw st_mode value.
        permissions: Permission bits as four octal digits, e.g. ``"0644"``.
        owner: Owning user name, or the numeric uid when unresolvable.
        group: Owning group name, or the numeric gid when unresolvable.
        modified_time: Modification time as a Unix timestamp.
        symlink_target: Target of the symlink, or None for other entry types.
    """

    name: str
    path: str
    type: FileType
    size: int
    mode: int
    permissions: str
    owner: str
    group: str
    modified_time: float
    symlink_target: Optional[str]

    def is_file(self) -> bool:
        """Return True if this entry is a regular file."""
        return self.type == FileType.FILE

    def is_dir(self) -> bool:
        """Return True if this entry is a directory."""
        return self.type == FileType.DIRECTORY

    def is_symlink(self) -> bool:
        """Return True if this entry is a symbolic link."""
        return self.type == FileType.SYMLINK


@PublicAPI(stability="alpha")
@dataclass
class FileWatchEvent:
    """A filesystem change event.

    Attributes:
        paths: Absolute path(s) affected by the event.
        type: The kind of change observed.
    """

    paths: List[str]
    type: FileWatchEventType


__all__ = [
    "FileType",
    "FileWatchEventType",
    "FileInfo",
    "FileWatchEvent",
    "SandboxRuntime",
]
