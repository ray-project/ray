from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from ray.data._internal.datasource_v2.interfaces.file_manifest import UnitRun


@dataclass(frozen=True)
class FileChunks:
    """The footer-derived runs of row groups for a single file."""

    path: str
    size: int  # on-disk file size, from the file listing
    row_groups: Tuple[UnitRun, ...]
