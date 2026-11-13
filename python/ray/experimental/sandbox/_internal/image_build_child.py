"""Build one image's root filesystem in a child interpreter, for
``image_utils._build_rootfs_image_in_a_child``.

Run this file directly with Python -S. It loads image_utils without importing
Ray: empty packages stand in for ``ray``, ``ray.util`` and the sandbox's
packages, so only image_utils, the few modules it imports and the standard
library load, from the directories the parent loaded them from.
"""

import json
import os
import signal
import sys
import types
from typing import Dict, List

# The line a build's error goes back to the parent on, among whatever else
# this process prints.
CHILD_ERROR = "ray-sandbox-image-build-error: "


def _load_image_utils(packages: Dict[str, List[str]]) -> types.ModuleType:
    """Import image_utils with an empty stand-in for each of ``packages``,
    whose paths are the directories the parent loaded them from."""
    for name, path in packages.items():
        package = types.ModuleType(name)
        package.__path__ = path
        sys.modules[name] = package
    from ray.experimental.sandbox._internal import image_utils

    return image_utils


def _die_with_parent(parent: int) -> None:
    """Have the kernel kill this process when the process that started it dies.

    A build would otherwise go on downloading and writing to the image cache
    for a pull that nobody waits for anymore. Linux only: elsewhere the build
    finishes.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes

        ctypes.CDLL(None).prctl(1, signal.SIGKILL)  # PR_SET_PDEATHSIG
    except Exception:
        return
    if os.getppid() != parent:
        sys.exit(1)  # The parent died before the request took effect.


def main() -> None:
    packages, args = json.loads(sys.argv[1]), json.loads(sys.argv[2])
    _die_with_parent(int(sys.argv[3]))
    try:
        # Inside, so that a failed import comes back as an error too.
        _load_image_utils(packages)._build_rootfs_image(*args)
    except BaseException as err:
        # MemoryError and KeyboardInterrupt, among others, have no message.
        message = str(err) or type(err).__name__
        print(CHILD_ERROR + json.dumps(message), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
