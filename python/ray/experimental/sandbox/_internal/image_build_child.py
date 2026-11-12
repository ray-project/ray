"""Build one image's root filesystem in a child interpreter, for
``image_utils._build_rootfs_image_in_a_child``.

Run this file directly with Python -S. It loads image_utils without importing
Ray: empty packages stand in for ``ray``, ``ray.util`` and the sandbox's
packages, so only image_utils, the few modules it imports and the standard
library load, from the directories the parent loaded them from.
"""

import json
import sys
import types
from typing import Dict, List


def _load_image_utils(packages: Dict[str, List[str]]) -> types.ModuleType:
    """Import image_utils with an empty stand-in for each of ``packages``,
    whose paths are the directories the parent loaded them from."""
    for name, path in packages.items():
        package = types.ModuleType(name)
        package.__path__ = path
        sys.modules[name] = package
    from ray.experimental.sandbox._internal import image_utils

    return image_utils


def main() -> None:
    packages, args = json.loads(sys.argv[1]), json.loads(sys.argv[2])
    image_utils = _load_image_utils(packages)
    try:
        image_utils._build_rootfs_image(*args)
    except BaseException as err:
        # MemoryError and KeyboardInterrupt, among others, have no message.
        message = str(err) or type(err).__name__
        print(image_utils._CHILD_ERROR + json.dumps(message), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
