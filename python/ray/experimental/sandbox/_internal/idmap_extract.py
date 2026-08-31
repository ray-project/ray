"""Apply an image's recorded ownership to a freshly extracted rootfs.

Invoked as ``python -m ray.experimental.sandbox._internal.idmap_extract ROOTFS
OWNERSHIP_JSON`` by :func:`ray.experimental.sandbox._internal.image_utils.pull_and_extract_container_image`
through ``nsenter`` into a user namespace mapped with the node's subordinate
ranges; only there can ``lchown`` give files their true owners. The rootfs is
extracted worker-owned first, which is what root inside the sandbox maps to,
so only the entries the image ships with a non-root owner need changing.
"""

import argparse
import json
import os
import stat
import sys
from typing import Dict, Tuple


def apply_ownership(rootfs: str, ownership: Dict[str, Tuple[int, int]]) -> None:
    """Chown ``rootfs`` entries per ``ownership``, restoring the modes chown clears."""
    from ray.experimental.sandbox._internal.image_utils import _lchown_preserving

    for rel, (uid, gid) in ownership.items():
        path = os.path.join(rootfs, rel)
        try:
            st = os.lstat(path)
        except OSError:
            continue
        _lchown_preserving(path, uid, gid)
        if not stat.S_ISLNK(st.st_mode):
            # chown clears setuid/setgid on files; put the mode back.
            try:
                os.chmod(path, stat.S_IMODE(st.st_mode))
            except OSError:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("rootfs", help="extracted rootfs to chown in place")
    parser.add_argument(
        "ownership", help="JSON {path: [uid, gid]} recorded at extraction"
    )
    args = parser.parse_args()

    with open(args.ownership, encoding="utf-8") as f:
        ownership = {p: (int(ids[0]), int(ids[1])) for p, ids in json.load(f).items()}
    apply_ownership(args.rootfs, ownership)
    return 0


if __name__ == "__main__":
    sys.exit(main())
