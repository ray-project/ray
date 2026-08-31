"""Ownership-preserving tar extraction, run inside a mapped user namespace.

Invoked as ``python -m ray.experimental.sandbox._internal.idmap_extract`` by
:func:`ray.experimental.sandbox._internal.image_utils.ensure_idmapped_rootfs`
via ``nsenter`` into a namespace whose uid/gid maps cover the image's ids —
only there can ``lchown`` give extracted files their true owners. Kept to a
tiny argv surface so the parent can run it with a plain ``subprocess.run``.
"""

import argparse
import os
import shutil
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tar_path", help="tar archive to extract")
    parser.add_argument("dest", help="directory to materialize")
    parser.add_argument(
        "--subdir",
        default=None,
        help=(
            "archive subdirectory that holds the rootfs (e.g. 'rootfs' for "
            "cached image tars); omitted for archives that are a rootfs"
        ),
    )
    args = parser.parse_args()

    from ray.experimental.sandbox._internal.image_utils import extract_tar_layer

    extract_dir = f"{args.dest}.scratch" if args.subdir else args.dest
    os.makedirs(extract_dir, mode=0o755, exist_ok=True)
    with open(args.tar_path, "rb") as f:
        extract_tar_layer(f, extract_dir, preserve_owner=True)
    if args.subdir:
        os.replace(os.path.join(extract_dir, args.subdir), args.dest)
        shutil.rmtree(extract_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
