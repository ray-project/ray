"""Filesystem helpers for Ray sandboxing."""

import os
import shutil


def rmtree(path: str, ignore_errors: bool = False) -> None:
    """Delete ``path`` like ``shutil.rmtree``, even through directories that
    deny their owner write access.

    Image trees keep the image's directory modes, and images like UBI ship
    0555 directories, which keep even their owner from deleting what's in
    them. A sandbox's workdir can hold the same. If a plain rmtree leaves
    anything behind, this grants the owner rwx on every directory and
    retries.

    Args:
        path: The tree to delete.
        ignore_errors: Whether to ignore errors, as with ``shutil.rmtree``.
    """
    # shutil.rmtree reports a missing path or a symlink.
    if not os.path.lexists(path) or os.path.islink(path):
        shutil.rmtree(path, ignore_errors=ignore_errors)
        return

    # Most trees delete cleanly on the first try.
    shutil.rmtree(path, ignore_errors=True)
    if not os.path.lexists(path):
        return

    # Grant the owner rwx on every directory, starting with the top one, so
    # the second rmtree below can remove what they hold.
    try:
        os.chmod(path, os.lstat(path).st_mode | 0o700)
    except OSError:
        pass
    for dirpath, dirnames, _ in os.walk(path):
        # Chmod each subdirectory before os.walk descends into it.
        for d in dirnames:
            subdir = os.path.join(dirpath, d)
            try:
                if not os.path.islink(subdir):
                    os.chmod(subdir, os.lstat(subdir).st_mode | 0o700)
            except OSError:
                pass

    shutil.rmtree(path, ignore_errors=ignore_errors)
