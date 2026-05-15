"""Helpers for pinning release-test images via Buildkite meta-data.

Shape comparison (see `shape_of`) is how user-supplied override URIs are
matched against the set of tests scheduled in a release-test build.
"""

import re

from ray_release.test import BUILD_ID_PLACEHOLDER


def shape_of(uri: str) -> str:
    """Return `uri` with the `build_id` portion of its tag replaced by `BUILD_ID_PLACEHOLDER`.

    The `build_id` is everything before the first `py<digits>` segment in the tag.
    """
    if ":" not in uri:
        raise ValueError(f"image URI missing ':<tag>' suffix: {uri!r}")
    repo, tag = uri.rsplit(":", 1)
    if not tag:
        raise ValueError(f"image URI has empty tag: {uri!r}")
    parts = tag.split("-")
    # Python-version segment is always the first `py<N>` in the tag (build_id
    # may contain `-` but never a `py<digit>` segment), so first-match wins.
    for i, part in enumerate(parts):
        if re.fullmatch(r"py\d+", part):
            return f"{repo}:{'-'.join([BUILD_ID_PLACEHOLDER] + parts[i:])}"
    return f"{repo}:{BUILD_ID_PLACEHOLDER}"
