"""Helpers for pinning release-test images via Buildkite meta-data.

Shape comparison (see `shape_of`) is how user-supplied override URIs are
matched against the set of tests scheduled in a release-test build.
"""

import re
from typing import Dict, List

from ray_release.exception import ReleaseTestConfigError
from ray_release.test import BUILD_ID_PLACEHOLDER, Test
from ray_release.util import ANYSCALE_RAY_IMAGE_PREFIX

_RELEASED_PREFIX = f"{ANYSCALE_RAY_IMAGE_PREFIX}:"


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


def _uses_released_image(test: Test) -> bool:
    return test.get_anyscale_byod_image().startswith(_RELEASED_PREFIX)


def match_uris_to_tests(
    tests: List[Test],
    uri_list: List[str],
) -> Dict[str, str]:
    """Match each non-released test to exactly one URI in `uri_list` by shape.

    Released-image tests are excluded from the returned map; they fall back
    to `get_anyscale_byod_image()` at test-run time.
    """
    uris_by_shape: Dict[str, List[str]] = {}
    for uri in uri_list:
        uris_by_shape.setdefault(shape_of(uri), []).append(uri)

    failures: List[str] = []
    result: Dict[str, str] = {}
    for test in tests:
        if _uses_released_image(test):
            continue
        target_shape = test.get_anyscale_byod_image_shape()
        matches = uris_by_shape.get(target_shape, [])
        if len(matches) == 1:
            if test["name"] in result:
                failures.append(
                    f"  - {test['name']}: appears more than once in the test "
                    f"list; image pinning requires unique test names"
                )
                continue
            result[test["name"]] = matches[0]
        elif len(matches) == 0:
            failures.append(
                f"  - {test['name']}: no URI matched expected shape "
                f"{target_shape!r}"
            )
        else:
            failures.append(
                f"  - {test['name']}: {len(matches)} URIs matched shape "
                f"{target_shape!r} (matching: {', '.join(matches)})"
            )
    if failures:
        provided = ", ".join(uri_list) if uri_list else "(none)"
        raise ReleaseTestConfigError(
            f"Cannot resolve image URIs for {len(failures)} test(s):\n"
            + "\n".join(failures)
            + f"\n(provided URIs: {provided})"
        )
    return result
