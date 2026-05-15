"""Helpers for pinning release-test images via Buildkite meta-data.

Shape comparison (see `shape_of`) is how user-supplied override URIs are
matched against the set of tests scheduled in a release-test build.
"""

import json
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


def parse_override(raw_json: str) -> Dict[str, str]:
    """Parse the `release-test-image-override` JSON meta-data into `{test_name: image_uri}`.

    Input is `{image_uri: [test_name, ...]}`. Each test name must appear
    exactly once across all values. Raises `ReleaseTestConfigError` for
    JSON parse failures, type errors, duplicate test names, or empty
    payloads.
    """
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as e:
        raise ReleaseTestConfigError(
            f"Invalid JSON in release-test-image-override: {e}"
        ) from e
    if not isinstance(parsed, dict):
        raise ReleaseTestConfigError(
            f"release-test-image-override must be a JSON object; got "
            f"{type(parsed).__name__}"
        )
    result: Dict[str, str] = {}
    duplicates: Dict[str, List[str]] = {}
    for uri, names in parsed.items():
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            raise ReleaseTestConfigError(
                f"release-test-image-override value for URI {uri!r} must be a "
                f"list of test names; got {names!r}"
            )
        for name in names:
            if name in result:
                # First duplicate captures original; later ones append.
                duplicates.setdefault(name, [result[name]]).append(uri)
            else:
                result[name] = uri
    if duplicates:
        lines = [
            f"  - Test {name!r} is mapped to multiple URIs: {', '.join(uris)}"
            for name, uris in duplicates.items()
        ]
        raise ReleaseTestConfigError(
            "release-test-image-override has duplicate test names:\n" + "\n".join(lines)
        )
    if not result:
        raise ReleaseTestConfigError(
            "release-test-image-override contains no tests to run."
        )
    return result


def resolve_override_tests(
    test_collection: List[Test],
    override_map: Dict[str, str],
) -> List[Test]:
    """Look up each test name in `override_map` against `test_collection`.

    Returns the resolved tests in `override_map` iteration order. Raises
    `ReleaseTestConfigError` listing every unknown name.
    """
    by_name = {t.get_name(): t for t in test_collection}
    found: List[Test] = []
    missing: List[str] = []
    for name in override_map:
        if name in by_name:
            found.append(by_name[name])
        else:
            missing.append(name)
    if missing:
        raise ReleaseTestConfigError(
            "release-test-image-override names not found in test collection:\n"
            + "\n".join(f"  - {name}" for name in missing)
        )
    return found
