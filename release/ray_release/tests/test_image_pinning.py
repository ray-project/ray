import json

import pytest

from ray_release.bazel import bazel_runfile
from ray_release.buildkite.image_pinning import (
    _uses_released_image,
    match_uris_to_tests,
    parse_override,
    resolve_override_tests,
    shape_of,
)
from ray_release.configs.global_config import init_global_config
from ray_release.exception import ReleaseTestConfigError
from ray_release.test import BUILD_ID_PLACEHOLDER, Test
from ray_release.util import ANYSCALE_RAY_IMAGE_PREFIX


class TestShapeOf:
    @pytest.mark.parametrize(
        "uri, expected",
        [
            (
                "029272617770.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray:pr-63308.e61e71-py310-cpu",
                f"029272617770.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
            ),
            (
                "ecr/anyscale/ray-ml:abc.def-py310-cu121",
                f"ecr/anyscale/ray-ml:{BUILD_ID_PLACEHOLDER}-py310-cu121",
            ),
            (
                "ecr/anyscale/ray:abc.def-py310-cpu-deadbeef",
                f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu-deadbeef",
            ),
            (
                "ecr/anyscale/ray:abc.def-py312-cu130-cafebabe-2.55.1",
                f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py312-cu130-cafebabe-2.55.1",
            ),
            (
                # Released image — first dash-segment is the version, which
                # gets replaced too; that's by design (see module docstring).
                "anyscale/ray:2.55.1-py310-cpu",
                f"anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
            ),
        ],
    )
    def test_shape_of(self, uri, expected):
        assert shape_of(uri) == expected

    def test_shape_of_rejects_uri_without_tag(self):
        with pytest.raises(ValueError, match="missing ':<tag>'"):
            shape_of("ecr/anyscale/ray")

    def test_shape_of_rejects_uri_with_empty_tag(self):
        with pytest.raises(ValueError, match="empty tag"):
            shape_of("ecr/anyscale/ray:")

    def test_shape_of_no_py_segment(self):
        # Released-image tests' shapes have no py<N>; placeholder-only matches
        # what Test.get_anyscale_byod_image_shape() produces in that branch.
        assert (
            shape_of("ecr/anyscale/ray:single")
            == f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}"
        )


class _StubTest:
    """Minimal Test stand-in: only the methods match_uris_to_tests calls."""

    def __init__(self, name: str, byod_image: str, shape: str):
        self._name = name
        self._byod_image = byod_image
        self._shape = shape

    def __getitem__(self, key):
        if key == "name":
            return self._name
        raise KeyError(key)

    def get_anyscale_byod_image(self) -> str:
        return self._byod_image

    def get_anyscale_byod_image_shape(self) -> str:
        return self._shape


class TestMatchUrisToTests:
    def test_happy_path_single_match(self):
        t = _StubTest(
            name="cpu_test",
            byod_image="ecr/anyscale/ray:bid1-py310-cpu",
            shape=f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        result = match_uris_to_tests([t], ["ecr/anyscale/ray:other.id-py310-cpu"])
        assert result == {"cpu_test": "ecr/anyscale/ray:other.id-py310-cpu"}

    def test_skips_released_image_test(self):
        t = _StubTest(
            name="released_test",
            byod_image=f"{ANYSCALE_RAY_IMAGE_PREFIX}:2.55.1-py310-cpu",
            shape=f"{ANYSCALE_RAY_IMAGE_PREFIX}:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        result = match_uris_to_tests([t], [])
        assert result == {}

    def test_no_match_raises_with_details(self):
        t = _StubTest(
            name="cpu_test",
            byod_image="ecr/anyscale/ray:bid1-py310-cpu",
            shape=f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        with pytest.raises(ReleaseTestConfigError) as exc:
            match_uris_to_tests([t], ["ecr/anyscale/ray:bid2-py311-cpu"])
        msg = str(exc.value)
        assert "cpu_test" in msg
        assert "no URI matched" in msg
        assert f"{BUILD_ID_PLACEHOLDER}-py310-cpu" in msg

    def test_ambiguous_match_raises_with_both_uris(self):
        t = _StubTest(
            name="cpu_test",
            byod_image="ecr/anyscale/ray:bid1-py310-cpu",
            shape=f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        with pytest.raises(ReleaseTestConfigError) as exc:
            match_uris_to_tests(
                [t],
                [
                    "ecr/anyscale/ray:bid2-py310-cpu",
                    "ecr/anyscale/ray:bid3-py310-cpu",
                ],
            )
        msg = str(exc.value)
        assert "2 URIs matched" in msg
        assert "bid2" in msg and "bid3" in msg

    def test_collects_multiple_failures(self):
        a = _StubTest(
            "a",
            "ecr/anyscale/ray:bid1-py310-cpu",
            f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        b = _StubTest(
            "b",
            "ecr/anyscale/ray:bid1-py311-cpu",
            f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py311-cpu",
        )
        with pytest.raises(ReleaseTestConfigError) as exc:
            match_uris_to_tests([a, b], [])
        msg = str(exc.value)
        # Both failures collected into a single raise (NOT first-failure-wins).
        assert "Cannot resolve image URIs for 2 test(s)" in msg
        assert "a" in msg and "b" in msg

    def test_duplicate_test_names_collected_as_failure(self):
        shape = "ecr/anyscale/ray:__BUILD_ID__-py310-cpu"
        a = _StubTest("dup", "ecr/anyscale/ray:bid1-py310-cpu", shape)
        b = _StubTest("dup", "ecr/anyscale/ray:bid2-py310-cpu", shape)
        with pytest.raises(ReleaseTestConfigError) as exc:
            match_uris_to_tests([a, b], ["ecr/anyscale/ray:other-py310-cpu"])
        msg = str(exc.value)
        assert "dup" in msg
        assert "more than once" in msg

    def test_unused_uris_are_not_errors(self):
        t = _StubTest(
            "cpu_test",
            "ecr/anyscale/ray:bid1-py310-cpu",
            f"ecr/anyscale/ray:{BUILD_ID_PLACEHOLDER}-py310-cpu",
        )
        result = match_uris_to_tests(
            [t],
            [
                "ecr/anyscale/ray:other-py310-cpu",
                "ecr/anyscale/ray:other-py311-cu121",
            ],
        )
        assert result == {"cpu_test": "ecr/anyscale/ray:other-py310-cpu"}


class TestParseOverride:
    def test_happy_path_single_uri(self):
        raw = json.dumps({"ecr/r:bid-py310-cpu": ["test_a", "test_b"]})
        result = parse_override(raw)
        assert result == {
            "test_a": "ecr/r:bid-py310-cpu",
            "test_b": "ecr/r:bid-py310-cpu",
        }

    def test_happy_path_multiple_uris(self):
        raw = json.dumps(
            {
                "ecr/r:bid-py310-cpu": ["a"],
                "ecr/r:bid-py311-cpu": ["b", "c"],
            }
        )
        result = parse_override(raw)
        assert result == {
            "a": "ecr/r:bid-py310-cpu",
            "b": "ecr/r:bid-py311-cpu",
            "c": "ecr/r:bid-py311-cpu",
        }

    @pytest.mark.parametrize(
        "raw, match",
        [
            # JSON parse failure
            ("not-json", "Invalid JSON"),
            # top-level not dict
            (json.dumps(["a", "b"]), "must be a JSON object"),
            # value not list
            (json.dumps({"uri-a": "not-a-list"}), "must be a list of test names"),
            # value contains non-string element
            (json.dumps({"uri-a": ["ok", 123]}), "must be a list of test names"),
            # empty top-level
            (json.dumps({}), "contains no tests to run"),
            # all values empty
            (json.dumps({"uri-a": []}), "contains no tests to run"),
        ],
    )
    def test_parse_override_rejects_invalid_payload(self, raw, match):
        with pytest.raises(ReleaseTestConfigError, match=match):
            parse_override(raw)

    def test_duplicate_test_name(self):
        raw = json.dumps({"uri-a": ["dup", "x"], "uri-b": ["y", "dup"]})
        with pytest.raises(ReleaseTestConfigError) as exc:
            parse_override(raw)
        msg = str(exc.value)
        assert "dup" in msg
        assert "uri-a" in msg and "uri-b" in msg

    def test_type_error_raises_before_duplicate_detection(self):
        # Payload has BOTH a duplicate test name (across uri-a and uri-b) AND
        # a malformed value at uri-c. The type error should surface first
        # because duplicate detection requires well-typed values.
        raw = json.dumps(
            {
                "uri-a": ["dup", "x"],
                "uri-b": ["y", "dup"],
                "uri-c": "not-a-list",
            }
        )
        with pytest.raises(
            ReleaseTestConfigError, match="must be a list of test names"
        ):
            parse_override(raw)


class _StubCollectionTest:
    def __init__(self, name: str):
        self._name = name

    def get_name(self) -> str:
        return self._name

    def __getitem__(self, key):
        if key == "name":
            return self._name
        raise KeyError(key)


class TestResolveOverrideTests:
    def _collection(self, names):
        return [_StubCollectionTest(n) for n in names]

    def test_happy_path(self):
        coll = self._collection(["a", "b", "c"])
        override = {"a": "uri1", "c": "uri2"}
        result = resolve_override_tests(coll, override)
        assert [t.get_name() for t in result] == ["a", "c"]

    def test_unknown_name_raises_listing_all(self):
        coll = self._collection(["a"])
        override = {"a": "uri1", "ghost1": "uri2", "ghost2": "uri3"}
        with pytest.raises(ReleaseTestConfigError) as exc:
            resolve_override_tests(coll, override)
        msg = str(exc.value)
        assert "ghost1" in msg and "ghost2" in msg
        # The known test 'a' should not appear in the missing-names section.
        assert "a" not in msg.split("not found in test collection")[1]

    def test_preserves_override_iteration_order(self):
        # Test grouping in the UI follows user-provided JSON order,
        # not collection order.
        coll = self._collection(["a", "b", "c"])
        override = {"c": "u3", "a": "u1"}
        result = resolve_override_tests(coll, override)
        assert [t.get_name() for t in result] == ["c", "a"]

    def test_empty_override_returns_empty(self):
        # Defensive: parse_override raises on empty, but resolve_override_tests
        # should still behave sanely if it ever receives an empty map.
        result = resolve_override_tests(self._collection(["a"]), {})
        assert result == []


def test_uses_released_image_does_not_require_rayci_build_id(monkeypatch):
    """_uses_released_image must work in any environment, even when
    RAYCI_BUILD_ID is unset.

    The build_id is irrelevant to the released-vs-custom decision (which is
    driven by ray_version + require_custom_byod_image()), so passing
    BUILD_ID_PLACEHOLDER lets this module be called without first setting
    the env var — which match_uris_to_tests relies on.
    """
    init_global_config(bazel_runfile("release/ray_release/configs/oss_config.yaml"))
    monkeypatch.delenv("RAYCI_BUILD_ID", raising=False)

    # Custom BYOD test (no ray_version): would previously raise ValueError
    # because get_anyscale_byod_image(build_id=None) tries to read
    # RAYCI_BUILD_ID. Must now succeed and return False.
    t_custom = Test(
        {
            "name": "x",
            "team": "reef",
            "group": "g",
            "frequency": "nightly",
            "working_dir": "wd",
            "python": "3.10",
            "cluster": {"byod": {}, "cluster_compute": "c.yaml"},
            "run": {"timeout": 60, "script": "echo hi"},
        }
    )
    assert _uses_released_image(t_custom) is False

    # A released-image test (ray_version set) is correctly identified.
    t_released = Test(
        {
            "name": "y",
            "team": "reef",
            "group": "g",
            "frequency": "nightly",
            "working_dir": "wd",
            "python": "3.10",
            "cluster": {
                "byod": {},
                "cluster_compute": "c.yaml",
                "ray_version": "2.55.1",
            },
            "run": {"timeout": 60, "script": "echo hi"},
        }
    )
    assert _uses_released_image(t_released) is True
