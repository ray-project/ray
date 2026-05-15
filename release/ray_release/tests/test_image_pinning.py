import pytest

from ray_release.buildkite.image_pinning import match_uris_to_tests, shape_of
from ray_release.exception import ReleaseTestConfigError
from ray_release.test import BUILD_ID_PLACEHOLDER
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

    def get_name(self) -> str:
        return self._name

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
        assert "a" in msg and "b" in msg

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
