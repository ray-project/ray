import pytest

from ray_release.buildkite.image_pinning import shape_of
from ray_release.test import BUILD_ID_PLACEHOLDER


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
