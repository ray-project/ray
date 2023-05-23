import os

DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.7").split(".")
)


class Test(dict):
    """A class represents a test to run on buildkite"""

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        return self["cluster"].get("byod", False)

    def get_name(self) -> str:
        return self["name"]

    def get_python_version(self) -> str:
        """
        Returns the python version to use for this test. If not specified, use
        the default python version.
        """
        return self.get("python", ".".join(str(v) for v in DEFAULT_PYTHON_VERSION))

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test. If the commit hash is not
        specified, use the nightly ray image.
        """
        # TDOD(can): re-enable this test once we have a custom image
        # ray_version = os.environ.get("BUILDKITE_COMMIT", "")[:6] or "nightly"
        ray_version = "nightly"
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"rayproject/ray:{ray_version}-{python_version}"

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return self.get_ray_image().replace("rayproject", "anyscale")


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
