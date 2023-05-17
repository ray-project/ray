import os
from typing import Optional, List

DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.7").split(".")
)
DOCKER_REPO = "029272617770.dkr.ecr.us-west-2.amazonaws.com/anyscale"


class Test(dict):
    """A class represents a test to run on buildkite"""

    def is_byod_cluster(self) -> bool:
        """
        Returns whether this test is running on a BYOD cluster.
        """
        return self["cluster"].get("byod", False)

    def get_byod_type(self) -> Optional[str]:
        """
        Returns the type of the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return None
        return self["cluster"]["byod"]["type"]

    def get_byod_pre_run_cmds(self) -> List[str]:
        """
        Returns the list of pre-run commands for the BYOD cluster.
        """
        if not self.is_byod_cluster():
            return []
        return self["cluster"]["byod"].get("pre_run_cmds", [])

    def get_name(self) -> str:
        """
        Returns the name of the test.
        """
        return self["name"]

    def get_python_version(self) -> str:
        """
        Returns the python version to use for this test. If not specified, use
        the default python version.
        """
        return self.get("python", ".".join(str(v) for v in DEFAULT_PYTHON_VERSION))

    def _get_ray_image_for_pr(self) -> str:
        """
        Returns the ray docker image built for a PR
        """
        commit = os.environ.get("BUILDKITE_COMMIT")
        if not commit:
            raise ValueError("BUILDKITE_COMMIT is not set for a PR build")
        if self.get_byod_type() == "gpu":
            return f"{RAY_CI_ERC_REPO}:oss-ci-gpu_{commit}"
        else:
            return f"{RAY_CI_ERC_REPO}:oss-ci-build_{commit}"

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test. If the commit hash is not
        specified, use the nightly ray image.
        """
        if os.environ.get("BUILDKITE_PULL_REQUEST"):
            return self._get_ray_image_for_pr()
        ray_version = os.environ.get("BUILDKITE_COMMIT", "")[:6] or "nightly"
        ray_project = "ray-ml" if self.get_byod_type() == "gpu" else "ray"
        image_suffix = "-gpu" if self.get_byod_type() == "gpu" else ""
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"rayproject/{ray_project}:{ray_version}-{python_version}{image_suffix}"

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        tag = (
            self.get_ray_image()
            .replace("rayproject/", "")
            .replace(RAY_CI_ERC_REPO, "")
            .replace(":", "-")
        )
        return f"{DATAPLANE_ECR_REPO}:{tag}"


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
