import os
from typing import Optional, List

DEFAULT_PYTHON_VERSION = tuple(
    int(v) for v in os.environ.get("RELEASE_PY", "3.7").split(".")
)
DATAPLANE_ECR = "029272617770.dkr.ecr.us-west-2.amazonaws.com"
DATAPLANE_ECR_REPO = "anyscale/ray"
DATAPLANE_ECR_ML_REPO = "anyscale/ray-ml"


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

    def get_byod_image_tag(self) -> str:
        """
        Returns the byod image tag to use for this test.
        """
        commit = os.environ.get(
            "COMMIT_TO_TEST",
            os.environ["BUILDKITE_COMMIT"],
        )
        ray_version = commit[:6]
        image_suffix = "-gpu" if self.get_byod_type() == "gpu" else ""
        python_version = f"py{self.get_python_version().replace('.',   '')}"
        return f"{ray_version}-{python_version}{image_suffix}"

    def get_byod_repo(self) -> str:
        """
        Returns the byod repo to use for this test.
        """
        return (
            DATAPLANE_ECR_ML_REPO
            if self.get_byod_type() == "gpu"
            else DATAPLANE_ECR_REPO
        )

    def get_ray_image(self) -> str:
        """
        Returns the ray docker image to use for this test.
        """
        ray_project = "ray-ml" if self.get_byod_type() == "gpu" else "ray"
        return f"rayproject/{ray_project}:{self.get_byod_image_tag()}"

    def get_anyscale_byod_image(self) -> str:
        """
        Returns the anyscale byod image to use for this test.
        """
        return f"{DATAPLANE_ECR}/{self.get_byod_repo()}:{self.get_byod_image_tag()}"


class TestDefinition(dict):
    """
    A class represents a definition of a test, such as test name, group, etc. Comparing
    to the test class, there are additional field, for example variations, which can be
    used to define several variations of a test.
    """

    pass
