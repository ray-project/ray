import hashlib
import sys
from typing import TypedDict
import os

import runfiles
import pytest

_REPO_NAME = "com_github_ray_project_ray"

_runfiles = runfiles.Create()


class ExternalDoc(TypedDict):
    file: str
    digest: str
    ref: str


# Files here are referenced on external pages as examples, and are tested
# to make sure exteranl referenced Ray examples are working with latest version
# of Ray. If you need to make changes, make sure to update the external examples
# too, and then update the digests here as a confirmation.
docs = [
    ExternalDoc(
        file="pytorch_tutorials_hyperparameter_tuning_tutorial.py",
        digest="04f8bab9fda98bceaf541984482faacab7bd8d35d6e5850ae610bfea08709743",
        ref="https://pytorch.org/tutorials/beginner/hyperparameter_tuning_tutorial.html"
    ),
]


def test_hashes():
    for doc in docs:
        path = os.path.join(_REPO_NAME, "doc", "external", doc["file"])
        runfile = _runfiles.Rlocation(path)
        with open(runfile, "rb") as f:
            content = f.read()
        want = doc["digest"]
        got = hashlib.sha256(content).hexdigest()
        name = doc["file"]
        ref = doc["ref"]
        assert got == want, f"{name} ({ref}) has sha256 {got}, want {want}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
