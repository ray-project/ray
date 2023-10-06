import os
from typing import TypedDict

from ci.ray_ci.container import Container


class PythonVersionInfo(TypedDict):
    bin_path: str
    numpy_version: str


PYTHON_VERSIONS = {
    "3.8": PythonVersionInfo(bin_path="cp38-cp38", numpy_version="1.19.3"),
    "3.9": PythonVersionInfo(bin_path="cp39-cp39", numpy_version="1.19.3"),
    "3.10": PythonVersionInfo(bin_path="cp310-cp310", numpy_version="1.22.0"),
    "3.11": PythonVersionInfo(bin_path="cp311-cp311", numpy_version="1.22.0"),
}


class BuilderContainer(Container):
    def __init__(self, python_version: str) -> None:
        super().__init__(
            "manylinux",
            volumes=[f"{os.environ.get('RAYCI_CHECKOUT_DIR')}:/rayci"],
        )
        python_version_info = PYTHON_VERSIONS.get(python_version)
        self.bin_path = python_version_info["bin_path"]
        self.numpy_version = python_version_info["numpy_version"]

    def run(self) -> None:
        # chown is required to allow forge to upload the wheel
        self.run_script(
            [
                "./ci/build/build-manylinux-ray.sh",
                "./ci/build/build-manylinux-wheel.sh "
                f"{self.bin_path} {self.numpy_version}",
                "chown -R 2000:100 /artifact-mount",
            ]
        )
