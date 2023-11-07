import os
from typing import TypedDict

from ci.ray_ci.container import Container

BUILD_TYPE = [
    "optimized",
    "debug",
]


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
    def __init__(self, python_version: str, build_type: str) -> None:
        super().__init__(
            "manylinux",
            volumes=[f"{os.environ.get('RAYCI_CHECKOUT_DIR')}:/rayci"],
        )
        python_version_info = PYTHON_VERSIONS.get(python_version)
        assert build_type in BUILD_TYPE, f"build_type must be one of {BUILD_TYPE}"
        self.build_type = build_type
        self.bin_path = python_version_info["bin_path"]
        self.numpy_version = python_version_info["numpy_version"]

    def run(self) -> None:
        # chown is required to allow forge to upload the wheel
        cmds = []
        if self.build_type == "debug":
            cmds += ["export RAY_DEBUG_BUILD=debug"]

        if os.environ.get("RAYCI_DISABLE_CPP_WHEEL") == "true":
            cmds += ["export RAY_DISABLE_EXTRA_CPP=1"]
        if os.environ.get("RAYCI_DISABLE_JAVA", "") == "true":
            cmds += ["export RAY_INSTALL_JAVA=0"]

        cmds += [
            "./ci/build/build-manylinux-ray.sh",
            f"./ci/build/build-manylinux-wheel.sh {self.bin_path} {self.numpy_version}",
            "chown -R 2000:100 /artifact-mount",
        ]
        self.run_script(cmds)
