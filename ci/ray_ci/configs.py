from typing import TypedDict


class PythonVersionInfo(TypedDict):
    bin_path: str


BUILD_TYPES = [
    "optimized",
    "debug",
]
ARCHITECTURE = [
    "x86_64",
    "aarch64",
]
PYTHON_VERSIONS = {
    "3.9": PythonVersionInfo(bin_path="cp39-cp39"),
    "3.10": PythonVersionInfo(bin_path="cp310-cp310"),
    "3.11": PythonVersionInfo(bin_path="cp311-cp311"),
    "3.12": PythonVersionInfo(bin_path="cp312-cp312"),
    "3.13": PythonVersionInfo(bin_path="cp313-cp313"),
}
DEFAULT_PYTHON_VERSION = "3.9"
DEFAULT_BUILD_TYPE = "optimized"
DEFAULT_ARCHITECTURE = "x86_64"
