"""Wanda binary dependency for RayCI build cache digest computation."""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_file")

# Keep in sync with .rayciversion.
WANDA_VERSION = "0.36.0"

def wanda_setup():
    """Download platform-specific wanda binaries."""
    http_file(
        name = "wanda_linux_x86_64",
        downloaded_file_path = "wanda",
        executable = True,
        sha256 = "1614738a4a0721c75c1e950dc42165699a0eeb29acc867d0d66250f8b06b1de7",
        urls = [
            "https://github.com/ray-project/rayci/releases/download/v{}/wanda-linux-amd64".format(WANDA_VERSION),
        ],
    )

    http_file(
        name = "wanda_darwin_arm64",
        downloaded_file_path = "wanda",
        executable = True,
        sha256 = "ac28e9dcd0979d333eb7852cd21e48fbf5f2bf4eeaff649748b2f9b68cfc8e28",
        urls = [
            "https://github.com/ray-project/rayci/releases/download/v{}/wanda-darwin-arm64".format(WANDA_VERSION),
        ],
    )
