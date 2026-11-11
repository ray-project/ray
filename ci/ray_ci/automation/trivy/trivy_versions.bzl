"""The Trivy release pinned for image scanning.

Bump only by the checklist in the image-scan plan:
1. the release is at least 14 days old, with no advisory against it;
2. `cosign verify-blob` passes on trivy_<v>_checksums.txt with
   TRIVY_SIGNER_IDENTITY;
3. TRIVY_SHA256 is copied from that verified checksums file;
4. `go version -m` shows the tag's commit as vcs.revision.

Never update by tag alone: v0.69.4 and the v0.69.5/v0.69.6 Docker Hub images
were malicious (GHSA-69fq-xp46-6x23).
"""

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

TRIVY_VERSION = "0.76.1"

# sha256 of trivy_<version>_Linux-64bit.tar.gz, from the cosign-verified
# checksums file.
TRIVY_SHA256 = "c7cbe3137fb8a48a17574bede3dd6beb16742d1e9772929940fc90082c39d9c0"

# Certificate identity that must have signed the release's checksums file.
TRIVY_SIGNER_IDENTITY = (
    "https://github.com/aquasecurity/trivy/.github/workflows/" +
    "reusable-release.yaml@refs/tags/v" + TRIVY_VERSION
)

def trivy_repositories():
    http_archive(
        name = "trivy_linux_x86_64",
        build_file_content = 'exports_files(["trivy"])\n',
        sha256 = TRIVY_SHA256,
        urls = [
            "https://github.com/aquasecurity/trivy/releases/download/v{v}/trivy_{v}_Linux-64bit.tar.gz".format(v = TRIVY_VERSION),
        ],
    )
