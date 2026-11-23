"""The Trivy release pinned for image scanning.

Never update by tag alone. v0.69.4 and the v0.69.5/v0.69.6 Docker Hub images
were malicious (GHSA-69fq-xp46-6x23), and Trivy's own release pipeline built
and signed v0.69.4 from a pushed tag, so a valid signature alone proves
nothing. Every bump follows these steps, and its PR description records
their output:
1. The release is at least 14 days old, and neither Trivy's security
   advisories nor OSV list a problem with it.
2. The tag's commit is on main or its release branch: for `main` or
   `release/v<major>.<minor>`, this prints "behind" or "identical":
       gh api repos/aquasecurity/trivy/compare/<branch>...v<v> --jq .status
3. The release's checksums file was signed by TRIVY_SIGNER_IDENTITY. A
   changed identity stops the bump until someone finds out why.
       cosign verify-blob \
         --bundle trivy_<v>_checksums.txt.sigstore.json \
         --certificate-oidc-issuer https://token.actions.githubusercontent.com \
         --certificate-identity <TRIVY_SIGNER_IDENTITY> \
         trivy_<v>_checksums.txt
4. TRIVY_SHA256 is copied from that verified checksums file.
5. `go version -m` on the extracted binary shows the tag's commit as
   vcs.revision. Ignore "+dirty": about half of Trivy's releases are built
   dirty.
6. A scan build on the new binary is compared with the last one, and any
   change in its finding counts is explained.
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
