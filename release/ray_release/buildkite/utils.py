import os
import subprocess
from typing import List


def is_in_buildkite() -> bool:
    return "BUILDKITE" in os.environ


def get_buildkite_artifact_urls(pattern: str) -> List[str]:
    """
    Get a list of download URLs to artifacts uploaded by the current job.

    Args:
        pattern: Pattern to match artifact names against."""
    return subprocess.check_output(
        ["buildkite-agent", "artifact", "search", pattern, "-format", "%u\n"]
    ).split()


def upload_buildkite_artifacts(path: str) -> str:
    return subprocess.check_output(["buildkite-agent", "artifact", "upload", path])
