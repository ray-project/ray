"""Pre-commit entry point for the Ray Data pyrefly check.

Mirrors ci/lint/pyrefly-check.sh, but runs only on the files pre-commit
passes in, skipping the ones in ci/lint/pyrefly-excluded-files.txt so the
hook and the CI job agree on scope.
"""

import subprocess
import sys
from pathlib import Path

EXCLUDED_FILES_LIST = Path(__file__).parent / "pyrefly-excluded-files.txt"


def main() -> int:
    excluded = set(EXCLUDED_FILES_LIST.read_text().split())
    files = [f for f in sys.argv[1:] if f not in excluded]
    if not files:
        return 0
    return subprocess.call(["pyrefly", "check", *files])


if __name__ == "__main__":
    sys.exit(main())
