import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_EXCLUDED_FILES = _REPO_ROOT / "ci/lint/pyrefly-excluded-files.txt"


def main() -> int:
    excluded = {
        line.strip()
        for line in _EXCLUDED_FILES.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    files = [path for path in sys.argv[1:] if path not in excluded]
    if not files:
        return 0

    return subprocess.run(
        [
            "pyrefly",
            "check",
            "--config",
            str(_REPO_ROOT / "pyrefly.toml"),
            "--python-version",
            "3.10",
            *files,
        ],
        cwd=_REPO_ROOT,
        check=False,
    ).returncode


if __name__ == "__main__":
    raise SystemExit(main())
