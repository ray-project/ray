import os
import subprocess
import sys

if __name__ == "__main__":
    if os.environ.get("BUILDKITE") == "true":
        subprocess.run(
            "unshare -fpn --mount-proc"
            f' bash -c "ip link set lo up && pytest -vs {sys.argv[1]}"',
            shell=True,
            check=True,
        )
    else:
        import pytest

        sys.exit(pytest.main(["-vs", sys.argv[1]]))
