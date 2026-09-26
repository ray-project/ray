import os
import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    curr_dir = Path(__file__).parent
    test_paths = curr_dir.rglob("test_*.py")
    sorted_path = sorted(map(lambda path: str(path.absolute()), test_paths))
    serve_tests_files = list(sorted_path)

    print("Testing the following files")
    for test_file in serve_tests_files:
        print("->", test_file.split("/")[-1])

    # Set by the target's `env` in BUILD.bazel, and inherited from here by the
    # raylet and so by the controller actor. Printed because a run without it
    # injects nothing at all, which is how this target rotted in the first place.
    key = "RAY_SERVE_CRASH_PROBABILITY_TESTING"
    print(f"{key}={os.environ.get(key, '0')}")

    # Bazel's python stub builds sys.path in-process, so subprocesses need it
    # passed through explicitly to import ray and the test modules.
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}

    # One pytest process per file: sharing one lets the first failure skip
    # serve.shutdown() and cascade into every later test (ray#42898).
    failed = []
    for test_file in serve_tests_files:
        returncode = subprocess.run(
            [sys.executable, "-m", "pytest", "-v", "-s", test_file], env=env
        ).returncode
        # 5 is "no tests collected", returned by this file and the common/ helpers.
        if returncode not in (0, 5):
            failed.append(Path(test_file).name)

    if failed:
        print("Failed files:", ", ".join(failed))
    sys.exit(1 if failed else 0)
