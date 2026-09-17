import os
import subprocess
import sys
from pathlib import Path

# Fraction of checkpoint writes after which the controller kills itself.
CRASH_PROBABILITY = "0.1"

if __name__ == "__main__":
    curr_dir = Path(__file__).parent
    test_paths = curr_dir.rglob("test_*.py")
    sorted_path = sorted(map(lambda path: str(path.absolute()), test_paths))
    serve_tests_files = list(sorted_path)

    print("Testing the following files")
    for test_file in serve_tests_files:
        print("->", test_file.split("/")[-1])

    # Set in the driver so the raylet, and therefore the controller actor,
    # inherits it. Assigning a module global here would never reach that process.
    print(f"Setting RAY_SERVE_CRASH_AFTER_CHECKPOINT_PROBABILITY={CRASH_PROBABILITY}")
    os.environ["RAY_SERVE_CRASH_AFTER_CHECKPOINT_PROBABILITY"] = CRASH_PROBABILITY

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
