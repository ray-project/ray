import os
import sys
import unittest
import tempfile
import subprocess


class ThirdPartyIsolationTest(unittest.TestCase):
    """Tests that Ray's vendored libraries don't conflict with user code."""

    def _run_test_script(self, script_content, test_name):
        """Helper method to run a test script and verify its output."""
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
            script_path = f.name
            f.write(script_content)

        try:
            # Run the script in a separate process
            print(f"Running {test_name} script at: {script_path}")
            completed_process = subprocess.run(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60,
                text=True,
            )

            # Print output for debugging
            stdout = completed_process.stdout
            stderr = completed_process.stderr

            print(f"Test script stdout:\n{stdout}")
            if stderr:
                print(f"Test script stderr:\n{stderr}")

            # Check the return code
            if completed_process.returncode != 0:
                self.fail(
                    f"Test script exited with code {completed_process.returncode}"
                )

            # Verify the test passed
            self.assertIn("SUCCESS", stdout, "Test did not report success")

        except subprocess.TimeoutExpired as e:
            print(f"Test timed out after {e.timeout} seconds")
            if e.stdout:
                print(
                    f"Stdout before timeout:\n{e.stdout.decode('utf-8', errors='replace')}"
                )
            if e.stderr:
                print(
                    f"Stderr before timeout:\n{e.stderr.decode('utf-8', errors='replace')}"
                )
            raise
        finally:
            # Clean up temporary file
            os.unlink(script_path)

    def test_path_isolation(self):
        """Test that Ray's vendored libraries directory isn't in sys.path."""
        script = """
import sys
import ray

# Initialize Ray
ray.init(log_to_driver=True)
print("Ray initialized")

# Check for thirdparty_files in sys.path
for path in sys.path:
    if path.endswith("thirdparty_files"):
        print(f"ERROR: Found thirdparty_files in sys.path: {path}")
        sys.exit(1)
print("Confirmed thirdparty_files is not in sys.path")
ray.shutdown()
print("SUCCESS: sys.path isolation test passed")
"""
        self._run_test_script(script, "path isolation")

    def test_psutil_import(self):
        """Test that system psutil can be imported and used after Ray init."""
        script = """
import sys
try:
    import psutil
    print(f"Imported system psutil version: {psutil.__version__}")
except ImportError:
    print("System psutil not available, skipping test")
    print("SUCCESS: psutil test skipped")
    sys.exit(0)

# Initialize Ray
import ray
ray.init(log_to_driver=True)
print("Ray initialized")

# Verify psutil still works after Ray initialization
process_count = len(psutil.pids())
print(f"Process count: {process_count}")

ray.shutdown()
print("SUCCESS: psutil isolation test passed")
"""
        self._run_test_script(script, "psutil import")

    def test_setproctitle_import(self):
        """Test that system setproctitle can be imported and used after Ray init."""
        script = """
import sys
try:
    import setproctitle
    print("Imported system setproctitle")
except ImportError:
    print("System setproctitle not available, skipping test")
    print("SUCCESS: setproctitle test skipped")
    sys.exit(0)

# Initialize Ray
import ray
ray.init(log_to_driver=True)
print("Ray initialized")

# Get current process title (as set by Ray)
ray_title = setproctitle.getproctitle()
print(f"Current process title: {ray_title}")

ray.shutdown()
print("SUCCESS: setproctitle isolation test passed")
"""
        self._run_test_script(script, "setproctitle import")


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
