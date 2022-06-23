import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from ray_release.result import ExitCode


class RunScriptTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.tempdir, "state.txt")
        self.test_script = os.path.join(
            os.path.dirname(__file__), "..", "..", "run_release_test.sh"
        )

        os.environ["NO_INSTALL"] = "1"
        os.environ["NO_CLONE"] = "1"
        os.environ["NO_ARTIFACTS"] = "1"
        os.environ["RAY_TEST_SCRIPT"] = "ray_release/tests/_test_run_release_test_sh.py"
        os.environ["OVERRIDE_SLEEP_TIME"] = "0"
        os.environ["MAX_RETRIES"] = "3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir)

    def _read_state(self):
        with open(self.state_file, "rt") as f:
            return int(f.read())

    def _run(self, *exits) -> int:
        assert len(exits) == 3

        if os.path.exists(self.state_file):
            os.unlink(self.state_file)

        try:
            return subprocess.check_call(
                f"{self.test_script} "
                f"{self.state_file} "
                f"{' '.join(str(e.value) for e in exits)}",
                shell=True,
            )
        except subprocess.CalledProcessError as e:
            return e.returncode

    def testRepeat(self):
        self.assertEquals(
            self._run(ExitCode.SUCCESS, ExitCode.SUCCESS, ExitCode.SUCCESS),
            ExitCode.SUCCESS.value,
        )
        self.assertEquals(self._read_state(), 1)

        self.assertEquals(
            self._run(ExitCode.RAY_WHEELS_TIMEOUT, ExitCode.SUCCESS, ExitCode.SUCCESS),
            ExitCode.SUCCESS.value,
        )
        self.assertEquals(self._read_state(), 2)

        self.assertEquals(
            self._run(
                ExitCode.RAY_WHEELS_TIMEOUT,
                ExitCode.CLUSTER_ENV_BUILD_TIMEOUT,
                ExitCode.SUCCESS,
            ),
            ExitCode.SUCCESS.value,
        )
        self.assertEquals(self._read_state(), 3)

        self.assertEquals(
            self._run(
                ExitCode.CLUSTER_STARTUP_TIMEOUT,
                ExitCode.CLUSTER_WAIT_TIMEOUT,
                ExitCode.RAY_WHEELS_TIMEOUT,
            ),
            ExitCode.RAY_WHEELS_TIMEOUT.value,
        )
        self.assertEquals(self._read_state(), 3)

        self.assertEquals(
            self._run(
                ExitCode.RAY_WHEELS_TIMEOUT, ExitCode.COMMAND_ALERT, ExitCode.SUCCESS
            ),
            ExitCode.COMMAND_ALERT.value,
        )
        self.assertEquals(self._read_state(), 2)

    def testParameters(self):
        os.environ["RAY_TEST_SCRIPT"] = "ray_release/tests/_test_catch_args.py"
        argv_file = tempfile.mktemp()

        subprocess.check_call(
            f"{self.test_script} " f"{argv_file} " f"--smoke-test",
            shell=True,
        )

        with open(argv_file, "rt") as fp:
            data = json.load(fp)

        os.unlink(argv_file)

        self.assertIn("--smoke-test", data)


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
