import glob
import os
import shlex
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.nsight import NsightPlugin, parse_nsight_config
from ray.core.generated.common_pb2 import Language
from ray.exceptions import RuntimeEnvSetupError


def wait_for_report(profilers_dir, num_reports):
    assert len(os.listdir(profilers_dir)) == num_reports
    return True


NSIGHT_FAKE_DIR = str(Path(os.path.realpath(__file__)).parent / "nsight_fake")


@pytest.fixture(scope="class")
def nsight_fake_dependency():
    subprocess.check_call(
        ["pip", "install", NSIGHT_FAKE_DIR],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    yield
    subprocess.check_call(
        ["pip", "uninstall", "nsys", "--y"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


class TestNsightProfiler:
    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.usefixtures("nsight_fake_dependency")
    def test_nsight_basic(
        self,
        shutdown_only,
    ):
        """Test Nsight profile generate report on logs dir"""
        ray.init()
        session_dir = ray.worker._global_node.get_session_dir_path()
        profilers_dir = Path(session_dir) / "logs" / "nsight"

        # test nsight default config
        @ray.remote(runtime_env={"nsight": "default"})
        def test_generate_report():
            return 0

        ray.get(test_generate_report.remote())

        wait_for_condition(wait_for_report, profilers_dir=profilers_dir, num_reports=1)
        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 1
        for report in nsys_reports:
            assert "worker_process_" in report
            os.remove(report)

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.usefixtures("nsight_fake_dependency")
    def test_nsight_custom_option(
        self,
        shutdown_only,
    ):
        """Test Nsight profile generate report on logs dir"""
        ray.init()
        session_dir = ray.worker._global_node.get_session_dir_path()
        profilers_dir = Path(session_dir) / "logs" / "nsight"

        # test nsight custom filename
        CUSTOM_REPORT = "custom_report"

        @ray.remote(
            runtime_env={
                "nsight": {
                    "t": "cuda,cublas,cudnn",
                    "stop-on-exit": "true",
                    "o": CUSTOM_REPORT,
                }
            }
        )
        def test_generate_custom_report():
            return 0

        ray.get(test_generate_custom_report.remote())

        wait_for_condition(wait_for_report, profilers_dir=profilers_dir, num_reports=1)
        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 1
        for report in nsys_reports:
            assert f"{CUSTOM_REPORT}.nsys-rep" in report
            os.remove(report)

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.usefixtures("nsight_fake_dependency")
    def test_nsight_multiple_tasks(
        self,
        shutdown_only,
    ):
        """Test Nsight profile on multiple ray tasks/actors"""
        ray.init()
        session_dir = ray.worker._global_node.get_session_dir_path()
        profilers_dir = Path(session_dir) / "logs" / "nsight"

        # test ray task
        @ray.remote(
            runtime_env={
                "nsight": {"o": "ray_task_%p"},
            }
        )
        def test_generate_report():
            return 0

        # test ray actor
        @ray.remote(runtime_env={"nsight": {"o": "ray_actor"}})
        class NsightActor:
            def run(self):
                return 0

        ray_actor = NsightActor.remote()
        ray.get(
            [
                test_generate_report.remote(),
                test_generate_report.remote(),
                ray_actor.run.remote(),
                ray_actor.run.remote(),
            ]
        )
        ray.kill(ray_actor)

        wait_for_condition(wait_for_report, profilers_dir=profilers_dir, num_reports=3)
        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 3  # ray actor used only one worker process
        for report in nsys_reports:
            assert "ray_task" in report or "ray_actor" in report
            os.remove(report)

    @pytest.mark.skipif(
        os.environ.get("CI") and sys.platform != "linux",
        reason="Requires PR wheels built in CI, so only run on linux CI machines.",
    )
    @pytest.mark.usefixtures("nsight_fake_dependency")
    def test_nsight_invalid_option(
        self,
        shutdown_only,
    ):
        """Test Nsight profile for invalid option"""
        ray.init()
        session_dir = ray.worker._global_node.get_session_dir_path()
        profilers_dir = Path(session_dir) / "logs" / "nsight"

        # test nsight invalid config
        @ray.remote(
            runtime_env={
                "nsight": {
                    "not-option": "random",
                }
            }
        )
        def test_invalid_nsight():
            return 0

        with pytest.raises(
            RuntimeEnvSetupError,
            match="nsight profile failed to run with the following error message",
        ):
            ray.get(test_invalid_nsight.remote())

        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 0

        """Test Nsight profile for unavailable string option"""

        # test nsight not supported string config
        @ray.remote(runtime_env={"nsight": "not_default"})
        def test_wrong_config_nsight():
            return 0

        with pytest.raises(
            RuntimeEnvSetupError, match="Unsupported nsight config: not_default."
        ):
            ray.get(test_wrong_config_nsight.remote())

        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 0

    @pytest.mark.skipif(
        sys.platform == "linux",
        reason="For non linux system, Nsight CLI is not supported",
    )
    def test_nsight_unsupported(
        self,
        shutdown_only,
    ):
        """Test Nsight profile for unsupported OS"""
        ray.init()
        session_dir = ray.worker._global_node.get_session_dir_path()
        profilers_dir = Path(session_dir) / "logs" / "nsight"

        # test nsight default config
        @ray.remote(runtime_env={"nsight": "default"})
        def test_nsight_not_supported():
            return 0

        with pytest.raises(
            RuntimeEnvSetupError, match="Nsight CLI is only available in Linux."
        ):
            ray.get(test_nsight_not_supported.remote())

        nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
        assert len(nsys_reports) == 0

    def test_parse_nsight_config(self):
        """Test parse nsight config into nsight command prefix"""
        nsight_config = {
            "o": "single_dash",
            "two-dash": "double_dash",
        }
        nsight_cmd = parse_nsight_config(nsight_config)
        assert nsight_cmd == [
            "nsys",
            "profile",
            "-o",
            "single_dash",
            "--two-dash=double_dash",
        ]


@pytest.mark.skipif(
    os.environ.get("CI") and sys.platform != "linux",
    reason="Test only for compatible OS (linux) with minmial installation",
)
def test_nsight_not_installed():
    """Test Nsight profile not installed"""
    ray.init()
    session_dir = ray.worker._global_node.get_session_dir_path()
    profilers_dir = Path(session_dir) / "logs" / "nsight"

    # test nsight default config
    @ray.remote(runtime_env={"nsight": "default"})
    def test_nsight_not_supported():
        return 0

    with pytest.raises(RuntimeEnvSetupError, match="nsight is not installed"):
        ray.get(test_nsight_not_supported.remote())

    nsys_reports = glob.glob(os.path.join(f"{profilers_dir}/*.nsys-rep"))
    assert len(nsys_reports) == 0


@pytest.fixture
def nsight_plugin():
    with patch("ray._private.runtime_env.nsight.try_to_create_directory"):
        plugin = NsightPlugin("/tmp/ray/session/runtime_resources")
    plugin.nsight_cmd = parse_nsight_config(
        {"o": "/tmp/ray/session/logs/nsight/'worker_process_%p'"}
    )
    return plugin


@pytest.mark.parametrize(
    "py_executable, expected_app_args",
    [
        ("/opt/ray/venv/bin/python", ["/opt/ray/venv/bin/python"]),
        ("python -u", ["python", "-u"]),
    ],
)
def test_nsight_preserves_py_executable(
    nsight_plugin, py_executable, expected_app_args
):
    """Test modify_context prepends nsys and preserves the Python command"""
    context = Mock(spec=RuntimeEnvContext)
    context.py_executable = py_executable

    nsight_plugin.modify_context([], None, context)

    prefix = "nsys profile -o /tmp/ray/session/logs/nsight/'worker_process_%p'"
    assert context.py_executable == f"{prefix} {py_executable}"
    assert shlex.split(context.py_executable) == [
        "nsys",
        "profile",
        "-o",
        "/tmp/ray/session/logs/nsight/worker_process_%p",
        *expected_app_args,
    ]


def test_nsight_noop_without_config(nsight_plugin):
    """Test modify_context leaves the context unchanged without a nsight_cmd"""
    nsight_plugin.nsight_cmd = []
    context = RuntimeEnvContext()
    original = context.py_executable

    nsight_plugin.modify_context([], None, context)

    assert context.py_executable == original


@pytest.mark.skipif(sys.platform == "win32", reason="Uses the POSIX worker command.")
def test_nsight_preserves_worker_command(nsight_plugin):
    """Test the wrapped py_executable survives worker command assembly"""
    context = RuntimeEnvContext(
        command_prefix=["source '/opt/ray venv/bin/activate'", "&&"],
        env_vars={"NSIGHT_TEST": "preserved"},
        py_executable="'/opt/ray venv/bin/python' -u",
    )
    nsight_plugin.modify_context([], None, context)

    with (
        patch("ray._private.runtime_env.context.update_envs") as update_envs,
        patch("ray._private.runtime_env.context.os.execvp") as execvp,
        patch.dict(os.environ, {}, clear=True),
    ):
        context.exec_worker(
            ["/tmp/ray worker.py", "--node-ip-address=127.0.0.1"],
            Language.PYTHON,
        )

    update_envs.assert_called_once_with({"NSIGHT_TEST": "preserved"})
    execvp.assert_called_once_with(
        "bash",
        args=[
            "bash",
            "-c",
            (
                "source '/opt/ray venv/bin/activate' && exec "
                "nsys profile -o /tmp/ray/session/logs/nsight/'worker_process_%p' "
                "'/opt/ray venv/bin/python' -u "
                "'/tmp/ray worker.py' --node-ip-address=127.0.0.1"
            ),
        ],
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
