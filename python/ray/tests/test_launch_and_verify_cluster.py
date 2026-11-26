"""Unit tests for `ray.autoscaler.launch_and_verify_cluster`."""
import re
import shlex
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest
import yaml

# Needs boto3, botocore and google-cloud-storage, which test-requirements.txt
# installs for the core tests.
from ray.autoscaler import launch_and_verify_cluster as launcher


def test_print_raylet_logs_preserves_default_worker_start_commands():
    # Minimal configs omit `worker_start_ray_commands` and rely on provider defaults.
    config = {
        "cluster_name": "test-cluster",
        "provider": {"type": "aws", "region": "us-west-2"},
    }

    launcher.print_raylet_logs_when_worker_start_fails(config)

    cmds = config["worker_start_ray_commands"]
    assert any("ray stop" in c for c in cmds)
    ray_start_cmds = [c for c in cmds if "ray start" in c]
    assert len(ray_start_cmds) == 1
    assert ray_start_cmds[0].startswith("{ ")
    assert launcher.RAYLET_LOGS_MARKER in ray_start_cmds[0]


def test_print_raylet_logs_handles_trailing_semicolon():
    config = {
        "cluster_name": "test-cluster",
        "provider": {"type": "aws", "region": "us-west-2"},
        "worker_start_ray_commands": [
            "ray stop",
            "ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6379;",
        ],
    }

    launcher.print_raylet_logs_when_worker_start_fails(config)

    [stop_cmd, start_cmd] = config["worker_start_ray_commands"]
    assert stop_cmd == "ray stop"
    assert "; ;" not in start_cmd
    assert start_cmd.startswith(
        "{ ulimit -n 65536; ray start --address=$RAY_HEAD_IP:6379; } || "
    )


def _get_failure_logs_script(monkeypatch, config: dict, tmp_path: Path) -> str:
    """Return the script that `get_failure_logs` runs on the head node."""
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout=b"ok")

    monkeypatch.setattr(launcher.subprocess, "run", fake_run)

    assert launcher.get_failure_logs(config, tmp_path / "cluster.yaml", False, {}) == (
        "ok"
    )
    return captured["cmd"][3]


def test_get_failure_logs_resolves_default_ssh_user(monkeypatch, tmp_path):
    # Configs like aws/tests/aws_cluster.yaml omit `auth`.
    config = {
        "cluster_name": "test-cluster",
        "provider": {"type": "aws", "region": "us-west-2"},
        "docker": {"container_name": "ray_container"},
    }

    script = _get_failure_logs_script(monkeypatch, config, tmp_path)

    assert "ubuntu@$ip" in script


@pytest.mark.parametrize(
    "marker_line",
    [
        launcher.RAYLET_LOGS_MARKER,
        # Output from a terminal ends lines with a carriage return.
        launcher.RAYLET_LOGS_MARKER + "\r",
        # A log prefix before the marker.
        "(worker 10.0.0.2) " + launcher.RAYLET_LOGS_MARKER,
    ],
)
def test_get_failure_logs_finds_marker_but_not_logged_start_commands(
    monkeypatch, tmp_path, marker_line
):
    config = {
        "cluster_name": "test-cluster",
        "provider": {"type": "aws", "region": "us-west-2"},
        "worker_start_ray_commands": ["ray start --address=$RAY_HEAD_IP:6379"],
    }
    launcher.print_raylet_logs_when_worker_start_fails(config)
    [start_cmd] = config["worker_start_ray_commands"]
    # The autoscaler logs the commands it runs, which hold the marker.
    monitor_log = tmp_path / "monitor.out"
    monitor_log.write_text(
        f"Full command is `ssh -tt ubuntu@10.0.0.2 bash -c {start_cmd}`\n"
        f"Running `{start_cmd}`\n"
        f"{marker_line}\n"
        "==> /tmp/ray/session_latest/logs/raylet.out <==\n"
        "raylet crashed\n"
    )
    script = _get_failure_logs_script(monkeypatch, config, tmp_path)
    [grep] = re.findall(r"grep -hE -A 125 '[^']*'", script)
    monkeypatch.undo()

    # Bytes, as text mode would turn the carriage return into a newline.
    out = subprocess.run(
        f"{grep} {shlex.quote(str(monitor_log))}",
        shell=True,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode()

    assert out.split("\n") == [
        marker_line,
        "==> /tmp/ray/session_latest/logs/raylet.out <==",
        "raylet crashed",
        "",
    ]


@pytest.mark.parametrize(
    "file_mounts",
    [
        {},
        # An empty `file_mounts:` key in the YAML.
        {"file_mounts": None},
        {"file_mounts": {"~/a": "/local/a"}},
    ],
)
def test_override_wheel_with_local_mounts_wheel_directory(file_mounts, tmp_path):
    wheel = tmp_path / "ray-2.61.0-cp310-cp310-manylinux2014_x86_64.whl"
    wheel.write_bytes(b"wheel")
    files_dir = tmp_path / "files"
    files_dir.mkdir()
    config = {
        "cluster_name": "test-cluster",
        "provider": {"type": "aws", "region": "us-west-2"},
        **file_mounts,
    }

    launcher.override_wheel_with_local(config, str(wheel), str(files_dir))

    # A directory, as `ray up` warns that Docker may not update a mounted file.
    local_dir = files_dir / launcher.REMOTE_WHEEL_DIR
    assert config["file_mounts"] == {
        **(file_mounts.get("file_mounts") or {}),
        f"~/{launcher.REMOTE_WHEEL_DIR}": str(local_dir),
    }
    assert (local_dir / wheel.name).read_bytes() == b"wheel"
    assert f"$HOME/{launcher.REMOTE_WHEEL_DIR}/{wheel.name}[default]" in (
        config["setup_commands"][-1]
    )


def test_override_wheel_with_local_keeps_azure_conda_env(tmp_path):
    # The Azure test YAML installs Ray in a conda env that its own setup
    # commands create and activate in ~/.bashrc.
    path = Path(launcher.__file__).parent / "azure" / "tests" / "azure-cluster.yaml"
    config = yaml.safe_load(path.read_text())
    yaml_setup_commands = list(config["setup_commands"])
    wheel = tmp_path / "ray-2.61.0-cp310.whl"
    wheel.write_bytes(b"wheel")

    launcher.override_wheel_with_local(config, str(wheel), str(tmp_path))

    *kept, use_python, install = config["setup_commands"]
    assert kept == [c for c in yaml_setup_commands if not c.startswith("which ray")]
    assert any("conda activate ray-env" in c for c in kept)
    # After the conda env's activation, so that a venv it adds comes first.
    assert use_python == launcher.use_job_python_command()
    assert "ray-2.61.0-cp310.whl[default]" in install


def _job_python() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


@pytest.mark.parametrize(
    "override,tag",
    [
        ("latest", "latest-py{}"),
        ("latest-gpu", "latest-py{}-gpu"),
        ("nightly", "nightly-py{}"),
    ],
)
def test_get_docker_image_uses_job_python(monkeypatch, override, tag):
    # Not a fixed Python, so that the image keeps matching the wheel when the
    # release tests move to another Python.
    monkeypatch.setattr(launcher.ray, "__version__", "3.0.0.dev0")
    py = _job_python().replace(".", "")

    assert launcher.get_docker_image(override) == f"rayproject/ray:{tag.format(py)}"


def test_get_docker_image_release_uses_job_python(monkeypatch):
    monkeypatch.setattr(launcher.ray, "__version__", "2.61.0")
    monkeypatch.setattr(launcher.ray, "__commit__", "abcdef123456")
    py = _job_python().replace(".", "")

    assert (
        launcher.get_docker_image("release") == f"rayproject/ray:2.61.0.abcdef-py{py}"
    )


def test_find_local_ray_wheel_picks_job_python(monkeypatch, tmp_path):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(launcher.ray, "__version__", "2.61.0")
    cp = "cp" + _job_python().replace(".", "")
    wheel = tmp_path / f"ray-2.61.0-{cp}-{cp}-manylinux2014_x86_64.whl"
    wheel.touch()
    # A wheel of another Python, which wouldn't install on the image.
    (tmp_path / "ray-2.61.0-cp39-cp39-manylinux2014_x86_64.whl").touch()

    assert launcher.find_local_ray_wheel() == str(wheel)


def test_latest_image_needs_wheel_override(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["launch", "--docker-override", "latest", "cluster.yaml"]
    )
    with pytest.raises(SystemExit):
        launcher.check_arguments()

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "launch",
            "--docker-override",
            "latest",
            "--wheel-override",
            "commit",
            "c.yaml",
        ],
    )
    assert launcher.check_arguments()[4:] == ("latest", "commit")


def _run_use_job_python(tmp_path: Path, node_python: str) -> str:
    """Run the command of `use_job_python_command` on a fake node whose
    `python` is `node_python`, and return what `python` and `pip` were asked
    to do."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "calls"
    major, minor = node_python.split(".")
    (bin_dir / "python").write_text(
        "#!/bin/bash\n"
        'if [ "$1" = -c ]; then\n'
        f'  exec {shlex.quote(sys.executable)} -c "import sys; '
        f'sys.version_info = ({major}, {minor}); exec(sys.argv[1])" "$2"\n'
        "fi\n"
        f'echo "python $*" >> {shlex.quote(str(calls))}\n'
    )
    (bin_dir / "pip").write_text(
        f'#!/bin/bash\necho "pip $*" >> {shlex.quote(str(calls))}\n'
    )
    for f in bin_dir.iterdir():
        f.chmod(0o755)
    subprocess.run(
        ["bash", "-c", launcher.use_job_python_command()],
        check=True,
        env={"HOME": str(tmp_path), "PATH": f"{bin_dir}:/usr/bin:/bin"},
    )
    return calls.read_text() if calls.exists() else ""


def test_use_job_python_command_keeps_matching_python(tmp_path):
    assert _run_use_job_python(tmp_path, _job_python()) == ""
    assert not (tmp_path / ".bashrc").exists()


def test_use_job_python_command_creates_venv_of_job_python(tmp_path):
    calls = _run_use_job_python(tmp_path, "3.9")

    venv = f"{tmp_path}/{launcher.REMOTE_VENV_DIR}"
    assert calls.splitlines() == [
        "pip install -U uv",
        f"python -m uv venv --seed --python {_job_python()} {venv}",
    ]
    # Expanded when ~/.bashrc runs, as in the providers' default setup commands.
    assert (tmp_path / ".bashrc").read_text() == (
        f'export PATH="$HOME/{launcher.REMOTE_VENV_DIR}/bin:$PATH"\n'
    )


def test_run_ray_commands_preserves_ray_up_error_if_cleanup_fails(
    monkeypatch, tmp_path
):
    err = subprocess.CalledProcessError(1, ["ray", "up"], output=b"out", stderr=b"err")
    monkeypatch.setattr(launcher.subprocess, "run", mock.Mock(side_effect=err))
    monkeypatch.setattr(
        launcher, "cleanup_cluster", mock.Mock(side_effect=RuntimeError("cleanup boom"))
    )

    with pytest.raises(subprocess.CalledProcessError) as exc_info:
        launcher.run_ray_commands(
            {"provider": {"type": "aws"}}, tmp_path / "cluster.yaml", 1, False
        )

    assert exc_info.value is err
    launcher.cleanup_cluster.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
