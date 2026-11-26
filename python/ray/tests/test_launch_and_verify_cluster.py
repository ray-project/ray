"""Unit tests for `ray.autoscaler.launch_and_verify_cluster`."""
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

# Needs boto3, botocore and google-cloud-storage, which test-requirements.txt
# installs for the core tests.
from ray.autoscaler import launch_and_verify_cluster as launcher


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


def _job_python() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"


@pytest.mark.parametrize(
    "override,tag",
    [
        ("latest", "latest-py{}"),
        ("latest-gpu", "latest-py{}-gpu"),
    ],
)
def test_get_docker_image_uses_job_python(monkeypatch, override, tag):
    # Not a fixed Python, so that the image keeps matching the wheel when the
    # release tests move to another Python.
    py = _job_python().replace(".", "")

    assert launcher.get_docker_image(override) == f"rayproject/ray:{tag.format(py)}"


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
