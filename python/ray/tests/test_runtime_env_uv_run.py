# End-to-end tests for using "uv run"

import json
import os
from pathlib import Path
import pytest
import subprocess
import sys
import tempfile

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)


@pytest.fixture(scope="function")
def with_uv():
    import platform
    import stat
    import tarfile
    from urllib import request

    arch = "aarch64" if platform.machine() in ["aarch64", "arm64"] else "i686"
    system = "unknown-linux-gnu" if platform.system() == "Linux" else "apple-darwin"
    name = f"uv-{arch}-{system}"
    url = f"https://github.com/astral-sh/uv/releases/download/0.5.27/{name}.tar.gz"
    with tempfile.TemporaryDirectory() as tmp_dir:
        with request.urlopen(request.Request(url), timeout=15.0) as response:
            with tarfile.open(fileobj=response, mode="r|*") as tar:
                tar.extractall(tmp_dir)
        uv = Path(tmp_dir) / name / "uv"
        uv.chmod(uv.stat().st_mode | stat.S_IEXEC)
        yield uv


PYPROJECT_TOML = """
[project]
name = "test"
version = "0.1"
dependencies = [
  "emoji",
]
requires-python = ">=3.9"
"""


@pytest.fixture(scope="function")
def tmp_working_dir():
    """A test fixture, which writes a pyproject.toml."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        script_file = path / "pyproject.toml"
        with script_file.open(mode="w") as f:
            f.write(PYPROJECT_TOML)

        yield tmp_dir


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_simple(shutdown_only, with_uv):
    uv = with_uv

    runtime_env = {
        "py_executable": f"{uv} run --with emoji --no-project",
    }
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_pyproject(shutdown_only, with_uv, tmp_working_dir):
    uv = with_uv
    tmp_dir = tmp_working_dir

    ray.init(
        runtime_env={
            "working_dir": tmp_dir,
            # We want to run in the system environment so the current installation of Ray can be found here
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {uv} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_editable(shutdown_only, with_uv, tmp_working_dir):
    uv = with_uv
    tmp_dir = tmp_working_dir

    subprocess.run(
        ["git", "clone", "https://github.com/carpedm20/emoji/", "emoji_copy"],
        cwd=tmp_dir,
    )

    subprocess.run(
        ["git", "reset", "--hard", "08c5cc4789d924ad4215e2fb2ee8f0b19a0d421f"],
        cwd=Path(tmp_dir) / "emoji_copy",
    )

    subprocess.run(
        [uv, "add", "--editable", "./emoji_copy"],
        cwd=tmp_dir,
    )

    # Now edit the package
    content = ""
    with open(Path(tmp_dir) / "emoji_copy" / "emoji" / "core.py") as f:
        content = f.read()

    content = content.replace(
        "return pattern.sub(replace, string)", 'return "The package was edited"'
    )

    with open(Path(tmp_dir) / "emoji_copy" / "emoji" / "core.py", "w") as f:
        f.write(content)

    ray.init(
        runtime_env={
            "working_dir": tmp_dir,
            # We want to run in the system environment so the current installation of Ray can be found here
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {uv} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "The package was edited"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_runtime_env_hook(with_uv):

    import ray._private.runtime_env.uv_runtime_env_hook

    uv = with_uv

    def check_uv_run(
        cmd, runtime_env, expected_output, subprocess_kwargs=None, expected_error=None
    ):
        result = subprocess.run(
            cmd
            + [ray._private.runtime_env.uv_runtime_env_hook.__file__]
            + [json.dumps(runtime_env)],
            capture_output=True,
            **(subprocess_kwargs if subprocess_kwargs else {}),
        )
        output = result.stdout.strip().decode()
        if result.returncode != 0:
            assert expected_error
            assert expected_error in result.stderr.decode()
        else:
            assert json.loads(output) == expected_output

    check_uv_run(
        cmd=[uv, "run", "--no-project"],
        runtime_env={},
        expected_output={
            "py_executable": f"{uv} run --no-project",
            "working_dir": os.getcwd(),
        },
    )
    check_uv_run(
        cmd=[uv, "run", "--no-project", "--directory", "/tmp"],
        runtime_env={},
        expected_output={
            "py_executable": f"{uv} run --no-project",
            "working_dir": os.path.realpath("/tmp"),
        },
    )
    check_uv_run(
        [uv, "run", "--no-project"],
        {"working_dir": "/some/path"},
        {"py_executable": f"{uv} run --no-project", "working_dir": "/some/path"},
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        with open(tmp_dir / "pyproject.toml", "w") as file:
            file.write("[project]\n")
            file.write('name = "test"\n')
            file.write('version = "0.1"\n')
            file.write('dependencies = ["psutil"]\n')
        check_uv_run(
            cmd=[uv, "run"],
            runtime_env={},
            expected_output={"py_executable": f"{uv} run", "working_dir": f"{tmp_dir}"},
            subprocess_kwargs={"cwd": tmp_dir},
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        os.makedirs(tmp_dir / "cwd")
        requirements = tmp_dir / "requirements.txt"
        with open(requirements, "w") as file:
            file.write("psutil\n")
        check_uv_run(
            cmd=[uv, "run", "--with-requirements", requirements],
            runtime_env={},
            expected_output={
                "py_executable": f"{uv} run --with-requirements {requirements}",
                "working_dir": f"{tmp_dir}",
            },
            subprocess_kwargs={"cwd": tmp_dir},
        )

    # Check things fail if there is a pyproject.toml upstream of the current working directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        os.makedirs(tmp_dir / "cwd")
        with open(tmp_dir / "pyproject.toml", "w") as file:
            file.write("[project]\n")
            file.write('name = "test"\n')
            file.write('version = "0.1"\n')
            file.write('dependencies = ["psutil"]\n')
        check_uv_run(
            cmd=[uv, "run"],
            runtime_env={},
            expected_output=None,
            subprocess_kwargs={"cwd": tmp_dir / "cwd"},
            expected_error="Make sure the pyproject.toml file is in the working directory.",
        )

    # Check things fail if there is a requirements.txt upstream to the current working directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        os.makedirs(tmp_dir / "cwd")
        with open(tmp_dir / "requirements.txt", "w") as file:
            file.write("psutil\n")
        check_uv_run(
            cmd=[uv, "run", "--with-requirements", tmp_dir / "requirements.txt"],
            runtime_env={},
            expected_output=None,
            subprocess_kwargs={"cwd": tmp_dir / "cwd"},
            expected_error="Make sure the requirements file is in the working directory.",
        )

    # Check without uv run
    subprocess.check_output(
        [sys.executable, ray._private.runtime_env.uv_runtime_env_hook.__file__, "{}"]
    ).strip().decode() == "{}"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_runtime_env_hook_e2e(shutdown_only, with_uv, temp_dir):

    uv = with_uv
    tmp_out_dir = Path(temp_dir)

    script = f"""
import json
import ray
import os

@ray.remote
def f():
    import emoji
    return {{"working_dir_files": os.listdir(os.getcwd())}}

with open("{tmp_out_dir / "output.txt"}", "w") as out:
    json.dump(ray.get(f.remote()), out)
"""

    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(script)
        f.close()
        subprocess.run(
            [
                uv,
                "run",
                # We want to run in the system environment so the current installation of Ray can be found here
                "--python-preference=only-system",
                "--with",
                "emoji",
                "--no-project",
                f.name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                "RAY_RUNTIME_ENV_HOOK": "ray._private.runtime_env.uv_runtime_env_hook.hook",
                "PYTHONPATH": ":".join(sys.path),
                "PATH": os.environ["PATH"],
            },
            cwd=os.path.dirname(uv),
            check=True,
        )
        with open(tmp_out_dir / "output.txt") as f:
            assert json.load(f) == {
                "working_dir_files": os.listdir(os.path.dirname(uv))
            }


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "env_vars": {
                "RAY_RUNTIME_ENV_HOOK": "ray._private.runtime_env.uv_runtime_env_hook.hook"
            },
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_uv_run_runtime_env_hook_e2e_job(
    ray_start_cluster_head_with_env_vars, with_uv, temp_dir
):
    cluster = ray_start_cluster_head_with_env_vars
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = format_web_url(cluster.webui_url)

    uv = with_uv
    tmp_out_dir = Path(temp_dir)

    script = f"""
import json
import ray
import os

@ray.remote
def f():
    import emoji
    return {{"working_dir_files": os.listdir(os.getcwd())}}

with open("{tmp_out_dir / "output.txt"}", "w") as out:
    json.dump(ray.get(f.remote()), out)
"""

    with tempfile.NamedTemporaryFile(
        "w", suffix=".py", delete=False
    ) as f, tempfile.NamedTemporaryFile("w", delete=False) as requirements:
        f.write(script)
        f.close()
        requirements.write("emoji\n")
        requirements.close()
        # Test job submission
        runtime_env_json = (
            '{"env_vars": {"PYTHONPATH": "'
            + ":".join(sys.path)
            + '"}, "working_dir": "."}'
        )
        subprocess.run(
            [
                "ray",
                "job",
                "submit",
                "--runtime-env-json",
                runtime_env_json,
                "--",
                uv,
                "run",
                "--with-requirements",
                requirements.name,
                "--no-project",
                f.name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={
                "PATH": os.environ["PATH"],
                "RAY_ADDRESS": webui_url,
            },
            cwd=os.path.dirname(uv),
            check=True,
        )
        with open(tmp_out_dir / "output.txt") as f:
            assert json.load(f) == {
                "working_dir_files": os.listdir(os.path.dirname(uv))
            }


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
