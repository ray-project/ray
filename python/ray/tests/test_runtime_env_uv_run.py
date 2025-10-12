import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
from uv import find_uv_bin

import ray
from ray._private.test_utils import (
    format_web_url,
    wait_until_server_available,
)

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
def test_uv_run_simple(shutdown_only):
    runtime_env = {
        "py_executable": f"{find_uv_bin()} run --with emoji --no-project",
    }
    ray.init(runtime_env=runtime_env)

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_pyproject(shutdown_only, tmp_working_dir):
    tmp_dir = tmp_working_dir

    ray.init(
        runtime_env={
            "working_dir": tmp_dir,
            # We want to run in the system environment so the current installation of Ray can be found here
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {find_uv_bin()} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "Ray rocks 👍"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_editable(shutdown_only, tmp_working_dir):
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
        [find_uv_bin(), "add", "--editable", "./emoji_copy"],
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
            "py_executable": f"env PYTHONPATH={':'.join(sys.path)} {find_uv_bin()} run --python-preference=only-system",
        }
    )

    @ray.remote
    def emojize():
        import emoji

        return emoji.emojize("Ray rocks :thumbs_up:")

    assert ray.get(emojize.remote()) == "The package was edited"


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_runtime_env_hook():

    import ray._private.runtime_env.uv_runtime_env_hook

    def check_uv_run(
        cmd, runtime_env, expected_output, subprocess_kwargs=None, expected_error=None
    ):
        result = subprocess.run(
            cmd + [json.dumps(runtime_env)],
            capture_output=True,
            **(subprocess_kwargs if subprocess_kwargs else {}),
        )
        output = result.stdout.strip().decode()
        if result.returncode != 0:
            assert expected_error, result.stderr.decode()
            assert expected_error in result.stderr.decode()
        else:
            assert json.loads(output) == expected_output

    script = ray._private.runtime_env.uv_runtime_env_hook.__file__

    check_uv_run(
        cmd=[find_uv_bin(), "run", "--no-project", script],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.getcwd(),
        },
    )
    check_uv_run(
        cmd=[find_uv_bin(), "run", "--no-project", "--directory", "/tmp", script],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.path.realpath("/tmp"),
        },
    )
    check_uv_run(
        [find_uv_bin(), "run", "--no-project", script],
        {"working_dir": "/some/path"},
        {
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": "/some/path",
        },
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        with open(tmp_dir / "pyproject.toml", "w") as file:
            file.write("[project]\n")
            file.write('name = "test"\n')
            file.write('version = "0.1"\n')
            file.write('dependencies = ["psutil"]\n')
        check_uv_run(
            cmd=[find_uv_bin(), "run", script],
            runtime_env={},
            expected_output={
                "py_executable": f"{find_uv_bin()} run",
                "working_dir": f"{tmp_dir}",
            },
            subprocess_kwargs={"cwd": tmp_dir},
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_dir = Path(tmp_dir).resolve()
        os.makedirs(tmp_dir / "cwd")
        requirements = tmp_dir / "requirements.txt"
        with open(requirements, "w") as file:
            file.write("psutil\n")
        check_uv_run(
            cmd=[find_uv_bin(), "run", "--with-requirements", requirements, script],
            runtime_env={},
            expected_output={
                "py_executable": f"{find_uv_bin()} run --with-requirements {requirements}",
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
            cmd=[find_uv_bin(), "run", script],
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
            cmd=[
                find_uv_bin(),
                "run",
                "--with-requirements",
                tmp_dir / "requirements.txt",
                script,
            ],
            runtime_env={},
            expected_output=None,
            subprocess_kwargs={"cwd": tmp_dir / "cwd"},
            expected_error="Make sure the requirements file is in the working directory.",
        )

    # Make sure the runtime environment hook gives the appropriate error message
    # when combined with the 'pip' or 'uv' environment.
    for runtime_env in [{"uv": ["emoji"]}, {"pip": ["emoji"]}]:
        check_uv_run(
            cmd=[find_uv_bin(), "run", "--no-project", script],
            runtime_env=runtime_env,
            expected_output=None,
            expected_error="You are using the 'pip' or 'uv' runtime environments together with 'uv run'.",
        )

    # Check without uv run
    subprocess.check_output([sys.executable, script, "{}"]).strip().decode() == "{}"

    # Check in the case that there is one more level of subprocess indirection between
    # the "uv run" process and the process that checks the environment
    check_uv_run(
        cmd=[find_uv_bin(), "run", "--no-project", script],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.getcwd(),
        },
        subprocess_kwargs={
            "env": {**os.environ, "RAY_TEST_UV_ADD_SUBPROCESS_INDIRECTION": "1"}
        },
    )

    # Check in the case that the script is started with multiprocessing spawn
    check_uv_run(
        cmd=[find_uv_bin(), "run", "--no-project", script],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.getcwd(),
        },
        subprocess_kwargs={
            "env": {**os.environ, "RAY_TEST_UV_MULTIPROCESSING_SPAWN": "1"}
        },
    )

    # Check in the case that a module is used for "uv run" (-m or --module)
    check_uv_run(
        cmd=[
            find_uv_bin(),
            "run",
            "--no-project",
            "-m",
            "ray._private.runtime_env.uv_runtime_env_hook",
        ],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.getcwd(),
        },
    )

    # Check in the case that a module is use for "uv run" and there is
    # an argument immediately behind it
    check_uv_run(
        cmd=[
            find_uv_bin(),
            "run",
            "--no-project",
            "-m",
            "ray._private.runtime_env.uv_runtime_env_hook",
            "--extra-args",
        ],
        runtime_env={},
        expected_output={
            "py_executable": f"{find_uv_bin()} run --no-project",
            "working_dir": os.getcwd(),
        },
    )


def test_uv_run_parser():
    from ray._private.runtime_env.uv_runtime_env_hook import (
        _create_uv_run_parser,
        _parse_args,
    )

    parser = _create_uv_run_parser()

    options, command = _parse_args(parser, ["script.py"])
    assert command == ["script.py"]

    options, command = _parse_args(parser, ["--with", "requests", "example.py"])
    assert options.with_packages == ["requests"]
    assert command == ["example.py"]

    options, command = _parse_args(parser, ["--python", "3.10", "example.py"])
    assert options.python == "3.10"
    assert command == ["example.py"]

    options, command = _parse_args(
        parser, ["--no-project", "script.py", "some", "args"]
    )
    assert options.no_project
    assert command == ["script.py", "some", "args"]

    options, command = _parse_args(
        parser, ["--isolated", "-m", "module_name", "--extra-args"]
    )
    assert options.module == "module_name"
    assert options.isolated
    assert command == ["--extra-args"]

    options, command = _parse_args(
        parser,
        [
            "--isolated",
            "--extra",
            "vllm",
            "-m",
            "my_module.submodule",
            "--model",
            "Qwen/Qwen3-32B",
        ],
    )
    assert options.isolated
    assert options.extras == ["vllm"]
    assert options.module == "my_module.submodule"
    assert command == ["--model", "Qwen/Qwen3-32B"]


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
def test_uv_run_runtime_env_hook_e2e(shutdown_only, temp_dir):

    tmp_dir = Path(temp_dir)

    script = f"""
import json
import ray
import os

@ray.remote
def f():
    import emoji
    return {{"working_dir_files": os.listdir(os.getcwd())}}

with open("{tmp_dir / "output.txt"}", "w") as out:
    json.dump(ray.get(f.remote()), out)
"""

    working_dir = tmp_dir / "working_dir"
    working_dir.mkdir(parents=True, exist_ok=True)

    script_file = working_dir / "script.py"
    with open(script_file, "w") as f:
        f.write(script)
        f.close()

    subprocess.run(
        [
            find_uv_bin(),
            "run",
            # We want to run in the system environment so the current installation of Ray can be found here
            "--python-preference=only-system",
            "--with",
            "emoji",
            "--no-project",
            str(script_file),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={
            "PYTHONPATH": ":".join(sys.path),
            "PATH": os.environ["PATH"],
        },
        cwd=working_dir,
        check=True,
    )
    with open(tmp_dir / "output.txt") as f:
        assert json.load(f) == {"working_dir_files": os.listdir(working_dir)}


@pytest.mark.skipif(sys.platform == "win32", reason="Not ported to Windows yet.")
@pytest.mark.parametrize(
    "ray_start_cluster_head_with_env_vars",
    [
        {
            "include_dashboard": True,
        }
    ],
    indirect=True,
)
def test_uv_run_runtime_env_hook_e2e_job(
    ray_start_cluster_head_with_env_vars, temp_dir
):
    cluster = ray_start_cluster_head_with_env_vars
    assert wait_until_server_available(cluster.webui_url) is True
    webui_url = format_web_url(cluster.webui_url)

    tmp_dir = Path(temp_dir)

    script = f"""
import json
import ray
import os

@ray.remote
def f():
    import emoji
    return {{"working_dir_files": os.listdir(os.getcwd())}}

with open("{tmp_dir / "output.txt"}", "w") as out:
    json.dump(ray.get(f.remote()), out)
"""

    working_dir = tmp_dir / "working_dir"
    working_dir.mkdir(parents=True, exist_ok=True)

    script_file = working_dir / "script.py"
    with open(script_file, "w") as f:
        f.write(script)
        f.close()

    requirements_file = working_dir / "requirements.txt"
    with open(requirements_file, "w") as f:
        f.write("emoji\n")
        f.close()

    # Test job submission
    runtime_env_json = (
        '{"env_vars": {"PYTHONPATH": "' + ":".join(sys.path) + '"}, "working_dir": "."}'
    )
    subprocess.run(
        [
            "ray",
            "job",
            "submit",
            "--runtime-env-json",
            runtime_env_json,
            "--",
            find_uv_bin(),
            "run",
            "--with-requirements",
            str(requirements_file),
            "--no-project",
            str(script_file),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={
            "PATH": os.environ["PATH"],
            "RAY_ADDRESS": webui_url,
        },
        cwd=working_dir,
        check=True,
    )
    with open(tmp_dir / "output.txt") as f:
        assert json.load(f) == {"working_dir_files": os.listdir(working_dir)}


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
