import os
import pytest
import sys
import tempfile
from pathlib import Path
from ray import job_config
import yaml

from ray._private.runtime_env.validation import (
    parse_and_validate_excludes,
    parse_and_validate_working_dir,
    parse_and_validate_conda,
    parse_and_validate_pip,
    parse_and_validate_env_vars,
    parse_and_validate_py_modules,
)
from ray.runtime_env import RuntimeEnv

CONDA_DICT = {"dependencies": ["pip", {"pip": ["pip-install-test==0.5"]}]}

PIP_LIST = ["requests==1.0.0", "pip-install-test"]


@pytest.fixture
def test_directory():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        subdir = path / "subdir"
        subdir.mkdir(parents=True)
        requirements_file = subdir / "requirements.txt"
        with requirements_file.open(mode="w") as f:
            print("\n".join(PIP_LIST), file=f)

        good_conda_file = subdir / "good_conda_env.yaml"
        with good_conda_file.open(mode="w") as f:
            yaml.dump(CONDA_DICT, f)

        bad_conda_file = subdir / "bad_conda_env.yaml"
        with bad_conda_file.open(mode="w") as f:
            print("% this is not a YAML file %", file=f)

        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield subdir, requirements_file, good_conda_file, bad_conda_file
        os.chdir(old_dir)


def test_key_with_value_none():
    parsed_runtime_env = RuntimeEnv(pip=None)
    assert parsed_runtime_env == {}


class TestValidateWorkingDir:
    def test_validate_bad_uri(self):
        with pytest.raises(ValueError, match="a valid URI"):
            parse_and_validate_working_dir("unknown://abc")

    def test_validate_invalid_type(self):
        with pytest.raises(TypeError):
            parse_and_validate_working_dir(1)

    def test_validate_remote_invalid_extensions(self):
        for uri in [
            "https://some_domain.com/path/file",
            "s3://bucket/file",
            "gs://bucket/file",
        ]:
            with pytest.raises(
                ValueError, match="Only .zip files supported for remote URIs."
            ):
                parse_and_validate_working_dir(uri)

    def test_validate_remote_valid_input(self):
        for uri in [
            "https://some_domain.com/path/file.zip",
            "s3://bucket/file.zip",
            "gs://bucket/file.zip",
        ]:
            working_dir = parse_and_validate_working_dir(uri)
            assert working_dir == uri


class TestValidatePyModules:
    def test_validate_not_a_list(self):
        with pytest.raises(TypeError, match="must be a list of strings"):
            parse_and_validate_py_modules(".")

    def test_validate_bad_uri(self):
        with pytest.raises(ValueError, match="a valid URI"):
            parse_and_validate_py_modules(["unknown://abc"])

    def test_validate_invalid_type(self):
        with pytest.raises(TypeError):
            parse_and_validate_py_modules([1])

    def test_validate_remote_invalid_extension(self):
        uris = [
            "https://some_domain.com/path/file",
            "s3://bucket/file",
            "gs://bucket/file",
        ]
        with pytest.raises(
            ValueError, match="Only .zip files supported for remote URIs."
        ):
            parse_and_validate_py_modules(uris)

    def test_validate_remote_valid_input(self):
        uris = [
            "https://some_domain.com/path/file.zip",
            "s3://bucket/file.zip",
            "gs://bucket/file.zip",
        ]
        py_modules = parse_and_validate_py_modules(uris)
        assert py_modules == uris


class TestValidateExcludes:
    def test_validate_excludes_invalid_types(self):
        with pytest.raises(TypeError):
            parse_and_validate_excludes(1)

        with pytest.raises(TypeError):
            parse_and_validate_excludes(True)

        with pytest.raises(TypeError):
            parse_and_validate_excludes("string")

        with pytest.raises(TypeError):
            parse_and_validate_excludes(["string", 1])

    def test_validate_excludes_empty_list(self):
        assert RuntimeEnv(excludes=[]) == {}


@pytest.mark.skipif(
    sys.platform == "win32", reason="Conda option not supported on Windows."
)
class TestValidateConda:
    def test_validate_conda_invalid_types(self):
        with pytest.raises(TypeError):
            parse_and_validate_conda(1)

        with pytest.raises(TypeError):
            parse_and_validate_conda(True)

    def test_validate_conda_str(self, test_directory):
        assert parse_and_validate_conda("my_env_name") == "my_env_name"

    def test_validate_conda_invalid_path(self):
        with pytest.raises(ValueError):
            parse_and_validate_conda("../bad_path.yaml")

    @pytest.mark.parametrize("absolute_path", [True, False])
    def test_validate_conda_valid_file(self, test_directory, absolute_path):
        _, _, good_conda_file, _ = test_directory

        if absolute_path:
            good_conda_file = good_conda_file.resolve()

        assert parse_and_validate_conda(str(good_conda_file)) == CONDA_DICT

    @pytest.mark.parametrize("absolute_path", [True, False])
    def test_validate_conda_invalid_file(self, test_directory, absolute_path):
        _, _, _, bad_conda_file = test_directory

        if absolute_path:
            bad_conda_file = bad_conda_file.resolve()

        with pytest.raises(ValueError):
            parse_and_validate_conda(str(bad_conda_file))

    def test_validate_conda_valid_dict(self):
        assert parse_and_validate_conda(CONDA_DICT) == CONDA_DICT


@pytest.mark.skipif(
    sys.platform == "win32", reason="Pip option not supported on Windows."
)
class TestValidatePip:
    def test_validate_pip_invalid_types(self):
        with pytest.raises(TypeError):
            parse_and_validate_pip(1)

        with pytest.raises(TypeError):
            parse_and_validate_pip(True)

    def test_validate_pip_invalid_path(self):
        with pytest.raises(ValueError):
            parse_and_validate_pip("../bad_path.txt")

    @pytest.mark.parametrize("absolute_path", [True, False])
    def test_validate_pip_valid_file(self, test_directory, absolute_path):
        _, requirements_file, _, _ = test_directory

        if absolute_path:
            requirements_file = requirements_file.resolve()

        result = parse_and_validate_pip(str(requirements_file))
        assert result["packages"] == PIP_LIST
        assert not result["pip_check"]
        assert "pip_version" not in result

    def test_validate_pip_valid_list(self):
        result = parse_and_validate_pip(PIP_LIST)
        assert result["packages"] == PIP_LIST
        assert not result["pip_check"]
        assert "pip_version" not in result

    def test_validate_ray(self):
        result = parse_and_validate_pip(["pkg1", "ray", "pkg2"])
        assert result["packages"] == ["pkg1", "ray", "pkg2"]
        assert not result["pip_check"]
        assert "pip_version" not in result


class TestValidateEnvVars:
    def test_type_validation(self):
        # Only strings allowed.
        with pytest.raises(TypeError, match=".*Dict[str, str]*"):
            parse_and_validate_env_vars({"INT_ENV": 1})

        with pytest.raises(TypeError, match=".*Dict[str, str]*"):
            parse_and_validate_env_vars({1: "hi"})


class TestParsedRuntimeEnv:
    def test_empty(self):
        assert RuntimeEnv() == {}

    @pytest.mark.skipif(
        sys.platform == "win32", reason="Pip option not supported on Windows."
    )
    def test_serialization(self):
        env1 = RuntimeEnv(pip=["requests"], env_vars={"hi1": "hi1", "hi2": "hi2"})

        env2 = RuntimeEnv(env_vars={"hi2": "hi2", "hi1": "hi1"}, pip=["requests"])

        assert env1 == env2

        serialized_env1 = env1.serialize()
        serialized_env2 = env2.serialize()

        # Key ordering shouldn't matter.
        assert serialized_env1 == serialized_env2

        deserialized_env1 = RuntimeEnv.deserialize(serialized_env1)
        deserialized_env2 = RuntimeEnv.deserialize(serialized_env2)

        assert env1 == deserialized_env1 == env2 == deserialized_env2

    def test_reject_pip_and_conda(self):
        with pytest.raises(ValueError):
            RuntimeEnv(pip=["requests"], conda="env_name")

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Conda and pip options not supported on Windows.",
    )
    def test_ray_commit_injection(self):
        # Should not be injected if no pip and conda.
        result = RuntimeEnv(env_vars={"hi": "hi"})
        assert "_ray_commit" not in result

        # Should be injected if pip or conda present.
        result = RuntimeEnv(pip=["requests"])
        assert "_ray_commit" in result

        result = RuntimeEnv(conda="env_name")
        assert "_ray_commit" in result

        # Should not override if passed.
        result = RuntimeEnv(conda="env_name", _ray_commit="Blah")
        assert result["_ray_commit"] == "Blah"

    def test_inject_current_ray(self):
        # Should not be injected if not provided by env var.
        result = RuntimeEnv(env_vars={"hi": "hi"})
        assert "_inject_current_ray" not in result

        os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"] = "1"

        # Should be injected if provided by env var.
        result = RuntimeEnv()
        assert result["_inject_current_ray"]

        # Should be preserved if passed.
        result = RuntimeEnv(_inject_current_ray=False)
        assert not result["_inject_current_ray"]

        del os.environ["RAY_RUNTIME_ENV_LOCAL_DEV_MODE"]


class TestParseJobConfig:
    def test_parse_runtime_env_from_json_env_variable(self):
        job_config_json = {"runtime_env": {"working_dir": "uri://abc"}}
        config = job_config.JobConfig.from_json(job_config_json)
        assert config.runtime_env == job_config_json.get("runtime_env")
        assert config.metadata == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
