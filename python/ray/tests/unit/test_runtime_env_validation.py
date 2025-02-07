# TODO(hjiang): Move conda related unit test to this file also, after addressing the `
# yaml` third-party dependency issue.

from ray._private.runtime_env import validation

import os
from pathlib import Path
import tempfile
import pytest
import sys

_PIP_LIST = ["requests==1.0.0", "pip-install-test"]


@pytest.fixture
def test_directory():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        subdir = path / "subdir"
        subdir.mkdir(parents=True)
        requirements_file = subdir / "requirements.txt"
        with requirements_file.open(mode="w") as f:
            print("\n".join(_PIP_LIST), file=f)

        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield subdir, requirements_file
        os.chdir(old_dir)


class TestVaidationUv:
    def test_parse_and_validate_uv(self, test_directory):
        # Valid case w/o duplication.
        result = validation.parse_and_validate_uv({"packages": ["tensorflow"]})
        assert result == {
            "packages": ["tensorflow"],
            "uv_check": False,
            "uv_pip_install_options": ["--no-cache"],
        }

        # Valid case w/ duplication.
        result = validation.parse_and_validate_uv(
            {"packages": ["tensorflow", "tensorflow"]}
        )
        assert result == {
            "packages": ["tensorflow"],
            "uv_check": False,
            "uv_pip_install_options": ["--no-cache"],
        }

        # Valid case, use `list` to represent necessary packages.
        result = validation.parse_and_validate_uv(
            ["requests==1.0.0", "aiohttp", "ray[serve]"]
        )
        assert result == {
            "packages": ["requests==1.0.0", "aiohttp", "ray[serve]"],
            "uv_check": False,
        }

        # Invalid case, unsupport keys.
        with pytest.raises(ValueError):
            result = validation.parse_and_validate_uv({"random_key": "random_value"})

        # Valid case w/ uv version.
        result = validation.parse_and_validate_uv(
            {"packages": ["tensorflow"], "uv_version": "==0.4.30"}
        )
        assert result == {
            "packages": ["tensorflow"],
            "uv_version": "==0.4.30",
            "uv_check": False,
            "uv_pip_install_options": ["--no-cache"],
        }

        # Valid requirement files.
        _, requirements_file = test_directory
        requirements_file = requirements_file.resolve()
        result = validation.parse_and_validate_uv(str(requirements_file))
        assert result == {
            "packages": ["requests==1.0.0", "pip-install-test"],
            "uv_check": False,
        }

        # Invalid requiremnt files.
        with pytest.raises(ValueError):
            result = validation.parse_and_validate_uv("some random non-existent file")

        # Invalid uv install options.
        with pytest.raises(TypeError):
            result = validation.parse_and_validate_uv(
                {
                    "packages": ["tensorflow"],
                    "uv_version": "==0.4.30",
                    "uv_pip_install_options": [1],
                }
            )

        # Valid uv install options.
        result = validation.parse_and_validate_uv(
            {
                "packages": ["tensorflow"],
                "uv_version": "==0.4.30",
                "uv_pip_install_options": ["--no-cache"],
            }
        )
        assert result == {
            "packages": ["tensorflow"],
            "uv_check": False,
            "uv_pip_install_options": ["--no-cache"],
            "uv_version": "==0.4.30",
        }


class TestValidatePip:
    def test_validate_pip_invalid_types(self):
        with pytest.raises(TypeError):
            validation.parse_and_validate_pip(1)

        with pytest.raises(TypeError):
            validation.parse_and_validate_pip(True)

    def test_validate_pip_invalid_path(self):
        with pytest.raises(ValueError):
            validation.parse_and_validate_pip("../bad_path.txt")

    @pytest.mark.parametrize("absolute_path", [True, False])
    def test_validate_pip_valid_file(self, test_directory, absolute_path):
        _, requirements_file = test_directory

        if absolute_path:
            requirements_file = requirements_file.resolve()

        result = validation.parse_and_validate_pip(str(requirements_file))
        assert result["packages"] == _PIP_LIST
        assert not result["pip_check"]
        assert "pip_version" not in result

    def test_validate_pip_valid_list(self):
        result = validation.parse_and_validate_pip(_PIP_LIST)
        assert result["packages"] == _PIP_LIST
        assert not result["pip_check"]
        assert "pip_version" not in result

    def test_validate_ray(self):
        result = validation.parse_and_validate_pip(["pkg1", "ray", "pkg2"])
        assert result["packages"] == ["pkg1", "ray", "pkg2"]
        assert not result["pip_check"]
        assert "pip_version" not in result


class TestValidateEnvVars:
    def test_type_validation(self):
        # Only strings allowed.
        with pytest.raises(TypeError, match=".*Dict[str, str]*"):
            validation.parse_and_validate_env_vars({"INT_ENV": 1})

        with pytest.raises(TypeError, match=".*Dict[str, str]*"):
            validation.parse_and_validate_env_vars({1: "hi"})

        with pytest.raises(TypeError, match=".*value 123 is of type <class 'int'>*"):
            validation.parse_and_validate_env_vars({"hi": 123})

        with pytest.raises(TypeError, match=".*value True is of type <class 'bool'>*"):
            validation.parse_and_validate_env_vars({"hi": True})

        with pytest.raises(TypeError, match=".*key 1.23 is of type <class 'float'>*"):
            validation.parse_and_validate_env_vars({1.23: "hi"})


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
