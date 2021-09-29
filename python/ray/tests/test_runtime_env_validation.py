import os
import pytest
import tempfile
from pathlib import Path
import yaml

from ray._private.runtime_env import (validate_runtime_env,
                                      override_task_or_actor_runtime_env)

VALID_CONDA_DICT = {
    "dependencies": ["pip", {
        "pip": ["pip-install-test==0.5"]
    }]
}

VALID_PIP_LIST = ["requests==1.0.0", "pip-install-test"]


@pytest.fixture
def test_directory():
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)
        subdir = path / "subdir"
        subdir.mkdir(parents=True)
        requirements_file = subdir / "requirements.txt"
        with requirements_file.open(mode="w") as f:
            print("\n".join(VALID_PIP_LIST), file=f)

        good_conda_file = subdir / "good_conda_env.yaml"
        with good_conda_file.open(mode="w") as f:
            yaml.dump(VALID_CONDA_DICT, f)

        bad_conda_file = subdir / "bad_conda_env.yaml"
        with bad_conda_file.open(mode="w") as f:
            print("% this is not a YAML file %", file=f)

        old_dir = os.getcwd()
        os.chdir(tmp_dir)
        yield subdir, requirements_file, good_conda_file, bad_conda_file
        os.chdir(old_dir)


class TestValidateWorkingDir:
    @pytest.mark.parametrize("absolute_path", [True, False])
    def test_validate_working_dir_valid_path(self, test_directory,
                                             absolute_path):
        subdir, _, _, _ = test_directory

        rel1 = {"working_dir": "."}
        assert validate_runtime_env(rel1, is_task_or_actor=False) == rel1

        if absolute_path:
            subdir = subdir.resolve()

        rel2 = {"working_dir": str(subdir)}
        assert validate_runtime_env(rel2, is_task_or_actor=False) == rel2

    def test_validate_working_dir_absolute_path(self, test_directory):
        subdir, _, _, _ = test_directory

        abspath = {"working_dir": str(subdir.resolve())}
        assert validate_runtime_env(abspath, is_task_or_actor=False) == abspath

    def test_validate_working_dir_invalid_path(self):
        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "working_dir": "fake_path"
                }, is_task_or_actor=False)

    def test_validate_working_dir_invalid_types(self):
        with pytest.raises(TypeError):
            validate_runtime_env({"working_dir": 1}, is_task_or_actor=False)

    def test_validate_working_dir_reject_task_or_actor(self):
        # Can't pass working_dir for tasks/actors.
        with pytest.raises(NotImplementedError):
            validate_runtime_env({"working_dir": "."}, is_task_or_actor=True)


class TestValidateConda:
    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_invalid_types(self, is_task_or_actor):
        with pytest.raises(TypeError):
            validate_runtime_env(
                {
                    "conda": 1
                }, is_task_or_actor=is_task_or_actor)

        with pytest.raises(TypeError):
            validate_runtime_env(
                {
                    "conda": True
                }, is_task_or_actor=is_task_or_actor)

    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_str(self, test_directory, is_task_or_actor):
        env = "my_env_name"
        result = validate_runtime_env(
            {
                "conda": env
            }, is_task_or_actor=is_task_or_actor)
        assert result["conda"] == env
        assert "_ray_commit" in result

    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_invalid_path(self, is_task_or_actor):
        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "conda": "../bad_path.yaml"
                },
                is_task_or_actor=is_task_or_actor)

        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "conda": "../bad_path.yaml"
                },
                is_task_or_actor=is_task_or_actor)

    @pytest.mark.parametrize("absolute_path", [True, False])
    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_valid_file(self, test_directory, absolute_path,
                                       is_task_or_actor):
        _, _, good_conda_file, _ = test_directory

        if absolute_path:
            good_conda_file = good_conda_file.resolve()

        result = validate_runtime_env(
            {
                "conda": str(good_conda_file)
            }, is_task_or_actor=is_task_or_actor)
        assert result["conda"] == VALID_CONDA_DICT
        assert "_ray_commit" in result

    @pytest.mark.parametrize("absolute_path", [True, False])
    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_invalid_file(self, test_directory, absolute_path,
                                         is_task_or_actor):
        _, _, _, bad_conda_file = test_directory

        if absolute_path:
            bad_conda_file = bad_conda_file.resolve()

        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "conda": str(bad_conda_file)
                },
                is_task_or_actor=is_task_or_actor)

    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_conda_valid_dict(self, is_task_or_actor):
        result = validate_runtime_env(
            {
                "conda": VALID_CONDA_DICT
            }, is_task_or_actor=is_task_or_actor)
        assert result["conda"] == VALID_CONDA_DICT
        assert "_ray_commit" in result


class TestValidatePip:
    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_pip_invalid_types(self, is_task_or_actor):
        with pytest.raises(TypeError):
            validate_runtime_env({"pip": 1}, is_task_or_actor=is_task_or_actor)

        with pytest.raises(TypeError):
            validate_runtime_env(
                {
                    "pip": True
                }, is_task_or_actor=is_task_or_actor)

    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_pip_invalid_path(self, is_task_or_actor):
        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "pip": "../bad_path.txt"
                }, is_task_or_actor=is_task_or_actor)

        with pytest.raises(ValueError):
            validate_runtime_env(
                {
                    "pip": "../bad_path.txt"
                }, is_task_or_actor=is_task_or_actor)

    @pytest.mark.parametrize("absolute_path", [True, False])
    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_pip_valid_file(self, test_directory, absolute_path,
                                     is_task_or_actor):
        _, requirements_file, _, _ = test_directory

        if absolute_path:
            requirements_file = requirements_file.resolve()

        result = validate_runtime_env(
            {
                "pip": str(requirements_file)
            }, is_task_or_actor=is_task_or_actor)
        assert result["pip"] == "\n".join(VALID_PIP_LIST) + "\n"
        assert "_ray_commit" in result

    @pytest.mark.parametrize("is_task_or_actor", [True, False])
    def test_validate_pip_valid_dict(self, is_task_or_actor):
        result = validate_runtime_env(
            {
                "pip": VALID_PIP_LIST
            }, is_task_or_actor=is_task_or_actor)
        assert result["pip"] == "\n".join(VALID_PIP_LIST) + "\n"
        assert "_ray_commit" in result


@pytest.mark.parametrize("is_task_or_actor", [True, False])
def test_no_pip_and_conda(is_task_or_actor):
    with pytest.raises(ValueError):
        validate_runtime_env(
            {
                "pip": ["requests"],
                "conda": "env_name"
            },
            is_task_or_actor=is_task_or_actor)


@pytest.mark.parametrize("is_task_or_actor", [True, False])
def test_validate_env_vars(is_task_or_actor):
    # Only strings allowed.
    with pytest.raises(TypeError, match=".*Dict[str, str]*"):
        validate_runtime_env(
            {
                "env_vars": {
                    "INT_ENV": 1
                }
            }, is_task_or_actor=is_task_or_actor)

    with pytest.raises(TypeError, match=".*Dict[str, str]*"):
        validate_runtime_env(
            {
                "env_vars": {
                    1: "hi"
                }
            }, is_task_or_actor=is_task_or_actor)


@pytest.mark.parametrize("is_task_or_actor", [True, False])
def test_empty(is_task_or_actor):
    assert validate_runtime_env({}, is_task_or_actor=is_task_or_actor) == {}


class TestOverrideRuntimeEnvs:
    def test_override_uris(self):
        child = {}
        parent = {"uris": ["a", "b"]}
        assert override_task_or_actor_runtime_env(child, parent) == parent

        child = {"uris": ["a", "b"]}
        parent = {"uris": ["c", "d"]}
        assert override_task_or_actor_runtime_env(child, parent) == child

        child = {"uris": ["a", "b"]}
        parent = {}
        assert override_task_or_actor_runtime_env(child, parent) == child

    def test_override_env_vars(self):
        # (child, parent, expected)
        TEST_CASES = [({}, {}, {}), (None, None, None), ({
            "a": "b"
        }, {}, {
            "a": "b"
        }), ({
            "a": "b"
        }, None, {
            "a": "b"
        }), ({}, {
            "a": "b"
        }, {
            "a": "b"
        }), (None, {
            "a": "b"
        }, {
            "a": "b"
        }), ({
            "a": "b"
        }, {
            "a": "d"
        }, {
            "a": "b"
        }), ({
            "a": "b"
        }, {
            "c": "d"
        }, {
            "a": "b",
            "c": "d"
        }), ({
            "a": "b"
        }, {
            "a": "e",
            "c": "d"
        }, {
            "a": "b",
            "c": "d"
        })]

        for idx, (child, parent, expected) in enumerate(TEST_CASES):
            child = {"env_vars": child} if child is not None else {}
            parent = {"env_vars": parent} if parent is not None else {}
            expected = {"env_vars": expected} if expected is not None else {}
            assert override_task_or_actor_runtime_env(
                child, parent) == expected, f"TEST_INDEX:{idx}"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-sv", __file__]))
