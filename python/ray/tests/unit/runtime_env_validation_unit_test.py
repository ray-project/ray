# TODO(hjiang): Move conda related unit test to this file also, after addressing the `
# yaml` third-party dependency issue.

from python.ray._private.runtime_env import validation

import unittest
import os
import shutil
from pathlib import Path
import tempfile

_PIP_LIST = ["requests==1.0.0", "pip-install-test"]


class TestVaidationUv(unittest.TestCase):
    def test_parse_and_validate_uv(self):
        # Valid case w/o duplication.
        result = validation.parse_and_validate_uv({"packages": ["tensorflow"]})
        self.assertEqual(result, {"packages": ["tensorflow"]})

        # Valid case w/ duplication.
        result = validation.parse_and_validate_uv(
            {"packages": ["tensorflow", "tensorflow"]}
        )
        self.assertEqual(result, {"packages": ["tensorflow"]})

        # Valid case, use `list` to represent necessary packages.
        result = validation.parse_and_validate_uv(
            ["requests==1.0.0", "aiohttp", "ray[serve]"]
        )
        self.assertEqual(
            result, {"packages": ["requests==1.0.0", "aiohttp", "ray[serve]"]}
        )

        # Invalid case, `str` is not supported for now.
        with self.assertRaises(TypeError) as _:
            result = validation.parse_and_validate_uv("./requirements.txt")

        # Invalid case, unsupport keys.
        with self.assertRaises(ValueError) as _:
            result = validation.parse_and_validate_uv({"random_key": "random_value"})


class TestValidatePip(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        path = Path(self.tmp_dir)
        self.subdir = path / "subdir"
        self.subdir.mkdir(parents=True)
        self.requirements_file = self.subdir / "requirements.txt"
        with self.requirements_file.open(mode="w") as f:
            print("\n".join(_PIP_LIST), file=f)

        self.old_dir = os.getcwd()
        os.chdir(self.tmp_dir)

    def tearDown(self):
        os.chdir(self.old_dir)
        shutil.rmtree(self.tmp_dir)

    def test_validate_pip_invalid_types(self):
        with self.assertRaises(TypeError) as _:
            validation.parse_and_validate_pip(1)

        with self.assertRaises(TypeError) as _:
            validation.parse_and_validate_pip(True)

    def test_validate_pip_invalid_path(self):
        with self.assertRaises(ValueError) as _:
            validation.parse_and_validate_pip("../bad_path.txt")

    def test_validate_pip_valid_file(self):
        result = validation.parse_and_validate_pip(str(self.requirements_file))
        self.assertEqual(result["packages"], _PIP_LIST)
        self.assertFalse(result["pip_check"])
        self.assertFalse("pip_version" in result)

    def test_validate_pip_valid_list(self):
        result = validation.parse_and_validate_pip(_PIP_LIST)
        self.assertEqual(result["packages"], _PIP_LIST)
        self.assertFalse(result["pip_check"])
        self.assertFalse("pip_version" in result)

    def test_validate_ray(self):
        result = validation.parse_and_validate_pip(["pkg1", "ray", "pkg2"])
        self.assertEqual(result["packages"], ["pkg1", "ray", "pkg2"])
        self.assertFalse(result["pip_check"])
        self.assertFalse("pip_version" in result)


class TestValidateEnvVars(unittest.TestCase):
    def test_type_validation(self):
        with self.assertRaises(TypeError) as context:
            validation.parse_and_validate_env_vars({"INT_ENV": 1})
            self.assertRegex(str(context.exception), ".*Dict\\[str, str\\]*")

        with self.assertRaises(TypeError) as context:
            validation.parse_and_validate_env_vars({1: "hi"})
            self.assertRegex(str(context.exception), ".*Dict\\[str, str\\]*")

        with self.assertRaises(TypeError) as context:
            validation.parse_and_validate_env_vars({"hi": 123})
            self.assertRegex(
                str(context.exception), ".*value 123 is of type <class 'int'>*"
            )

        with self.assertRaises(TypeError) as context:
            validation.parse_and_validate_env_vars({"hi": True})
            self.assertRegex(
                str(context.exception), ".*value True is of type <class 'bool'>*"
            )

        with self.assertRaises(TypeError) as context:
            validation.parse_and_validate_env_vars({1.23: "hi"})
            self.assertRegex(
                str(context.exception), ".*key 1.23 is of type <class 'float'>*"
            )


if __name__ == "__main__":
    unittest.main()
