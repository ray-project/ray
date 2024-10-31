from python.ray._private.runtime_env import validation

import unittest


class TestVaidation(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
