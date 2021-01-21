import sys
import os
import tempfile

import mypy.api as mypy_api
import pytest

TYPING_TEST_DIRS = os.path.join(os.path.dirname(__file__), "typing_files")

MYPY_INI = """
[mypy]
plugins = ray.mypy
"""


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_typing_good():
    with tempfile.NamedTemporaryFile("w") as f:
        f.write(MYPY_INI)
        f.flush()

        script = os.path.join(TYPING_TEST_DIRS, "check_typing_good.py")
        msg, _, status_code = mypy_api.run(["--config", f.name, script])
        assert status_code == 0, msg


@pytest.mark.skipif(sys.platform == "win32", reason="Failing on Windows.")
def test_typing_bad():
    with tempfile.NamedTemporaryFile("w") as f:
        f.write(MYPY_INI)
        f.flush()

        script = os.path.join(TYPING_TEST_DIRS, "check_typing_bad.py")
        msg, _, status_code = mypy_api.run(["--config", f.name, script])

        assert status_code == 1, msg

        # def f(a: int) -> str:
        # def g(s: str) -> str:
        # def h(a: str, b: int) -> str:
        # class MyActor:
        #     def return_int(self, i: str) -> int:
        expected_errors = [
            # a = h.remote(1, 1)
            '22: error: No overload variant of "remote" of "RemoteFunction" matches argument types "int", "int"',  # noqa: E501
            # b = f.remote("hello")
            '23: error: No overload variant of "remote" of "RemoteFunction" matches argument type "str"',  # noqa: E501
            # c = f.remote(1, 1)
            '24: error: No overload variant of "remote" of "RemoteFunction" matches argument types "int", "int"',  # noqa: E501
            # d = f.remote(1) + 1
            '25: error: Unsupported operand types for + ("ObjectRef[str]" and "int")',  # noqa: E501
            # unwrapped_str = ray.get(ref_to_str)
            # unwrapped_str + 100
            '30: error: Unsupported operand types for + ("str" and "int")',  # noqa: E501
            # f.remote(ref_to_str)
            '33: error: Argument 1 to "remote" of "RemoteFunction" has incompatible type "ObjectRef[str]"; expected "Union[int, ObjectRef[int]]"',  # noqa: E501
            # int_ref = actor.return_int.remote()
            '43: error: Too few arguments for "return_int" of "MyActor"',  # noqa: E501
            # int_ref = actor.return_int.remote(42)
            '44: error: Argument 1 to "return_int" of "MyActor" has incompatible type "int"; expected "str"',  # noqa: E501
            # int_ref = actor.missing_method.remote()
            '45: error: Actor "MyActor" has no method "missing_method"',  # noqa: E501
        ]
        for error in expected_errors:
            assert error in msg

        assert msg.count("error:") == len(expected_errors)


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
