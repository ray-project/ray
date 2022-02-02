import sys
import os

import mypy.api as mypy_api
import pytest

TYPING_TEST_DIRS = os.path.join(os.path.dirname(__file__), "typing_files")

typing_good = """
import ray

ray.init()


@ray.remote
def f(a: int) -> str:
    return "a = {}".format(a + 1)


@ray.remote
def g(s: str) -> str:
    return s + " world"


@ray.remote
def h(a: str, b: int) -> str:
    return a


# Make sure the function arg is check
print(f.remote(1))
object_ref_str = f.remote(1)

# Make sure the ObjectRef[T] variant of function arg is checked
print(g.remote(object_ref_str))

# Make sure there can be mixed T0 and ObjectRef[T1] for args
print(h.remote(object_ref_str, 100))

# Make sure the return type is checked.
xy = ray.get(object_ref_str) + "y"
"""

typing_bad = """
import ray

ray.init()


@ray.remote
def f(a: int) -> str:
    return "a = {}".format(a + 1)


@ray.remote
def g(s: str) -> str:
    return s + " world"


@ray.remote
def h(a: str, b: int) -> str:
    return a


# Does not typecheck due to incorrect input type:
a = h.remote(1, 1)
b = f.remote("hello")
c = f.remote(1, 1)
d = f.remote(1) + 1

# Check return type
ref_to_str = f.remote(1)
unwrapped_str = ray.get(ref_to_str)
unwrapped_str + 100  # Fail

# Check ObjectRef[T] as args
f.remote(ref_to_str)  # Fail
"""

def test_typing_good():
    script = "check_typing_good.py"
    check_typing_good = open(script, "w+")
    check_typing_good.write(typing_good)
    check_typing_good.close()
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 0, msg


def test_typing_bad():
    script = "check_typing_bad.py"
    check_typing_bad = open(script, "w+")
    check_typing_bad.write(typing_bad)
    check_typing_bad.close()
    _, msg, status_code = mypy_api.run([script])
    assert status_code == 1, msg


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
