import sys
import warnings

import pytest

from ray.util.annotations import Deprecated
from ray._private.test_utils import (
    run_string_as_driver,
)


# Use default filterwarnings behavior for this test
@pytest.mark.filterwarnings("default")
@pytest.mark.parametrize("warning", [True, False])
def test_deprecated(warning):
    class_deprecated_message = "class is deprecated"
    class_method_deprecated_message = "class method is deprecated"
    function_deprecated_message = "function is deprecated"

    @Deprecated(message=class_deprecated_message, warning=warning)
    class A:
        def __init__(self):
            self.i = 13

        @Deprecated(message=class_method_deprecated_message, warning=warning)
        def method(self):
            self.i = 14

    @Deprecated(message=function_deprecated_message, warning=warning)
    def func():
        return 15

    with warnings.catch_warnings(record=True) as w:
        a = A()
        assert a.i == 13
        assert class_deprecated_message in A.__doc__
        if warning:
            assert any(
                class_deprecated_message in str(warning.message) for warning in w
            )
        else:
            assert not w

    with warnings.catch_warnings(record=True) as w:
        a.method()
        assert a.i == 14
        assert class_method_deprecated_message in A.method.__doc__
        if warning:
            assert any(
                class_method_deprecated_message in str(warning.message) for warning in w
            )
        else:
            assert not w

    with warnings.catch_warnings(record=True) as w:
        ret = func()
        assert ret == 15
        assert function_deprecated_message in func.__doc__
        if warning:
            assert any(
                function_deprecated_message in str(warning.message) for warning in w
            )
        else:
            assert not w


# Use default filterwarnings behavior for this test
@pytest.mark.filterwarnings("default")
@pytest.mark.skipif(sys.platform == "win32", reason="Windows failed reading stdout")
def test_only_warn_once():
    log = run_string_as_driver(
        """
from ray.util.annotations import Deprecated

@Deprecated(message="functionisdeprecated", warning=True)
def func():
    return 15

for _ in range(3):
    func()
    """
    )
    assert log.count("functionisdeprecated") == 1


# Use default filterwarnings behavior for this test
@pytest.mark.filterwarnings("default")
@pytest.mark.skipif(sys.platform == "win32", reason="Windows failed reading stdout")
def test_warn_suppressed():
    log = run_string_as_driver(
        """
from ray.util.annotations import Deprecated

@Deprecated(message="functionisdeprecated", warning=True)
def func():
    return 15

for _ in range(3):
    func()
    """,
        env={"PYTHONWARNINGS": "ignore::DeprecationWarning"},
    )
    assert log.count("functionisdeprecated") == 0


if __name__ == "__main__":
    import os

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
