import sys
from unittest.mock import patch

import pytest

from ray._common.deprecation import (
    DEPRECATED_VALUE,
    Deprecated,
    deprecation_warning,
)


def test_deprecation_warning_warn():
    with patch("ray._common.deprecation.logger.warning") as mock_warning:
        deprecation_warning("old_feature", "new_feature")

        mock_warning.assert_called_once()
        args, _ = mock_warning.call_args
        assert (
            "DeprecationWarning: `old_feature` has been deprecated. Use `new_feature` instead."
            in args[0]
        )


def test_deprecation_warning_error():
    with pytest.raises(ValueError) as excinfo:
        deprecation_warning("old_feature", error=True)
    assert "`old_feature` has been deprecated." in str(excinfo.value)


def test_deprecated_decorator_function():
    with patch("ray._common.deprecation.logger.warning") as mock_warning, patch(
        "ray._common.deprecation.log_once"
    ) as mock_log_once:
        mock_log_once.return_value = True

        @Deprecated(old="old_func", new="new_func", error=False)
        def old_func():
            return "result"

        result = old_func()
        assert result == "result"
        mock_warning.assert_called_once()


def test_deprecated_decorator_class():
    with patch("ray._common.deprecation.logger.warning") as mock_warning, patch(
        "ray._common.deprecation.log_once"
    ) as mock_log_once:
        mock_log_once.return_value = True

        @Deprecated(old="OldClass", new="NewClass", error=False)
        class OldClass:
            pass

        instance = OldClass()
        assert isinstance(instance, OldClass)
        mock_warning.assert_called_once()


def test_deprecated_decorator_method():
    with patch("ray._common.deprecation.logger.warning") as mock_warning, patch(
        "ray._common.deprecation.log_once"
    ) as mock_log_once:
        mock_log_once.return_value = True

        class MyClass:
            @Deprecated(old="old_method", new="new_method", error=False)
            def old_method(self):
                return "method_result"

        instance = MyClass()
        result = instance.old_method()
        assert result == "method_result"
        mock_warning.assert_called_once()


def test_deprecated_decorator_error():
    with patch("ray._common.deprecation.log_once") as mock_log_once:
        mock_log_once.return_value = True

        @Deprecated(old="old_func", error=True)
        def old_func():
            pass

        with pytest.raises(ValueError):
            old_func()


def test_deprecated_value_constant():
    assert (
        DEPRECATED_VALUE == -1
    ), f"DEPRECATED_VALUE should be -1, but got {DEPRECATED_VALUE}"


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
