import io
import sys
from unittest.mock import patch

import pytest

from ray.autoscaler._private import cli_logger


def test_colorful_mock_with_style():
    cm = cli_logger._ColorfulMock()
    with cm.with_style("some_style") as c:
        assert c.color_choice("text") == "text"
        assert c.another_color_choice("more_text") == "more_text"


def test_colorful_mock_random_function():
    cm = cli_logger._ColorfulMock()
    assert cm.bold("abc") == "abc"


def test_pathname():
    # Ensure that the `pathname` of the `LogRecord` points to the
    # caller of `cli_logger`, not `cli_logger` itself.
    with patch("sys.stdout", new=io.StringIO()) as mock_stdout:
        cli_logger.cli_logger.info("123")
        assert "test_cli_logger.py" in mock_stdout.getvalue()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
