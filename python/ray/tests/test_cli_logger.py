from ray.autoscaler._private import cli_logger
import pytest


def test_colorful_mock_with_style():
    cm = cli_logger._ColorfulMock()
    with cm.with_style("some_style") as c:
        assert c.color_choice("text") == "text"
        assert c.another_color_choice("more_text") == "more_text"


def test_colorful_mock_random_function():
    cm = cli_logger._ColorfulMock()
    assert cm.bold("abc") == "abc"


if __name__ == "__main__":
    import os
    import sys

    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
