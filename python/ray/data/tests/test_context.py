import pytest

import ray


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
