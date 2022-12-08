import time

import pytest

from ray.tune.search.variant_generator import format_vars
from ray.tune.utils.util import retry_fn


def test_format_vars():

    # Format brackets correctly
    assert (
        format_vars(
            {
                ("a", "b", "c"): 8.1234567,
                ("a", "b", "d"): [7, 8],
                ("a", "b", "e"): [[[3, 4]]],
            }
        )
        == "c=8.1235,d=7_8,e=3_4"
    )
    # Sorted by full keys, but only last key is reported
    assert (
        format_vars(
            {
                ("a", "c", "x"): [7, 8],
                ("a", "b", "x"): 8.1234567,
            }
        )
        == "x=8.1235,x=7_8"
    )
    # Filter out invalid chars. It's ok to have empty keys or values.
    assert (
        format_vars(
            {
                ("a  c?x",): " <;%$ok ",
                ("some",): " ",
            }
        )
        == "a_c_x=ok,some="
    )


def test_retry_fn_repeat(tmpdir):
    success = tmpdir / "success"
    marker = tmpdir / "marker"

    def _fail_once():
        if marker.exists():
            success.write_text(".", encoding="utf-8")
            return
        marker.write_text(".", encoding="utf-8")
        raise RuntimeError("Failing")

    assert not success.exists()
    assert not marker.exists()

    assert retry_fn(
        fn=_fail_once,
        exception_type=RuntimeError,
        sleep_time=0,
    )

    assert success.exists()
    assert marker.exists()


def test_retry_fn_timeout(tmpdir):
    success = tmpdir / "success"
    marker = tmpdir / "marker"

    def _fail_once():
        if not marker.exists():
            marker.write_text(".", encoding="utf-8")
            raise RuntimeError("Failing")
        time.sleep(5)
        success.write_text(".", encoding="utf-8")
        return

    assert not success.exists()
    assert not marker.exists()

    assert not retry_fn(
        fn=_fail_once, exception_type=RuntimeError, sleep_time=0, timeout=0.1
    )

    assert not success.exists()
    assert marker.exists()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
