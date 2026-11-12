import io
import logging
import sys
import threading
import time

import pytest

from ray.tune.search.variant_generator import format_vars
from ray.tune.utils.util import Tee, logger as util_logger, retry_fn, warn_if_slow


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


def test_retry_fn_stale_thread_does_not_poison_next_attempt():
    attempt = 0
    attempt2_started = threading.Event()
    attempt1_finished = threading.Event()

    def _fn():
        nonlocal attempt
        attempt += 1
        if attempt == 1:
            attempt2_started.wait(timeout=2.0)
            try:
                raise RuntimeError("stale attempt failed after timeout")
            finally:
                attempt1_finished.set()
        attempt2_started.set()
        assert attempt1_finished.wait(timeout=2.0)

    assert retry_fn(
        fn=_fn,
        exception_type=RuntimeError,
        num_retries=2,
        sleep_time=0,
        timeout=0.05,
    )


def test_warn_if_slow_zero_threshold():
    ctx = warn_if_slow("fast_op", threshold=0.0)
    assert ctx.threshold == 0.0
    ctx_int = warn_if_slow("fast_op", threshold=0)
    assert ctx_int.threshold == 0


def test_tee_recursion():
    f = io.StringIO()
    g = io.StringIO()

    tee = Tee(f, g)

    hdlr = logging.StreamHandler(tee)
    util_logger.addHandler(hdlr)

    util_logger.info("BEFORE")
    f.close()
    util_logger.info("AFTER")

    util_logger.removeHandler(hdlr)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
