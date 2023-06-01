import time
from unittest import mock

import pytest

from ray.tune.search.variant_generator import format_vars
from ray.tune.utils.resource_updater import _ResourceUpdater, _Resources
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


def test_resource_updater_automatic():
    """Test that resources are automatically updated when they get out of sync.

    We instantiate a resource updater. When the reported resources are less than
    what is available, we don't force an update.
    However, if any of the resources (cpu, gpu, or custom) are higher than what
    the updater currently think is available, we force an update from the
    Ray cluster.
    """
    resource_updater = _ResourceUpdater()
    resource_updater._avail_resources = _Resources(
        cpu=2,
        gpu=1,
        memory=1,
        object_store_memory=1,
        custom_resources={"a": 4},
    )
    resource_updater._last_resource_refresh = 2

    # Should not trigger
    with mock.patch.object(
        _ResourceUpdater,
        "update_avail_resources",
        wraps=resource_updater.update_avail_resources,
    ) as upd:
        # No update
        assert "2/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 4}
        )
        assert upd.call_count == 0

        # Too many CPUs
        assert "4/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 4, "GPU": 1, "a": 0}
        )
        assert upd.call_count == 1

        # Too many GPUs
        assert "8/1 GPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 8, "a": 0}
        )
        assert upd.call_count == 2

        # Too many `a`
        assert "6/4 a" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 6}
        )
        assert upd.call_count == 3

        # No update again
        assert "2/2 CPUs" in resource_updater.debug_string(
            total_allocated_resources={"CPU": 2, "GPU": 1, "a": 4}
        )
        assert upd.call_count == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
